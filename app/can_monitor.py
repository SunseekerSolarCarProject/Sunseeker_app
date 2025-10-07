"""PyQt tab implementing a live CAN monitor and transmitter."""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import List, Optional

from PyQt6.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    Qt,
    QThread,
    pyqtSignal,
)
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableView,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

try:
    import can  # type: ignore
except ImportError:  # pragma: no cover - surfaced via UI
    can = None

try:
    import serial  # type: ignore
    import serial.tools.list_ports  # type: ignore
except ImportError:  # pragma: no cover - surfaced via UI
    serial = None


@dataclass
class ReceivedFrame:
    """Typed container for incoming CAN frames."""

    timestamp: float
    arbitration_id: int
    is_extended_id: bool
    is_remote_frame: bool
    dlc: int
    data: bytes


class ReceivedFramesModel(QAbstractTableModel):
    """Lazy table model so large capture sets stay responsive."""

    HEADERS = ["Time", "ID", "Ext", "RTR", "DLC", "Data"]

    def __init__(self, max_rows: int = 5000) -> None:
        super().__init__()
        self._frames: List[ReceivedFrame] = []
        self._max_rows = max_rows

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        return 0 if parent.isValid() else len(self._frames)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        return 0 if parent.isValid() else len(self.HEADERS)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole):  # type: ignore[override]
        if not index.isValid() or not (0 <= index.row() < len(self._frames)):
            return None
        frame = self._frames[index.row()]

        if role == Qt.ItemDataRole.DisplayRole:
            column = index.column()
            if column == 0:
                return _dt.datetime.fromtimestamp(frame.timestamp).strftime("%H:%M:%S.%f")[:-3]
            if column == 1:
                return (
                    f"{frame.arbitration_id:08X}"
                    if frame.is_extended_id
                    else f"{frame.arbitration_id:03X}"
                )
            if column == 2:
                return "Yes" if frame.is_extended_id else "No"
            if column == 3:
                return "Yes" if frame.is_remote_frame else "No"
            if column == 4:
                return str(frame.dlc)
            if column == 5:
                return " ".join(f"{byte:02X}" for byte in frame.data[: frame.dlc])
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            return int(Qt.AlignmentFlag.AlignCenter)
        return None

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.ItemDataRole.DisplayRole,
    ):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal and 0 <= section < len(self.HEADERS):
            return self.HEADERS[section]
        return super().headerData(section, orientation, role)

    def append_frame(self, frame: ReceivedFrame) -> None:
        insert_row = len(self._frames)
        self.beginInsertRows(QModelIndex(), insert_row, insert_row)
        self._frames.append(frame)
        self.endInsertRows()

        if len(self._frames) > self._max_rows:
            remove_count = len(self._frames) - self._max_rows
            self.beginRemoveRows(QModelIndex(), 0, remove_count - 1)
            del self._frames[:remove_count]
            self.endRemoveRows()

    def clear(self) -> None:
        if not self._frames:
            return
        self.beginResetModel()
        self._frames.clear()
        self.endResetModel()


class CanReceiverThread(QThread):
    """Background reader that polls python-can buses."""

    frame_received = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, bus: "can.BusABC") -> None:
        super().__init__()
        self._bus = bus
        self._running = True

    def run(self) -> None:  # type: ignore[override]
        try:
            while self._running:
                message = self._bus.recv(timeout=0.5)
                if not self._running:
                    break
                if message is None:
                    continue
                frame = ReceivedFrame(
                    timestamp=message.timestamp or _dt.datetime.now().timestamp(),
                    arbitration_id=message.arbitration_id,
                    is_extended_id=bool(message.is_extended_id),
                    is_remote_frame=bool(message.is_remote_frame),
                    dlc=int(message.dlc),
                    data=bytes(message.data),
                )
                self.frame_received.emit(frame)
        except Exception as exc:  # pragma: no cover - hardware-specific
            self.error.emit(str(exc))

    def stop(self) -> None:
        self._running = False
        self.wait(1500)

def _parse_can232_line(line: str) -> Optional[ReceivedFrame]:
    """Convert a CAN232 ASCII line into a ReceivedFrame."""
    line = line.strip()
    if not line:
        return None

    prefix = line[0]
    if prefix not in "tTrR":
        return None

    is_remote = prefix in "rR"
    is_extended = prefix in "TR"
    if is_extended:
        if len(line) < 1 + 8 + 1:
            return None
        id_hex = line[1:9]
        cursor = 9
    else:
        if len(line) < 1 + 3 + 1:
            return None
        id_hex = line[1:4]
        cursor = 4

    try:
        arbitration_id = int(id_hex, 16)
    except ValueError:
        return None

    try:
        dlc = int(line[cursor], 16)
    except ValueError:
        return None
    cursor += 1

    data_bytes = bytes()
    if not is_remote:
        expected_len = dlc * 2
        data_hex = line[cursor : cursor + expected_len]
        if len(data_hex) != expected_len:
            return None
        try:
            data_bytes = bytes.fromhex(data_hex)
        except ValueError:
            return None

    return ReceivedFrame(
        timestamp=_dt.datetime.now().timestamp(),
        arbitration_id=arbitration_id,
        is_extended_id=is_extended,
        is_remote_frame=is_remote,
        dlc=dlc,
        data=data_bytes,
    )


class Can232ReaderThread(QThread):
    """Poll a CAN232 serial device for ASCII traffic."""

    frame_received = pyqtSignal(object)
    text_received = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, serial_port: "serial.Serial") -> None:  # type: ignore[name-defined]
        super().__init__()
        self._serial = serial_port
        self._running = True

    def run(self) -> None:  # type: ignore[override]
        buffer = ""
        try:
            while self._running:
                try:
                    chunk = self._serial.read(256)
                except Exception as exc:
                    self.error.emit(str(exc))
                    break
                if not chunk:
                    continue
                buffer += chunk.decode("ascii", errors="ignore")
                while "\r" in buffer:
                    line, buffer = buffer.split("\r", 1)
                    line = line.strip()
                    if not line:
                        continue
                    frame = _parse_can232_line(line)
                    if frame:
                        self.frame_received.emit(frame)
                    else:
                        self.text_received.emit(line)
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False
        self.wait(1500)


MODE_PYTHON_CAN = "python-can bus"
MODE_CAN232 = "CAN232 (serial)"


class CanMonitorWidget(QWidget):
    """Interactive CAN monitor with support for python-can buses and CAN232 devices."""

    MAX_ROWS = 10000

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)

        self._bus: Optional["can.BusABC"] = None
        self._reader: Optional[CanReceiverThread] = None
        self._serial: Optional["serial.Serial"] = None  # type: ignore[name-defined]
        self._can232_thread: Optional[Can232ReaderThread] = None
        self._current_mode: str = MODE_PYTHON_CAN

        self._mode_combo: QComboBox
        self._python_group: QGroupBox
        self._can232_group: QGroupBox

        self._received_model: ReceivedFramesModel
        self._received_view: QTableView
        self._log: QTextEdit
        self._send_button: QPushButton
        self._result_label: QLabel

        self._bustype_combo: QComboBox
        self._channel_edit: QLineEdit
        self._bitrate_combo: QComboBox
        self._connect_button: QPushButton
        self._disconnect_button: QPushButton

        self._serial_port_combo: QComboBox
        self._refresh_ports_button: QPushButton
        self._serial_speed_combo: QComboBox
        self._can_bitrate_combo: QComboBox
        self._serial_connect_button: QPushButton
        self._serial_disconnect_button: QPushButton
        self._setup_button: QPushButton
        self._can_open_button: QPushButton
        self._can_close_button: QPushButton
        self._version_button: QPushButton
        self._flags_button: QPushButton
        self._serial_number_button: QPushButton
        self._poll_one_button: QPushButton
        self._poll_all_button: QPushButton
        self._time_on_button: QPushButton
        self._time_off_button: QPushButton
        self._auto_on_button: QPushButton
        self._auto_off_button: QPushButton
        self._can232_command_buttons: List[QWidget] = []

        self._id_edit: QLineEdit
        self._data_edits: List[QLineEdit] = []
        self._extended_checkbox: QCheckBox
        self._rtr_checkbox: QCheckBox

        self._build_ui()
        self._set_python_can_state(False)
        self._set_can232_state(False)
        self._update_mode_visibility()
        self._refresh_serial_ports()

        if can is None:
            self._disable_python_can_support()
        if serial is None:
            self._disable_can232_support()

    # ------------------------------------------------------------------ UI --
    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        mode_row = QHBoxLayout()
        mode_row.addWidget(QLabel("Connection mode"))
        self._mode_combo = QComboBox()
        self._mode_combo.addItems([MODE_PYTHON_CAN, MODE_CAN232])
        self._mode_combo.currentTextChanged.connect(self._on_mode_changed)
        mode_row.addWidget(self._mode_combo, 1)
        layout.addLayout(mode_row)

        self._python_group = QGroupBox("python-can connection")
        py_form = QFormLayout(self._python_group)
        self._bustype_combo = QComboBox()
        self._bustype_combo.addItems(["socketcan", "slcan", "pcan", "ixxat", "kvaser", "virtual"])
        self._bustype_combo.setEditable(True)
        py_form.addRow("Interface", self._bustype_combo)

        self._channel_edit = QLineEdit("can0")
        py_form.addRow("Channel", self._channel_edit)

        self._bitrate_combo = QComboBox()
        self._bitrate_combo.addItems(["", "125000", "250000", "500000", "1000000"])
        self._bitrate_combo.setEditable(True)
        self._bitrate_combo.setCurrentText("500000")
        py_form.addRow("Bitrate", self._bitrate_combo)

        py_buttons = QHBoxLayout()
        self._connect_button = QPushButton("Connect")
        self._disconnect_button = QPushButton("Disconnect")
        self._connect_button.clicked.connect(self._connect_bus)
        self._disconnect_button.clicked.connect(self._disconnect_bus)
        py_buttons.addWidget(self._connect_button)
        py_buttons.addWidget(self._disconnect_button)
        py_form.addRow(py_buttons)
        layout.addWidget(self._python_group)

        self._can232_group = QGroupBox("CAN232 connection")
        can_layout = QVBoxLayout(self._can232_group)
        can_form = QFormLayout()
        can_layout.addLayout(can_form)

        port_row = QHBoxLayout()
        self._serial_port_combo = QComboBox()
        self._refresh_ports_button = QPushButton("Refresh")
        self._refresh_ports_button.clicked.connect(self._refresh_serial_ports)
        port_row.addWidget(self._serial_port_combo, 1)
        port_row.addWidget(self._refresh_ports_button)
        can_form.addRow("Serial port", port_row)

        self._serial_speed_combo = QComboBox()
        self._serial_speed_combo.addItems(["2400", "9600", "19200", "38400", "57600", "115200"])
        self._serial_speed_combo.setCurrentText("57600")
        can_form.addRow("Port speed", self._serial_speed_combo)

        bitrate_row = QHBoxLayout()
        self._can_bitrate_combo = QComboBox()
        self._can_bitrate_combo.addItems([
            "10Kbit",
            "20Kbit",
            "50Kbit",
            "100Kbit",
            "125Kbit",
            "250Kbit",
            "500Kbit",
            "800Kbit",
            "1Mbit",
        ])
        self._can_bitrate_combo.setCurrentIndex(4)
        self._setup_button = QPushButton("Setup")
        self._setup_button.clicked.connect(self._on_setup_clicked)
        bitrate_row.addWidget(self._can_bitrate_combo, 1)
        bitrate_row.addWidget(self._setup_button)
        can_form.addRow("CAN bitrate", bitrate_row)

        port_buttons = QHBoxLayout()
        self._serial_connect_button = QPushButton("Open Port")
        self._serial_disconnect_button = QPushButton("Close Port")
        self._serial_connect_button.clicked.connect(self._connect_can232)
        self._serial_disconnect_button.clicked.connect(self._disconnect_can232)
        port_buttons.addWidget(self._serial_connect_button)
        port_buttons.addWidget(self._serial_disconnect_button)
        can_form.addRow(port_buttons)

        command_grid = QGridLayout()
        self._can_open_button = QPushButton("CAN Open")
        self._can_close_button = QPushButton("CAN Close")
        self._version_button = QPushButton("Version")
        self._flags_button = QPushButton("Flags")
        self._serial_number_button = QPushButton("S/No")
        self._poll_one_button = QPushButton("Poll One")
        self._poll_all_button = QPushButton("Poll All")
        self._time_on_button = QPushButton("Time On")
        self._time_off_button = QPushButton("Time Off")
        self._auto_on_button = QPushButton("Auto On")
        self._auto_off_button = QPushButton("Auto Off")

        self._can_open_button.clicked.connect(self._on_can_open_clicked)
        self._can_close_button.clicked.connect(self._on_can_close_clicked)
        self._version_button.clicked.connect(self._on_version_clicked)
        self._flags_button.clicked.connect(self._on_flags_clicked)
        self._serial_number_button.clicked.connect(self._on_serial_number_clicked)
        self._poll_one_button.clicked.connect(self._on_poll_one_clicked)
        self._poll_all_button.clicked.connect(self._on_poll_all_clicked)
        self._time_on_button.clicked.connect(self._on_time_on_clicked)
        self._time_off_button.clicked.connect(self._on_time_off_clicked)
        self._auto_on_button.clicked.connect(self._on_auto_on_clicked)
        self._auto_off_button.clicked.connect(self._on_auto_off_clicked)

        self._can232_command_buttons = [
            self._setup_button,
            self._can_open_button,
            self._can_close_button,
            self._version_button,
            self._flags_button,
            self._serial_number_button,
            self._poll_one_button,
            self._poll_all_button,
            self._time_on_button,
            self._time_off_button,
            self._auto_on_button,
            self._auto_off_button,
        ]

        command_grid.addWidget(self._can_open_button, 0, 0)
        command_grid.addWidget(self._can_close_button, 0, 1)
        command_grid.addWidget(self._version_button, 1, 0)
        command_grid.addWidget(self._flags_button, 1, 1)
        command_grid.addWidget(self._serial_number_button, 2, 0)
        command_grid.addWidget(self._poll_one_button, 2, 1)
        command_grid.addWidget(self._poll_all_button, 3, 0)
        command_grid.addWidget(self._time_on_button, 3, 1)
        command_grid.addWidget(self._time_off_button, 4, 0)
        command_grid.addWidget(self._auto_on_button, 4, 1)
        command_grid.addWidget(self._auto_off_button, 5, 0)
        can_layout.addLayout(command_grid)

        layout.addWidget(self._can232_group)
        receive_box = QGroupBox("CAN Receive Frames")
        receive_layout = QVBoxLayout(receive_box)
        self._received_model = ReceivedFramesModel(max_rows=self.MAX_ROWS)
        self._received_view = QTableView()
        self._received_view.setModel(self._received_model)
        self._received_view.setAlternatingRowColors(True)
        self._received_view.verticalHeader().setVisible(False)
        self._received_view.setEditTriggers(QTableView.EditTrigger.NoEditTriggers)
        self._received_view.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self._received_view.setSelectionMode(QTableView.SelectionMode.SingleSelection)
        self._received_view.setWordWrap(False)
        header_font = QFont()
        header_font.setBold(True)
        self._received_view.horizontalHeader().setFont(header_font)
        self._received_view.horizontalHeader().setStretchLastSection(True)
        receive_layout.addWidget(self._received_view)

        clear_button = QPushButton("Clear")
        clear_button.clicked.connect(self._clear_received)
        clear_row = QHBoxLayout()
        clear_row.addStretch(1)
        clear_row.addWidget(clear_button)
        receive_layout.addLayout(clear_row)
        layout.addWidget(receive_box)

        transmit_box = QGroupBox("CAN Transmit Frame")
        transmit_layout = QFormLayout(transmit_box)
        self._id_edit = QLineEdit("0x123")
        transmit_layout.addRow("CAN ID", self._id_edit)

        data_row = QHBoxLayout()
        for _ in range(8):
            edit = QLineEdit()
            edit.setPlaceholderText("--")
            edit.setMaxLength(2)
            edit.setFixedWidth(36)
            self._data_edits.append(edit)
            data_row.addWidget(edit)
        transmit_layout.addRow("Data (hex)", data_row)

        flags_row = QHBoxLayout()
        self._extended_checkbox = QCheckBox("Extended ID (29-bit)")
        self._rtr_checkbox = QCheckBox("RTR Frame")
        flags_row.addWidget(self._extended_checkbox)
        flags_row.addWidget(self._rtr_checkbox)
        transmit_layout.addRow(flags_row)

        self._send_button = QPushButton("Send Frame")
        self._send_button.clicked.connect(self._send_frame)
        transmit_layout.addRow(self._send_button)

        self._result_label = QLabel("Resulting command: -")
        transmit_layout.addRow(self._result_label)
        layout.addWidget(transmit_box)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setPlaceholderText("Status messages will appear here.")
        layout.addWidget(self._log)

    # ---------------------------------------------------------- Mode control --
    def _on_mode_changed(self, mode: str) -> None:
        self._current_mode = mode
        self._update_mode_visibility()
        self._update_send_enabled()

    def _update_mode_visibility(self) -> None:
        self._python_group.setVisible(self._current_mode == MODE_PYTHON_CAN)
        self._can232_group.setVisible(self._current_mode == MODE_CAN232)

    def _disable_python_can_support(self) -> None:
        self._python_group.setDisabled(True)
        self._connect_button.setDisabled(True)
        self._disconnect_button.setDisabled(True)
        self._append_status("python-can is not installed; python-can mode disabled.")

    def _disable_can232_support(self) -> None:
        self._can232_group.setDisabled(True)
        self._append_status("pyserial is not installed; CAN232 mode disabled.")

    # --------------------------------------------------------------- Helpers --
    def _append_status(self, message: str) -> None:
        timestamp = _dt.datetime.now().strftime("%H:%M:%S")
        self._log.append(f"[{timestamp}] {message}")
        self._log.verticalScrollBar().setValue(self._log.verticalScrollBar().maximum())

    def _clear_received(self) -> None:
        self._received_model.clear()

    def _update_send_enabled(self) -> None:
        if self._current_mode == MODE_PYTHON_CAN:
            enabled = self._bus is not None
        elif self._current_mode == MODE_CAN232:
            enabled = self._serial is not None and self._serial.is_open if self._serial else False
        else:
            enabled = False
        self._send_button.setEnabled(enabled)

    # -------------------------------------------------------- python-can flow --
    def _set_python_can_state(self, connected: bool) -> None:
        allow = can is not None
        self._connect_button.setEnabled(allow and not connected)
        self._disconnect_button.setEnabled(connected)
        self._update_send_enabled()

    def _connect_bus(self) -> None:
        if can is None:
            QMessageBox.warning(self, "CAN Monitor", "python-can is not installed.")
            return
        if self._bus is not None:
            return

        bustype = self._bustype_combo.currentText().strip()
        channel = self._channel_edit.text().strip()
        bitrate_text = self._bitrate_combo.currentText().strip()
        bitrate = None
        if bitrate_text:
            try:
                bitrate = int(bitrate_text)
            except ValueError:
                QMessageBox.warning(self, "CAN Monitor", f"Invalid bitrate: {bitrate_text}")
                return

        try:
            kwargs = dict(bustype=bustype, channel=channel)
            if bitrate is not None:
                kwargs["bitrate"] = bitrate
            self._bus = can.Bus(**kwargs)
        except Exception as exc:
            QMessageBox.critical(self, "CAN Monitor", f"Failed to connect: {exc}")
            self._bus = None
            return

        self._reader = CanReceiverThread(self._bus)
        self._reader.frame_received.connect(self._on_frame_received)
        self._reader.error.connect(self._on_python_can_error)
        self._reader.start()

        self._set_python_can_state(True)
        self._append_status(f"python-can connected to {bustype} channel '{channel}'.")

    def _disconnect_bus(self) -> None:
        if self._reader is not None:
            self._reader.stop()
            self._reader = None

        if self._bus is not None:
            try:
                self._bus.shutdown()
            except Exception:
                pass
            self._bus = None

        self._set_python_can_state(False)
        self._append_status("python-can disconnected.")

    def _on_python_can_error(self, message: str) -> None:
        self._append_status(f"python-can error: {message}")
        self._disconnect_bus()

    # ----------------------------------------------------------- CAN232 flow --
    def _refresh_serial_ports(self) -> None:
        if serial is None:
            return
        try:
            ports = [port.device for port in serial.tools.list_ports.comports()]
        except Exception:
            ports = []
        self._serial_port_combo.clear()
        self._serial_port_combo.addItems(ports)

    def _set_can232_state(self, connected: bool) -> None:
        available = serial is not None
        self._serial_connect_button.setEnabled(available and not connected)
        self._serial_disconnect_button.setEnabled(connected)
        self._serial_port_combo.setEnabled(not connected)
        self._serial_speed_combo.setEnabled(not connected)
        for widget in self._can232_command_buttons:
            widget.setEnabled(connected)
        self._update_send_enabled()
    def _connect_can232(self) -> None:
        if serial is None:
            QMessageBox.warning(self, "CAN Monitor", "pyserial is not installed.")
            return
        if self._serial is not None and self._serial.is_open:
            return

        port = self._serial_port_combo.currentText().strip()
        if not port:
            QMessageBox.warning(self, "CAN Monitor", "Select a serial port.")
            return

        try:
            baudrate = int(self._serial_speed_combo.currentText())
        except ValueError:
            QMessageBox.warning(self, "CAN Monitor", "Invalid port speed.")
            return

        try:
            self._serial = serial.Serial(port=port, baudrate=baudrate, timeout=0.1, write_timeout=1)
        except Exception as exc:
            QMessageBox.critical(self, "CAN Monitor", f"Failed to open serial port: {exc}")
            self._serial = None
            return

        self._can232_thread = Can232ReaderThread(self._serial)
        self._can232_thread.frame_received.connect(self._on_frame_received)
        self._can232_thread.text_received.connect(self._on_can232_text)
        self._can232_thread.error.connect(self._on_can232_error)
        self._can232_thread.start()

        self._set_can232_state(True)
        self._append_status(f"Serial port {port} opened at {baudrate} baud.")

    def _disconnect_can232(self) -> None:
        if self._can232_thread is not None:
            self._can232_thread.stop()
            self._can232_thread = None

        if self._serial is not None:
            try:
                self._serial.close()
            except Exception:
                pass
            self._serial = None

        self._set_can232_state(False)
        self._append_status("Serial port closed.")

    def _send_can232_command(self, command: str) -> None:
        if self._serial is None or not self._serial.is_open:
            QMessageBox.warning(self, "CAN Monitor", "Open the serial port first.")
            return
        if not command.endswith("\r"):
            command = command + "\r"
        try:
            self._serial.write(command.encode("ascii"))
            self._append_status(f"Sent: {command.strip()!r}")
        except Exception as exc:
            QMessageBox.critical(self, "CAN Monitor", f"Failed to send command: {exc}")

    def _on_setup_clicked(self) -> None:
        index = self._can_bitrate_combo.currentIndex()
        self._send_can232_command(f"S{index}")

    def _on_can_open_clicked(self) -> None:
        self._send_can232_command("O")

    def _on_can_close_clicked(self) -> None:
        self._send_can232_command("C")

    def _on_version_clicked(self) -> None:
        self._send_can232_command("V")

    def _on_flags_clicked(self) -> None:
        self._send_can232_command("F")

    def _on_serial_number_clicked(self) -> None:
        self._send_can232_command("N")

    def _on_poll_one_clicked(self) -> None:
        self._send_can232_command("P")

    def _on_poll_all_clicked(self) -> None:
        self._send_can232_command("A")

    def _on_time_on_clicked(self) -> None:
        self._send_can232_command("Z1")

    def _on_time_off_clicked(self) -> None:
        self._send_can232_command("Z0")

    def _on_auto_on_clicked(self) -> None:
        self._send_can232_command("X1")

    def _on_auto_off_clicked(self) -> None:
        self._send_can232_command("X0")

    def _on_can232_text(self, text: str) -> None:
        if text:
            self._append_status(f"Device: {text}")

    def _on_can232_error(self, message: str) -> None:
        self._append_status(f"Serial error: {message}")
        self._disconnect_can232()

    # --------------------------------------------------------------- Transmit --
    def _build_can232_frame(self) -> Optional[str]:
        can_id_text = self._id_edit.text().strip().upper()
        if not can_id_text:
            QMessageBox.warning(self, "CAN Monitor", "Enter a CAN ID.")
            return None
        if can_id_text.startswith("0X"):
            can_id_text = can_id_text[2:]
        try:
            can_id = int(can_id_text, 16)
        except ValueError:
            QMessageBox.warning(self, "CAN Monitor", f"Invalid CAN ID: {can_id_text}")
            return None

        is_extended = self._extended_checkbox.isChecked()
        is_remote = self._rtr_checkbox.isChecked()

        if is_extended:
            if not 0 <= can_id < (1 << 29):
                QMessageBox.warning(self, "CAN Monitor", "Extended ID must be < 0x20000000.")
                return None
            id_field = f"{can_id:08X}"
            frame_type = "R" if is_remote else "T"
        else:
            if not 0 <= can_id < (1 << 11):
                QMessageBox.warning(self, "CAN Monitor", "Standard ID must be < 0x800.")
                return None
            id_field = f"{can_id:03X}"
            frame_type = "r" if is_remote else "t"

        data_bytes: List[int] = []
        for edit in self._data_edits:
            text = edit.text().strip()
            if text:
                if text.startswith("0x") or text.startswith("0X"):
                    text = text[2:]
                try:
                    value = int(text, 16)
                except ValueError:
                    QMessageBox.warning(self, "CAN Monitor", f"Invalid data byte: '{text}'")
                    return None
                if not 0 <= value <= 0xFF:
                    QMessageBox.warning(self, "CAN Monitor", f"Data byte out of range: {value}")
                    return None
                data_bytes.append(value)
        if len(data_bytes) > 8:
            QMessageBox.warning(self, "CAN Monitor", "Maximum 8 data bytes allowed.")
            return None

        dlc = len(data_bytes)
        base = f"{frame_type}{id_field}{dlc:X}"
        if is_remote:
            return base
        data = "".join(f"{byte:02X}" for byte in data_bytes)
        return base + data

    def _send_frame(self) -> None:
        if self._current_mode == MODE_PYTHON_CAN:
            if self._bus is None or can is None:
                QMessageBox.warning(self, "CAN Monitor", "Connect to a python-can bus first.")
                return

            id_text = self._id_edit.text().strip()
            if id_text.startswith("0x") or id_text.startswith("0X"):
                id_text = id_text[2:]
            try:
                arbitration_id = int(id_text, 16)
            except ValueError:
                QMessageBox.warning(self, "CAN Monitor", "Invalid CAN ID.")
                return

            data: List[int] = []
            for edit in self._data_edits:
                text = edit.text().strip()
                if not text:
                    continue
                if text.startswith("0x") or text.startswith("0X"):
                    text = text[2:]
                try:
                    value = int(text, 16)
                except ValueError:
                    QMessageBox.warning(self, "CAN Monitor", f"Invalid data byte: {text}")
                    return
                if not 0 <= value <= 0xFF:
                    QMessageBox.warning(self, "CAN Monitor", f"Data byte out of range: {text}")
                    return
                data.append(value)

            try:
                message = can.Message(  # type: ignore[union-attr]
                    arbitration_id=arbitration_id,
                    is_extended_id=self._extended_checkbox.isChecked(),
                    is_remote_frame=self._rtr_checkbox.isChecked(),
                    data=bytes(data),
                )
                self._bus.send(message)  # type: ignore[union-attr]
            except Exception as exc:
                QMessageBox.critical(self, "CAN Monitor", f"Send failed: {exc}")
                return

            self._append_status(
                f"Sent python-can frame ID={arbitration_id:#x} Data={message.data.hex(' ').upper() or 'None'}"
            )
            self._result_label.setText("Frame sent via python-can.")
            return

        command = self._build_can232_frame()
        if command is None:
            return
        self._result_label.setText(f"Resulting command: {command}[CR]")
        self._send_can232_command(command)

    def _on_frame_received(self, frame: ReceivedFrame) -> None:
        self._received_model.append_frame(frame)
        self._received_view.scrollToBottom()

    def _on_python_can_error(self, message: str) -> None:
        self._append_status(f"python-can error: {message}")
        self._disconnect_bus()

    # ---------------------------------------------------------------- Lifecycle --
    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._disconnect_bus()
        self._disconnect_can232()
        super().closeEvent(event)
