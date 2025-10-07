"""PyQt widget for driving the synthetic telemetry generator."""
from __future__ import annotations

import time
from typing import Optional

from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QMessageBox,
)

try:  # pragma: no cover - optional dependency
    import serial  # type: ignore
    import serial.tools.list_ports  # type: ignore
except ImportError:  # pragma: no cover - surfaced via UI
    serial = None

from .telemetry_core import TelemetryGeneratorCore, calculate_runtime


class TelemetryWorker(QThread):
    log = pyqtSignal(str)
    error = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, port: str, baud: int, endianness: str, interval: float) -> None:
        super().__init__()
        self._port = port
        self._baud = baud
        self._endianness = endianness
        self._interval = max(0.1, interval)
        self._running = True

    def run(self) -> None:  # type: ignore[override]
        if serial is None:
            self.error.emit("pyserial is not installed.")
            self.finished.emit()
            return

        try:
            generator = TelemetryGeneratorCore(self._endianness)
        except ValueError as exc:
            self.error.emit(str(exc))
            self.finished.emit()
            return

        try:
            ser = serial.Serial(self._port, self._baud, timeout=1)
        except Exception as exc:
            self.error.emit(f"Failed to open {self._port}: {exc}")
            self.finished.emit()
            return

        with ser:
            self.log.emit(f"Opened {self._port} @ {self._baud} baud.")
            start_time = time.time()
            while self._running:
                loop_start = time.time()
                runtime = calculate_runtime(start_time)
                payload = generator.build_block(runtime).encode("utf-8")
                try:
                    ser.write(payload)
                except Exception as exc:
                    self.error.emit(f"Serial write failed: {exc}")
                    break
                timestamp = time.strftime("%H:%M:%S")
                self.log.emit(f"[{timestamp}] Sent block at {runtime} ({len(payload)} bytes)")

                remaining = self._interval - (time.time() - loop_start)
                if remaining > 0:
                    end_time = time.time() + remaining
                    while self._running and time.time() < end_time:
                        self.msleep(100)
                else:
                    self.log.emit(f"Warning: loop behind by {-remaining:.2f} seconds")

        self.finished.emit()

    def stop(self) -> None:
        self._running = False


class TelemetryGeneratorWidget(QWidget):
    """GUI tab for streaming synthetic telemetry over serial."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._worker: Optional[TelemetryWorker] = None

        self._port_combo: QComboBox
        self._refresh_button: QPushButton
        self._baud_combo: QComboBox
        self._endianness_combo: QComboBox
        self._interval_spin: QDoubleSpinBox
        self._start_button: QPushButton
        self._stop_button: QPushButton
        self._log: QTextEdit

        self._build_ui()
        self._refresh_ports()
        self._update_buttons(running=False)

        if serial is None:
            self._disable_ui("pyserial is not installed; install dependencies to enable the generator.")

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        group = QGroupBox("Telemetry Generator")
        form = QFormLayout(group)

        port_row = QHBoxLayout()
        self._port_combo = QComboBox()
        self._refresh_button = QPushButton("Refresh")
        self._refresh_button.clicked.connect(self._refresh_ports)
        port_row.addWidget(self._port_combo, 1)
        port_row.addWidget(self._refresh_button)
        form.addRow("Serial port", port_row)

        self._baud_combo = QComboBox()
        self._baud_combo.addItems(["9600", "19200", "38400", "57600", "115200", "230400"])
        self._baud_combo.setCurrentText("115200")
        form.addRow("Baud rate", self._baud_combo)

        self._endianness_combo = QComboBox()
        self._endianness_combo.addItems(["big", "little"])
        form.addRow("Endianness", self._endianness_combo)

        self._interval_spin = QDoubleSpinBox()
        self._interval_spin.setRange(0.1, 60.0)
        self._interval_spin.setSingleStep(0.1)
        self._interval_spin.setValue(5.0)
        form.addRow("Interval (s)", self._interval_spin)

        button_row = QHBoxLayout()
        self._start_button = QPushButton("Start")
        self._start_button.clicked.connect(self._start)
        self._stop_button = QPushButton("Stop")
        self._stop_button.clicked.connect(self._stop)
        button_row.addWidget(self._start_button)
        button_row.addWidget(self._stop_button)
        form.addRow(button_row)

        layout.addWidget(group)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setPlaceholderText("Generator status will appear here.")
        layout.addWidget(self._log)

    def _disable_ui(self, message: str) -> None:
        self._start_button.setDisabled(True)
        self._stop_button.setDisabled(True)
        self._refresh_button.setDisabled(True)
        self._append_log(message)

    def _refresh_ports(self) -> None:
        if serial is None:
            return
        ports = [port.device for port in serial.tools.list_ports.comports()]  # type: ignore[attr-defined]
        self._port_combo.clear()
        self._port_combo.addItems(ports)
        if ports:
            self._port_combo.setCurrentIndex(0)

    def _start(self) -> None:
        if self._worker is not None:
            return
        if serial is None:
            QMessageBox.warning(self, "Telemetry Generator", "pyserial is not installed.")
            return

        port = self._port_combo.currentText().strip()
        if not port:
            QMessageBox.warning(self, "Telemetry Generator", "Select a serial port to transmit on.")
            return
        try:
            baud = int(self._baud_combo.currentText())
        except ValueError:
            QMessageBox.warning(self, "Telemetry Generator", "Invalid baud rate.")
            return

        endianness = self._endianness_combo.currentText()
        interval = float(self._interval_spin.value())

        self._worker = TelemetryWorker(port, baud, endianness, interval)
        self._worker.log.connect(self._append_log)
        self._worker.error.connect(self._handle_error)
        self._worker.finished.connect(self._on_worker_finished)
        self._worker.start()
        self._update_buttons(running=True)
        self._append_log("Generator started.")

    def _stop(self) -> None:
        if self._worker is not None:
            self._worker.stop()
            self._worker = None
        self._update_buttons(running=False)
        self._append_log("Generator stopped.")

    def _on_worker_finished(self) -> None:
        if self._worker is not None:
            self._worker = None
        self._update_buttons(running=False)

    def _handle_error(self, message: str) -> None:
        QMessageBox.critical(self, "Telemetry Generator", message)
        self._append_log(f"Error: {message}")

    def _append_log(self, message: str) -> None:
        self._log.append(message)
        self._log.verticalScrollBar().setValue(self._log.verticalScrollBar().maximum())

    def _update_buttons(self, running: bool) -> None:
        self._start_button.setEnabled(not running)
        self._stop_button.setEnabled(running)
        self._refresh_button.setEnabled(not running)
        self._port_combo.setEnabled(not running)
        self._baud_combo.setEnabled(not running)
        self._endianness_combo.setEnabled(not running)
        self._interval_spin.setEnabled(not running)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._stop()
        super().closeEvent(event)
