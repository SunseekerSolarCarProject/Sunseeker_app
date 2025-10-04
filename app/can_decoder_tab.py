"""PyQt tab for streaming CAN decoder."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from can_analyzer import (
    EXCEL_XLS_MAX_ROWS,
    EXCEL_XLSX_MAX_ROWS,
    decode_csv_one,
    decode_csv_split,
)


class DecoderWorker(QThread):
    """Runs the decode step off the UI thread."""

    finished = pyqtSignal(list)
    failed = pyqtSignal(str)

    def __init__(
        self,
        *,
        input_path: str,
        include_bits: bool,
        channel: Optional[str],
        row_limit: int,
        split_outputs: bool,
        single_output: Optional[str],
        can0_output: Optional[str],
        can1_output: Optional[str],
    ) -> None:
        super().__init__()
        self._input_path = input_path
        self._include_bits = include_bits
        self._channel = channel
        self._row_limit = row_limit
        self._split_outputs = split_outputs
        self._single_output = single_output
        self._can0_output = can0_output
        self._can1_output = can1_output

    def run(self) -> None:  # type: ignore[override]
        try:
            if self._split_outputs:
                assert self._can0_output and self._can1_output
                outputs = decode_csv_split(
                    self._input_path,
                    self._can0_output,
                    self._can1_output,
                    include_bits=self._include_bits,
                    channel=self._channel,
                    row_limit=self._row_limit,
                )
            else:
                assert self._single_output
                outputs = decode_csv_one(
                    self._input_path,
                    self._single_output,
                    include_bits=self._include_bits,
                    channel=self._channel,
                    row_limit=self._row_limit,
                )
        except Exception as exc:  # pragma: no cover - surfaced via UI
            self.failed.emit(str(exc))
            return
        self.finished.emit(outputs)


class CanDecoderWidget(QWidget):
    """Interactive tab for running the CAN decoder."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._worker: Optional[DecoderWorker] = None

        self._input_edit: QLineEdit
        self._output_edit: QLineEdit
        self._channel_combo: QComboBox
        self._split_checkbox: QCheckBox
        self._bits_checkbox: QCheckBox
        self._xls_limit_checkbox: QCheckBox
        self._progress: QProgressBar
        self._log: QPlainTextEdit

        self._build_ui()

    # --- UI construction -------------------------------------------------
    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        inputs_box = QGroupBox("Source and destination")
        inputs_form = QFormLayout(inputs_box)

        self._input_edit = QLineEdit()
        browse_in = QPushButton("Browse...")
        browse_in.clicked.connect(self._browse_input)
        in_row = QHBoxLayout()
        in_row.addWidget(self._input_edit)
        in_row.addWidget(browse_in)
        inputs_form.addRow(QLabel("Input CSV"), in_row)

        self._output_edit = QLineEdit("decoded.csv")
        browse_out = QPushButton("Browse...")
        browse_out.clicked.connect(self._browse_output)
        out_row = QHBoxLayout()
        out_row.addWidget(self._output_edit)
        out_row.addWidget(browse_out)
        inputs_form.addRow(QLabel("Output file/base"), out_row)

        self._channel_combo = QComboBox()
        self._channel_combo.addItems(["All channels", "can0", "can1"])
        inputs_form.addRow(QLabel("Channel filter"), self._channel_combo)

        layout.addWidget(inputs_box)

        options_box = QGroupBox("Options")
        options_form = QFormLayout(options_box)

        self._split_checkbox = QCheckBox("Split into CAN0 / CAN1 files")
        self._split_checkbox.setChecked(True)
        options_form.addRow(self._split_checkbox)

        self._bits_checkbox = QCheckBox("Expand raw bytes and bit flags")
        options_form.addRow(self._bits_checkbox)

        self._xls_limit_checkbox = QCheckBox("Old Excel .xls row cap (65,536)")
        options_form.addRow(self._xls_limit_checkbox)

        layout.addWidget(options_box)

        button_row = QHBoxLayout()
        button_row.addStretch(1)
        self._decode_button = QPushButton("Decode")
        self._decode_button.clicked.connect(self._start_decode)
        button_row.addWidget(self._decode_button)
        layout.addLayout(button_row)

        self._progress = QProgressBar()
        self._progress.setRange(0, 1)
        self._progress.setValue(0)
        layout.addWidget(self._progress)

        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setMaximumBlockCount(500)
        self._log.setPlaceholderText("Decoder status will appear here...")
        layout.addWidget(self._log)

        layout.addStretch(1)

    # --- Helpers ---------------------------------------------------------
    def _browse_input(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Select CAN CSV", "", "CSV files (*.csv);;All files (*.*)")
        if path:
            self._input_edit.setText(path)

    def _browse_output(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Select output file", self._output_edit.text() or "decoded.csv", "CSV files (*.csv)")
        if path:
            self._output_edit.setText(path)

    def _start_decode(self) -> None:
        if self._worker is not None:
            return

        input_path = self._input_edit.text().strip()
        if not input_path:
            QMessageBox.warning(self, "CAN Decoder", "Select an input CSV file to decode.")
            return
        if not Path(input_path).exists():
            QMessageBox.warning(self, "CAN Decoder", "The selected input file does not exist.")
            return

        output_entry = self._output_edit.text().strip() or "decoded.csv"
        include_bits = self._bits_checkbox.isChecked()
        use_split = self._split_checkbox.isChecked()
        channel_choice = self._channel_combo.currentText()
        channel = None if channel_choice.startswith("All") else channel_choice
        row_limit = EXCEL_XLS_MAX_ROWS if self._xls_limit_checkbox.isChecked() else EXCEL_XLSX_MAX_ROWS

        single_output: Optional[str] = None
        can0_output: Optional[str] = None
        can1_output: Optional[str] = None

        out_path = Path(output_entry)
        if use_split:
            base = out_path
            if base.suffix.lower() == ".csv":
                base = base.with_suffix("")
            base_str = str(base)
            can0_output = f"{base_str}_can0.csv"
            can1_output = f"{base_str}_can1.csv"
            for target in (can0_output, can1_output):
                parent = Path(target).parent
                if parent and not parent.exists():
                    parent.mkdir(parents=True, exist_ok=True)
            log_summary = (
                f"Input: {input_path}\n"
                f"Outputs: {can0_output}, {can1_output}\n"
                f"Channel: {channel or 'all'} | Bits: {include_bits} | Row limit: {row_limit}"
            )
        else:
            if not out_path.suffix:
                out_path = out_path.with_suffix(".csv")
            parent = out_path.parent
            if parent and not parent.exists():
                parent.mkdir(parents=True, exist_ok=True)
            single_output = str(out_path)
            log_summary = (
                f"Input: {input_path}\n"
                f"Output: {single_output}\n"
                f"Channel: {channel or 'all'} | Bits: {include_bits} | Row limit: {row_limit}"
            )

        self._append_status("\nStarting decode...")
        self._append_status(log_summary)
        self._set_busy(True)

        self._worker = DecoderWorker(
            input_path=input_path,
            include_bits=include_bits,
            channel=channel,
            row_limit=row_limit,
            split_outputs=use_split,
            single_output=single_output,
            can0_output=can0_output,
            can1_output=can1_output,
        )
        self._worker.finished.connect(self._on_decode_finished)
        self._worker.failed.connect(self._on_decode_failed)
        self._worker.finished.connect(self._clear_worker)
        self._worker.failed.connect(self._clear_worker)
        self._worker.start()

    def _set_busy(self, busy: bool) -> None:
        self._decode_button.setDisabled(busy)
        self._input_edit.setDisabled(busy)
        self._output_edit.setDisabled(busy)
        self._channel_combo.setDisabled(busy)
        self._split_checkbox.setDisabled(busy)
        self._bits_checkbox.setDisabled(busy)
        self._xls_limit_checkbox.setDisabled(busy)
        self._progress.setRange(0, 0 if busy else 1)
        if not busy:
            self._progress.setValue(0)

    def _append_status(self, message: str) -> None:
        self._log.appendPlainText(message)
        self._log.verticalScrollBar().setValue(self._log.verticalScrollBar().maximum())

    def _on_decode_finished(self, outputs: List[str]) -> None:
        self._set_busy(False)
        self._append_status("Decode complete.")
        for path in outputs:
            self._append_status(f"  wrote: {path}")
        joined = "\n".join(outputs)
        QMessageBox.information(self, "CAN Decoder", f"Finished decoding.\n\nWrote:\n{joined}")

    def _on_decode_failed(self, error: str) -> None:
        self._set_busy(False)
        self._append_status(f"Error: {error}")
        QMessageBox.critical(self, "CAN Decoder", f"Decoding failed:\n{error}")

    def _clear_worker(self) -> None:
        if self._worker is not None:
            self._worker.deleteLater()
            self._worker = None
