"""Main window hosting the Sunseeker data tools."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import pandas as pd
from matplotlib import cm
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QAction, QCloseEvent, QKeySequence
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QAbstractItemView,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QStatusBar,
    QTableView,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from .data_loader import (
    CSVLoadError,
    get_datetime_columns,
    get_numeric_columns,
    load_csv,
)
from .models import DataFrameModel
from .plotter import PlotCanvas, PlotConfig, PlotManager, SUPPORTED_CHARTS
from .can_decoder_tab import CanDecoderWidget

COLORMAPS = {
    "Default": None,
    "Viridis": "viridis",
    "Plasma": "plasma",
    "Cividis": "cividis",
    "Magma": "magma",
    "Turbo": "turbo",
}


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Sunseeker Data Toolkit")
        self.resize(1280, 720)

        self.dataframe: Optional[pd.DataFrame] = None

        self.table_model = DataFrameModel()
        self.canvas = PlotCanvas()
        self.plot_manager = PlotManager(self.canvas)

        self._build_ui()
        self._create_actions()
        self._create_menu()
        self._create_status_bar()
        self._set_chart_ready(False)

    # --- UI Construction -------------------------------------------------
    def _build_ui(self) -> None:
        central = QWidget(self)
        outer_layout = QVBoxLayout(central)
        outer_layout.setContentsMargins(0, 0, 0, 0)

        self.tab_widget = QTabWidget(central)
        outer_layout.addWidget(self.tab_widget)
        self.setCentralWidget(central)

        graph_tab = QWidget()
        graph_layout = QHBoxLayout(graph_tab)
        graph_layout.setContentsMargins(0, 0, 0, 0)

        splitter = QSplitter(Qt.Orientation.Horizontal, graph_tab)

        controls_container = self._create_controls_panel()
        splitter.addWidget(controls_container)

        right_container = self._create_display_panel()
        splitter.addWidget(right_container)

        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        graph_layout.addWidget(splitter)
        self.tab_widget.addTab(graph_tab, "CSV Graph Explorer")

        self.can_decoder_widget = CanDecoderWidget(self)
        self.tab_widget.addTab(self.can_decoder_widget, "CAN Decoder")

    def _create_controls_panel(self) -> QWidget:
        container = QWidget()
        layout = QVBoxLayout(container)

        info_label = QLabel(
            "1. Load a CSV file using the buttons below or File > Open.\n"
            "2. Select the X and Y columns you want to compare.\n"
            "3. Click Render Chart, then Export Chart to save an image."
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        file_buttons = QHBoxLayout()
        self.load_csv_button = QPushButton("Load CSV...")
        self.load_csv_button.clicked.connect(self._open_csv)

        self.export_button = QPushButton("Export Chart...")
        self.export_button.clicked.connect(self._export_chart)

        file_buttons.addWidget(self.load_csv_button)
        file_buttons.addWidget(self.export_button)
        layout.addLayout(file_buttons)

        self.chart_type_combo = QComboBox()
        self.chart_type_combo.addItems(SUPPORTED_CHARTS)
        self.chart_type_combo.currentTextChanged.connect(self._update_option_visibility)

        self.x_column_combo = QComboBox()
        self.x_column_combo.addItem("Index")

        self.y_column_list = QListWidget()
        self.y_column_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)

        controls_group = QGroupBox("Chart Setup")
        controls_form = QFormLayout(controls_group)
        controls_form.addRow("Chart type", self.chart_type_combo)
        controls_form.addRow("X axis", self.x_column_combo)
        controls_form.addRow("Y axis columns", self.y_column_list)
        layout.addWidget(controls_group)

        options_group = QGroupBox("Chart Options")
        options_layout = QFormLayout(options_group)

        self.bins_spin = QSpinBox()
        self.bins_spin.setRange(1, 500)
        self.bins_spin.setValue(30)

        self.normalize_histogram_checkbox = QCheckBox("Normalise histogram")
        self.stacked_checkbox = QCheckBox("Stack series (bar/area)")
        self.show_grid_checkbox = QCheckBox("Show grid")
        self.show_grid_checkbox.setChecked(True)
        self.show_legend_checkbox = QCheckBox("Show legend")
        self.show_legend_checkbox.setChecked(True)

        self.marker_combo = QComboBox()
        self.marker_combo.addItem("Circle", "o")
        self.marker_combo.addItem("Square", "s")
        self.marker_combo.addItem("Triangle", "^")
        self.marker_combo.addItem("Diamond", "D")
        self.marker_combo.addItem("Plus", "+")

        self.colormap_combo = QComboBox()
        for label in COLORMAPS:
            self.colormap_combo.addItem(label)

        options_layout.addRow("Histogram bins", self.bins_spin)
        options_layout.addRow("Histogram scale", self.normalize_histogram_checkbox)
        options_layout.addRow("Stacking", self.stacked_checkbox)
        options_layout.addRow("Scatter marker", self.marker_combo)
        options_layout.addRow("Colour scheme", self.colormap_combo)
        options_layout.addRow(self.show_grid_checkbox)
        options_layout.addRow(self.show_legend_checkbox)

        layout.addWidget(options_group)

        self.plot_button = QPushButton("Render Chart")
        self.plot_button.clicked.connect(self._handle_plot)
        layout.addWidget(self.plot_button)

        self.clear_chart_button = QPushButton("Clear Chart")
        self.clear_chart_button.clicked.connect(self._handle_clear_chart)
        layout.addWidget(self.clear_chart_button)

        layout.addStretch(1)

        scroll = QScrollArea()
        scroll.setWidget(container)
        scroll.setWidgetResizable(True)
        scroll_container = QWidget()
        scroll_layout = QVBoxLayout(scroll_container)
        scroll_layout.addWidget(scroll)
        return scroll_container

    def _create_display_panel(self) -> QWidget:
        container = QWidget()
        layout = QVBoxLayout(container)

        tab_widget = QTabWidget()

        chart_tab = QWidget()
        chart_layout = QVBoxLayout(chart_tab)
        chart_layout.addWidget(self.canvas)
        tab_widget.addTab(chart_tab, "Chart")

        self.table_view = QTableView()
        self.table_view.setModel(self.table_model)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        tab_widget.addTab(self.table_view, "Data Table")

        layout.addWidget(tab_widget)
        return container

    def _create_actions(self) -> None:
        self.open_action = QAction("Open CSV...", self)
        self.open_action.setShortcut(QKeySequence("Ctrl+O"))
        self.open_action.triggered.connect(self._open_csv)

        self.export_plot_action = QAction("Export Chart...", self)
        self.export_plot_action.setShortcut(QKeySequence("Ctrl+S"))
        self.export_plot_action.triggered.connect(self._export_chart)

        self.exit_action = QAction("Exit", self)
        self.exit_action.setShortcut(QKeySequence("Ctrl+Q"))
        self.exit_action.triggered.connect(self.close)

    def _create_menu(self) -> None:
        file_menu = self.menuBar().addMenu("&File")
        file_menu.addAction(self.open_action)
        file_menu.addAction(self.export_plot_action)
        file_menu.addSeparator()
        file_menu.addAction(self.exit_action)

    def _create_status_bar(self) -> None:
        status = QStatusBar(self)
        self.setStatusBar(status)
        status.showMessage("Ready")

    # --- Actions ---------------------------------------------------------
    def _open_csv(self) -> None:
        dialog = QFileDialog(self, "Open CSV")
        dialog.setNameFilters(["CSV files (*.csv)", "All files (*)"])
        dialog.setFileMode(QFileDialog.FileMode.ExistingFile)
        if dialog.exec() == QFileDialog.DialogCode.Accepted:
            file_path = dialog.selectedFiles()[0]
            self.tab_widget.setCurrentIndex(0)
            self._load_dataframe(Path(file_path))

    def _load_dataframe(self, file_path: Path) -> None:
        try:
            df = load_csv(file_path)
        except CSVLoadError as exc:
            self._show_error(str(exc))
            return

        self.dataframe = df
        self.table_model.set_dataframe(df)
        self.statusBar().showMessage(f"Loaded {file_path.name} with {len(df)} rows")
        self._populate_column_controls(df)
        self._set_chart_ready(False)

    def _populate_column_controls(self, df: pd.DataFrame) -> None:
        self.x_column_combo.blockSignals(True)
        self.y_column_list.blockSignals(True)

        self.x_column_combo.clear()
        self.x_column_combo.addItem("Index")
        for column in df.columns:
            self.x_column_combo.addItem(column)

        self.y_column_list.clear()
        numeric_cols = set(get_numeric_columns(df))
        for column in df.columns:
            item = QListWidgetItem(column)
            self.y_column_list.addItem(item)
            if column in numeric_cols:
                item.setSelected(True)

        # Prefer using a datetime column for the X axis when available.
        datetime_cols = get_datetime_columns(df)
        if datetime_cols:
            target = datetime_cols[0]
            index = self.x_column_combo.findText(target)
            if index >= 0:
                self.x_column_combo.setCurrentIndex(index)
        self.x_column_combo.blockSignals(False)
        self.y_column_list.blockSignals(False)
        self._update_option_visibility(self.chart_type_combo.currentText())

    def _handle_plot(self) -> None:
        if self.dataframe is None:
            self._show_error("Load a CSV file before plotting.")
            return

        y_columns = [item.text() for item in self.y_column_list.selectedItems()]
        if not y_columns:
            self._show_error("Select one or more Y-axis columns.")
            return

        try:
            config = self._build_plot_config(y_columns)
            self.plot_manager.plot(self.dataframe, config)
        except ValueError as exc:
            self._show_error(str(exc))
            return

        self._set_chart_ready(True)

    def _build_plot_config(self, y_columns: List[str]) -> PlotConfig:
        chart_type = self.chart_type_combo.currentText()
        x_value = self.x_column_combo.currentText()
        x_column = None if x_value == "Index" or chart_type == "Pie" else x_value

        cmap_choice = self.colormap_combo.currentText()
        colors: List[str] = []
        if cmap_choice != "Default" and len(y_columns) > 0:
            cmap = cm.get_cmap(COLORMAPS[cmap_choice], len(y_columns))
            colors = [cmap(i) for i in range(len(y_columns))]

        config = PlotConfig(
            chart_type=chart_type,
            x_column=x_column,
            y_columns=y_columns,
            bins=self.bins_spin.value(),
            stacked=self.stacked_checkbox.isChecked(),
            normalize_histogram=self.normalize_histogram_checkbox.isChecked(),
            show_grid=self.show_grid_checkbox.isChecked(),
            show_legend=self.show_legend_checkbox.isChecked(),
            marker=self.marker_combo.currentData(),
            color_cycle=colors,
        )
        return config

    def _export_chart(self) -> None:
        if self.canvas.figure.axes:
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Export Chart",
                "chart.png",
                "PNG Image (*.png);;PDF Document (*.pdf);;SVG Vector (*.svg)",
            )
            if file_path:
                try:
                    self.canvas.figure.savefig(file_path, dpi=300)
                    self.statusBar().showMessage(f"Chart exported to {file_path}")
                except Exception as exc:  # pragma: no cover - surfaced via UI
                    self._show_error(f"Failed to export chart: {exc}")
        else:
            self._show_error("There is no chart to export yet.")

    def _handle_clear_chart(self) -> None:
        self.canvas.clear()
        self._set_chart_ready(False)

    def _set_chart_ready(self, ready: bool) -> None:
        if hasattr(self, "export_button"):
            self.export_button.setEnabled(ready)
        if hasattr(self, "export_plot_action"):
            self.export_plot_action.setEnabled(ready)

    def _update_option_visibility(self, chart_type: str) -> None:
        is_histogram = chart_type == "Histogram"
        is_scatter = chart_type == "Scatter"
        is_bar = chart_type == "Bar"
        is_pie = chart_type == "Pie"

        self.bins_spin.setVisible(is_histogram)
        self.normalize_histogram_checkbox.setVisible(is_histogram)

        self.marker_combo.setVisible(is_scatter)
        self.stacked_checkbox.setVisible(is_bar or chart_type == "Area")

        self.x_column_combo.setEnabled(not is_pie)

    def _show_error(self, message: str) -> None:
        QMessageBox.warning(self, "Sunseeker Data Toolkit", message)

    def closeEvent(self, event: QCloseEvent) -> None:  # type: ignore[override]
        self.canvas.figure.clear()
        super().closeEvent(event)


def launch_app() -> None:
    import sys

    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
