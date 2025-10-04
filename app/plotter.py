"""Plotting utilities using matplotlib."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np
import pandas as pd
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure


SUPPORTED_CHARTS = (
    "Line",
    "Scatter",
    "Bar",
    "Area",
    "Histogram",
    "Box",
    "Pie",
)


@dataclass
class PlotConfig:
    chart_type: str
    x_column: Optional[str]
    y_columns: List[str]
    bins: int = 30
    stacked: bool = False
    normalize_histogram: bool = False
    show_grid: bool = True
    show_legend: bool = True
    marker: str = "o"
    color_cycle: List[str] = field(default_factory=list)


class PlotCanvas(FigureCanvas):
    """Matplotlib canvas embedded in Qt."""

    def __init__(self) -> None:
        self.figure = Figure(figsize=(6, 4))
        super().__init__(self.figure)
        self.setMinimumSize(640, 480)

    def clear(self) -> None:
        self.figure.clf()
        self.draw_idle()


class PlotManager:
    def __init__(self, canvas: PlotCanvas) -> None:
        self.canvas = canvas

    def plot(self, df: pd.DataFrame, config: PlotConfig) -> None:
        if not config.y_columns:
            raise ValueError("Select at least one Y-axis column to plot.")

        chart_type = config.chart_type
        if chart_type not in SUPPORTED_CHARTS:
            raise ValueError(f"Unsupported chart type: {chart_type}")

        non_numeric = [
            col
            for col in config.y_columns
            if not pd.api.types.is_numeric_dtype(df[col])
        ]
        if non_numeric and chart_type in {"Line", "Scatter", "Bar", "Area", "Histogram"}:
            raise ValueError(
                "The selected chart requires numeric data. "
                f"Convert or deselect: {', '.join(non_numeric)}"
            )

        figure = self.canvas.figure
        figure.clf()
        axis = figure.add_subplot(111)
        if config.color_cycle:
            axis.set_prop_cycle(color=config.color_cycle)

        x_values, x_label = self._resolve_x(df, config.x_column)

        if chart_type == "Line":
            self._plot_line(axis, df, x_values, config)
        elif chart_type == "Scatter":
            self._plot_scatter(axis, df, x_values, config)
        elif chart_type == "Bar":
            self._plot_bar(axis, df, x_values, config)
        elif chart_type == "Area":
            self._plot_area(axis, df, x_values, config)
        elif chart_type == "Histogram":
            self._plot_histogram(axis, df, config)
        elif chart_type == "Box":
            self._plot_box(axis, df, config)
        elif chart_type == "Pie":
            self._plot_pie(axis, df, config)

        axis.set_xlabel(x_label)
        if chart_type not in {"Histogram", "Pie", "Box"}:
            axis.set_ylabel(", ".join(config.y_columns))
        axis.grid(config.show_grid)
        axis.tick_params(axis="x", labelrotation=25)
        if config.show_legend and chart_type not in {"Histogram", "Pie", "Box"}:
            axis.legend(loc="best")

        figure.tight_layout()
        self.canvas.draw_idle()

    @staticmethod
    def _resolve_x(df: pd.DataFrame, x_column: Optional[str]):
        if x_column is None or x_column == "Index":
            x_values = np.arange(len(df.index))
            x_label = "Index"
        else:
            x_values = df[x_column]
            x_label = x_column
        return x_values, x_label

    def _plot_line(self, axis, df: pd.DataFrame, x_values, config: PlotConfig) -> None:
        for column in config.y_columns:
            axis.plot(x_values, df[column], label=column)

    def _plot_scatter(self, axis, df: pd.DataFrame, x_values, config: PlotConfig) -> None:
        for column in config.y_columns:
            axis.scatter(
                x_values,
                df[column],
                label=column,
                marker=config.marker,
                alpha=0.8,
            )

    def _plot_bar(self, axis, df: pd.DataFrame, x_values, config: PlotConfig) -> None:
        indices = np.arange(len(df.index))
        if config.stacked or len(config.y_columns) == 1:
            bottom = np.zeros(len(df.index))
            for column in config.y_columns:
                axis.bar(indices, df[column], bottom=bottom, width=0.8, label=column)
                bottom = bottom + df[column].fillna(0).to_numpy()
            axis.set_xticks(indices)
        else:
            width = 0.8 / max(len(config.y_columns), 1)
            for offset, column in enumerate(config.y_columns):
                axis.bar(indices + offset * width, df[column], width=width, label=column)
            axis.set_xticks(indices + width * (len(config.y_columns) - 1) / 2)
        axis.set_xticklabels(self._format_xticklabels(x_values))

    def _plot_area(self, axis, df: pd.DataFrame, x_values, config: PlotConfig) -> None:
        y_data = [df[column].fillna(0).values for column in config.y_columns]
        axis.stackplot(x_values, *y_data, labels=config.y_columns)

    def _plot_histogram(self, axis, df: pd.DataFrame, config: PlotConfig) -> None:
        for column in config.y_columns:
            axis.hist(
                df[column].dropna(),
                bins=config.bins,
                density=config.normalize_histogram,
                alpha=0.7,
                label=column,
            )
        axis.set_ylabel("Density" if config.normalize_histogram else "Frequency")

    def _plot_box(self, axis, df: pd.DataFrame, config: PlotConfig) -> None:
        data = [df[column].dropna() for column in config.y_columns]
        axis.boxplot(data, labels=config.y_columns, vert=True)

    def _plot_pie(self, axis, df: pd.DataFrame, config: PlotConfig) -> None:
        if len(config.y_columns) != 1:
            raise ValueError("Pie charts require exactly one column.")
        column = config.y_columns[0]
        series = df[column].dropna()
        if series.empty:
            raise ValueError("Selected column has no data for pie chart.")
        if not pd.api.types.is_numeric_dtype(series):
            counts = series.value_counts().head(8)
            axis.pie(counts.values, labels=counts.index, autopct="%1.1f%%")
        else:
            axis.pie(series.values, labels=None, autopct="%1.1f%%")
        axis.set_ylabel("")

    @staticmethod
    def _format_xticklabels(x_values) -> List[str]:
        if isinstance(x_values, pd.Series):
            if pd.api.types.is_datetime64_any_dtype(x_values):
                formatted = x_values.dt.strftime("%Y-%m-%d %H:%M:%S")
                return [str(value) for value in formatted]
            return [str(value) for value in x_values]
        return [str(value) for value in x_values]
