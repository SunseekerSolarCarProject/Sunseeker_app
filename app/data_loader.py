"""Utilities for loading and cleaning CSV data."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import pandas as pd
import warnings

# Common strings that should be treated as missing values.
DEFAULT_NA_VALUES = {
    "",
    " ",
    "na",
    "n/a",
    "null",
    "none",
    "nan",
    "N/A",
    "NULL",
    "None",
    "NaN",
}


class CSVLoadError(Exception):
    """Raised when a CSV cannot be loaded."""


def load_csv(file_path: str | Path) -> pd.DataFrame:
    """Load the CSV file into a pandas DataFrame.

    The function attempts to clean column names, infer datetimes, and coerce
    numeric columns while leaving textual columns untouched.
    """

    path = Path(file_path)
    if not path.exists():
        raise CSVLoadError(f"CSV file not found: {file_path}")

    try:
        df = pd.read_csv(
            path,
            na_values=DEFAULT_NA_VALUES,
            keep_default_na=True,
            low_memory=False,
        )
    except Exception as exc:  # pragma: no cover - surfaced in UI dialog
        raise CSVLoadError(f"Failed to read CSV: {exc}") from exc

    df = _normalize_columns(df)
    df = _infer_column_types(df)
    return df


def get_numeric_columns(df: pd.DataFrame) -> List[str]:
    """Return the list of numeric columns available for plotting."""

    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
    return numeric_cols


def get_datetime_columns(df: pd.DataFrame) -> List[str]:
    """Return columns that are likely to be datetime-like."""

    datetime_cols = [
        col
        for col in df.columns
        if pd.api.types.is_datetime64_any_dtype(df[col])
    ]
    return datetime_cols


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace and ensure column names are unique."""

    clean_columns: List[str] = []
    seen: set[str] = set()

    for index, column in enumerate(df.columns):
        candidate = str(column).strip() or f"column_{index}"
        base = candidate
        counter = 1
        while candidate in seen:
            candidate = f"{base}_{counter}"
            counter += 1
        seen.add(candidate)
        clean_columns.append(candidate)

    df.columns = clean_columns
    return df


def _infer_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """Attempt to coerce object columns into numeric or datetime types."""

    for column in df.columns:
        series = df[column]
        if pd.api.types.is_object_dtype(series):
            series = _coerce_datetime(series)
            if not pd.api.types.is_datetime64_any_dtype(series):
                series = _coerce_numeric(series)
            df[column] = series
    return df


def _coerce_datetime(series: pd.Series) -> pd.Series:
    sample = series.dropna().astype(str).head(100)
    if sample.empty:
        return series

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*infer format.*", category=UserWarning)
        converted = pd.to_datetime(sample, errors="coerce", cache=True)
    success_ratio = converted.notna().mean()
    if success_ratio >= 0.7:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*infer format.*", category=UserWarning)
            return pd.to_datetime(series, errors="coerce", cache=True)
    return series


def _coerce_numeric(series: pd.Series) -> pd.Series:
    sample = series.dropna().astype(str).head(100)
    if sample.empty:
        return series

    # Replace common thousands separators before coercion.
    cleaned_sample = sample.str.replace(",", "", regex=False)
    converted = pd.to_numeric(cleaned_sample, errors="coerce")
    success_ratio = converted.notna().mean()
    if success_ratio >= 0.7:
        cleaned_full = series.astype(str).str.replace(",", "", regex=False)
        return pd.to_numeric(cleaned_full, errors="coerce")
    return series
