# Sunseeker Data Toolkit

A PyQt6 desktop application that bundles two data-analysis tools:

- **CSV Graph Explorer** - load telemetry CSV files, inspect raw data, and render interactive charts.
- **CAN Decoder** - stream large CAN log CSVs into decoded engineering values, with optional channel filtering and split outputs.

## Features
- Unified tabbed interface with shared menus and status bar.
- Supports both ad-hoc graphing and CAN decoding without leaving the app.
- Streaming CAN decoder keeps memory usage low and writes partitioned CSVs when needed.
- Parallel CAN0/CAN1 decoding with real-time progress feedback.
- Charting backed by Matplotlib with multiple chart types, colour maps, and export options.

## Getting Started
1. Create and activate a virtual environment (recommended).
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Launch the application:
   ```bash
   python main.py
   ```

## Using the Application
- Switch between **CSV Graph Explorer** and **CAN Decoder** via the tabs along the top of the window.
- CSV Graph Explorer:
  - Click **Load CSV...** (or use File > Open CSV... / Ctrl+O) to import data.
  - Choose a chart type, configure axes and options, then click **Render Chart**.
  - Export the current chart with **Export Chart...** or File > Export Chart... / Ctrl+S.
- CAN Decoder:
  - Select an input CAN log CSV and choose an output file or base name.
  - Pick channel filters, toggle bit expansion, and decide whether to split CAN0 / CAN1 outputs.
  - Press **Decode** to run the decoder; progress and file locations appear in the log panel.

## Notes
- Histogram, bar, area, scatter, and line charts require numeric data; pie charts expect a single column selection.
- Extremely large CSVs may take time to load in the graph tab; consider pre-filtering when working with millions of rows.
- The CAN decoder honours the modern Excel row limit by default and can be switched to the legacy .xls cap when needed.

## Development
- Application code lives in the `app/` package. `main.py` is the executable entry point.
- Graph-specific logic resides in `app/plotter.py`, while the CAN streaming decoder lives in `can_analyzer.py`.