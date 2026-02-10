aggsim example

This folder contains a runnable example of the aggsim pipeline simulator.

Prerequisites
- Python 3.8+
- Install runtime dependencies:

```bash
pip install pandas sortedcontainers
```

Running the example
From the `examples/` folder run:

```bash
python simulator.py --input input_data.csv --output data --semantics AMO --was 53,59 --wss 61,67 --wa-ws-index 0 --resolution 1000 --pane-creation 0.000001 --pane-update 0.000001 --pane-aggregation 0.000001 --pane-merge 0.000001 --pane-delete 0.000001 --tuple-sending 0
```

Notes
- The example script adds `src/` to `sys.path` so you can run it from the `examples/` folder without installing the package.
- Output CSV files will be written into the directory specified by `--output` (the example uses `data`).
