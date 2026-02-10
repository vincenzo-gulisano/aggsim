aggsim example

This folder contains a runnable example of the aggsim pipeline simulator.

Prerequisites
- Python 3.8+
- Install runtime dependencies:

```bash
pip install pandas sortedcontainers
```

**Using conda**

To create a conda environment named `aggsim` and install the required packages:

```bash
conda create -n aggsim python=3.8 -y
conda activate aggsim
pip install pandas sortedcontainers
```

Or install from `conda-forge`:

```bash
conda create -n aggsim -c conda-forge python=3.8 pandas sortedcontainers -y
conda activate aggsim
```

This example can be run after activating the `aggsim` environment.

Running the example
From the `examples/` folder run:

```bash
python simulator.py --input input_data.csv --output-folder . --semantics AMO --was 53,59 --wss 61,67 --wa-ws-index 0 --resolution 1000 --pane-creation 0.000001 --pane-update 0.000001 --pane-aggregation 0.000001 --pane-merge 0.000001 --pane-delete 0.000001 --tuple-sending 0
```

Notes
- The example script adds `src/` to `sys.path` so you can run it from the `examples/` folder without installing the package.
- Output CSV files will be written into the directory specified by `--output` (the example uses `data`).
