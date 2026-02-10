# aggsim

aggsim is a small event-stream aggregation simulator that models pane/window
aggregation behavior and collects time-windowed statistics.

**Features**
- Simulates source ingestion and windowed aggregation (pane-level ops).
- Collects metrics: injection rate, throughput, latency, active panes, CPU, output rate.
- Supports AMO and EO semantics and dynamic WA/WS reconfiguration.

**Requirements**
- Python 3.8+
- Install runtime dependencies:

```bash
pip install pandas sortedcontainers
```

**Using conda**

If you prefer to use conda, create and activate an environment named `aggsim` and install the required packages. Example using `conda` + `pip`:

```bash
conda create -n aggsim python=3.8 -y
conda activate aggsim
pip install pandas sortedcontainers
```

Or install packages from conda-forge directly:

```bash
conda create -n aggsim -c conda-forge python=3.8 pandas sortedcontainers -y
conda activate aggsim
```

You can also export an `environment.yml` if you want reproducible environments.

**Quick sanity check**
From the repository root run:

```bash
python3 -c "import sys; sys.path.insert(0,'src'); import aggsim; print('OK')"
```

If you see `OK`, package imports are working.

**Installation**
To make `aggsim` available to other projects you can install it from the
repository root. For an editable (development) install run:

```bash
pip install -e .
```

For a normal install (non-editable):

```bash
pip install .
```

You can also install the package into another project's virtualenv by
referencing the local path or using a VCS URL in `requirements.txt`.

**Code quality**
All code passes flake8 linting (max line length: 120).

**Project structure**
- `src/aggsim/` — package source
  - `simulator/` — `source_simulator.py`, `aggregate_simulator.py`, `pipeline_simulator.py`
  - `stats/` — `base_stat.py`, `sum_stat.py`, `avg_stat.py`
  - `util/` — helper utilities (e.g., `win_and_pane_boundary_calculator.py`, `common.py`)

**Notes about imports**
- Module imports were standardized to use package-relative imports (e.g. `from ..stats.sum_stat import SumStat`) so the package can be imported when `src/` is on `PYTHONPATH`.
- Ensure you run tools and scripts from the repo root and add `src/` to `PYTHONPATH` (or use `pip install -e .`).

**Next steps I can take**
- Add a tiny example runner script and sample input CSV.

**Examples**
- Example runner: see the `examples/` folder for a runnable demo and sample input.
  The example script takes the input CSV and writes metric CSVs into the folder
  passed via `--output-folder`.

Example usage (from `examples/`):

```bash
python simulator.py --input input_data.csv --output-folder . --semantics AMO --was 53,59 --wss 61,67 --wa-ws-index 0 --resolution 1000 --pane-creation 0.000001 --pane-update 0.000001 --pane-aggregation 0.000001 --pane-merge 0.000001 --pane-delete 0.000001 --tuple-sending 0
```