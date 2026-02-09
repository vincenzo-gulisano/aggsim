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

**Project structure**
- `src/aggsim/` — package source
  - `simulator/` — `source_simulator.py`, `aggregate_simulator.py`, `pipeline_simulator.py`
  - `stats/` — `base_stat.py`, `sum_stat.py`, `avg_stat.py`
  - `util/` — helper utilities (e.g., `win_and_pane_boundary_calculator.py`, `common.py`)

**Notes about imports**
- Module imports were standardized to use package-relative imports (e.g. `from ..stats.sum_stat import SumStat`) so the package can be imported when `src/` is on `PYTHONPATH`.
- Ensure you run tools and scripts from the repo root and add `src/` to `PYTHONPATH` (or use `pip install -e .`).

**Next steps I can take**
- Run a lint/pass (flake8) and add a `requirements.txt`.
- Add a tiny example runner script and sample input CSV.

If you want, I can add `requirements.txt`, run static checks, or create an example run.
