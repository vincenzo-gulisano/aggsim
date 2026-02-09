"""Pipeline runner for the streaming simulator.

This module provides `PipelineSimulator` which advances a `SourceSimulator`
and an `AggregateSimulator` using a simple discrete-event loop. Emitted
metrics are collected into a time-indexed pandas DataFrame for inspection
or persistence by the caller.
"""

from typing import List, Optional, Tuple
from math import inf
import pandas as pd

from .source_simulator import SourceSimulator
from .aggregate_simulator import AggregateSimulator
from ..util.common import Metrics, OptionalMetrics


class PipelineSimulator:
    """Drive the source and aggregate simulators in a coordinated loop.

    The pipeline advances the component with the smallest next-event time.
    Emitted rows from both components are concatenated (source metrics first,
    aggregate metrics second) so their order matches `metric_names()`.
    """

    def __init__(self, source: SourceSimulator, aggregate: AggregateSimulator) -> None:
        self.source = source
        self.aggregate = aggregate
        self._src_done = False
        self._agg_done = False

        self._metric_names = self.source.metric_names() + self.aggregate.metric_names()

    @staticmethod
    def _min_time(a: float, b: float) -> float:
        if a == -inf:
            return b
        if b == -inf:
            return a
        return a if a <= b else b

    def metric_names(self) -> List[str]:
        return self._metric_names

    def step(self) -> Tuple[float, bool, OptionalMetrics]:
        """Advance one component and return the current simulation state.

        Returns a tuple (cur_time, has_more, emitted) where `emitted` is either
        None or a list of per-metric emitted rows. The order of metrics in
        `emitted` matches `self.metric_names()`.
        """
        src_n = len(self.source.metric_names())
        agg_n = len(self.aggregate.metric_names())

        def empty_src() -> Metrics:
            return [[] for _ in range(src_n)]

        def empty_agg() -> Metrics:
            return [[] for _ in range(agg_n)]

        def step_source() -> OptionalMetrics:
            _, more, emitted = self.source.step()
            self._src_done = not more
            if emitted is None:
                return None
            return emitted + empty_agg()

        def step_agg() -> OptionalMetrics:
            _, more, emitted = self.aggregate.step()
            self._agg_done = not more
            if emitted is None:
                return None
            return empty_src() + emitted

        # Both simulators finished: nothing more to do
        if self._src_done and self._agg_done:
            cur_t = self._min_time(self.source.delta, self.aggregate.delta)
            return (cur_t, False, None)

        # choose which component to advance next
        if not self._src_done and self.source.delta == -inf:
            emitted_all = step_source()
        elif not self._agg_done and self.aggregate.delta == -inf:
            emitted_all = step_agg()
        elif self._agg_done:
            emitted_all = step_source()
        elif self._src_done:
            emitted_all = step_agg()
        else:
            emitted_all = (
                step_source()
                if self.source.delta <= self.aggregate.delta
                else step_agg()
            )

        cur_t = self._min_time(self.source.delta, self.aggregate.delta)
        has_more_any = (not self._src_done) or (not self._agg_done)
        return (cur_t, has_more_any, emitted_all)

    def run(self, print_every: int = 10_000) -> None:
        """Run the pipeline until both components are exhausted.

        Emitted metric rows are aggregated into an in-memory pandas DataFrame
        that is periodically printed (controlled by `print_every`).
        """

        t0: Optional[float] = None
        step_count = 0

        # accumulate counts of emitted rows per metric between prints
        metric_names = self.metric_names()

        # Time-indexed table: index = ts, columns = metric names, values = v
        df = pd.DataFrame(columns=metric_names)
        df.index.name = "time"

        MAX_ROWS = 50_000

        while True:
            cur_t, has_more_any, emitted = self.step()

            if emitted is not None:
                for col_idx, rows in enumerate(emitted):
                    if not rows:
                        continue
                    col = metric_names[col_idx]
                    for ts, v in rows:
                        # Set the cell
                        df.at[ts, col] = v

            # check time is not decreasing
            if t0 is not None and cur_t < t0:
                raise RuntimeError(
                    f"Simulation time decreased: previous time {t0}, current time {cur_t}."
                )

            if t0 is None and cur_t != -inf:
                t0 = cur_t

            if step_count % print_every == 0:
                elapsed = 0.0 if (t0 is None or cur_t == -inf) else (cur_t - t0)

                # Sort by time (index), show last 100 rows
                recent = df.sort_index().tail(100)

                # show '-' instead of NaN for readability
                recent_str = recent.to_string(na_rep="-")

                # Multi-line output: print a snapshot of the recent rows
                print(f"\nSim time: {elapsed:10.3f} more={has_more_any}\n{recent_str}\n", flush=True)

            step_count += 1
            if not has_more_any:
                break

            if len(df) > MAX_ROWS:
                df = df.sort_index().tail(MAX_ROWS)

        # finalize both components
        self.aggregate.finalize()
        self.source.finalize()
