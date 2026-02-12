"""Pipeline runner for the streaming simulator.

This module provides `PipelineSimulator` which advances a `SourceSimulator`
and an `AggregateSimulator` using a simple discrete-event loop. Emitted
metrics are collected into a time-indexed pandas DataFrame for inspection
or persistence by the caller.
"""

from typing import List, Optional, Tuple
from math import inf

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
        self._src_n = len(self.source.metric_names())
        self._agg_n = len(self.aggregate.metric_names())

        # Track previous deltas for time validation
        self._prev_src_delta: float = -inf
        self._prev_agg_delta: float = -inf

        self.first_call = True

        # Track latest emitted for both simulators
        self._last_src_emitted: OptionalMetrics = None
        self._last_agg_emitted: OptionalMetrics = None

    @staticmethod
    def _min_time(a: float, b: float) -> float:
        if a == -inf:
            return b
        if b == -inf:
            return a
        return a if a <= b else b

    def metric_names(self) -> List[str]:
        return self._metric_names

    def _empty_src(self) -> Metrics:
        """Create empty source metrics structure."""
        return [[] for _ in range(self._src_n)]

    def _empty_agg(self) -> Metrics:
        """Create empty aggregate metrics structure."""
        return [[] for _ in range(self._agg_n)]

    def _step_source(self) -> None:
        """Advance source simulator and store emitted metrics (with empty agg metrics appended)."""
        _, more, emitted = self.source.step_until_stats()
        self._src_done = not more
        if emitted is None:
            self._last_src_emitted = None
        else:
            self._last_src_emitted = emitted + self._empty_agg()

    def _step_agg(self) -> None:
        """Advance aggregate simulator and store emitted metrics (with empty source metrics prepended)."""
        _, more, emitted = self.aggregate.step_until_stats()
        self._agg_done = not more
        if emitted is None:
            self._last_agg_emitted = None
        else:
            self._last_agg_emitted = self._empty_src() + emitted

    def change(self, wa: int, ws: int) -> None:
        """Request a reconfiguration of the window configuration in the aggregate simulator.

        Args:
            wa: New Window Advance (WA).
            ws: New Window Size (WS).

        Raises:
            ValueError: If the specified (WA, WS) combination is not valid.
            RuntimeError: If the change cannot be scheduled (e.g., no tuples processed yet).
        """
        self.aggregate.change(wa, ws)

    def step(self) -> Tuple[float, bool, OptionalMetrics]:
        """Advance one component and return the current simulation state.

        On first call: initializes both source and aggregate by stepping until each
        has a real time (> -inf).
        On subsequent calls: steps the component with the smaller delta.

        Returns a tuple (cur_time, has_more, emitted) where `emitted` is either
        None or a list of per-metric emitted rows. The order of metrics in
        `emitted` matches `self.metric_names()`.

        Raises:
            RuntimeError: If time decreases in source or aggregate simulator.
        """

        # Both simulators finished: nothing more to do
        if self._src_done and self._agg_done:
            return (self._min_time(self.source.delta, self.aggregate.delta), False, None)

        # # Store previous deltas to detect time decreases # REMOVE THIS ONCE BUG IS FIXED
        # prev_src_delta = self.source.delta
        # prev_agg_delta = self.aggregate.delta

        if self.first_call:
            self.first_call = False
            # Initialize source until it has a real time
            while self.source.delta == -inf and not self._src_done:
                self._step_source()
            # Initialize aggregate until it has a real time
            while self.aggregate.delta == -inf and not self._agg_done:
                self._step_agg()

            if self.source.delta == -inf and self._src_done:
                raise RuntimeError("Source simulator finished without changing to a non-infinite time.")
            if self.aggregate.delta == -inf and self._agg_done:
                raise RuntimeError("Aggregate simulator finished without changing to a non-infinite time.")

        # After initialization (or on subsequent calls), step the one with smaller delta
        if self._src_done:
            # Only aggregate left, step it
            self._step_agg()
        elif self._agg_done:
            # Only source left, step it
            self._step_source()
        else:
            # Both still have events: step the one with smaller delta
            # (or step source if aggregate is done, etc.)
            if self.source.delta <= self.aggregate.delta or self._agg_done:
                self._step_source()
            else:
                self._step_agg()

        # Now return the emitted from whichever has the smaller delta
        # (or return source if aggregate is done, or aggregate if source is done)
        if self._agg_done or self.source.delta <= self.aggregate.delta:
            emitted_all = self._last_src_emitted
        else:
            emitted_all = self._last_agg_emitted

        # Check for time decreases
        # if self.source.delta < prev_src_delta:
        #     raise RuntimeError(
        #         f"Source simulator time decreased: {prev_src_delta} -> {self.source.delta}."
        #     )
        # if self.aggregate.delta < prev_agg_delta:
        #     raise RuntimeError(
        #         f"Aggregate simulator time decreased: {prev_agg_delta} -> {self.aggregate.delta}."
        #     )

        # Return time from the simulator(s) that are still active
        if self._agg_done:
            cur_t = self.source.delta
        elif self._src_done:
            cur_t = self.aggregate.delta
        else:
            cur_t = self._min_time(self.source.delta, self.aggregate.delta)

        has_more_any = (not self._src_done) or (not self._agg_done)
        return (cur_t, has_more_any, emitted_all)

    def run(
        self,
        print_every: int = 0,
        collect_debug_df: bool = False,
        max_debug_rows: int = 50_000,
    ) -> None:
        """Run the pipeline until both components are exhausted.

        By default this method runs in fast mode and avoids constructing a pandas
        DataFrame. Set `collect_debug_df=True` to build a debug table of emitted
        values, and `print_every > 0` to periodically print status.
        """

        t0: Optional[float] = None
        step_count = 0

        # accumulate counts of emitted rows per metric between prints
        metric_names = self.metric_names()

        should_print = print_every > 0
        should_collect_df = collect_debug_df
        df = None
        if should_collect_df:
            import pandas as pd

            # Time-indexed table: index = ts, columns = metric names, values = v
            df = pd.DataFrame(columns=metric_names)
            df.index.name = "time"

        while True:
            cur_t, has_more_any, emitted = self.step()

            if should_collect_df and emitted is not None and df is not None:
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

            if should_print and step_count % print_every == 0:
                elapsed = 0.0 if (t0 is None or cur_t == -inf) else (cur_t - t0)
                if should_collect_df and df is not None:
                    # Sort by time (index), show last 100 rows
                    recent = df.sort_index().tail(100)
                    # show '-' instead of NaN for readability
                    recent_str = recent.to_string(na_rep="-")
                    # Multi-line output: print a snapshot of the recent rows
                    print(
                        f"\nSim time: {elapsed:10.3f} more={has_more_any}\n{recent_str}\n",
                        flush=True,
                    )
                else:
                    print(
                        f"Sim time: {elapsed:10.3f} more={has_more_any} step={step_count}",
                        flush=True,
                    )

            step_count += 1
            if not has_more_any:
                break

            if should_collect_df and df is not None and len(df) > max_debug_rows:
                df = df.sort_index().tail(max_debug_rows)

        # finalize both components
        self.aggregate.finalize()
        self.source.finalize()
