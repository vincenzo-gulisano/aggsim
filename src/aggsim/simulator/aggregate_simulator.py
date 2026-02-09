
"""Aggregate simulator for simulating pane/window aggregation operations.

This module provides the AggregateSimulator class which processes an event stream
and simulates windowed aggregation operations with configurable cost estimators
for various pane operations (creation, update, aggregation, merge, deletion).
"""
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Iterator,
    TextIO,
    Set,
)
from math import inf
import csv
from ..util import common
from ..stats.sum_stat import SumStat
from ..stats.avg_stat import AvgStat
from ..util.win_and_pane_boundary_calculator import WindowAndPaneBoundaryCalculator
from sortedcontainers import SortedDict

# TO DOs:
# 2) check the logic if statistics are not kept

class AggregateSimulator:
    """Simulates windowed aggregation with pane-level operations.

    Processes an event stream and tracks pane/window operations including
    creation, updates, aggregation, merging, and deletion, with support for
    both At-Most-Once (AMO) and Exactly-Once (EO) semantics. Collects metrics
    on throughput, latency, active panes, CPU cost, and output rate.
    """

    def __init__(
        self,
        input_path: str,
        output_folder: str,
        extractor: common.ExtractorFunctions,
        estimators: common.EstimatorFunctions,
        was: Set[int],
        wss: Set[int],
        wa_ws_index: int,
        resolution: int,
        semantics: common.Semantics,
        ts_offset: int,
    ) -> None:
        """Initialize the aggregate simulator.

        Args:
            input_path: Path to the file containing the input stream (one tuple per line).
            output_folder: Path to the folder where simulation results will be written.
            extractor: ExtractorFunctions instance containing functions for extracting fields from raw lines.
            estimators: EstimatorFunctions instance containing cost estimators for various operations.
            was: Set of Window Advances (WA), in same time unit as event time.
            wss: Set of Window Sizes (WS), in same time unit as event time.
            wa_ws_index: Index selecting which (WA, WS) combination from the cartesian product.
            resolution: Time resolution for internal quantization of event times.
            semantics: Processing semantics (AMO or EO).
            ts_offset: Time offset to apply to all timestamps.

        Raises:
            ValueError: If the cartesian product of was/wss is empty or if semantics is invalid.
            IndexError: If wa_ws_index is out of bounds.
        """
        self.input_path: str = input_path
        self.output_folder: str = output_folder
        self.extractor: common.ExtractorFunctions = extractor
        self.estimators: common.EstimatorFunctions = estimators
        self.resolution: int = int(resolution)

        # -----------------------------------------
        # Build the list of (WA, WS) combinations
        # -----------------------------------------
        # Sort for determinism; create all pairs where WS >= WA is typically expected,
        # but we do not enforce that here unless needed.
        self.wa_ws_list: List[Tuple[int, int]] = [
            (wa, ws) for wa in sorted(was) for ws in sorted(wss)
        ]
        if not self.wa_ws_list:
            raise ValueError("The cartesian product of 'was' and 'wss' is empty.")

        if not (0 <= wa_ws_index < len(self.wa_ws_list)):
            raise IndexError(
                f"wa_ws_index {wa_ws_index} is out of bounds for {len(self.wa_ws_list)} combinations."
            )
        self.wa_ws_index: int = wa_ws_index

        # Max timestamp seen so far (None until known)
        self.max_tau: Optional[int] = None

        # When a reconfiguration is requested, these will hold the next index and the tau at which to trigger the reconfiguration (if needed, depending on semantics)
        self.next_wa_ws_index: Optional[int] = None
        self.reconfiguration_tau: Optional[int] = None

        # store also the largest window size, needed to remove stale panes for EO semantics
        self.max_ws: int = max(wss)
        self.min_wa: int = min(was)

        # Stream state for step()
        self._fh: Optional[TextIO] = None
        self._reader: Optional[Iterator[List[str]]] = None
        self._next_parts: Optional[List[str]] = None
        self._done: bool = False

        # Left boundary of the earliest window that is currently relevant (None until known)
        self.earliest_win_left: Optional[int] = None

        # Boundaries of the earliest pane (None until known)
        self.earliest_pane_left: Optional[int] = None
        self.earliest_pane_right: Optional[int] = None

        self.delta: float = -inf

        # Statistics
        self.throughput_stat: SumStat = SumStat(
            self.output_folder + "/throughput.rate.csv", self.resolution, -1
        )
        self.latency_stat: AvgStat = AvgStat(
            self.output_folder + "/latency.average.csv", self.resolution, -1, 1000
        )
        self.panes_stat: SumStat = SumStat(
            self.output_folder + "/paneActive.count.csv", self.resolution, -1, 1, False
        )
        self.cpu_stat: SumStat = SumStat(
            self.output_folder + "/cpu.csv", self.resolution, -1, 100
        )
        self.output_rate_stat: SumStat = SumStat(
            self.output_folder + "/outrate.rate.csv", self.resolution, -1
        )
        self.stats_initialized: bool = False
        self._metric_names: List[str] = [
            "throughput.rate",
            "latency.average",
            "paneActive.count",
            "cpu",
            "outrate.rate",
        ]
        self._metric_idx: Dict[str, int] = {
            n: i for i, n in enumerate(self._metric_names)
        }

        # Semantics of processing (At-Most-Once or Exactly-Once)
        if not isinstance(semantics, common.Semantics):
            raise ValueError(
                f"Invalid semantics: {semantics}. Must be a Semantics enum value."
            )
        self.semantics: common.Semantics = semantics
        self.ts_offset: int = ts_offset

        self.pane_calculator = WindowAndPaneBoundaryCalculator(was, wss)

        self.panes: SortedDict[int, Set[int]] = SortedDict()

        # Tracks whether each pane has already been aggregated
        self.aggregated_panes: Dict[int, bool] = {}

        # self.last_tuple_tau: Optional[int] = None
        # self.last_tuple_key: Optional[int] = None

    def _empty_emitted(self) -> common.OptionalMetrics:
        """Create an empty emitted statistics structure."""
        return [[] for _ in self._metric_names]

    def _collect_stat_rows(self, id, rows, emitted) -> common.OptionalMetrics:
        """Collect statistic rows into the emitted structure.

        Args:
            id: Metric identifier.
            rows: Rows to collect.
            emitted: Current emitted structure to update.

        Returns:
            Updated emitted structure.
        """
        if rows:
            if emitted is None:
                emitted = self._empty_emitted()
            emitted[self._metric_idx[id]].extend(rows)

        return emitted

    def _get_earliest_boundary(self, tau: int) -> Tuple[int, int, int]:
        """Compute earliest left boundaries of the window and pane for a tuple.

        Calculates the left boundaries of the window and pane to which a tuple with
        the given timestamp falls into, based on the current semantics.

        Args:
            tau: Event time (same unit as WA/WS, e.g., epoch milliseconds).

        Returns:
            Tuple of (window_left_boundary, pane_left_boundary, pane_right_boundary).
        """
        if self.semantics == common.Semantics.AMO:
            return self.pane_calculator.get_earliest_boundaries_amo(
                tau, self.wa_ws_index
            )
        elif self.semantics == common.Semantics.EO:
            return self.pane_calculator.get_earliest_boundaries_eo(
                tau, self.wa_ws_index
            )

    def __process(self, tau: int, k: int) -> common.OptionalMetrics:
        """Process a single tuple with the given timestamp and key.

        Updates pane states, window aggregations, and collects statistics on
        throughput, latency, pane counts, CPU cost, and output rates.

        Args:
            tau: Event time of the tuple.
            k: Key value extracted from the tuple.

        Returns:
            Emitted statistics from this processing step.
        """

        emitted = None

        # Update max timestamp seen so far
        self.max_tau = tau if self.max_tau is None else max(self.max_tau, tau)

        # Update the current time
        self.delta = max(self.delta, tau + self.ts_offset)
        delta_start = self.delta

        if not self.stats_initialized:
            self.throughput_stat.initialize(self.delta)
            self.latency_stat.initialize(self.delta)
            self.panes_stat.initialize(self.delta)
            self.cpu_stat.initialize(self.delta)
            self.output_rate_stat.initialize(self.delta)
            self.stats_initialized = True

        # Handle reconfiguration if needed
        if (
            self.reconfiguration_tau is not None
            and self.max_tau >= self.reconfiguration_tau
        ):

            self.reconfiguration_tau = None
            self.wa_ws_index = self.next_wa_ws_index
            self.next_wa_ws_index = None
            for pane_tau in self.panes.keys():
                if self.aggregated_panes[pane_tau]:
                    emitted = self._collect_stat_rows(
                        "paneActive.count",
                        self.panes_stat.update(
                            -1 * len(self.panes[pane_tau]), self.delta
                        ),
                        emitted,
                    )

            self.panes.clear()
            self.aggregated_panes.clear()
            self.earliest_win_left = None
            self.earliest_pane_left = None
            self.earliest_pane_right = None

        # Retrieve boundaries of window and pane to which tuple with timestamp tau falls in (if they have changed)
        if self.earliest_pane_right is None or (
            self.earliest_pane_right is not None and tau >= self.earliest_pane_right
        ):
            win_left, pane_left, pane_right = self._get_earliest_boundary(tau)
        else:
            win_left = self.earliest_win_left
            pane_left = self.earliest_pane_left
            pane_right = self.earliest_pane_right

        # Compute partial aggregates of previous panes when entering a new pane
        if self.earliest_pane_left is not None and self.earliest_pane_left < pane_left:
            if self.aggregated_panes[self.earliest_pane_left]:
                raise RuntimeError(
                    "Trying to re-compute the partial aggregate of a pane that has already been computed."
                )
            for k in self.panes[self.earliest_pane_left]:
                # The partial aggregate of this pane has not been computed yet
                delta_before = self.delta
                self.delta = self.delta + self.estimators.pane_aggregation_est()
                emitted = self._collect_stat_rows(
                    "cpu",
                    self.cpu_stat.update(self.delta - delta_before, self.delta),
                    emitted,
                )
            # Mark the partial aggregate of this pane as computed
            self.aggregated_panes[self.earliest_pane_left] = True
            emitted = self._collect_stat_rows(
                "paneActive.count",
                self.panes_stat.update(
                    len(self.panes[self.earliest_pane_left]), self.delta
                ),
                emitted,
            )

        # Produce results and shift window instances, delete stale panes
        while self.earliest_win_left is not None and self.earliest_win_left < win_left:

            # Create variables to store keys of output tuples
            outputs = set()

            # Produce outputs for the current window
            for pane_tau in list(self.panes.keys()):
                if (
                    pane_tau >= self.earliest_win_left
                    and pane_tau
                    < self.earliest_win_left + self.wa_ws_list[self.wa_ws_index][1]
                ):
                    for k in self.panes[pane_tau]:
                        if not self.aggregated_panes[pane_tau]:
                            raise RuntimeError(
                                "Trying to retrieve the partial aggregate of a pane that has not been computed."
                            )
                        if k not in outputs:  # Output the aggregate for key k only once
                            outputs.add(k)
                        else:
                            delta_before = self.delta
                            self.delta = self.delta + self.estimators.pane_merge_est()
                            emitted = self._collect_stat_rows(
                                "cpu",
                                self.cpu_stat.update(
                                    self.delta - delta_before, self.delta
                                ),
                                emitted,
                            )

            temp_bool = True
            for k in outputs:
                emitted = self._collect_stat_rows(
                    "latency.average",
                    self.latency_stat.update(self.delta - delta_start, self.delta),
                    emitted,
                )
                emitted = self._collect_stat_rows(
                    "outrate.rate", self.output_rate_stat.update(1, self.delta), emitted
                )
                if temp_bool:
                    temp_bool = False
            self.earliest_win_left += self.wa_ws_list[self.wa_ws_index][0]

            # Delete stale panes
            for pane_tau in list(self.panes.keys()):
                # which panes are safe to delete depends on the semantics!
                if self.semantics == common.Semantics.AMO:
                    pane_deletion_boundary = self.earliest_win_left
                elif self.semantics == common.Semantics.EO:
                    pane_deletion_boundary = tau - self.max_ws + self.min_wa
                else:
                    raise RuntimeError("Unknown semantics")
                if pane_tau < pane_deletion_boundary:
                    # Delete all stale panes
                    for k in self.panes[pane_tau]:
                        delta_before = self.delta
                        self.delta = self.delta + self.estimators.pane_delete_est()
                        emitted = self._collect_stat_rows(
                            "cpu",
                            self.cpu_stat.update(self.delta - delta_before, self.delta),
                            emitted,
                        )
                    emitted = self._collect_stat_rows(
                        "paneActive.count",
                        self.panes_stat.update(
                            -1 * len(self.panes[pane_tau]), self.delta
                        ),
                        emitted,
                    )
                    self.panes.pop(pane_tau)

        if pane_left not in self.panes:
            # Create a new pane if it does not exist yet
            self.panes[pane_left] = set()
            self.aggregated_panes[pane_left] = False

        if k not in self.panes[pane_left]:
            # Create a new key in the pane if it does not exist yet
            # The partial aggregate of this key has not been computed yet
            self.panes[pane_left].add(k)
            delta_before = self.delta
            self.delta = self.delta + self.estimators.pane_creation_est()
            emitted = self._collect_stat_rows(
                "cpu",
                self.cpu_stat.update(self.delta - delta_before, self.delta),
                emitted,
            )
        delta_before = self.delta
        self.delta = self.delta + self.estimators.pane_update_est()
        emitted = self._collect_stat_rows(
            "cpu", self.cpu_stat.update(self.delta - delta_before, self.delta), emitted
        )
        emitted = self._collect_stat_rows(
            "throughput.rate", self.throughput_stat.update(1, self.delta), emitted
        )

        self.earliest_pane_left = pane_left
        self.earliest_pane_right = pane_right
        self.earliest_win_left = win_left

        return emitted

    def __change(self, wa: int, ws: int) -> None:
        """Change the current (WA, WS) configuration to the specified values.

        For AMO semantics, schedules the reconfiguration at a future time.
        For EO semantics, applies the reconfiguration immediately.

        Args:
            wa: New Window Advance (WA).
            ws: New Window Size (WS).

        Raises:
            ValueError: If the specified (WA, WS) combination is not in the pre-defined set.
            RuntimeError: If trying to schedule reconfiguration before any tuples are processed.
        """

        if (wa, ws) not in self.wa_ws_list:
            raise ValueError(
                f"The specified (WA, WS) = ({wa}, {ws}) is not in the pre-defined set."
            )

        if self.semantics == common.Semantics.AMO:
            if self.max_tau is None:
                raise RuntimeError(
                    "Cannot schedule reconfiguration: no tuples have been processed yet."
                )
            self.reconfiguration_tau = self.max_tau + 1 * self.resolution
            self.next_wa_ws_index = self.wa_ws_list.index((wa, ws))
        elif self.semantics == common.Semantics.EO:
            self.wa_ws_index = self.wa_ws_list.index((wa, ws))
            # reset boundaries, they will be recomputed at next tuple
            self.earliest_win_left = None
            self.earliest_pane_left = None
            self.earliest_pane_right = None

    def metric_names(self) -> List[str]:
        """Get the list of metric names tracked by this simulator.

        Returns:
            List of metric identifiers.
        """
        return self._metric_names

    def step(self) -> Tuple[float, bool, common.OptionalMetrics]:
        """Process at most one row from the input file.

        Reads the next tuple from the input stream and processes it, handling
        both data tuples and configuration change commands.

        Returns:
            Tuple of (current_time, has_more, emitted_metrics) where:
                - current_time: The current simulation time (float)
                - has_more: Whether there are more rows to process (bool)
                - emitted_metrics: Updated statistics from this step (Emitted)
        """
        emitted = None

        # Lazy-open on first call (lets you call step() outside run())
        if self._reader is None and not self._done:
            self._fh = open(self.input_path, newline="")
            self._reader = csv.reader(self._fh)
            self._next_parts = next(self._reader, None)

        # EOF (or file was empty)
        if self._next_parts is None:
            if self._fh is not None:
                self._fh.close()
            self._fh = None
            self._reader = None
            self._done = True
            return (self.delta, False, emitted)

        parts = self._next_parts

        # Prefetch next row now, so we can return whether there is more
        self._next_parts = (
            next(self._reader, None) if self._reader is not None else None
        )
        has_more = self._next_parts is not None

        # Handle row
        t_type = self.extractor.extract_tuple_type(parts)
        if t_type == common.TupleType.NORMAL:
            tau = self.extractor.extract_time(parts)
            k = self.extractor.extract_key(parts)
            emitted = self.__process(tau, k)
        elif t_type == common.TupleType.CHANGE:
            wa, ws = self.extractor.extract_change_tuple_info(parts)
            self.__change(wa, ws)

        return (self.delta, has_more, emitted)

    def finalize(self) -> Tuple[float, bool, common.OptionalMetrics]:
        """Finalize statistics collection after all tuples are processed.

        Completes all metrics with a final update.

        Returns:
            Tuple of (current_time, has_more, emitted_metrics).
        """
        emitted = None

        # Finalize stats (called by PipelineSimulator.run())
        emitted = self._collect_stat_rows(
            "throughput.rate",
            self.throughput_stat.finalize(self.delta + 1 * self.resolution),
            emitted,
        )
        emitted = self._collect_stat_rows(
            "latency.average",
            self.latency_stat.finalize(self.delta + 1 * self.resolution),
            emitted,
        )
        emitted = self._collect_stat_rows(
            "paneActive.count",
            self.panes_stat.finalize(self.delta + 1 * self.resolution),
            emitted,
        )
        emitted = self._collect_stat_rows(
            "cpu", self.cpu_stat.finalize(self.delta + 1 * self.resolution), emitted
        )
        emitted = self._collect_stat_rows(
            "outrate.rate",
            self.output_rate_stat.finalize(self.delta + 1 * self.resolution),
            emitted,
        )

        return (self.delta, False, emitted)

    def run(self) -> None:
        """Process all tuples from the input file and finalize statistics.

        Standalone method that processes the entire stream in one call.
        """
        while True:
            _, has_more, _ = self.step()
            if not has_more:
                break
        self.finalize()

