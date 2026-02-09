"""Source simulator for processing event streams and tracking injection rates.

This module provides the SourceSimulator class which reads raw event data
from a CSV file, extracts timestamps, and maintains injection rate statistics.
"""

from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Iterator,
    TextIO,
)
from math import inf
import csv
import common
from sum_stat import SumStat

# TO DOs:
# 1) as of now, whether a tuple is to be processed or not (type 0) is hardcoded and for LinearRoad, this should be generalized
# 2) check the logic if statistics are not kept


class SourceSimulator:
    """Simulates event source by reading from CSV and tracking injection rates.

    Processes tuples from an input file, extracts their timestamps, and maintains
    running statistics about the injection rate of events into the system.
    """

    def __init__(
        self,
        input_path: str,
        output_folder: str,
        extractor: common.ExtractorFunctions,
        estimators: common.EstimatorFunctions,
        resolution: int,
        ts_offset: int,
    ) -> None:
        """Initialize the source simulator.

        Args:
            input_path: Path to the input CSV file containing raw events.
            output_folder: Directory where statistics files will be written.
            extractor: ExtractorFunctions instance for extracting fields from raw events.
            estimators: EstimatorFunctions instance containing cost estimators.
            resolution: Time window resolution for statistics aggregation.
            ts_offset: Time offset to apply to all extracted timestamps.
        """
        self.input_path: str = input_path
        self.output_folder: str = output_folder
        self.extractor: common.ExtractorFunctions = extractor
        self.estimators: common.EstimatorFunctions = estimators
        self.resolution: int = int(resolution)

        self.delta: float = -inf
        self.injection_rate_stat: SumStat = SumStat(
            self.output_folder + "/" + common.INJECTION_RATE_STAT_NAME + ".csv",
            self.resolution,
            0,
        )
        self.stats_initialized: bool = False
        self._metric_names: List[str] = [common.INJECTION_RATE_STAT_NAME]
        self._metric_idx: Dict[str, int] = {
            n: i for i, n in enumerate(self._metric_names)
        }

        self.ts_offset: int = ts_offset

        # Stream state for step()
        self._fh: Optional[TextIO] = None
        self._reader: Optional[Iterator[List[str]]] = None
        self._next_parts: Optional[List[str]] = None
        self._done: bool = False

    def _empty_emitted(self) -> common.OptionalMetrics:
        """Create an empty emitted statistics structure."""
        return [[] for _ in self._metric_names]

    def _close_stream(self) -> None:
        """Close the input file stream and reset reader state."""
        if self._fh is not None:
            self._fh.close()
        self._fh = None
        self._reader = None
        self._done = True

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

    def __process(self, tau: int) -> common.OptionalMetrics:
        """Process a single tuple with the given timestamp.

        Updates the current time (delta) and injection rate statistics.

        Args:
            tau: The timestamp of the tuple to process.

        Returns:
            Emitted statistics from this processing step.
        """
        emitted = None

        # Adjust tau with the provided offset
        tau = tau + self.ts_offset

        # Update the current time
        self.delta = max(self.delta, tau)

        if not self.stats_initialized:
            # Initialize stats with the time of the first tuple
            self.injection_rate_stat.initialize(self.delta)
            self.stats_initialized = True

        self.delta = self.delta + self.estimators.tuple_sending_est()

        # Update stats and collect emitted values
        emitted = self._collect_stat_rows(
            common.INJECTION_RATE_STAT_NAME,
            self.injection_rate_stat.update(1, self.delta),
            emitted,
        )
        return emitted

    # ----------
    # Public API
    # ----------

    def metric_names(self) -> List[str]:
        """Get the list of metric names tracked by this simulator.

        Returns:
            List of metric identifiers.
        """
        return self._metric_names

    def step(self) -> Tuple[float, bool, common.OptionalMetrics]:
        """Process at most one row from the input file.

        Reads the next tuple from the input stream, extracts its timestamp,
        and updates injection rate statistics.

        Returns:
            Tuple of (current_time, has_more, emitted_stats) where:
                - current_time: The current simulation time (float)
                - has_more: Whether there are more rows to process (bool)
                - emitted_stats: Updated statistics from this step (Emitted)
        """
        emitted = None

        # Lazy-open on first call (lets you call step() outside run())
        if self._reader is None and not self._done:
            self._fh = open(self.input_path, newline="")
            self._reader = csv.reader(self._fh)
            self._next_parts = next(self._reader, None)

        # EOF (or file was empty)
        if self._next_parts is None:
            self._close_stream()
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
            emitted = self.__process(tau)

        return (self.delta, has_more, emitted)

    def finalize(self) -> Tuple[float, bool, common.OptionalMetrics]:
        """Finalize statistics collection after all tuples are processed.

        Completes the injection rate statistics with a final update.

        Returns:
            Tuple of (current_time, has_more, emitted_stats).
        """
        emitted = None

        # Finalize stats (called by PipelineSimulator.run())
        emitted = self._collect_stat_rows(
            common.INJECTION_RATE_STAT_NAME,
            self.injection_rate_stat.finalize(self.delta + 1 * self.resolution),
            emitted,
        )

        return (self.delta, False, emitted)

    def run(self) -> None:
        """Process all tuples from the input file and finalize statistics.

        Standalone method that processes the entire stream in one call.
        """
        try:
            while True:
                _, has_more, _ = self.step()
                if not has_more:
                    break
        finally:
            self._close_stream()
            self.finalize()
