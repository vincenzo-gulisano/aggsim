"""
BaseStat: Base class for time-windowed statistics aggregation.

This module provides an abstract base class for collecting and computing
statistics over fixed time intervals, with optional persistence to CSV files.
Subclasses implement specific aggregation strategies (sum, average, etc.).
"""

from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, TextIO

Row = Tuple[int, int]
Rows = List[Row]


class BaseStat(ABC):
    """
    Abstract base class for time-windowed statistics aggregation.

    Subclasses should implement _calculate_value() to define the aggregation
    logic (sum, average, etc.), _reset_state() to define reset behavior, and
    _process_value() to define how values are accumulated.

    Attributes:
        file_path: Path where CSV output will be written
        resolution: Time window size for aggregation
        missing_value: Default value for empty/uninitialized windows
        mul_factor: Multiplication factor applied to output values
        write_to_disk: Whether to persist output to CSV
    """

    def __init__(
        self,
        file_path: str,
        resolution: int,
        missing_value: int,
        mul_factor: float = 1,
        write_to_disk: bool = True,
    ) -> None:
        """
        Initialize the statistics aggregator.

        Args:
            file_path: Path to output CSV file
            resolution: Time interval for aggregation windows (in time units)
            missing_value: Value for windows with no data
            mul_factor: Scaling factor for output values (default: 1)
            write_to_disk: Whether to write results to CSV (default: True)
        """
        self.file_path: str = file_path
        self.resolution: int = resolution
        self.missing_value: int = missing_value
        self.mul_factor: float = mul_factor
        self.write_to_disk: bool = write_to_disk
        self.time: Optional[int] = None

        # Open CSV once and write header
        self.file: Optional[TextIO] = None
        self.writer: Optional[csv.writer] = None
        if self.write_to_disk:
            self.file = open(self.file_path, mode="w", newline="")
            self.writer = csv.writer(self.file)
            self.writer.writerow(["ts", "v"])

    @abstractmethod
    def _calculate_value(self) -> int:
        """
        Calculate the aggregated value for the current window.

        Subclasses must implement this to define their aggregation logic.

        Returns:
            The calculated value or missing_value if no data
        """
        pass

    @abstractmethod
    def _reset_state(self) -> None:
        """
        Reset the aggregation state for a new window.

        Subclasses must implement this to define what gets reset.
        """
        pass

    @abstractmethod
    def _process_value(self, v: int) -> None:
        """
        Process and store a value in the current window.

        Subclasses must implement this to define how values are accumulated.

        Args:
            v: Value to process
        """
        pass

    def _emit_row(self, ts: int) -> Row:
        """
        Emit a row for the given timestamp.

        Args:
            ts: Timestamp to emit

        Returns:
            Tuple of (timestamp, scaled_value)
        """
        v = self._calculate_value()
        row = (ts, v)
        if self.write_to_disk and self.writer is not None:
            self.writer.writerow([row[0], row[1]])
        return row

    def initialize(self, start_time: int) -> None:
        """
        Initialize the aggregator with a starting timestamp.

        Args:
            start_time: Initial timestamp (will be normalized by resolution)
        """
        self.time = start_time // self.resolution

    def update(self, v: int, t: int) -> Optional[Rows]:
        """
        Update aggregation with a new value at the given time.

        Emits rows for all complete time windows between the last
        update and the current time.

        Args:
            v: Value to add to the current window
            t: Timestamp of the value (in time units)

        Returns:
            List of emitted rows as (timestamp, value) tuples, or None if none emitted

        Raises:
            ValueError: If initialize() has not been called before update()
        """
        # NOT CHECKING self.time is None here to save time, as we assume the caller will call initialize() before update().
        # if self.time is None:
        #     raise ValueError(
        #         f"{self.__class__.__name__} not initialized. "
        #         "Call 'initialize(start_time)' before 'update()'."
        #     )

        emitted = None
        t_res = t // self.resolution

        # Write all full intervals until reaching current t_res
        while self.time < t_res:
            if emitted is None:
                emitted = []
            emitted.append(self._emit_row(self.time))
            self._reset_state()
            self.time += 1

        # Process current value
        self._process_value(v)

        return emitted

    def finalize(self, t: int) -> Optional[Rows]:
        """
        Finalize aggregation and close the output file.

        Emits all remaining rows up to the final timestamp, then
        closes the CSV file if output was directed to disk.

        Args:
            t: Final timestamp (in time units)

        Returns:
            List of all remaining emitted rows as (timestamp, value) tuples, or None if none
        """
        emitted = None
        t_res = t // self.resolution

        while self.time <= t_res:
            if emitted is None:
                emitted = []
            emitted.append(self._emit_row(self.time))
            self._reset_state()
            self.time += 1

        if self.write_to_disk and self.file:
            self.file.flush()
            self.file.close()
            self.file = None
            self.writer = None

        return emitted
