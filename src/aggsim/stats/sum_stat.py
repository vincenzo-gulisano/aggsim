"""
SumStat: Time-windowed summation statistics.

This module provides a class for collecting and aggregating sum values over
fixed time intervals, with optional persistence to CSV files.
"""

from __future__ import annotations

from typing import Optional

from base_stat import BaseStat, Rows


class SumStat(BaseStat):
    """
    Collects and aggregates sum values over fixed time intervals.

    Attributes:
        file_path: Path where CSV output will be written
        resolution: Time window size for aggregation (timestamps are assumed to be in milliseconds)
        missing_value: Default value for uninitialized windows
        mul_factor: Multiplication factor applied to output values
        reset: Whether to reset sum after each interval
        write_to_disk: Whether to persist output to CSV
    """

    def __init__(
        self,
        file_path: str,
        resolution: int,
        missing_value: int,
        mul_factor: float = 1,
        reset: bool = True,
        write_to_disk: bool = True,
    ) -> None:
        """
        Initialize the SumStat aggregator.

        Args:
            file_path: Path to output CSV file
            resolution: Time interval for aggregation windows (timestamps are in milliseconds)
            missing_value: Value representing missing/uninitialized data
            mul_factor: Scaling factor for output values (default: 1)
            reset: Whether to reset sum after emitting each window (default: True)
            write_to_disk: Whether to write results to CSV (default: True)
        """
        super().__init__(file_path, resolution, missing_value, mul_factor, write_to_disk)
        self.reset: bool = reset
        self.sum: int = self.missing_value

    def _process_value(self, v: int) -> None:
        """
        Add a value to the current window's sum.

        Args:
            v: Value to add
        """
        # Initialize to 0 if this is the first value in the window
        if self.sum == self.missing_value:
            self.sum = 0
        self.sum += v

    def _calculate_value(self) -> int:
        """
        Return the current window's sum.

        Returns:
            The sum or missing_value if uninitialized
        """
        return int(self.sum * self.mul_factor)

    def _reset_state(self) -> None:
        """Reset sum to missing_value if reset is enabled."""
        if self.reset:
            self.sum = self.missing_value
