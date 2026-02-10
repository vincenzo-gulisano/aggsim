"""
AvgStat: Time-windowed average aggregation statistics.

This module provides a class for collecting and computing averages over
fixed time intervals, with optional persistence to CSV files.
"""

from .base_stat import BaseStat


class AvgStat(BaseStat):
    """
    Computes and aggregates average values over fixed time intervals.

    Attributes:
        file_path: Path where CSV output will be written
        resolution: Time window size for aggregation
        missing_value: Default value for empty windows (no values received)
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
        Initialize the average statistics aggregator.

        Args:
            file_path: Path to output CSV file
            resolution: Time interval for aggregation windows (in time units)
            missing_value: Value to use when a window has no data
            mul_factor: Scaling factor for output values (default: 1)
            write_to_disk: Whether to write results to CSV (default: True)
        """
        super().__init__(
            file_path, resolution, missing_value, mul_factor, write_to_disk
        )
        self.sum: int = 0
        self.count: int = 0

    def _process_value(self, v: int) -> None:
        """
        Add a value to the current window's average.

        Args:
            v: Value to add
        """
        self.sum += v
        self.count += 1

    def _calculate_value(self) -> int:
        """
        Return the current window's average.

        Returns:
            The average value or missing_value if no data received
        """
        return (
            int((self.sum / self.count) * self.mul_factor)
            if self.count > 0
            else self.missing_value
        )

    def _reset_state(self) -> None:
        """Reset sum and count for a new window."""
        self.sum = 0
        self.count = 0
