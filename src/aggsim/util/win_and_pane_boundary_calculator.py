from math import ceil
from typing import Iterable, List, Tuple


class WindowAndPaneBoundaryCalculator:
    """
    A utility class to compute window and pane boundaries based on combinations
    of window advances (WA) and window sizes (WS).

    Assumptions:
    - All WA and WS values are strictly positive.
    - For every pair (WA, WS), WA <= WS.
    - Any given ts used in boundary calculations satisfies ts > WS.
    """

    def __init__(self, was: Iterable[int], wss: Iterable[int]) -> None:
        """
        Constructs a WindowAndPaneBoundaryCalculator with all combinations of the given WAs and WSs.
        Each combination is stored as a tuple (WA, WS).
        """
        self.wa_ws_pairs: List[Tuple[int, int]] = []
        for wa in was:
            for ws in wss:
                if wa > 0 and ws > 0 and wa <= ws:
                    self.wa_ws_pairs.append((int(wa), int(ws)))

    def _compute_number_of_contributing_wins(self, ts: int, wa: int, ws: int) -> int:
        """
        Computes Omega (Ω) - the number of windows that a tuple with given ts falls into.
        """
        return int(
            ceil(ws / wa)
            if (ts % wa) < (ws % wa) or (ws % wa) == 0
            else ceil(ws / wa) - 1
        )

    def _compute_pane_left_boundary(self, ts: int, wa: int, ws: int, omega: int) -> int:
        return max((ts // wa) * wa, ((ts // wa) - omega) * wa + ws)

    def _compute_pane_right_boundary(
        self, ts: int, wa: int, ws: int, omega: int
    ) -> int:
        return min(((ts // wa) - (omega - 1)) * wa + ws, ((ts // wa) + 1) * wa)

    def get_win_left_boundary(self, ts: int, i: int) -> int:
        """Get the left boundary of the window for the i-th (WA, WS) pair."""
        wa = self.wa_ws_pairs[i][0]
        ws = self.wa_ws_pairs[i][1]
        omega = self._compute_number_of_contributing_wins(ts, wa, ws)
        return max(0, ((ts // wa) - (omega - 1)) * wa)

    def get_pane_boundaries_eo(self, ts: int) -> Tuple[int, int]:
        """
        Returns the strictest pane boundaries for a given ts across all (WA, WS) combinations.
        The start is the latest of all left boundaries, and the end is the earliest of all right boundaries.
        """
        left_boundary = -(2**63)  # Java Long.MIN_VALUE
        right_boundary = (2**63) - 1  # Java Long.MAX_VALUE

        for pair in self.wa_ws_pairs:
            wa = pair[0]
            ws = pair[1]
            omega = self._compute_number_of_contributing_wins(ts, wa, ws)

            current_left = self._compute_pane_left_boundary(ts, wa, ws, omega)
            if current_left > left_boundary:
                left_boundary = current_left
                if right_boundary - left_boundary == 1:
                    break

            current_right = self._compute_pane_right_boundary(ts, wa, ws, omega)
            if current_right < right_boundary:
                right_boundary = current_right
                if right_boundary - left_boundary == 1:
                    break

        if left_boundary > right_boundary:
            raise ValueError("Invalid pane: left boundary exceeds right boundary")

        return (left_boundary, right_boundary)

    def get_earliest_boundaries_eo(self, ts: int, i: int) -> Tuple[int, int, int]:
        pane_boundaries = self.get_pane_boundaries_eo(ts)
        return (
            self.get_win_left_boundary(ts, i),
            pane_boundaries[0],
            pane_boundaries[1],
        )

    def get_pane_boundaries_amo(self, ts: int, i: int) -> Tuple[int, int]:
        """Get the pane boundaries for the i-th (WA, WS) pair in AMO mode."""
        wa = self.wa_ws_pairs[i][0]
        ws = self.wa_ws_pairs[i][1]
        omega = self._compute_number_of_contributing_wins(ts, wa, ws)

        return (
            self._compute_pane_left_boundary(ts, wa, ws, omega),
            self._compute_pane_right_boundary(ts, wa, ws, omega),
        )

    def get_earliest_boundaries_amo(self, ts: int, i: int) -> Tuple[int, int, int]:
        pane_boundaries = self.get_pane_boundaries_amo(ts, i)
        return (
            self.get_win_left_boundary(ts, i),
            pane_boundaries[0],
            pane_boundaries[1],
        )
