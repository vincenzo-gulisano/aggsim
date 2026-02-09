from typing import Final, Optional, List, Tuple, Callable, Any
from enum import Enum

INJECTION_RATE_STAT_NAME: Final[str] = "injectionrate.rate"

# Define a shared type alias (optional but keeps signatures readable)

Metrics = List[List[Tuple[int, int]]]
OptionalMetrics = Optional[Metrics]


class Semantics(Enum):
    AMO = "AMO"  # At-Most-Once semantics
    EO = "EO"  # Exactly-Once semantics


class TupleType(Enum):
    NORMAL = "NORMAL"
    CHANGE = "CHANGE"


# Types / callables for clarity
# takes parsed CSV parts (List[str]), returns event time (e.g., epoch ms)
extract_time_fn = Callable[[List[str]], int]
# takes parsed CSV parts (List[str]), returns a key (hashable)
extract_key_fn = Callable[[List[str]], Any]
# takes parsed CSV parts (List[str]), returns a tuple type (e.g., NORMAL or CHANGE)
extract_tuple_type_fn = Callable[[List[str]], TupleType]
# takes parsed CSV parts (List[str]), returns a window advance and size for a change tuple
extract_change_tuple_info = Callable[[List[str]], Tuple[int, int]]

# returns a (possibly random) cost estimate
estimator_fn = Callable[[], float]


# ========================
# Extractor Functions
# ========================

class ExtractorFunctions:
    """Encapsulates all tuple extraction functions.

    Provides a unified interface for extracting different fields from raw input lines.
    """

    def __init__(
        self,
        extract_time: extract_time_fn,
        extract_key: extract_key_fn,
        extract_tuple_type: extract_tuple_type_fn,
        extract_change_tuple_info: extract_change_tuple_info,
    ) -> None:
        """Initialize the extractor with functions for each field type.

        Args:
            extract_time: Function that extracts event time from a raw line.
            extract_key: Function that extracts the key from a raw line.
            extract_tuple_type: Function that extracts the tuple type from a raw line.
            extract_change_tuple_info: Function that extracts window advance and size from a change tuple.
        """
        self.extract_time = extract_time
        self.extract_key = extract_key
        self.extract_tuple_type = extract_tuple_type
        self.extract_change_tuple_info = extract_change_tuple_info


# ========================
# Estimator Functions
# ========================

class EstimatorFunctions:
    """Encapsulates all cost estimator functions.

    Provides a unified interface for estimating costs of various operations
    in the streaming pipeline.
    """

    def __init__(
        self,
        pane_creation_est: estimator_fn,
        pane_update_est: estimator_fn,
        pane_aggregation_est: estimator_fn,
        pane_merge_est: estimator_fn,
        pane_delete_est: estimator_fn,
        tuple_sending_est: estimator_fn,
    ) -> None:
        """Initialize the estimator with functions for each operation type.

        Args:
            pane_creation_est: Function estimating cost of pane creation.
            pane_update_est: Function estimating cost of pane update.
            pane_aggregation_est: Function estimating cost of pane aggregation.
            pane_merge_est: Function estimating cost of pane merge.
            pane_delete_est: Function estimating cost of pane deletion.
            tuple_sending_est: Function estimating cost of tuple sending.
        """
        self.pane_creation_est = pane_creation_est
        self.pane_update_est = pane_update_est
        self.pane_aggregation_est = pane_aggregation_est
        self.pane_merge_est = pane_merge_est
        self.pane_delete_est = pane_delete_est
        self.tuple_sending_est = tuple_sending_est
