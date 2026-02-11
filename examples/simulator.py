"""Entry point for running the pipeline simulator.

This module orchestrates the creation and execution of the streaming simulator
with source and aggregate components.
"""

import time
import argparse
import random
import os
import sys
from typing import Set

# Ensure the package in `src/` is importable when running the example in-place
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from aggsim.util import common
from aggsim.simulator.source_simulator import SourceSimulator
from aggsim.simulator.aggregate_simulator import AggregateSimulator
from aggsim.simulator.pipeline_simulator import PipelineSimulator


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Run the Simulator over an input stream file."
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to the input stream file (CSV lines).",
    )
    parser.add_argument(
        "--output-folder",
        "-o",
        required=True,
        help="Path to the output folder where simulation results will be written.",
    )
    parser.add_argument(
        "--semantics",
        "-s",
        choices=["AMO", "EO"],
        default="EO",
        help="Processing semantics.",
    )
    parser.add_argument(
        "--was",
        required=True,
        help="Comma-separated Window Advances (e.g., '1000,2000').",
    )
    parser.add_argument(
        "--wss",
        required=True,
        help="Comma-separated Window Sizes (e.g., '5000,10000').",
    )
    parser.add_argument(
        "--wa-ws-index",
        type=int,
        default=0,
        help="Index of (WA,WS) combination to use.",
    )
    parser.add_argument(
        "--resolution",
        type=int,
        default=1,
        help="Time resolution for internal quantization.",
    )
    # --- Estimator parameters ---
    parser.add_argument(
        "--pane-creation",
        type=float,
        required=True,
        help="Base time (in seconds) for pane creation.",
    )
    parser.add_argument(
        "--pane-update",
        type=float,
        required=True,
        help="Base time (in seconds) for pane update.",
    )
    parser.add_argument(
        "--pane-aggregation",
        type=float,
        required=True,
        help="Base time (in seconds) for pane aggregation.",
    )
    parser.add_argument(
        "--pane-merge",
        type=float,
        required=True,
        help="Base time (in seconds) for pane merge.",
    )
    parser.add_argument(
        "--pane-delete",
        type=float,
        required=True,
        help="Base time (in seconds) for pane delete.",
    )
    parser.add_argument(
        "--tuple-sending",
        type=float,
        required=True,
        help="Base time (in seconds) for tuple sending.",
    )

    args = parser.parse_args()

    first_tuple_offset = 0

    # Simple CSV extractors: work with parsed CSV parts (List[str])
    def extract_time(parts: list) -> int:
        return int(parts[1])

    def extract_key(parts: list):
        return int(parts[2])

    def extract_tuple_type(parts: list) -> common.TupleType:
        # Map numeric codes to tuple types: 0 -> NORMAL, 1 -> CHANGE
        # Any unknown code will be treated as IGNORE.
        try:
            t_type = int(parts[0])
        except Exception:
            return common.TupleType.IGNORE
        if t_type == 0:
            return common.TupleType.NORMAL
        if t_type == 2:
            return common.TupleType.CHANGE
        return common.TupleType.IGNORE

    def extract_change_tuple_info(parts: list) -> tuple:
        # Extract window advance and size from parts[2] and parts[3]
        return (int(parts[2]) * args.resolution, int(parts[3]) * args.resolution)

    # Parse WA/WS sets
    was_set = {int(x) * args.resolution for x in args.was.split(",") if x.strip()}
    wss_set = {int(x) * args.resolution for x in args.wss.split(",") if x.strip()}

    # --- Estimator factory ---

    def make_estimator(base_value: float):
        """Return a callable producing samples within ±5% of base_value."""
        return lambda: random.uniform(base_value * 0.95, base_value * 1.05)

    # --- Create estimators using input values ---
    pane_creation_est = make_estimator(args.pane_creation)
    pane_update_est = make_estimator(args.pane_update)
    pane_aggregation_est = make_estimator(args.pane_aggregation)
    pane_merge_est = make_estimator(args.pane_merge)
    pane_delete_est = make_estimator(args.pane_delete)
    tuple_sending_est = make_estimator(args.tuple_sending)

    semantics = common.Semantics[args.semantics]

    extractor = common.ExtractorFunctions(
        extract_time=extract_time,
        extract_key=extract_key,
        extract_tuple_type=extract_tuple_type,
        extract_change_tuple_info=extract_change_tuple_info,
    )

    estimators = common.EstimatorFunctions(
        pane_creation_est=pane_creation_est,
        pane_update_est=pane_update_est,
        pane_aggregation_est=pane_aggregation_est,
        pane_merge_est=pane_merge_est,
        pane_delete_est=pane_delete_est,
        tuple_sending_est=tuple_sending_est,
    )

    # Enable or disable writing metric CSVs to disk
    write_to_disk = True  # change to False to keep metrics in-memory only

    agg_sim = AggregateSimulator(
        input_path=args.input,
        output_folder=args.output_folder,
        extractor=extractor,
        estimators=estimators,
        was=was_set,
        wss=wss_set,
        wa_ws_index=args.wa_ws_index,
        resolution=args.resolution,
        semantics=semantics,
        ts_offset=first_tuple_offset,
        write_to_disk=write_to_disk,
    )

    source_sim = SourceSimulator(
        input_path=args.input,
        output_folder=args.output_folder,
        extractor=extractor,
        estimators=estimators,
        resolution=args.resolution,
        ts_offset=first_tuple_offset,
        write_to_disk=write_to_disk,
    )

    pipeline = PipelineSimulator(source_sim, agg_sim)

    start_time = time.perf_counter()
    pipeline.run(print_every=100000)
    end_time = time.perf_counter()

    print(f"Simulation completed in {(end_time - start_time):.3f} seconds.")
