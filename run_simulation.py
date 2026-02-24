import argparse
import os
import yaml
import pandas as pd
from tqdm import tqdm

from models.workload import Workload
from models.system_spec import SystemSpec
from models.scheduler import FIFOScheduler, EasyBackfillScheduler
from models.simulation_engine import SimulationEngine


DEFAULT_INPUT = "uploads/pbs_Sep_detail.csv"
DEFAULT_SYSTEM_SPEC = "uploads/system_spec.yaml"
OUTPUT_DIR = "output"


def parse_args():
    parser = argparse.ArgumentParser(
        description="PBS Trace Replay Simulation"
    )

    parser.add_argument(
        "--input",
        type=str,
        default=DEFAULT_INPUT,
        help="Input PBS CSV file"
    )

    parser.add_argument(
        "--system",
        type=str,
        default=DEFAULT_SYSTEM_SPEC,
        help="System spec YAML file"
    )

    parser.add_argument(
        "--scheduler",
        type=str,
        choices=["fifo", "backfill"],
        default="backfill",  # default = backfill
        help="Scheduling algorithm"
    )

    return parser.parse_args()


def load_system_spec(path):
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)["system"]

    return SystemSpec(
        total_ncpus=int(cfg["total_ncpus"]),
        total_ngpus=int(cfg.get("total_ngpus", 0))
    )


def build_base_filename(input_path):
    return os.path.splitext(os.path.basename(input_path))[0]


def main():

    args = parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Loading workload...")
    jobs = Workload.from_csv(args.input)

    print("Loading system specification...")
    system = load_system_spec(args.system)

    # Scheduler selection
    if args.scheduler == "fifo":
        scheduler = FIFOScheduler()
    else:
        scheduler = EasyBackfillScheduler()

    print(f"Running simulation ({args.scheduler})...")
    engine = SimulationEngine(jobs, system, scheduler)

    engine.initialize()

    total_events_estimate = len(jobs) * 2

    with tqdm(total=total_events_estimate, desc="Simulating", unit="event") as pbar:
        while not engine.is_finished():
            engine.step()
            pbar.update(1)

    results = engine.jobs

    base_filename = build_base_filename(args.input)

    # -------------------------------------------------
    # Detailed Output
    # -------------------------------------------------
    detail_output_path = os.path.join(
        OUTPUT_DIR,
        f"{base_filename}_{args.scheduler}_out.csv"
    )

    df = pd.DataFrame([{
        "job_id": j.job_id,
        "arrival_time": j.arrival_time,
        "start_time": j.start_time,
        "end_time": j.end_time,
        "waiting_time": j.waiting_time,
        "execution_time": j.walltime,
        "turnaround_time": j.turnaround_time
    } for j in results])

    df.to_csv(detail_output_path, index=False)

    # -------------------------------------------------
    # Summary Metrics
    # -------------------------------------------------
    summary = {
        "scheduler": args.scheduler,
        "total_jobs": len(df),
        "avg_waiting_time": df["waiting_time"].mean(),
        "p95_waiting_time": df["waiting_time"].quantile(0.95),
        "avg_turnaround_time": df["turnaround_time"].mean(),
        "p95_turnaround_time": df["turnaround_time"].quantile(0.95),
        "avg_execution_time": df["execution_time"].mean(),
        "makespan": df["end_time"].max() - df["arrival_time"].min(),
    }

    summary_df = pd.DataFrame([summary])

    summary_output_path = os.path.join(
        OUTPUT_DIR,
        f"{base_filename}_{args.scheduler}_summary.csv"
    )

    summary_df.to_csv(summary_output_path, index=False)

    # -------------------------------------------------
    # Print Summary
    # -------------------------------------------------
    print("\n========== Simulation Summary ==========")
    for k, v in summary.items():
        print(f"{k:25}: {v}")
    print("========================================")

    print(f"\nDetail Output : {detail_output_path}")
    print(f"Summary Output: {summary_output_path}")
    print("Simulation completed.")


if __name__ == "__main__":
    main()