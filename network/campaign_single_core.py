#!/usr/bin/env python3
"""Single-Core Paper 1 baseline: 30-repetition statistical campaign for
Paper 2's evidence gate ("Single-Core Paper 1 ... mode as an explicit
baseline"). Reuses packetin_latency_breakdown.py's run_trial() unmodified
-- the existing, already-proven methodology for capturing a single
nanosecond-precision Packet-In decision from daim_bridge_controller.py
(the actual single-Core harness: same daim_core_bridge.py/DaimCoreBridge
ctypes path Paper 1's own manuscript measures, confirmed identical to the
frozen copy under 01_DAIM_OS_Implementation/ by diff, never modified here).

Scope call, stated explicitly rather than silently assumed: this reuses
packetin_latency_breakdown.py's single-switch/3-host topology, not Paper
2's 4-switch h1-s1-s2-s3-s4-h4 chain. A single Core's decision latency for
one switch does not depend on how many OTHER switches exist in the
topology (each Packet-In is handled independently, dispatched by dpid) --
what would change at larger N is aggregate load, which is a different
question (Paper 1's own stage2_full_compare*.py already covers proactive
per-switch install cost at N=8..64) from the per-decision latency this
campaign measures to contrast against the distributed and no-exchange
baselines' equivalent metrics. Building a new, unproven 4-switch variant
of the timing harness was rejected in favor of reusing what's already
validated -- documented here as a caveat, not hidden.

Mode: "process_per_rule" only (DAIM's default single-Core adapter -- the
ctypes-direct-call mode, matching how Paper 2's distributed nodes also call
into their C core directly rather than through a persistent side-channel).
"""
import hashlib
import json
import os
import random
import statistics
import time
from pathlib import Path

from mininet.log import setLogLevel

from packetin_latency_breakdown import run_trial

ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "results/network/distributed"
RUN_TIMESTAMP = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
RUN_ID = f"paper2-campaign-single-core-{RUN_TIMESTAMP}"

MODE = "process_per_rule"
BASE_SEED = int(os.environ.get("DAIM_CAMPAIGN_SEED", "20260810"))
REPETITIONS = int(os.environ.get("DAIM_CAMPAIGN_REPETITIONS", "30"))


def percentile(values, q):
    ordered = sorted(values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def bootstrap_median_ci(values, seed, repetitions=2000):
    if not values:
        return None
    rng = random.Random(seed)
    medians = []
    for _ in range(repetitions):
        sample = [values[rng.randrange(len(values))] for _ in values]
        medians.append(statistics.median(sample))
    return [percentile(medians, 0.025), percentile(medians, 0.975)]


def latency_summary(values, seed):
    if not values:
        return {"n": 0, "median": None, "p95": None, "p99": None,
                "maximum": None, "median_bootstrap_95_ci": None}
    return {
        "n": len(values),
        "median": statistics.median(values),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "maximum": max(values),
        "median_bootstrap_95_ci": bootstrap_median_ci(values, seed),
    }


def checksum_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    setLogLevel("warning")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    total_ns = []
    core_decision_ns = []
    core_install_ns = []
    confirmed_flags = []

    for rep in range(1, REPETITIONS + 1):
        print(f"=== single-core campaign: repetition {rep}/{REPETITIONS} ===", flush=True)
        row = run_trial(MODE, rep, persistent_port=None)
        rows.append(row)
        confirmed_flags.append(bool(row.get("confirmed")))
        if row.get("t_confirmed_ns") is not None and row.get("t_dispatch_enter_ns") is not None:
            total_ns.append(row["t_confirmed_ns"] - row["t_dispatch_enter_ns"])
        if row.get("c_decision_done_ns") is not None and row.get("c_entry_ns") is not None:
            core_decision_ns.append(row["c_decision_done_ns"] - row["c_entry_ns"])
        if row.get("c_install_done_ns") is not None and row.get("c_table_write_done_ns") is not None:
            core_install_ns.append(row["c_install_done_ns"] - row["c_table_write_done_ns"])
        print(json.dumps(row, indent=2), flush=True)

    def to_ms(values):
        return [v / 1e6 for v in values]

    metric_summary = {
        "total_dispatch_to_confirmed_ms": latency_summary(to_ms(total_ns), BASE_SEED),
        "core_decision_ms": latency_summary(to_ms(core_decision_ns), BASE_SEED),
        "core_table_write_to_install_ms": latency_summary(to_ms(core_install_ns), BASE_SEED),
    }

    raw_path = RESULTS_DIR / f"campaign_single_core_raw_{RUN_TIMESTAMP}.json"
    raw_path.write_text(json.dumps(rows, indent=2) + "\n")

    campaign_path = RESULTS_DIR / f"campaign_single_core_{RUN_TIMESTAMP}.json"
    campaign_path.write_text(json.dumps({
        "run_id": RUN_ID,
        "run_timestamp": RUN_TIMESTAMP,
        "mode": MODE,
        "base_seed": BASE_SEED,
        "repetitions": REPETITIONS,
        "n_confirmed": sum(confirmed_flags),
        "confirmed_rate": sum(confirmed_flags) / len(confirmed_flags) if confirmed_flags else 0.0,
        "topology_note": "single-switch/3-host (packetin_latency_breakdown.py's SingleSwitchTopo), "
                          "not Paper 2's 4-switch chain -- see module docstring for why",
        "metric_summary": metric_summary,
    }, indent=2) + "\n")

    manifest = {
        "run_id": RUN_ID,
        "run_timestamp": RUN_TIMESTAMP,
        "mode": MODE,
        "base_seed": BASE_SEED,
        "repetitions": REPETITIONS,
        "all_confirmed": all(confirmed_flags),
        "files": {
            str(p.relative_to(RESULTS_DIR)): checksum_file(p)
            for p in (campaign_path, raw_path)
        },
    }
    manifest_path = RESULTS_DIR / f"campaign_single_core_manifest_{RUN_TIMESTAMP}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    print(json.dumps(manifest, indent=2))
    print(json.dumps(metric_summary, indent=2))


if __name__ == "__main__":
    main()
