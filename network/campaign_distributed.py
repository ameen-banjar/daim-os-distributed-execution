#!/usr/bin/env python3
"""Distributed/versioned mode: 30-repetition statistical campaign for Paper
2's evidence gate ("At least 30 randomised repetitions per condition ...
median, p95, p99, maximum, and bootstrap intervals").

distributed_prototype_gate.py's main() already *is* one complete, independent
repetition: fresh Mininet/cluster bring-up, G1,G2,G3,G4,G6,G8,G5 in that
fixed order (G8 before G5 -- G5 permanently kills node4), full teardown, and
its own timestamped raw_events/gate_results/manifest evidence triple. This
script does not reimplement any of that; it invokes the gate script as a
subprocess REPETITIONS times (a fresh Python process per repetition avoids
the gate script's module-level RUN_TIMESTAMP being computed once and reused
across repetitions, which would make every repetition overwrite the same
output files), each with its own seed, and aggregates the per-repetition
gate_results.json files this produces.

Condition order is NOT reshuffled between repetitions (unlike Paper 1's
stage2_full_compare_paired.py convention) -- G8-before-G5 is a correctness
requirement, not a style choice, and reordering already-debugged gate logic
risks reintroducing the exact class of bugs this project's own gate report
documents having to diagnose. What *is* independent per repetition is the
entire cluster: fresh processes, fresh Mininet/OVS state, a fresh seed.
"""
import hashlib
import json
import os
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GATE_SCRIPT = ROOT / "network" / "distributed_prototype_gate.py"
RESULTS_DIR = ROOT / "results/network/distributed"
RUN_TIMESTAMP = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
RUN_ID = f"paper2-campaign-distributed-{RUN_TIMESTAMP}"

BASE_SEED = int(os.environ.get("DAIM_CAMPAIGN_SEED", "20260810"))
REPETITIONS = int(os.environ.get("DAIM_CAMPAIGN_REPETITIONS", "30"))

CONDITIONS = ["G1", "G2", "G3", "G4", "G5", "G6", "G8"]


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


def run_one_repetition(rep, seed):
    """Invokes distributed_prototype_gate.py --include-g8 as a fresh
    subprocess. Its final stdout line block is the manifest JSON it always
    prints (see distributed_prototype_gate.py's main()) -- parsed here
    rather than re-globbing RESULTS_DIR by mtime, so there is no race and
    no dependency on filesystem timing."""
    env = dict(os.environ)
    env["DAIM_GATE_SEED"] = str(seed)
    proc = subprocess.run(
        [sys.executable, str(GATE_SCRIPT), "--include-g8"],
        capture_output=True, text=True, env=env,
    )
    stdout_lines = proc.stdout.splitlines()
    brace_starts = [i for i, line in enumerate(stdout_lines) if line == "{"]
    if not brace_starts:
        return {
            "rep": rep, "seed": seed, "returncode": proc.returncode,
            "manifest": None, "gate_results": None,
            "parse_error": "no manifest JSON found in stdout",
            "stdout_tail": "\n".join(stdout_lines[-40:]),
            "stderr_tail": "\n".join(proc.stderr.splitlines()[-40:]),
        }
    manifest = json.loads("\n".join(stdout_lines[brace_starts[-1]:]))
    # gate_results filename follows the same run_timestamp as the manifest.
    gate_results_name = f"gate_results_{manifest['run_timestamp']}.json"
    gate_results_path = RESULTS_DIR / gate_results_name
    gate_results = json.loads(gate_results_path.read_text()) if gate_results_path.exists() else None
    return {
        "rep": rep, "seed": seed, "returncode": proc.returncode,
        "manifest": manifest, "gate_results": gate_results, "parse_error": None,
    }


def extract_metrics(gate_results):
    """Pulls the numeric series each condition contributes for cross-
    repetition aggregation. Only conditions with a meaningful continuous
    metric are extracted here; pass/fail alone is tracked for the rest."""
    metrics = {}
    g1 = gate_results.get("G1") or {}
    g1_lat = (g1.get("latency_summary_ms") or {}).get("median")
    if g1_lat is not None:
        metrics.setdefault("G1_propagation_latency_median_ms", []).append(g1_lat)
    g6 = gate_results.get("G6") or {}
    if g6.get("detection_time_s") is not None:
        metrics.setdefault("G6_detection_time_s", []).append(g6["detection_time_s"])
    if g6.get("reconvergence_time_s") is not None:
        metrics.setdefault("G6_reconvergence_time_s", []).append(g6["reconvergence_time_s"])
    return metrics


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    repetitions_data = []
    pass_by_condition = {c: [] for c in CONDITIONS}
    metric_series = {}

    for rep in range(REPETITIONS):
        seed = BASE_SEED + rep
        print(f"=== distributed campaign: repetition {rep+1}/{REPETITIONS} (seed={seed}) ===",
              flush=True)
        result = run_one_repetition(rep, seed)
        repetitions_data.append(result)
        gate_results = result.get("gate_results")
        if gate_results is None:
            print(f"  repetition {rep+1} produced no parsable gate_results "
                  f"(returncode={result['returncode']}) -- recorded as a failed repetition, not discarded",
                  flush=True)
            for c in CONDITIONS:
                pass_by_condition[c].append(False)
            continue
        for c in CONDITIONS:
            pass_by_condition[c].append(bool((gate_results.get(c) or {}).get("pass")))
        for name, values in extract_metrics(gate_results).items():
            metric_series.setdefault(name, []).extend(values)
        print(f"  repetition {rep+1}: " +
              ", ".join(f"{c}={'PASS' if pass_by_condition[c][-1] else 'FAIL'}" for c in CONDITIONS),
              flush=True)

    condition_summary = {
        c: {
            "repetitions": len(pass_by_condition[c]),
            "n_passed": sum(pass_by_condition[c]),
            "pass_rate": sum(pass_by_condition[c]) / len(pass_by_condition[c]) if pass_by_condition[c] else 0.0,
        }
        for c in CONDITIONS
    }
    metric_summary = {name: latency_summary(values, BASE_SEED) for name, values in metric_series.items()}

    campaign_path = RESULTS_DIR / f"campaign_distributed_{RUN_TIMESTAMP}.json"
    campaign_path.write_text(json.dumps({
        "run_id": RUN_ID,
        "run_timestamp": RUN_TIMESTAMP,
        "base_seed": BASE_SEED,
        "repetitions": REPETITIONS,
        "conditions": CONDITIONS,
        "condition_summary": condition_summary,
        "metric_summary": metric_summary,
    }, indent=2) + "\n")

    raw_path = RESULTS_DIR / f"campaign_distributed_raw_{RUN_TIMESTAMP}.json"
    raw_path.write_text(json.dumps(repetitions_data, indent=2) + "\n")

    manifest = {
        "run_id": RUN_ID,
        "run_timestamp": RUN_TIMESTAMP,
        "base_seed": BASE_SEED,
        "repetitions": REPETITIONS,
        "all_repetitions_all_conditions_passed": all(
            all(pass_by_condition[c]) for c in CONDITIONS
        ),
        "condition_pass_rate": {c: condition_summary[c]["pass_rate"] for c in CONDITIONS},
        "files": {
            str(p.relative_to(RESULTS_DIR)): checksum_file(p)
            for p in (campaign_path, raw_path)
        },
    }
    manifest_path = RESULTS_DIR / f"campaign_distributed_manifest_{RUN_TIMESTAMP}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    print(json.dumps(manifest, indent=2))
    print(json.dumps(condition_summary, indent=2))
    print(json.dumps(metric_summary, indent=2))


if __name__ == "__main__":
    main()
