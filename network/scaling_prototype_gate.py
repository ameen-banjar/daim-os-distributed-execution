#!/usr/bin/env python3
"""G7 (node-count scaling) gate: the same distributed DAIM-OS mechanism
exercised by distributed_prototype_gate.py's G1 (independent-process
propagation), generalized from a fixed 4-node chain to a swept sequence of
chain sizes -- h1-s1-...-sN-hN, full-mesh peer state exchange (node i
dials node j only for i<j, same non-redundant policy as G1-G6; see the
long comment on start_cluster in distributed_prototype_gate.py for why).

For each N in NODE_COUNTS, this module builds a fresh N-node cluster (its
own Mininet topology, torn down and rebuilt for the next N -- not one
topology growing in place), measures:

  convergence_s   wall-clock from a single barrier release -- the instant
                   every node process is already running and OpenFlow-ready
                   AND every node has been simultaneously signalled to start
                   dialing its peers for the first time -- to every node
                   reporting a "snapshot_received" lifecycle event for all
                   N-1 of its peers, counting only events recorded at or
                   after that release. Earlier revisions started the clock
                   after build_cluster()'s staged, one-node-every-0.5s
                   launch without gating when dialing itself began, so an
                   early-spawned node's connection to an already-running
                   peer could fully converge *during* that staged launch,
                   before the clock started -- silently excluding real
                   protocol work from the measurement and understating
                   convergence time by an amount that grew with the staged
                   launch's own duration, i.e. with N. The barrier (see
                   daim_distributed_controller.py's DAIM_PEER_BARRIER_FILE)
                   makes "peers start dialing" and "the clock starts" the
                   same instant instead of two independently-timed ones, so
                   the measurement is of the mesh's actual convergence work,
                   not of how much of that work happened to fit inside the
                   harness's own launch staggering.
  propagation      the highest-numbered host pings host 1, forcing a fresh
                   table-miss each repetition; every other node's "apply"
                   event for that origin is timed learned_at_ns ->
                   applied_at_ns, exactly as G1 does at N=4.
  network overhead  connection count and total apply/lifecycle event
                   volume observed across the cluster, as a proxy -- the
                   C transport's own byte/message counters
                   (daim_peer_transport_get_stats) are not wired through
                   the ctypes boundary in daim_distributed_node.py, so
                   this does not claim byte-level throughput.

This is deliberately a *characterization* gate, not a single pass/fail
like G1-G6: "pass" per N means the cluster reached full mesh connectivity
and propagation was observed at every node that should have seen it: the
resulting curve across N is the actual research output. A failure at a
given N (including a resource-driven timeout on a constrained VM) is kept
as a data point, not retried into silence or dropped.

Every raw event from every node, at every N, is retained in one
run's raw_events_g7_<timestamp>.jsonl, tagged with n_nodes. A manifest
with the seed, environment, and a SHA-256 of every output file is written
alongside it, plus a scaling_summary table for direct use in figures.
"""
import argparse
import csv
import hashlib
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import OVSSwitch
from mininet.topo import Topo

ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_APP = ROOT / "network/daim_distributed_controller.py"
RESULTS_DIR = ROOT / "results/network/distributed"
RUN_TIMESTAMP = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
RUN_ID = f"paper2-g7-scaling-{RUN_TIMESTAMP}"

OFP_BASE_PORT = 16700
PEER_BASE_PORT = 16800
DEFAULT_OWNER_EPOCH = 1
RANDOM_SEED = int(os.environ.get("DAIM_GATE_SEED", "20260809"))

# Overridable for a quick smoke run (e.g. DAIM_G7_NODE_COUNTS=2,4) before
# committing to the full sweep, which can take a long time at N=32.
NODE_COUNTS = [int(x) for x in os.environ.get(
    "DAIM_G7_NODE_COUNTS", "2,4,8,16,32").split(",") if x]

# Startup/convergence contention grows non-linearly with N on this VM's
# fixed vCPU count (confirmed at N=4 in distributed_prototype_gate.py's
# start_cluster comment) -- budget a per-node timeout term, not a fixed one.
READY_TIMEOUT_BASE_S = 60
READY_TIMEOUT_PER_NODE_S = 8
MESH_TIMEOUT_BASE_S = 30
MESH_TIMEOUT_PER_NODE_S = 4
SPAWN_STAGGER_S = 0.5
PROPAGATION_REPETITIONS = 30
CLUSTER_START_MAX_ATTEMPTS = 3

_all_events_lock = threading.Lock()
_all_raw_events = []


def record_raw(source, event):
    entry = {"run_id": RUN_ID, "source": source, "recorded_wall_time": time.time(),
             "recorded_monotonic_s": time.monotonic(), **event}
    with _all_events_lock:
        _all_raw_events.append(entry)
    return entry


def host_ip(i):
    return f"10.0.0.{i}"


class ChainTopo(Topo):
    """h1-s1-...-sN-hN, one host per switch -- same port-numbering
    convention as distributed_prototype_gate.py's ChainTopo, generalized
    to arbitrary N via the build() kwarg instead of a module-level
    constant."""

    def build(self, n):
        switches = [self.addSwitch(f"s{i+1}", protocols="OpenFlow13", failMode="secure") for i in range(n)]
        hosts = [self.addHost(f"h{i+1}", ip=f"{host_ip(i+1)}/24") for i in range(n)]
        for switch, host in zip(switches, hosts):
            self.addLink(host, switch)
        for left, right in zip(switches, switches[1:]):
            self.addLink(left, right)


def chain_ports(node_id, n):
    lower = 0 if node_id == 1 else 2
    if node_id == 1:
        higher = 2
    elif node_id == n:
        higher = 0
    else:
        higher = 3
    return lower, higher


class NodeProcess:
    def __init__(self, node_id, n, owner_epoch, peer_addrs, barrier_file):
        self.node_id = node_id
        port_lower, port_higher = chain_ports(node_id, n)
        env = dict(os.environ)
        env.update({
            "DAIM_NODE_ID": str(node_id),
            "DAIM_OWNER_EPOCH": str(owner_epoch),
            "DAIM_CHAIN_ORDER": ",".join(str(i) for i in range(1, n + 1)),
            "DAIM_PORT_TOWARD_LOWER": str(port_lower),
            "DAIM_PORT_TOWARD_HIGHER": str(port_higher),
            "DAIM_PEER_LISTEN_PORT": str(PEER_BASE_PORT + node_id),
            "DAIM_PEER_ADDRS": ",".join(peer_addrs or []),
            "DAIM_PEER_BARRIER_FILE": barrier_file,
        })
        self.ofp_port = OFP_BASE_PORT + node_id
        self.peer_port = PEER_BASE_PORT + node_id
        self.proc = subprocess.Popen(
            ["osken-manager", str(CONTROLLER_APP), "--ofp-tcp-listen-port", str(self.ofp_port)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env,
        )
        self.events = []
        self._lock = threading.Lock()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            event = record_raw(f"node{self.node_id}", event)
            with self._lock:
                self.events.append(event)

    def wait_for(self, predicate, timeout_s):
        deadline = time.monotonic() + timeout_s
        seen = 0
        while time.monotonic() < deadline:
            with self._lock:
                for e in self.events[seen:]:
                    if predicate(e):
                        return e
                seen = len(self.events)
            time.sleep(0.02)
        return None

    def events_matching(self, predicate):
        with self._lock:
            return [e for e in self.events if predicate(e)]

    def terminate(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def build_cluster(n, ready_timeout_s):
    """Same i<j non-redundant full-mesh policy and retry discipline as
    distributed_prototype_gate.py's start_cluster, generalized to N.

    Every node is launched with peer dialing held behind a barrier file
    (DAIM_PEER_BARRIER_FILE, see daim_distributed_controller.py) that does
    not exist yet, so no node starts dialing during this function's own
    staged, one-node-every-SPAWN_STAGGER_S launch loop. Returns (nodes,
    barrier_path) with the barrier file's path reserved but not yet
    created -- the caller creates it (a single os.close/open touch) at the
    exact instant it wants convergence timing to start, which is what makes
    "every node begins dialing" and "the clock starts" the same instant."""
    owner_epochs = {i: DEFAULT_OWNER_EPOCH for i in range(1, n + 1)}
    peer_addrs_by_node = {
        i: [f"127.0.0.1:{PEER_BASE_PORT + j}" for j in range(i + 1, n + 1)]
        for i in range(1, n + 1)
    }
    last_exc = None
    for attempt in range(1, CLUSTER_START_MAX_ATTEMPTS + 1):
        nodes = {}
        barrier_fd, barrier_path = tempfile.mkstemp(prefix="daim_g7_barrier_")
        os.close(barrier_fd)
        os.remove(barrier_path)  # reserve the path only; must not exist until released
        try:
            for i in range(1, n + 1):
                nodes[i] = NodeProcess(i, n, owner_epochs[i], peer_addrs_by_node[i], barrier_path)
                time.sleep(SPAWN_STAGGER_S)
            for i, node in nodes.items():
                subprocess.run(["ovs-vsctl", "set-controller", f"s{i}", f"tcp:127.0.0.1:{node.ofp_port}"],
                                check=True)
            for i, node in nodes.items():
                ready = node.wait_for(lambda e: e.get("event") == "ready", ready_timeout_s)
                if ready is None:
                    raise RuntimeError(f"node{i} never became ready (n={n})")
            return nodes, barrier_path
        except Exception as exc:
            last_exc = exc
            record_raw("gate_driver", {
                "event": "cluster_start_attempt_failed", "n_nodes": n,
                "attempt": attempt, "max_attempts": CLUSTER_START_MAX_ATTEMPTS, "detail": str(exc),
            })
            stop_cluster(nodes)
            if os.path.exists(barrier_path):
                os.remove(barrier_path)
            subprocess.run(["pkill", "-9", "-f", str(CONTROLLER_APP)], check=False,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1.0)
    raise RuntimeError(f"cluster of size {n} failed to start after "
                        f"{CLUSTER_START_MAX_ATTEMPTS} attempts") from last_exc


def stop_cluster(nodes):
    for node in nodes.values():
        node.terminate()


def wait_for_full_mesh(nodes, n, timeout_s, since_monotonic_s):
    """Returns the monotonic timestamp at which every node has reported a
    'snapshot_received' lifecycle event for all n-1 of its peers, or None on
    timeout. Deliberately not 'connected': that event fires right after
    HELLO, before exchange_snapshots() even starts (daim_peer_transport.c),
    so counting it would measure TCP+HELLO establishment, not distributed-
    state convergence -- 'snapshot_received' is the event that actually
    means this node's state is synced with that peer.

    Only counts events recorded at or after since_monotonic_s (the barrier-
    release instant). Peer dialing cannot start before the barrier file
    exists (daim_distributed_controller.py), so in principle no
    snapshot_received should ever predate it -- this filter is defense in
    depth against exactly the failure mode this function used to have
    (counting historical events that happened before the harness started
    timing), not a mechanism this design depends on to be correct."""
    expected = n - 1
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if all(len(node.events_matching(
                lambda e: e.get("event") == "lifecycle" and e.get("name") == "snapshot_received"
                and e.get("recorded_monotonic_s", 0) >= since_monotonic_s)) >= expected
                for node in nodes.values()):
            return time.monotonic()
        # 1 ms, not the 50 ms this loop used before: convergence at small N
        # is fast enough (tens of ms) that a coarser poll interval becomes
        # the dominant term in convergence_s itself rather than a
        # negligible measurement overhead -- confirmed by an N=2/N=4 smoke
        # run at 50 ms polling landing suspiciously close to 50 ms for both
        # sizes. 1 ms costs nothing extra at the N<=32, few-second scale
        # this gate runs at.
        time.sleep(0.001)
    return None


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


def measure_propagation(net, nodes, n, repetitions=PROPAGATION_REPETITIONS):
    """Host n pings host 1, exactly as G1's h4->h1 at N=4, generalized:
    every node 1..n-1 should observe node n's disseminated location."""
    source_id = n
    h_source = net.get(f"h{source_id}")
    target_ip = host_ip(1)
    for _ in range(repetitions):
        subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "del-flows", f"s{source_id}", "in_port=1"],
                       check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        h_source.cmd(f"ping -c 1 -W 1 {target_ip}")
        time.sleep(0.15)

    samples = []
    for target in range(1, n):
        events = nodes[target].events_matching(
            lambda e: e.get("event") == "apply" and e.get("origin_node_id") == source_id
            and e.get("result") in ("new", "updated")
        )
        for e in events:
            # applied_at_ns is stamped by the C transport layer at receive
            # time (daim_peer_transport.c); see distributed_prototype_gate.py's G1.
            latency_ms = (e["applied_at_ns"] - e["learned_at_ns"]) / 1e6
            samples.append({"observing_node": target, "propagation_latency_ms": latency_ms})

    observing_nodes = {s["observing_node"] for s in samples}
    latencies = [s["propagation_latency_ms"] for s in samples]
    return {
        "n_repetitions_sent": repetitions,
        "n_samples": len(samples),
        "observing_nodes": sorted(observing_nodes),
        "propagation_ok": observing_nodes == set(range(1, n)),
        "latency_summary_ms": latency_summary(latencies, RANDOM_SEED),
    }


def connections_observed(nodes):
    return sum(len(node.events_matching(
        lambda e: e.get("event") == "lifecycle" and e.get("name") == "connected"))
        for node in nodes.values())


def run_one_size(n):
    record_raw("gate_driver", {"event": "size_start", "n_nodes": n})
    net = None
    nodes = {}
    barrier_path = None
    outcome = {"n_nodes": n, "pass": False, "fatal_error": None}
    launch_start = time.monotonic()  # covers the whole repetition, incl. staged node launch
    try:
        subprocess.run(["mn", "-c"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["pkill", "-9", "-f", str(CONTROLLER_APP)], check=False,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.3)

        net = Mininet(topo=ChainTopo(n=n), controller=None, switch=OVSSwitch,
                      link=TCLink, autoSetMacs=True)
        net.start()

        launch_start = time.monotonic()
        ready_timeout = READY_TIMEOUT_BASE_S + READY_TIMEOUT_PER_NODE_S * n
        nodes, barrier_path = build_cluster(n, ready_timeout)

        # Barrier release, not "after build_cluster() returns": every node
        # process already exists and is OpenFlow-ready at this point (that
        # is what build_cluster's own ready-wait loop confirmed), but until
        # this file is created none of them has dialed a single peer --
        # DAIM_PEER_BARRIER_FILE holds add_peer() back in
        # daim_distributed_controller.py specifically so this instant is
        # also the instant real protocol work (dialing, HELLO, snapshot
        # exchange) can first begin, closing the gap the previous
        # "convergence_s = mesh_ready_at - mesh_start" definition had: that
        # version excluded build_cluster's staged launch from the
        # *measurement window* without ever preventing the mesh from
        # actually converging *during* that window.
        Path(barrier_path).touch()
        mesh_start = time.monotonic()
        mesh_timeout = MESH_TIMEOUT_BASE_S + MESH_TIMEOUT_PER_NODE_S * n
        mesh_ready_at = wait_for_full_mesh(nodes, n, mesh_timeout, mesh_start)
        convergence_s = (mesh_ready_at - mesh_start) if mesh_ready_at is not None else None

        propagation = measure_propagation(net, nodes, n) if n >= 2 else {
            "n_repetitions_sent": 0, "n_samples": 0, "observing_nodes": [],
            "propagation_ok": True, "latency_summary_ms": latency_summary([], RANDOM_SEED),
        }

        outcome.update({
            "convergence_s": convergence_s,
            "connections_observed": connections_observed(nodes),
            "connections_expected": n * (n - 1),  # each edge fires "connected" on both endpoints
            "apply_events_total": sum(len(node.events_matching(lambda e: e.get("event") == "apply"))
                                       for node in nodes.values()),
            "lifecycle_events_total": sum(len(node.events_matching(lambda e: e.get("event") == "lifecycle"))
                                           for node in nodes.values()),
            "propagation": propagation,
        })
        outcome["pass"] = convergence_s is not None and propagation["propagation_ok"]
    except Exception as exc:
        fatal = {"type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()}
        outcome["fatal_error"] = fatal
        record_raw("gate_driver", {"event": "size_fatal_error", "n_nodes": n, **fatal})
    finally:
        if nodes:
            stop_cluster(nodes)
        if net is not None:
            net.stop()
        subprocess.run(["mn", "-c"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if barrier_path and os.path.exists(barrier_path):
            os.remove(barrier_path)

    outcome["wall_time_s"] = time.monotonic() - launch_start
    record_raw("gate_driver", {"event": "size_done", "n_nodes": n,
                                "pass": outcome["pass"], "wall_time_s": outcome["wall_time_s"]})
    return outcome


def checksum_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summary_row(n, repetition, outcome):
    lat = (outcome.get("propagation") or {}).get("latency_summary_ms") or {}
    return {
        "n_nodes": n,
        "repetition": repetition,
        "pass": outcome["pass"],
        "convergence_s": outcome.get("convergence_s"),
        "connections_observed": outcome.get("connections_observed"),
        "connections_expected": outcome.get("connections_expected"),
        "propagation_n_samples": (outcome.get("propagation") or {}).get("n_samples"),
        "propagation_latency_median_ms": lat.get("median"),
        "propagation_latency_p95_ms": lat.get("p95"),
        "propagation_latency_p99_ms": lat.get("p99"),
        "apply_events_total": outcome.get("apply_events_total"),
        "lifecycle_events_total": outcome.get("lifecycle_events_total"),
        "wall_time_s": outcome.get("wall_time_s"),
        "fatal_error": (outcome.get("fatal_error") or {}).get("message"),
    }


def campaign_row(n, rows_for_n):
    """One N's statistics across all its repetitions -- the evidence-gate
    shape (median/p95/p99/bootstrap CI over independent repetitions), not
    the per-repetition raw row summary_row() produces."""
    passes = [r["pass"] for r in rows_for_n]
    convergence = [r["convergence_s"] for r in rows_for_n if r["convergence_s"] is not None]
    prop_median = [r["propagation_latency_median_ms"] for r in rows_for_n
                   if r["propagation_latency_median_ms"] is not None]
    return {
        "n_nodes": n,
        "repetitions": len(rows_for_n),
        "pass_rate": sum(passes) / len(passes) if passes else 0.0,
        "n_passed": sum(passes),
        "convergence_s_summary": latency_summary(convergence, RANDOM_SEED),
        "propagation_latency_median_ms_summary": latency_summary(prop_median, RANDOM_SEED),
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repetitions", type=int, default=1,
                        help="Independent repetitions per N (fresh cluster bring-up/teardown "
                             "each time). Default 1 preserves the original single-sweep behavior.")
    return parser.parse_args()


def main():
    args = parse_args()
    setLogLevel("warning")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    gate_results = {}
    summary_rows = []
    for n in NODE_COUNTS:
        rows_for_n = []
        for rep in range(args.repetitions):
            label = f"n={n}" if args.repetitions == 1 else f"n={n} rep={rep+1}/{args.repetitions}"
            print(f"=== G7 scaling: {label} ===", flush=True)
            outcome = run_one_size(n)
            gate_results[f"n{n}_rep{rep}"] = outcome
            row = summary_row(n, rep, outcome)
            summary_rows.append(row)
            rows_for_n.append(row)
            print(json.dumps(row, indent=2), flush=True)
        if args.repetitions > 1:
            print(json.dumps(campaign_row(n, rows_for_n), indent=2), flush=True)

    raw_path = RESULTS_DIR / f"raw_events_g7_{RUN_TIMESTAMP}.jsonl"
    with raw_path.open("w") as handle:
        for event in _all_raw_events:
            handle.write(json.dumps(event) + "\n")

    results_path = RESULTS_DIR / f"gate_results_g7_{RUN_TIMESTAMP}.json"
    results_path.write_text(json.dumps(gate_results, indent=2) + "\n")

    summary_json_path = RESULTS_DIR / f"scaling_summary_g7_{RUN_TIMESTAMP}.json"
    summary_json_path.write_text(json.dumps(summary_rows, indent=2) + "\n")

    summary_csv_path = RESULTS_DIR / f"scaling_summary_g7_{RUN_TIMESTAMP}.csv"
    with summary_csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    output_files = [raw_path, results_path, summary_json_path, summary_csv_path]
    campaign_rows = None
    if args.repetitions > 1:
        campaign_rows = [campaign_row(n, [r for r in summary_rows if r["n_nodes"] == n])
                          for n in NODE_COUNTS]
        campaign_json_path = RESULTS_DIR / f"scaling_campaign_g7_{RUN_TIMESTAMP}.json"
        campaign_json_path.write_text(json.dumps(campaign_rows, indent=2) + "\n")
        output_files.append(campaign_json_path)

    manifest = {
        "run_timestamp": RUN_TIMESTAMP,
        "run_id": RUN_ID,
        "random_seed": RANDOM_SEED,
        "node_counts": NODE_COUNTS,
        "repetitions_per_n": args.repetitions,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "kernel": platform.release(),
        },
        "all_sizes_passed": all(r["pass"] for r in gate_results.values()),
        "size_pass": {k: r["pass"] for k, r in gate_results.items()},
        "files": {
            str(p.relative_to(RESULTS_DIR)): checksum_file(p)
            for p in output_files
        },
    }
    manifest_path = RESULTS_DIR / f"manifest_g7_{RUN_TIMESTAMP}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    print(json.dumps(manifest, indent=2))
    if campaign_rows is not None:
        print(json.dumps(campaign_rows, indent=2))


if __name__ == "__main__":
    main()
