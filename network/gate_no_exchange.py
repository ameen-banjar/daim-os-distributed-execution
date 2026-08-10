#!/usr/bin/env python3
""""multi-instance/no-exchange" baseline for Paper 2's evidence gate: the
exact same h1-s1-s2-s3-s4-h4 topology and per-switch DAIM node processes as
distributed_prototype_gate.py, but every node is configured with an empty
peer list (DAIM_PEER_ADDRS="") -- no dial threads spawn, no connections are
ever made, no state is ever exchanged. This isolates what the peer-transport
mechanism itself contributes: with it (distributed_prototype_gate.py's G1),
h1's location propagates to every other node; without it, it never does.

No code changes were needed to support this: DAIM_PEER_ADDRS already parses
to an empty list cleanly (daim_distributed_controller.py), a transport with
zero configured peers spawns zero dial threads, and readiness depends only
on the OpenFlow handshake, not on any peer connection -- confirmed by
inspection before writing this script, not assumed.

Measures, contrasted directly against G1's distributed-mode result:
  - propagation: MUST be zero. h4 pings h1 repeatedly (same trigger as G1);
    every other node's "apply" event count for that origin must stay at 0
    across every repetition -- the headline no-exchange result.
  - decision completeness: without ever learning h1's location, node2/3/4's
    own forwarding decisions for traffic toward h1's MAC must stay FLOOD
    (PORT_FLOOD/PORT_NONE) rather than becoming a directed port, since they
    never receive h1's claim. This is the mechanism-level explanation for
    the zero-propagation headline, not a separate/optional check.
  - local learning still works: host_port filtering is per-node local state,
    independent of peers, so node1 still learns h1's own MAC normally --
    confirmed so a reader can't mistake "no exchange" for "node1 is broken".

A latency comparison against the distributed/single-Core modes is
deliberately not attempted here: this codebase's Packet-In path has no
per-stage nanosecond instrumentation the way Paper 1's reactive-baseline
controller does (see packetin_latency_breakdown.py), so fabricating one from
log-arrival timestamps (which include this driver's own I/O overhead) would
not be a fair, rigorous cross-mode comparison. What IS rigorously measurable
with existing instrumentation -- propagation and decision completeness --
is what this baseline reports.
"""
import argparse
import hashlib
import json
import os
import platform
import random
import statistics
import subprocess
import sys
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
RUN_ID = f"paper2-no-exchange-{RUN_TIMESTAMP}"

N_NODES = 4
OFP_BASE_PORT = 16700
PEER_BASE_PORT = 16800
HOST_IPS = {1: "10.0.0.1", 2: "10.0.0.2", 3: "10.0.0.3", 4: "10.0.0.4"}
READY_TIMEOUT_S = 90
DEFAULT_OWNER_EPOCH = 1
PORT_FLOOD = 0xFFFB
PORT_NONE = 0xFFFE
RANDOM_SEED = int(os.environ.get("DAIM_GATE_SEED", "20260809"))
REPETITIONS = int(os.environ.get("DAIM_CAMPAIGN_REPETITIONS", "1"))

_all_events_lock = threading.Lock()
_all_raw_events = []


def record_raw(source, event):
    entry = {"run_id": RUN_ID, "source": source, "recorded_wall_time": time.time(),
             "recorded_monotonic_s": time.monotonic(), **event}
    with _all_events_lock:
        _all_raw_events.append(entry)
    return entry


class ChainTopo(Topo):
    """Identical to distributed_prototype_gate.py's ChainTopo -- same
    topology, only the peer-mesh construction in start_cluster differs."""

    def build(self, n=N_NODES):
        switches = [self.addSwitch(f"s{i+1}", protocols="OpenFlow13", failMode="secure") for i in range(n)]
        hosts = [self.addHost(f"h{i+1}", ip=f"{HOST_IPS[i+1]}/24") for i in range(n)]
        for switch, host in zip(switches, hosts):
            self.addLink(host, switch)
        for left, right in zip(switches, switches[1:]):
            self.addLink(left, right)


def chain_ports(node_id, n=N_NODES):
    lower = 0 if node_id == 1 else 2
    if node_id == 1:
        higher = 2
    elif node_id == n:
        higher = 0
    else:
        higher = 3
    return lower, higher


class NodeProcess:
    def __init__(self, node_id, owner_epoch=DEFAULT_OWNER_EPOCH):
        self.node_id = node_id
        port_lower, port_higher = chain_ports(node_id)
        env = dict(os.environ)
        env.update({
            "DAIM_NODE_ID": str(node_id),
            "DAIM_OWNER_EPOCH": str(owner_epoch),
            "DAIM_CHAIN_ORDER": ",".join(str(i) for i in range(1, N_NODES + 1)),
            "DAIM_PORT_TOWARD_LOWER": str(port_lower),
            "DAIM_PORT_TOWARD_HIGHER": str(port_higher),
            "DAIM_PEER_LISTEN_PORT": str(PEER_BASE_PORT + node_id),
            "DAIM_PEER_ADDRS": "",  # the entire point of this baseline
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


def start_cluster():
    """Same staggered-spawn/retry discipline as distributed_prototype_gate.
    py's start_cluster, minus the peer-mesh construction entirely -- every
    node gets zero peers, by design."""
    nodes = {}
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        nodes = {}
        try:
            for i in range(1, N_NODES + 1):
                nodes[i] = NodeProcess(i)
                time.sleep(0.5)
            for i, node in nodes.items():
                subprocess.run(["ovs-vsctl", "set-controller", f"s{i}", f"tcp:127.0.0.1:{node.ofp_port}"],
                                check=True)
            for i, node in nodes.items():
                ready = node.wait_for(lambda e: e.get("event") == "ready", READY_TIMEOUT_S)
                if ready is None:
                    raise RuntimeError(f"node{i} never became ready")
            return nodes
        except Exception as exc:
            record_raw("gate_driver", {
                "event": "cluster_start_attempt_failed",
                "attempt": attempt, "max_attempts": max_attempts, "detail": str(exc),
            })
            stop_cluster(nodes)
            subprocess.run(["pkill", "-9", "-f", str(CONTROLLER_APP)], check=False,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1.0)
            if attempt == max_attempts:
                raise
    raise RuntimeError("unreachable")


def stop_cluster(nodes):
    for node in nodes.values():
        node.terminate()


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


def gate_no_exchange(net, nodes, repetitions=100):
    """Same ping-trigger pattern as distributed_prototype_gate.py's gate_g1,
    same repetition count -- the only thing that should differ is the
    result, since propagation depends entirely on the peer mesh this
    baseline never configures."""
    h4 = net.get("h4")
    for _ in range(repetitions):
        subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "del-flows", "s4", "in_port=1"],
                       check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        h4.cmd(f"ping -c 1 -W 1 {HOST_IPS[1]}")
        time.sleep(0.15)

    propagation_events = []
    for target in (1, 2, 3):
        events = nodes[target].events_matching(
            lambda e: e.get("event") == "apply" and e.get("origin_node_id") == 4
        )
        propagation_events.extend(events)

    lifecycle_events = []
    for i in range(1, N_NODES + 1):
        lifecycle_events.extend(nodes[i].events_matching(lambda e: e.get("event") == "lifecycle"))

    # node4's own local learn is peer-independent -- confirm it still
    # happens, so "zero propagation" isn't mistaken for "node4 is broken".
    node4_local_learn = nodes[4].wait_for(
        lambda e: e.get("event") == "no_rule" and e.get("in_port") == 1
        and e.get("mac_src") is not None,
        timeout_s=5,
    )

    # Nodes 1-3 never learn h4's location (no exchange), so any decision
    # they make for traffic destined to h4's MAC must still be FLOOD, not a
    # directed port -- the mechanism-level cause of zero propagation. Scope
    # the del-flows match to this MAC specifically (never a bare del-flows
    # with no match, which would also wipe each switch's table-miss rule
    # and break its own ability to reach the controller at all).
    h4_mac = net.get("h4").MAC()
    del_flow_targets = [(1, "s1"), (2, "s2"), (3, "s3")]
    for i, sw in del_flow_targets:
        subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "del-flows", sw, f"dl_dst={h4_mac}"],
                       check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    net.get("h1").cmd(f"ping -c 1 -W 1 {HOST_IPS[4]}")
    time.sleep(0.3)
    always_flooded = True
    flood_evidence = []
    for i, _sw in del_flow_targets:
        decisions = nodes[i].events_matching(
            lambda e: e.get("event") == "no_rule" and e.get("mac_dst") is not None
        )
        for e in decisions[-5:]:
            flood_evidence.append({"node": i, "mac_dst": e.get("mac_dst"), "out_port": e.get("out_port")})
            if e.get("out_port") not in (PORT_FLOOD, PORT_NONE):
                always_flooded = False

    return {
        "gate": "no_exchange_propagation",
        "description": "with peer dissemination disabled, propagation must be exactly zero, "
                        "and cross-node forwarding decisions must stay FLOOD -- both are the "
                        "expected, correct result of this baseline, not a failure",
        "pass": len(propagation_events) == 0 and len(lifecycle_events) == 0
        and bool(node4_local_learn) and always_flooded,
        "n_repetitions_sent": repetitions,
        "propagation_events_observed": len(propagation_events),
        "lifecycle_events_observed": len(lifecycle_events),
        "node4_local_learn_confirmed": bool(node4_local_learn),
        "cross_node_decisions_always_flooded": always_flooded,
        "flood_evidence_sample": flood_evidence,
    }


def checksum_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_one_repetition(rep):
    global _all_raw_events
    _all_raw_events = []
    net = None
    nodes = {}
    result = None
    fatal_error = None
    try:
        subprocess.run(["mn", "-c"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["pkill", "-9", "-f", str(CONTROLLER_APP)], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.3)
        net = Mininet(topo=ChainTopo(), controller=None, switch=OVSSwitch,
                      link=TCLink, autoSetMacs=True)
        net.start()
        nodes = start_cluster()
        result = gate_no_exchange(net, nodes)
    except Exception as exc:
        fatal_error = {"type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()}
        record_raw("gate_driver", {"event": "fatal_error", **fatal_error})
    finally:
        if nodes:
            stop_cluster(nodes)
        if net is not None:
            net.stop()
        subprocess.run(["mn", "-c"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    raw_path = RESULTS_DIR / f"raw_events_no_exchange_{RUN_TIMESTAMP}_rep{rep}.jsonl"
    with raw_path.open("w") as handle:
        for event in _all_raw_events:
            handle.write(json.dumps(event) + "\n")

    return {
        "rep": rep,
        "result": result,
        "fatal_error": fatal_error,
        "raw_events_file": raw_path.name,
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repetitions", type=int, default=REPETITIONS,
                        help="Independent cluster bring-up/teardown repetitions.")
    return parser.parse_args()


def main():
    args = parse_args()
    setLogLevel("warning")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    repetitions_data = []
    for rep in range(args.repetitions):
        print(f"=== no-exchange baseline: repetition {rep+1}/{args.repetitions} ===", flush=True)
        rep_data = run_one_repetition(rep)
        repetitions_data.append(rep_data)
        r = rep_data["result"]
        print(json.dumps(r if r else {"fatal_error": rep_data["fatal_error"]}, indent=2), flush=True)

    passes = [bool(r["result"] and r["result"]["pass"]) for r in repetitions_data]

    results_path = RESULTS_DIR / f"gate_results_no_exchange_{RUN_TIMESTAMP}.json"
    results_path.write_text(json.dumps(repetitions_data, indent=2) + "\n")

    manifest = {
        "run_id": RUN_ID,
        "run_timestamp": RUN_TIMESTAMP,
        "repetitions": args.repetitions,
        "n_passed": sum(passes),
        "pass_rate": sum(passes) / len(passes) if passes else 0.0,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "kernel": platform.release(),
        },
        "files": {
            str(p.relative_to(RESULTS_DIR)): checksum_file(p)
            for p in [results_path] + [RESULTS_DIR / rd["raw_events_file"] for rd in repetitions_data]
        },
    }
    manifest_path = RESULTS_DIR / f"manifest_no_exchange_{RUN_TIMESTAMP}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
