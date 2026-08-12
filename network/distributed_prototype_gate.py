#!/usr/bin/env python3
"""Distributed DAIM-OS prototype gate: four independently executing DAIM
nodes (one OS process per switch, private memory -- see daim_distributed_
learning_app.h) on a linear h1-s1-s2-s3-s4-h4 topology, full-mesh peer
state exchange.

Gate definition (adopted from external review of Milestone 1, not the
original E1-E6 split in the initial spec -- this separates each mechanism
into its own testable claim):

  G1  A valid update propagates between 4 independently executing processes.
  G2  A duplicate update is rejected.
  G3  A stale (lower-sequence, same-epoch) update is rejected.
  G4  A newer-epoch update is accepted; an older-epoch update is rejected
      even with a higher sequence number.
  G5  Surviving nodes keep making new local decisions during a peer's
      failure -- both for traffic unrelated to the failed peer, and for
      traffic routed toward the failed peer's last-known cached location.
  G6  After a peer-network partition (OS-level, via iptables -- not an
      application-level pause hook) and reconnection, state resynchronises
      (snapshot exchange), with propagation/recovery timing measured.

G7 (node-count scaling) and G8 (ownership conflict) are explicitly out of
scope for this mechanism gate; both remain required by the master plan
before Paper 2 claims are allowed.

Every raw event from every node is retained (results/network/distributed/
raw_events.jsonl), including rejected/failed messages and connection
churn -- nothing is filtered before writing. A run manifest with the seed,
environment, and a SHA-256 of every output file is written alongside it.

G8 (ownership conflict) is available opt-in via --include-g8, appended
after G1-G6 in the same run; the default invocation (no flag) is
unchanged from the original G1-G6-only run this module always produced.
"""
import argparse
import hashlib
import json
import os
import platform
import random
import re
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

sys.path.insert(0, str(Path(__file__).resolve().parent))
from daim_synthetic_peer import SyntheticPeer

ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_APP = ROOT / "network/daim_distributed_controller.py"
RESULTS_DIR = ROOT / "results/network/distributed"
RUN_TIMESTAMP = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
RUN_ID = f"paper2-g1-g6-{RUN_TIMESTAMP}"

N_NODES = 4
OFP_BASE_PORT = 16700
PEER_BASE_PORT = 16800
HOST_IPS = {1: "10.0.0.1", 2: "10.0.0.2", 3: "10.0.0.3", 4: "10.0.0.4"}
READY_TIMEOUT_S = 90
DEFAULT_OWNER_EPOCH = 1
RANDOM_SEED = int(os.environ.get("DAIM_GATE_SEED", "20260809"))

_all_events_lock = threading.Lock()
_all_raw_events = []  # every event from every source, in arrival order, never filtered


def record_raw(source, event):
    # Both clocks recorded: recorded_wall_time (time.time()) for a
    # human-readable absolute timestamp in the raw log, and
    # recorded_monotonic_s (time.monotonic()) for ordering comparisons
    # within this run -- gate code below compares against monotonic
    # markers (partition_start etc.) and must use the same clock domain,
    # not mix it with wall time.
    entry = {"run_id": RUN_ID, "source": source, "recorded_wall_time": time.time(),
             "recorded_monotonic_s": time.monotonic(), **event}
    with _all_events_lock:
        _all_raw_events.append(entry)
    return entry


class ChainTopo(Topo):
    """h1-s1-s2-s3-s4-h4, one host per switch. Port numbering (matches
    Mininet's link-creation-order assignment, same pattern already relied
    on by stage2_full_compare.py's LinearBenchmarkTopo): host link added
    first on every switch (port 1), then switch-switch links s1-s2, s2-s3,
    s3-s4 in order -- so s1 has port 2 toward s2; s2 has port 2 toward s1
    and port 3 toward s3; s3 has port 2 toward s2 and port 3 toward s4; s4
    has port 2 toward s3."""

    def build(self, n=N_NODES):
        switches = [self.addSwitch(f"s{i+1}", protocols="OpenFlow13", failMode="secure") for i in range(n)]
        hosts = [self.addHost(f"h{i+1}", ip=f"{HOST_IPS[i+1]}/24") for i in range(n)]
        for switch, host in zip(switches, hosts):
            self.addLink(host, switch)
        for left, right in zip(switches, switches[1:]):
            self.addLink(left, right)


def chain_ports(node_id, n=N_NODES):
    """Returns (port_toward_lower, port_toward_higher) per the topology
    comment above; 0 (unused) at the chain's ends."""
    lower = 0 if node_id == 1 else 2
    if node_id == 1:
        higher = 2
    elif node_id == n:
        higher = 0
    else:
        higher = 3
    return lower, higher


class NodeProcess:
    def __init__(self, node_id, owner_epoch=DEFAULT_OWNER_EPOCH, peer_addrs=None):
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
            "DAIM_PEER_ADDRS": ",".join(peer_addrs or []),
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

    def kill(self):
        self.proc.kill()
        self.proc.wait(timeout=5)

    def terminate(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def start_cluster(owner_epochs=None):
    """Each unordered pair {i, j} gets exactly one TCP connection: node i
    dials node j only when i < j. This is a deliberate change from a
    naive "every node dials every other node" full mesh, which would give
    every pair two independent, redundant connections -- harmless for
    G1-G5, but it makes G6's OS-level partition test ill-defined (blocking
    only one node's *listen* port leaves its own *outbound*-dialed
    connections, on ephemeral source ports, untouched, so the node would
    not actually be isolated). With the i<j rule, the highest-numbered
    node (4) only ever accepts inbound connections and never dials out,
    so a single iptables rule on its listen port is a complete, correct
    partition -- see gate_g6."""
    owner_epochs = owner_epochs or {i: DEFAULT_OWNER_EPOCH for i in range(1, N_NODES + 1)}
    peer_addrs_by_node = {
        i: [f"127.0.0.1:{PEER_BASE_PORT + j}" for j in range(i + 1, N_NODES + 1)]
        for i in range(1, N_NODES + 1)
    }
    # Staggered, not simultaneous: spawning all 4 osken-manager processes
    # (each an eventlet reactor plus several native pthreads from the
    # ctypes-backed peer transport) at the same instant, alongside Mininet/
    # OVS bring-up, on this VM's 4 vCPUs was confirmed (isolated single-node
    # test vs. full 4-node runs) to amplify each node's ready time far
    # beyond its ~5s baseline -- not a hang, but contention. A small stagger
    # between spawns reduces peak concurrent load during the most
    # thread/socket-heavy startup phase (initial connection + snapshot
    # exchange on every configured peer).
    nodes = {}
    for i in range(1, N_NODES + 1):
        nodes[i] = NodeProcess(i, owner_epochs[i], peer_addrs_by_node[i])
        time.sleep(0.5)
    # If any node fails to become ready below, this function raises before
    # returning -- so `nodes = start_cluster()` in main() never completes
    # the assignment, and main()'s `finally: if nodes: stop_cluster(nodes)`
    # would see an empty dict and never terminate the processes already
    # spawned above. Confirmed directly: a failed run left all 4 node
    # processes (and their listening ports) running indefinitely. Clean up
    # this function's own partial state before propagating the failure.
    try:
        for i, node in nodes.items():
            subprocess.run(["ovs-vsctl", "set-controller", f"s{i}", f"tcp:127.0.0.1:{node.ofp_port}"], check=True)
        for i, node in nodes.items():
            ready = node.wait_for(lambda e: e.get("event") == "ready", READY_TIMEOUT_S)
            if ready is None:
                raise RuntimeError(f"node{i} never became ready")
    except Exception:
        stop_cluster(nodes)
        raise
    return nodes


def stop_cluster(nodes):
    for node in nodes.values():
        node.terminate()


# --- G1: propagation among 4 independent processes ---

def gate_g1(net, nodes, repetitions=100):
    h4 = net.get("h4")
    samples = []
    for _ in range(repetitions):
        subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "del-flows", "s4", "in_port=1"],
                       check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        h4.cmd(f"ping -c 1 -W 1 {HOST_IPS[1]}")
        # Give dissemination + apply a bounded window per repetition; every
        # apply event actually observed is kept (may be 0, 1, or 2 per
        # repetition -- ARP and ICMP each trigger a local-learn-and-
        # disseminate call -- see daim_distributed_learning_app.c).
        time.sleep(0.15)
    for target in (1, 2, 3):
        events = nodes[target].events_matching(
            lambda e, t=target: e.get("event") == "apply" and e.get("origin_node_id") == 4
            and e.get("result") in ("new", "updated")
        )
        for e in events:
            # applied_at_ns is stamped by the C transport layer with this
            # node's own monotonic_ns() at receive time (daim_peer_
            # transport.c), not carried over from the wire.
            latency_ms = (e["applied_at_ns"] - e["learned_at_ns"]) / 1e6
            samples.append({"observing_node": target, "propagation_latency_ms": latency_ms})

    observing_nodes = {sample["observing_node"] for sample in samples}
    distinct_pids = len({nodes[i].proc.pid for i in nodes}) == N_NODES
    latencies = [sample["propagation_latency_ms"] for sample in samples]
    return {
        "gate": "G1", "description": "valid update propagates between 4 independent processes",
        "pass": observing_nodes == {1, 2, 3} and distinct_pids,
        "n_repetitions_sent": repetitions,
        "n_apply_events_observed": len(samples),
        "samples": samples,
        "node_pids": {i: nodes[i].proc.pid for i in nodes},
        "distinct_pids": distinct_pids,
        "latency_summary_ms": latency_summary(latencies, RANDOM_SEED),
    }


# --- G2/G3/G4: synthetic-peer scripted sequences against node 1 ---

def _synthetic_sequence(target_ofp_port, target_peer_port, mac_suffix, sequence_events):
    """sequence_events: list of (owner_epoch, sequence) tuples to send in
    order, each as a fresh HOST_LOCATION_UPDATE from a synthetic origin
    node (999) for one fixed MAC. Returns the target node's own "apply"
    events for that MAC, in arrival order, via the caller's NodeProcess."""
    mac = bytes([0, 0, 0, 0, 0, mac_suffix])
    peer = SyntheticPeer(node_id=999, owner_epoch=1)
    peer.connect("127.0.0.1", target_peer_port)
    peer.handshake_and_drain_snapshot()
    for owner_epoch, sequence in sequence_events:
        peer.send_host_location_update(mac, origin_node_id=999, owner_dpid=9999, owner_port=1,
                                        owner_epoch=owner_epoch, sequence=sequence)
        time.sleep(0.2)
    time.sleep(0.3)
    peer.close()
    return mac


def gate_g2(nodes):
    node = nodes[1]
    mac = _synthetic_sequence(node.ofp_port, node.peer_port, 0xA1, [(1, 100), (1, 100)])
    events = node.events_matching(lambda e: e.get("event") == "apply" and e.get("mac") == _mac_str(mac))
    results = [e["result"] for e in events]
    return {
        "gate": "G2", "description": "duplicate update rejected",
        "pass": results == ["new", "duplicate"],
        "observed_results": results,
    }


def gate_g3(nodes):
    node = nodes[1]
    mac = _synthetic_sequence(node.ofp_port, node.peer_port, 0xA2, [(1, 200), (1, 150)])
    events = node.events_matching(lambda e: e.get("event") == "apply" and e.get("mac") == _mac_str(mac))
    results = [e["result"] for e in events]
    return {
        "gate": "G3", "description": "stale (lower-sequence, same-epoch) update rejected",
        "pass": results == ["new", "stale_rejected"],
        "observed_results": results,
    }


def gate_g4(nodes):
    node = nodes[1]
    # epoch 1/seq 300 (new) -> epoch 0/seq 999999 (older epoch, must be
    # rejected despite a much higher sequence) -> epoch 2/seq 1 (newer
    # epoch, must be accepted despite a much lower sequence).
    mac = _synthetic_sequence(node.ofp_port, node.peer_port, 0xA3,
                               [(1, 300), (0, 999999), (2, 1)])
    events = node.events_matching(lambda e: e.get("event") == "apply" and e.get("mac") == _mac_str(mac))
    results = [e["result"] for e in events]
    return {
        "gate": "G4", "description": "newer epoch accepted, older epoch rejected despite sequence",
        "pass": results == ["new", "stale_rejected", "updated"],
        "observed_results": results,
    }


def _mac_str(mac_bytes):
    return ":".join("%02x" % b for b in mac_bytes)


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


# --- G5: local decisions survive a peer's failure ---

def gate_g5(net, nodes):
    h2, h3, h1 = net.get("h2", "h3", "h1")

    # Pre-condition: node1 already has h4's location cached from G1.
    pre = nodes[1].events_matching(
        lambda e: e.get("event") == "apply" and e.get("origin_node_id") == 4
        and e.get("result") in ("new", "updated")
    )
    had_h4_cached = len(pre) > 0
    h4_mac = pre[-1]["mac"] if pre else None

    nodes[4].kill()
    time.sleep(0.3)

    # (a) General liveness: traffic between two surviving nodes unrelated
    # to the killed peer must still work end-to-end.
    unrelated_ping = h2.cmd(f"ping -c 2 -W 1 {HOST_IPS[3]}")
    unrelated_ok = "0% packet loss" in unrelated_ping

    # (b) Decision using cached info from the now-dead node: force a fresh
    # table-miss (not a hit on an already-installed flow) and confirm
    # node1 still makes a decision using its cached (pre-death) knowledge
    # of h4's location, and installs a flow on its own switch. End-to-end
    # delivery is NOT asserted here -- s4 has no live controller of its
    # own once node4 is dead, which is an expected, separate limitation
    # (see the gate report's claim boundary), not a distributed-state
    # failure.
    if h4_mac:
        subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "del-flows", "s1",
                         f"in_port=1,dl_dst={h4_mac}"], check=False)
        # h1 already knows h4_mac from G1's apply event -- seed the kernel
        # ARP entry explicitly rather than relying on it having been
        # populated incidentally by earlier traffic (e.g. it won't be if
        # an interface bounce elsewhere in the run flushed it, and node4
        # is already dead by this point so a fresh ARP request would never
        # get answered).
        h1.cmd(f"ip neigh replace {HOST_IPS[4]} lladdr {h4_mac} dev {h1.defaultIntf().name}")
    before = len(nodes[1].events_matching(lambda e: e.get("event") == "no_rule"))
    h1.cmd(f"ping -c 1 -W 1 {HOST_IPS[4]}")
    time.sleep(0.3)
    after_events = nodes[1].events_matching(lambda e: e.get("event") == "no_rule")
    new_decisions = after_events[before:]
    cached_decision_made = any(
        e.get("mac_dst") == h4_mac and e.get("out_port") not in (0xFFFB, 0xFFFE)
        for e in new_decisions
    )
    flow_dump = subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "dump-flows", "s1"],
                                capture_output=True, text=True).stdout
    flow_installed = bool(h4_mac) and (f"dl_dst={h4_mac}" in flow_dump)

    return {
        "gate": "G5", "description": "surviving nodes keep deciding during a peer failure",
        "pass": unrelated_ok and had_h4_cached and cached_decision_made and flow_installed,
        "had_h4_cached_before_kill": had_h4_cached,
        "unrelated_traffic_ok_after_kill": unrelated_ok,
        "cached_decision_made_after_kill": cached_decision_made,
        "flow_installed_after_kill": flow_installed,
    }


# --- G6: partition then reconnect/reconverge ---

def gate_g6(net, nodes):
    # Node 4 accepts all three peer connections and never dials, so blocking
    # its listening port isolates every peer-state path to it.  Node 3 would
    # be an invalid target here because its outbound connection to node 4
    # does not traverse node 3's listening port.
    target_node = 4
    source_node = 2
    target_port = nodes[target_node].peer_port
    rule = ["sudo", "iptables", "-A", "INPUT", "-p", "tcp", "--dport",
            str(target_port), "-j", "DROP"]
    delete_rule = rule.copy()
    delete_rule[2] = "-D"
    subprocess.run(rule, check=True)
    partition_start = time.monotonic()
    try:
        # Force a source-node table miss so a fresh versioned update is sent
        # while the path is blocked.  This also puts unacknowledged data on
        # the dialer's socket, allowing TCP_USER_TIMEOUT to detect the DROP.
        subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "del-flows", "s2", "in_port=1"],
                       check=False)
        source_traffic = net.get("h2").cmd(f"ping -c 1 -W 1 {HOST_IPS[1]}")
        disconnect_seen = nodes[source_node].wait_for(
            lambda e: e.get("event") == "lifecycle" and e.get("name") == "disconnected"
            and e.get("recorded_monotonic_s", 0) > partition_start,
            timeout_s=15,
        )
        detection_s = time.monotonic() - partition_start if disconnect_seen else None
        target_knows_during_partition = bool(nodes[target_node].events_matching(
            lambda e: e.get("event") == "apply" and e.get("origin_node_id") == source_node
            and e.get("recorded_monotonic_s", 0) > partition_start
        ))
    finally:
        subprocess.run(delete_rule, check=False)

    reconnect_start = time.monotonic()
    reconnected = nodes[target_node].wait_for(
        lambda e: e.get("event") == "lifecycle" and e.get("name") == "connected"
        and e.get("recorded_monotonic_s", 0) > reconnect_start,
        timeout_s=30,
    )
    # snapshot_done only confirms the snapshot mechanism engaged (part of
    # `pass`, below) -- it does NOT define reconvergence_time_s. snapshot_sent
    # means the target *served* its own snapshot to the peer, which says
    # nothing about whether the target itself has caught up; snapshot_received
    # means the target *imported* one, which may still race with the source's
    # own post-partition update settling (see the timing note below). Neither
    # is "target now holds the state it missed", which is what reconvergence
    # is supposed to mean.
    snapshot_done = nodes[target_node].wait_for(
        lambda e: e.get("event") == "lifecycle" and e.get("name") in ("snapshot_received", "snapshot_sent")
        and e.get("recorded_monotonic_s", 0) > reconnect_start,
        timeout_s=30,
    )

    # wait_for, not an instantaneous events_matching check: the source's
    # entry can arrive via live dissemination shortly *after* the snapshot
    # exchange itself completes with zero new entries (e.g. if the
    # snapshot request landed before the source's post-partition update
    # had settled) -- confirmed as a real race by a 30-repetition campaign
    # (1/30: snapshot_done fired, reconvergence_time_s recorded, but this
    # check ran before the apply event landed). Every other lifecycle
    # marker in this function already waits; this one should too.
    # This IS the reconvergence signal: the target's own log showing it
    # actually applied the state it missed during the partition, whether
    # that arrived via snapshot import or live dissemination -- not merely
    # that some snapshot-phase event fired. reconvergence_time_s is timed
    # from this event's own recorded timestamp, not from when this wait_for
    # call happens to return, so polling granularity doesn't inflate it.
    target_knows_after_event = nodes[target_node].wait_for(
        lambda e: e.get("event") == "apply" and e.get("origin_node_id") == source_node
        and e.get("recorded_monotonic_s", 0) > reconnect_start,
        timeout_s=10,
    )
    target_knows_after = bool(target_knows_after_event)
    reconvergence_s = (
        (target_knows_after_event["recorded_monotonic_s"] - reconnect_start)
        if target_knows_after_event else None
    )

    return {
        "gate": "G6", "description": "partition detected, then reconnect + snapshot convergence",
        "pass": bool(disconnect_seen) and not target_knows_during_partition
        and bool(reconnected) and bool(snapshot_done) and target_knows_after,
        "detection_time_s": detection_s,
        "reconvergence_time_s": reconvergence_s,
        "did_not_falsely_know_during_partition": not target_knows_during_partition,
        "knows_after_reconvergence": target_knows_after,
        "source_traffic_during_partition_output": source_traffic,
    }


# --- G8: ownership conflict (two nodes claim the same MAC) ---
#
# Explicitly opt-in (--include-g8): a real duplicate-MAC scenario, not a
# hand-crafted wire message. h1 and h4 -- opposite ends of the chain -- are
# both given the same MAC (an operator/config error or a spoofed duplicate,
# the kind of thing this mechanism must survive, not something the
# transport crafts synthetically). Each end's own node independently
# learns and claims the MAC as local via the real learn -> disseminate
# path (host_port filtering, daim_distributed_learning_app.c, means
# neither transit traffic nor the other end's claim is ever mistaken for a
# local one). The two claims race across the mesh; this proves the
# existing detect-only OWNERSHIP_CONFLICT path (apply_locked() in
# daim_distributed_state.c) rejects the losing claim outright on every
# node that sees both, rather than silently overwriting the winner or
# corrupting the OpenFlow table. Conflict *resolution* is explicitly out
# of scope -- daim_distributed_state.h documents detection-only by design.

CONFLICT_MAC = "02:00:00:00:99:99"


def _set_host_mac(host, mac):
    intf = host.defaultIntf().name
    host.cmd(f"ip link set {intf} down")
    host.cmd(f"ip link set {intf} address {mac}")
    host.cmd(f"ip link set {intf} up")


def _dump_flows_for(mac):
    """{switch_name: [flow lines mentioning dl_dst=mac]}, across all
    N_NODES switches -- same ovs-ofctl dump-flows pattern gate_g5 already
    uses to verify installed forwarding decisions."""
    out = {}
    for i in range(1, N_NODES + 1):
        dump = subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "dump-flows", f"s{i}"],
                              capture_output=True, text=True).stdout
        out[f"s{i}"] = [line.strip() for line in dump.splitlines() if f"dl_dst={mac}" in line]
    return out


def _flow_in_out_port(flow_line):
    """(in_port, out_port) for one ovs-ofctl dump-flows line matching
    dl_dst=<contested MAC>. A switch legitimately holds one such flow per
    distinct in_port (a decision for traffic arriving from that
    direction) -- e.g. s2 between h1 and h4 can hold both "from s3 side"
    and "from h2 itself" decisions for the same destination MAC
    simultaneously; that is not corruption. What must never happen is two
    of these decisions disagreeing on where to route the same MAC."""
    in_port_m = re.search(r"in_port=(\d+)", flow_line)
    out_port_m = re.search(r"actions=output:(\d+)", flow_line)
    return (int(in_port_m.group(1)) if in_port_m else None,
            int(out_port_m.group(1)) if out_port_m else None)


def gate_g8(net, nodes):
    h1, h2, h3, h4 = net.get("h1", "h2", "h3", "h4")
    # Restored in `finally` below: G5 runs after this gate and identifies
    # h4 by the MAC it cached during G1, so h1/h4 must return to their
    # original MACs regardless of how this gate finishes, not just on the
    # success path.
    h1_orig_mac, h4_orig_mac = h1.MAC(), h4.MAC()

    try:
        for sw in range(1, N_NODES + 1):
            subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "del-flows", f"s{sw}",
                            f"dl_dst={CONFLICT_MAC}"], check=False,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        _set_host_mac(h1, CONFLICT_MAC)
        _set_host_mac(h4, CONFLICT_MAC)
        time.sleep(0.3)  # let the interface bounce settle before generating traffic

        conflict_start = time.monotonic()

        # h1 claims first: node1 sees CONFLICT_MAC arrive on its own host
        # port (HOST_PORT=1), which is exactly what daim_host_learn_local's
        # host_port filter requires -- and disseminates it as origin=1.
        # Local learns never produce an "apply" event on the learning node
        # itself (that callback only fires for received *remote* updates,
        # see G1's gate -- it never checks node4 for its own origin=4
        # apply events either); "no_rule" with mac_src/in_port is the
        # observable proxy for a local learn, same event G5 uses for
        # decision evidence.
        h1.cmd(f"ping -c 1 -W 1 {HOST_IPS[3]}")
        node1_claimed = nodes[1].wait_for(
            lambda e: e.get("event") == "no_rule" and e.get("mac_src") == CONFLICT_MAC
            and e.get("in_port") == 1,
            timeout_s=10,
        )
        flows_before = _dump_flows_for(CONFLICT_MAC)

        # h4 claims second: node4 learns the SAME MAC as its own (origin=4)
        # and disseminates it -- the conflicting claim every other node
        # (and node1 itself, on receipt) must reject.
        h4.cmd(f"ping -c 1 -W 1 {HOST_IPS[2]}")
        node4_claimed = nodes[4].wait_for(
            lambda e: e.get("event") == "no_rule" and e.get("mac_src") == CONFLICT_MAC
            and e.get("in_port") == 1,
            timeout_s=10,
        )

        # Every node in a full mesh should see both claims: node1 and node4
        # each reject the *other's* remote claim against their own local
        # one, and node2/node3 (bystanders with no local claim of their
        # own) reject whichever of the two remote claims arrives second.
        # So the expected observing set is all N_NODES, not merely "at
        # least one" -- the gate's own description above says "every node
        # that sees both", and the oracle should actually enforce that.
        expected_conflict_nodes = set(range(1, N_NODES + 1))
        conflict_seen = set()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and conflict_seen != expected_conflict_nodes:
            for i in range(1, N_NODES + 1):
                if i in conflict_seen:
                    continue
                if nodes[i].events_matching(
                    lambda e: e.get("event") == "apply" and e.get("mac") == CONFLICT_MAC
                    and e.get("result") == "ownership_conflict"
                    and e.get("recorded_monotonic_s", 0) > conflict_start
                ):
                    conflict_seen.add(i)
            time.sleep(0.05)
        all_expected_nodes_observed_conflict = conflict_seen == expected_conflict_nodes
        conflict_seen = sorted(conflict_seen)

        time.sleep(0.5)  # let dissemination fully settle before inspecting flows
        flows_after = _dump_flows_for(CONFLICT_MAC)

        # No unsafe rule: every flow decision for this contested MAC on a
        # given switch -- regardless of how many distinct in_ports hold
        # one -- must route toward the same output port (the one accepted
        # claim's direction). No two decisions may disagree about who
        # owns the MAC.
        after_pairs = {sw: [_flow_in_out_port(f) for f in flows] for sw, flows in flows_after.items()}
        no_duplicate_flows = all(
            len({out for (_in, out) in pairs}) <= 1 for pairs in after_pairs.values()
        )
        # The rejected claim must not have overwritten any decision that
        # already existed before it arrived (new decisions, for in_ports
        # not previously exercised -- e.g. the unrelated-traffic check
        # below -- are expected and fine; regressing an existing one is
        # not).
        before_pairs = {sw: dict(map(_flow_in_out_port, flows)) for sw, flows in flows_before.items()}
        flows_unchanged_by_rejected_claim = all(
            before_pairs[sw].items() <= dict(after_pairs.get(sw, [])).items()
            for sw in before_pairs
        )

        # Unrelated traffic (h2 <-> h3, neither host touched by the
        # conflict) must be unaffected -- same check style as gate_g5.
        unrelated_ping = h2.cmd(f"ping -c 2 -W 1 {HOST_IPS[3]}")
        unrelated_ok = "0% packet loss" in unrelated_ping
    finally:
        _set_host_mac(h1, h1_orig_mac)
        _set_host_mac(h4, h4_orig_mac)

    return {
        "gate": "G8",
        "description": "ownership conflict (two nodes claim the same MAC) is detected and "
                        "rejected on every node that sees both, not silently resolved or "
                        "corrupted -- detection only, resolution out of scope",
        "pass": bool(node1_claimed) and bool(node4_claimed) and all_expected_nodes_observed_conflict
        and no_duplicate_flows and flows_unchanged_by_rejected_claim and unrelated_ok,
        "conflict_mac": CONFLICT_MAC,
        "node1_claimed_locally": bool(node1_claimed),
        "node4_claimed_locally": bool(node4_claimed),
        "nodes_observing_conflict": conflict_seen,
        "all_expected_nodes_observed_conflict": all_expected_nodes_observed_conflict,
        "no_duplicate_flows_installed": no_duplicate_flows,
        "flows_unchanged_by_rejected_claim": flows_unchanged_by_rejected_claim,
        "unrelated_traffic_ok": unrelated_ok,
        "flows_before": flows_before,
        "flows_after": flows_after,
    }


def checksum_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-g8", action="store_true",
                        help="Also run G8 (ownership conflict) after G1-G6. "
                             "Off by default so the original G1-G6 run stays reproducible.")
    return parser.parse_args()


def main():
    args = parse_args()
    global RUN_ID
    if args.include_g8:
        RUN_ID = f"paper2-g1-g8-{RUN_TIMESTAMP}"
    setLogLevel("warning")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    net = None
    nodes = {}
    results = {}
    fatal_error = None
    try:
        subprocess.run(["mn", "-c"], check=False, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        subprocess.run(["pkill", "-9", "-f", str(CONTROLLER_APP)], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.3)

        net = Mininet(topo=ChainTopo(), controller=None, switch=OVSSwitch,
                      link=TCLink, autoSetMacs=True)
        net.start()

        # Cluster bring-up (4 eventlet+native-pthread processes plus
        # Mininet/OVS, confirmed via isolated single-node testing to
        # otherwise succeed in ~5s) was observed to occasionally stall
        # under full contention on this VM's 4 vCPUs -- a resource-
        # contention symptom, not a reproducible deadlock (the same node
        # in isolation with the same peer count always became ready).
        # Retried, not silently one-shot: each attempt and its failure (if
        # any) is recorded as evidence, not discarded.
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                nodes = start_cluster()
                break
            except RuntimeError as exc:
                record_raw("gate_driver", {
                    "event": "cluster_start_attempt_failed",
                    "attempt": attempt, "max_attempts": max_attempts, "detail": str(exc),
                })
                subprocess.run(["pkill", "-9", "-f", str(CONTROLLER_APP)], check=False,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(1.0)
                if attempt == max_attempts:
                    raise

        print("running G1 (propagation)...", flush=True)
        results["G1"] = gate_g1(net, nodes)
        print("running G2 (duplicate rejection)...", flush=True)
        results["G2"] = gate_g2(nodes)
        print("running G3 (stale rejection)...", flush=True)
        results["G3"] = gate_g3(nodes)
        print("running G4 (epoch ordering)...", flush=True)
        results["G4"] = gate_g4(nodes)
        print("running G6 (partition/reconvergence)...", flush=True)
        results["G6"] = gate_g6(net, nodes)
        if args.include_g8:
            # Must run before G5: G5 deliberately kills node4 to test
            # failure isolation, and never restarts it, so G8 (which needs
            # every node alive to claim/disseminate) has to run on the
            # still-fully-live cluster.
            print("running G8 (ownership conflict)...", flush=True)
            results["G8"] = gate_g8(net, nodes)
        print("running G5 (failure isolation)...", flush=True)
        results["G5"] = gate_g5(net, nodes)
    except Exception as exc:  # Preserve negative/failed runs as evidence.
        fatal_error = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        record_raw("gate_driver", {"event": "fatal_error", **fatal_error})
    finally:
        if nodes:
            stop_cluster(nodes)
        if net is not None:
            net.stop()
        subprocess.run(["mn", "-c"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    raw_path = RESULTS_DIR / f"raw_events_{RUN_TIMESTAMP}.jsonl"
    with raw_path.open("w") as handle:
        for event in _all_raw_events:
            handle.write(json.dumps(event) + "\n")

    results_path = RESULTS_DIR / f"gate_results_{RUN_TIMESTAMP}.json"
    results_path.write_text(json.dumps(results, indent=2) + "\n")

    manifest = {
        "run_timestamp": RUN_TIMESTAMP,
        "run_id": RUN_ID,
        "random_seed": RANDOM_SEED,
        "n_nodes": N_NODES,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "kernel": platform.release(),
        },
        "all_gates_passed": fatal_error is None and len(results) == (7 if args.include_g8 else 6)
        and all(r["pass"] for r in results.values()),
        "gate_pass": {g: r["pass"] for g, r in results.items()},
        "fatal_error": fatal_error,
        "files": {
            str(p.relative_to(RESULTS_DIR)): checksum_file(p)
            for p in (raw_path, results_path)
        },
    }
    manifest_path = RESULTS_DIR / f"manifest_{RUN_TIMESTAMP}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
