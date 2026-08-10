#!/usr/bin/env python3
"""Stage-latency breakdown for the reactive Packet-In path (contrast with
stage2_full_compare.py, which measures proactive per-switch rule
installation). Extends the qualitative two-switch Packet-In smoke test
(STAGE_PACKETIN_BRIDGE_REPORT.md) with a repeated, decomposed, randomised-
order latency measurement, per external TNSM review feedback (manuscript
Section 8: "The Packet-In experiment is a functional acceptance test... not
a latency distribution"; "matched modes were run sequentially rather than
randomised").

One switch, three hosts (h1, h2, h3). Each repetition:
  1. h3 pings h2 once, untimed -- teaches DAIM Core h2's (mac, port)
     without touching h1's switch port.
  2. h1 pings h2 once -- the ICMP echo request is the first-ever packet on
     h1's port (table miss) with an already-known destination, so it
     deterministically triggers exactly one NO_RULE decision and flow
     install. daim_bridge_controller.py matches this event by parsed IPv4
     destination and reports per-stage timestamps as one JSON line.

30 repetitions per mode (DAIM process-per-rule, DAIM persistent, and a matched
DAIM-free reactive Os-Ken learning switch), 90 trials total, run in one
shuffled (seeded) order so mode is not confounded with host/VM drift.
"""
import csv
import json
import os
import random
import subprocess
import threading
import time
from pathlib import Path

from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import OVSSwitch
from mininet.topo import Topo

ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_APP = ROOT / "network/daim_bridge_controller.py"
REACTIVE_BASELINE_APP = ROOT / "network/osken_reactive_baseline_controller.py"
RAW = ROOT / "results/network/packetin_latency_breakdown_raw.csv"
MODES = os.environ.get(
    "PACKETIN_MODES", "process_per_rule,persistent,reactive_osken"
).split(",")
REPETITIONS = int(os.environ.get("PACKETIN_REPETITIONS", "30"))
SEED = 20260719
OFP_PORT = 6653
PERSISTENT_BASE_PORT = 17200
READY_TIMEOUT_S = 20
TIMING_TIMEOUT_S = 10
H1_IP, H2_IP, H3_IP = "10.0.0.1", "10.0.0.2", "10.0.0.3"


class SingleSwitchTopo(Topo):
    def build(self):
        switch = self.addSwitch("s1", protocols="OpenFlow13", failMode="secure")
        h1 = self.addHost("h1", ip=f"{H1_IP}/24")
        h2 = self.addHost("h2", ip=f"{H2_IP}/24")
        h3 = self.addHost("h3", ip=f"{H3_IP}/24")
        for host in (h1, h2, h3):
            self.addLink(host, switch)


def read_json_lines_until(proc, watchdog_fired, predicate, timeout_s):
    """Blocking readline() loop (not select(), for the same buffering reason
    documented in stage2_full_compare.py's run_direct_osken), bounded by a
    watchdog thread that kills the controller process on timeout."""
    deadline = time.monotonic() + timeout_s
    events = []
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            events.append({"event": "raw_output", "line": line})
            continue
        events.append(event)
        if predicate(event):
            return event, events
    return None, events


def run_trial(mode, repetition, persistent_port):
    subprocess.run(["mn", "-c"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["pkill", "-9", "-f", str(CONTROLLER_APP)], check=False,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["pkill", "-9", "-f", str(REACTIVE_BASELINE_APP)], check=False,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.2)

    env = dict(os.environ)
    env["DAIM_ADAPTER_MODE"] = mode
    env["DAIM_TARGET_IP"] = H2_IP
    if mode == "persistent":
        env["DAIM_PERSISTENT_PORT"] = str(persistent_port)

    controller_app = REACTIVE_BASELINE_APP if mode == "reactive_osken" else CONTROLLER_APP
    controller = subprocess.Popen(
        ["osken-manager", str(controller_app), "--ofp-tcp-listen-port", str(OFP_PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env,
    )
    net = None
    watchdog_fired = threading.Event()

    def kill_after_timeout():
        if not watchdog_fired.wait(READY_TIMEOUT_S + TIMING_TIMEOUT_S + 15):
            controller.kill()

    watchdog = threading.Thread(target=kill_after_timeout, daemon=True)
    watchdog.start()

    try:
        net = Mininet(topo=SingleSwitchTopo(), controller=None, switch=OVSSwitch,
                      link=TCLink, autoSetMacs=True)
        net.start()

        controller_targets = [f"tcp:127.0.0.1:{OFP_PORT}"]
        if mode == "persistent":
            controller_targets.append(f"tcp:127.0.0.1:{persistent_port}")
        subprocess.run(["ovs-vsctl", "set-controller", "s1", *controller_targets], check=True)

        ready, ready_events = read_json_lines_until(
            controller, watchdog_fired,
            lambda e: e.get("event") in ("ready", "adapter_error"),
            READY_TIMEOUT_S,
        )
        if ready is None:
            raise RuntimeError(
                f"{mode} rep {repetition}: controller never became ready; "
                f"last output={ready_events[-12:]}"
            )
        if ready.get("event") == "adapter_error":
            raise RuntimeError(f"{mode} rep {repetition}: adapter_error: {ready.get('detail')}")
        time.sleep(0.15)  # grace period: table-miss FlowMod applied on the switch

        h1, h2, h3 = net.get("h1", "h2", "h3")
        prime = h3.cmd(f"ping -c 2 -W 1 {H2_IP}")
        prime_ok = "0% packet loss" in prime or "1 packets transmitted, 2 received" in prime

        timed_ping = h1.cmd(f"ping -c 1 -W 1 {H2_IP}")
        ping_ok = "0% packet loss" in timed_ping

        timing, _ = read_json_lines_until(
            controller, watchdog_fired, lambda e: e.get("event") == "timing", TIMING_TIMEOUT_S,
        )
        watchdog_fired.set()
        if timing is None:
            raise RuntimeError(f"{mode} rep {repetition}: no timing event reported")

        row = {
            "mode": mode,
            "repetition": repetition,
            "priming_ok": int(bool(prime_ok)),
            "ping_success": int(bool(ping_ok)),
            "evidence_level": "measured_emulation",
        }
        row.update({k: v for k, v in timing.items() if k != "event"})
        return row
    finally:
        watchdog_fired.set()
        if net:
            net.stop()
        if controller.poll() is None:
            controller.terminate()
            try:
                controller.wait(timeout=5)
            except subprocess.TimeoutExpired:
                controller.kill()
        subprocess.run(["mn", "-c"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    setLogLevel("warning")
    RAW.parent.mkdir(parents=True, exist_ok=True)

    trials = [(mode, rep) for mode in MODES for rep in range(1, REPETITIONS + 1)]
    rng = random.Random(SEED)
    rng.shuffle(trials)

    rows = []
    persistent_port_counter = 0
    for order, (mode, repetition) in enumerate(trials, start=1):
        persistent_port = None
        if mode == "persistent":
            persistent_port = PERSISTENT_BASE_PORT + persistent_port_counter
            persistent_port_counter += 1
        print(f"trial {order}/{len(trials)}: mode={mode} repetition={repetition}", flush=True)
        row = run_trial(mode, repetition, persistent_port)
        row["trial_order"] = order
        rows.append(row)

    fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with RAW.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {RAW}")


if __name__ == "__main__":
    main()
