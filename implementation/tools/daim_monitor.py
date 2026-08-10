#!/usr/bin/env python3
"""Minimal independent DAIM monitor for OVS fault experiments.

The monitor owns detection, decision, and actuation timestamps. It does not
depend on the experiment process to call the repair function.
"""
import argparse, json, subprocess, time

def link_down(interface):
    p=subprocess.run(["ovs-vsctl","get","Interface",interface,"link_state"],text=True,capture_output=True)
    return p.returncode == 0 and "down" in p.stdout

def run(args):
    started=time.perf_counter_ns(); detected=None; decided=None; acted=None
    while time.perf_counter()-started < args.timeout:
        if link_down(args.interface):
            detected=time.perf_counter_ns()
            decided=time.perf_counter_ns()       # deterministic DAIM policy decision
            p=subprocess.run([args.repair],text=True,capture_output=True)
            acted=time.perf_counter_ns()
            if p.returncode:
                raise SystemExit(p.stderr.strip() or "repair command failed")
            break
        time.sleep(args.poll_ms/1000.0)
    result={"evidence_level":"measured_emulation_autonomous","interface":args.interface,
            "detected":detected is not None,"failure_to_detection_us":(detected-started)/1000 if detected else None,
            "decision_us":(decided-detected)/1000 if decided else None,
            "actuation_us":(acted-decided)/1000 if acted else None,
            "total_us":(acted-started)/1000 if acted else None}
    print(json.dumps(result));
    if args.output: open(args.output,"w").write(json.dumps(result,indent=2)+"\n")
    return 0 if detected else 2

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--interface",required=True); p.add_argument("--repair",required=True); p.add_argument("--output"); p.add_argument("--timeout",type=float,default=10); p.add_argument("--poll-ms",type=float,default=5); raise SystemExit(run(p.parse_args()))
