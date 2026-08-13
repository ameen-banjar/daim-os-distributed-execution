# DAIM-OS Distributed Execution Artifact

Reproducibility artifact for the paper:

**"Distributing the DAIM-OS Table-and-Signal Contract: Independently Executing
Per-Switch Cores, Explicit State Semantics, and a Statistically Validated
Evaluation"** — Ameen Banjar (2026). Paper 2 in the DAIM six-paper programme.

This repository contains only the code, raw data, logs, and figures cited by
that paper. It is deliberately scoped to that single paper: the author's
related work on autonomous link recovery, cross-environment reproducibility,
policy-conflict resolution, and intent assurance are separate, independent
contributions with their own artifacts, not included here.

## v1.2.0 (2026-08-13): two further defects found and fixed

A second, independent code-level review of `v1.1.0` found and fixed two more real defects:

1. **The deferred-update queue could still silently drop an update on overflow.**
   `v1.1.0`'s fix for the connection-setup race (below) queued a live
   `HOST_LOCATION_UPDATE` arriving mid-setup in a small bounded buffer
   (`MAX_DEFERRED_UPDATES`, 16 entries) — but if a 17th update arrived
   before the buffer drained, it was silently dropped, reproducing exactly
   the data-loss failure mode the queue was built to close. Fixed by
   aborting the connection on overflow instead: the dialing side's own
   pre-existing reconnect loop (bounded exponential backoff, already
   required for ordinary link loss) turns that into a fresh reconnect and
   snapshot pull, and by the time that happens the peer that sent the
   overflowing update has already applied it to its own local table, so
   the new snapshot carries it as ordinary state rather than losing it. A
   dedicated unit test (`test_peer_transport.c`) exercises this path
   directly with a raw-socket peer that withholds `STATE_SNAPSHOT_END`
   while sending seventeen updates.
2. **G7's `convergence_s` excluded the very work it was timing.** Peer
   dialing could start, and a small cluster could even fully converge,
   *during* `build_cluster`'s own staged per-node launch — before the
   harness's clock started, since nothing gated when dialing began
   relative to when timing began. This silently understated convergence
   time by an amount that grew with cluster size (the staged launch's own
   duration is `0.5 s × N`), manufacturing an apparent scaling plateau at
   N=16–32 out of a measurement artefact. Fixed with a barrier: node
   processes now hold every outbound `add_peer` call behind a
   `DAIM_PEER_BARRIER_FILE` the harness creates only once every node is
   already OpenFlow-ready, so "peers start dialing" and "the clock starts"
   are the same instant. The corrected campaign shows no plateau at all —
   convergence time grows smoothly and monotonically across N=2..32.

Both are narrated in full in the paper's Section 6.10. All G7 evidence in
`results/` was re-generated after both fixes (G1–G6/G8 were also re-run to
confirm no regression from the queue-overflow fix); pre-v1.2.0 evidence
remains on the `v1.1.0` and `v1.0.0` releases/tags.

## v1.1.0 (2026-08-12): two additional defects found and fixed

A post-v1.0.0 code audit of the transport and instrumentation layers found and
fixed two further real defects, on top of the four `v1.0.0` already reported:

1. **G1 propagation latency measured nothing.** The callback reporting each
   applied update received the raw struct decoded off the wire, and for a
   freshly learned local record the *origin* sets `applied_at_ns` equal to
   `learned_at_ns` before ever sending it — so every downstream latency
   computation (`applied_at_ns - learned_at_ns`) subtracted a value from
   itself, always exactly zero regardless of true delay. Fixed by stamping
   `applied_at_ns` with the *receiver's* own `monotonic_ns()` immediately
   before the callback fires (`daim_peer_transport.c`). Every
   propagation-latency number in the paper was re-measured after this fix.
2. **A live update could race past its own connection's handshake.**
   `daim_peer_transport_disseminate` selected send targets by a connection's
   `active` flag alone, set the instant a socket was accepted — before the
   HELLO handshake or snapshot exchange even began — and `exchange_snapshots`
   silently discarded any unexpected message type (`HOST_LOCATION_UPDATE`
   included) that arrived during that window. Fixed with a second `ready`
   flag, gated dissemination, and a bounded queue that applies any
   early-arriving update once the connection's own snapshot phase completes,
   instead of dropping it.

Both are narrated in full, with the exact code paths and fixes, in the
paper's Sections 6.8–6.9. All G1/G6/G7/G8 evidence in `results/` was
re-generated after both fixes; the pre-fix v1.0.0 evidence is not included
here (it remains on the `v1.0.0` release/tag for anyone who wants it).

## Relationship to Paper 1 and the DAIM-OS specification

This artifact **extends, without modifying**, the C Core reconstructed by
Paper 1 of this programme:

- Paper 1 artifact: https://github.com/ameen-banjar/daim-os-packetin-artifact
  (DOI: https://doi.org/10.5281/zenodo.21855229)
- Paper 1 manuscript: "Reconstructing the DAIM-OS Table-and-Signal Control
  Path" — under review, *Computer Networks*, manuscript COMNET-S-26-07153.
- DAIM-OS specification: https://github.com/ameen-banjar/DAIM-OS
  (DOI: https://doi.org/10.5281/zenodo.21426560)

`implementation/` here includes the base Core, adapters, and single-Core
learning application unchanged from Paper 1 (needed to build this artifact
standalone) *plus* four new modules this paper adds:
`daim_distributed_state`, `daim_peer_protocol`, `daim_peer_transport`, and
`daim_distributed_learning_app`. A pinned, vendored copy of the DAIM-OS
specification headers is included under `vendor/DAIM-OS-v1.0.0/` for the
same reason Paper 1's artifact vendors them: so this repository still builds
even if the specification repository changes in the future.

`network/` similarly includes both the new distributed-node/gate/campaign
scripts this paper introduces *and* three files reused unmodified from
Paper 1's artifact as an explicit baseline (`daim_bridge_controller.py`,
`daim_core_bridge.py`, `packetin_latency_breakdown.py`,
`osken_reactive_baseline_controller.py`) — see "Single-Core baseline" below.

## What is in this repository

- `implementation/` — the C core (inherited from Paper 1, unmodified) plus
  four new modules: `daim_distributed_state` (host-location table with
  `origin_node_id`/`owner_epoch`/`sequence` ordering and ownership-conflict
  detection), `daim_peer_protocol` (wire format), `daim_peer_transport`
  (persistent, non-redundant peer connections and symmetric snapshot
  exchange), and `daim_distributed_learning_app` (the `host_port`-filtered
  NO_RULE handler). Unit/concurrency tests for all four, including a
  peer-count boundary test and an ownership-conflict-via-snapshot-import
  test.
- `network/` —
  - `daim_distributed_node.py`, `daim_distributed_controller.py` — the
    per-node ctypes bridge and Os-Ken controller, one process per switch.
  - `daim_synthetic_peer.py` — an independent Python reimplementation of the
    wire protocol, used to adversarially script the duplicate/stale/epoch
    gates against the real C implementation.
  - `distributed_prototype_gate.py` — the eight-gate harness (G1
    propagation, G2 duplicate, G3 stale, G4 epoch ordering, G5 node-failure
    isolation, G6 partition/reconvergence, G8 ownership conflict; G8 is
    opt-in via `--include-g8`).
  - `scaling_prototype_gate.py` — G7 (node-count scaling, N = 2..32), with
    an optional `--repetitions` flag for the statistical campaign.
  - `gate_no_exchange.py` — the no-exchange baseline (same topology, empty
    peer list).
  - `campaign_distributed.py`, `campaign_single_core.py` — the 30-repetition
    statistical-campaign wrappers for the distributed mode and the
    single-Core baseline respectively.
  - `daim_bridge_controller.py`, `daim_core_bridge.py`,
    `packetin_latency_breakdown.py`, `osken_reactive_baseline_controller.py`
    — reused unmodified from Paper 1's artifact; `campaign_single_core.py`
    imports `run_trial` from `packetin_latency_breakdown.py` directly rather
    than reimplementing its nanosecond-precision Packet-In timing capture.
- `environment/` — the Lima/QEMU VM manifest and provisioning script.
  `daim-lab-qemu.yaml` reflects the **final** allocation (6 vCPU/10 GiB) the
  reported campaign results used; the paper's Section 6.6 documents that an
  earlier 4 vCPU/6 GiB allocation was tested and ruled out as the cause of a
  scaling failure before the real root cause (an eventlet/native-thread
  hazard, fixed in `daim_distributed_node.py`) was found -- included for
  that reason, not because more CPU was the fix.
- `results/network/distributed/` — every gate and campaign run's raw
  evidence: `DISTRIBUTED_GATE_REPORT.md` and `PAPER2_EVIDENCE_CAMPAIGN.md`
  (the two narrative reports the paper's Results section is drawn from
  directly), per-run `raw_events*.jsonl` (gzip-compressed above 2 MB --
  `gunzip` or `zcat` before use), `gate_results*.json`,
  `campaign_*.json`, `scaling_summary*.{json,csv}`, and `manifest_*.json`
  files with SHA-256 checksums of their companion outputs. Failed
  repetitions (including the pre-fix 29/30 campaign run Section 6.7 of the
  paper discusses) are retained, not deleted.

## Reproducing the results

1. Build and test the C layer (macOS or Linux; both are exercised in the
   paper):
   ```sh
   cd implementation
   make check      # strict build + unit/concurrency tests
   make tsan        # ThreadSanitizer build + tests
   make all         # also builds libdaim_core.so / libdaim_distributed.so
   ```
2. Provision a Mininet/OVS/Os-Ken environment. `environment/provision_ubuntu.sh`
   is the exact script used (see `environment/daim-lab-qemu.yaml` for the
   Lima VM manifest); `environment/controller_requirements.txt` pins the
   Python/Os-Ken side.
3. Run the eight-gate single-trial harness:
   ```sh
   cd network
   sudo env "PATH=$PATH" python3 distributed_prototype_gate.py --include-g8
   ```
4. Run the statistical campaigns (30 repetitions each; the G7 sweep alone is
   150 independent cluster bring-ups and can take on the order of an hour):
   ```sh
   sudo env "PATH=$PATH" python3 campaign_distributed.py
   sudo env "PATH=$PATH" python3 gate_no_exchange.py --repetitions 30
   sudo env "PATH=$PATH" python3 campaign_single_core.py
   sudo env "PATH=$PATH" python3 scaling_prototype_gate.py --repetitions 30
   ```
   Each writes its own timestamped raw-evidence/manifest triple under
   `results/network/distributed/` (or wherever `RESULTS_DIR` resolves to
   when run from a fresh checkout -- see each script's `RESULTS_DIR`
   constant).

## Single-Core baseline

The single-Core baseline (Table 5, Section 6.5 of the paper) reuses Paper
1's own `daim_bridge_controller.py`/`daim_core_bridge.py` and its
established nanosecond-precision Packet-In timing methodology
(`packetin_latency_breakdown.py`, `run_trial`) unmodified, rather than
reimplementing an equivalent measurement. It intentionally reuses Paper 1's
single-switch/three-host topology rather than this paper's four-switch
chain -- the paper's Section 8 (Threats to Validity) states why and treats
it as an explicit scope caveat, not a hidden mismatch.

## Data dictionary

See the paper's Sections 5-6 for the exact meaning of every field in the
JSON/JSONL outputs (e.g. `origin_node_id`, `owner_epoch`, `sequence`,
`applied_at_ns`/`learned_at_ns`, `detection_time_s`, `reconvergence_time_s`).
`DISTRIBUTED_GATE_REPORT.md` and `PAPER2_EVIDENCE_CAMPAIGN.md` in
`results/network/distributed/` narrate every number the paper cites, with
pointers to the exact source file for each.

## Manuscript

The manuscript itself is not included in this repository (it is not yet
published). This repository is cited by, and archives the evidence behind,
the paper referenced at the top of this file.
