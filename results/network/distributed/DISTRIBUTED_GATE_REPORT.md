# Distributed DAIM-OS — Milestone 1 Prototype Gate Report

> **SUPERSEDED — PRE-FIX NUMBERS, NOT THE REPORTED RESULTS.** Written against the `v1.0.0` code
> state and never regenerated across the `v1.1.0`/`v1.2.0` fix rounds; numbers below (e.g.
> propagation latency reported as "0.0 ms") reflect defects the manuscript's Sections 6.8–6.10
> describe finding and fixing. Kept unmodified per this repository's no-delete evidence-retention
> discipline. **For the current, reported results, use the paper's own Tables 1–4 and Sections
> 6.2–6.5, 6.10, or the latest timestamped `gate_results_*.json`/`manifest_*.json` files in this
> directory (sort by filename timestamp).**

Date: 9–10 August 2026
Environment: Ubuntu 24.04 ARM64 (`daim-lab-qemu`), Open vSwitch 3.3.4, Mininet 2.3.0, Os-Ken 2.6.0.
G1–G6 were first proven on the VM's original 4 vCPU/6GiB allocation; the final G1–G8 runs below
used an upgraded 6 vCPU/10GiB allocation, noted because it was tested as a candidate fix for the
G7 failure described below and found to make no difference (see "Three significant bugs found").
Evidence level: `measured_emulation` (up to 32 independent OS processes, real Mininet/OVS, real
fault injection)
Passing runs: `run_id = paper2-g1-g8-20260810T101545Z` (G1–G6 + G8, 4 nodes),
`run_id = paper2-g7-scaling-20260810T095229Z` (G7, N=2/4/8/16/32)

## Status: all eight gates pass

```
G1  valid update propagates between 4 independent processes   PASS
G2  duplicate update rejected                                 PASS
G3  stale (lower-sequence, same-epoch) update rejected        PASS
G4  newer epoch accepted, older epoch rejected despite seq    PASS
G5  surviving nodes keep deciding during a peer failure       PASS
G6  partition detected, then reconnect + snapshot convergence PASS
G7  cluster converges and propagates correctly at N=2/4/8/16/32 PASS
G8  ownership conflict detected and rejected, not corrupted   PASS
```

This is the mechanism gate defined for Milestone 1 (see the plan in
`/Users/abanjar/.claude/plans/pure-foraging-pinwheel.md`), **not** a Paper 2
submission result. Statistical campaigns (30+ trials per condition beyond G1's
propagation-latency sample) remain out of scope — see Limitations below.
Per explicit instruction, no file under `01_DAIM_OS_Implementation/` or the
programme master plan was touched to produce this result.

## What was built

Four new C modules in `experiments/implementation/` (`daim_distributed_state`,
`daim_peer_protocol`, `daim_peer_transport`, `daim_distributed_learning_app`),
a from-scratch Python peer-protocol reimplementation for adversarial testing
(`daim_synthetic_peer.py`), a four-process Mininet/OVS driver
(`distributed_prototype_gate.py`) running each node as an independent OS
process (private memory, no shared file or database, no global Python
state), on a `h1-s1-s2-s3-s4-h4` linear topology, and — for this session —
`scaling_prototype_gate.py` (the same design generalized to N=2..32 nodes for
G7) plus `gate_g8()` (opt-in via `--include-g8`, a real duplicate-MAC
scenario, not a hand-crafted protocol message). Full detail, including the
exact `(origin_node_id, owner_epoch, sequence)` semantics and their explicit
fencing/durability limitations, is documented in
`daim_distributed_state.h`.

## Three significant bugs found and fixed while reaching this result

All three were found through direct evidence (raw JSONL logs, `strace`/`gdb`
on the live process, not assumption), consistent with this project's
established practice of verifying before believing.

**1. `daim_init()` was never called from the Python node driver.**
`daim_distributed_learning_app_init()` only registers this process's
NO_RULE handler — it does not itself bring up DAIM Core, exactly like
Paper 1's `daim_learning_app_init()`. Paper 1's `daim_core_bridge.py` calls
`daim_init()` first for this reason; `daim_distributed_node.py` had
silently omitted it. Effect: `daim_core_emit(NO_RULE, ...)` was a no-op —
`on_no_rule` never ran, so no host was ever locally learned or
disseminated. G2–G4 still passed because the synthetic peer drives
`daim_host_apply_remote` directly and never goes through this path, which
is exactly why the bug did not show up until G1's real end-to-end test.
Confirmed via a local two-node repro (`node.packet_in()` returned `0`
instead of `PORT_FLOOD`) before and after the fix.

**2. Transit/flooded traffic was being claimed as a local host.**
`on_no_rule` unconditionally called `daim_host_learn_local` for every
`NO_RULE` event's source MAC, regardless of which switch port it arrived
on. A broadcast (e.g. ARP) from a host several hops away floods through
every switch in the chain; each switch it merely transits also called
`daim_host_learn_local` for that MAC — with its own `node_id` as the false
origin. Disseminating that false claim back to the MAC's real owner (and to
every other node) produced a cascade of spurious `OWNERSHIP_CONFLICT`
results, which is exactly what a first full run of G1–G6 (after fixing bug
1) showed: G5 and G6 failed because the node that should have owned a
given host's record instead saw its own legitimate data rejected as a
conflict against a false claim from elsewhere. Fixed by adding an explicit
`host_port` parameter to `daim_distributed_learning_app_init` — a source
MAC is now only local-learned when it arrives on that node's own
directly-attached host port; every other port is correctly treated as
transit. A regression test (`test_distributed_learning_app.c`) now asserts
this directly: a transit packet on an inter-switch port must not be
learned, while its destination lookup still works correctly if that
destination is already known.

**3. A greenlet-unsafe lock in the JSONL event emitter, invisible at N=4,
reliably broken at N=8+.** G7 hung indefinitely above N=4 — low-numbered
(high-peer-fan-out) nodes never reached OpenFlow readiness, high-numbered
ones always did. Ruled out resource contention first, directly: upgrading
the VM from 4 vCPU/6GiB to 6 vCPU/10GiB produced byte-for-byte identical
failure timing. A `BACKOFF_BASE_MS` 100→1000 experiment (an order of
magnitude fewer retry-driven callbacks) also had zero effect — both ruled
out CPU/GIL-contention theories. `strace`/`gdb` on the live VM showed native
dial-thread pthreads (`dial_thread_main` in `daim_peer_transport.c`)
crossing into Python via the ctypes/libffi trampoline and landing in
`greenlet::UserGreenlet::g_switch`/`inner_bootstrap` → `clock_nanosleep` —
eventlet's greenlet-cooperative scheduling machinery, invoked from an OS
thread it never spawned. Traced to the exact line: `_print_lock =
threading.Lock()` in `daim_distributed_node.py`, used by `emit()`, called
from `_on_apply`/`_on_lifecycle` — the ctypes callbacks the C transport
invokes directly from its own native pthreads. `os_ken/lib/hub.py` calls
`eventlet.monkey_patch(thread=True)`; `eventlet.patcher.monkey_patch`'s own
source confirms passing `thread=True` explicitly sets every *other* module's
default to off, so only `thread`/`threading`/`queue` are patched — meaning
that lock was eventlet's green (cooperative) lock, safe only for greenlets
eventlet itself spawned, not for the transport's native pthreads. This
explains every symptom: worse with more concurrent dial threads (more
foreign-thread lock acquisitions racing at once), identical regardless of
CPU/RAM (a correctness bug, not a performance one), and the irregular
multi-hundred-second stalls (scheduling an orphaned foreign greenlet has no
defined timing guarantee). Fixed with the same idiom `os_ken/lib/hub.py`
itself already uses for a related reason (`eventlet.patcher.original`):
`_print_lock = eventlet.patcher.original("threading").Lock()`, a real OS
mutex, falling back to plain `threading.Lock()` when eventlet isn't
installed (this module is documented as importable without Os-Ken). A
one-line fix, not the broader tpool/queue callback-architecture redesign
originally suspected before the exact mechanism was pinned down. Confirmed
to be a latent bug in the already-passing G1–G6 path too — N=4's low peer
fan-out (max 3 dial threads per node) just made it statistically rare to
trigger, not absent.

All three fixes are covered by the full test suite (`make check`) and by
ASan+UBSan and ThreadSanitizer builds, all clean, on both macOS ARM64 and
the target Ubuntu 24.04 ARM64 VM.

## Per-gate evidence

**G1** — 100 untimed trigger pings (h4 → h1) produced 300 observed `apply`
events across the three other nodes (`new`/`updated` results only), all
with `origin_node_id == 4` and distinct PIDs confirmed for every node.
Propagation latency (`applied_at_ns - learned_at_ns`, same-host
`CLOCK_MONOTONIC` domain, comparable across these processes the same way
Task #17's cross-process Packet-In latency work already established) was
at or below measurement resolution for all 300 samples (median/p95/p99/max
all 0.0 ms) — consistent with same-host loopback TCP at these payload
sizes. This is a propagation-latency *sample*, not yet a 100-repetition
*campaign* with independently seeded, randomised-order trials; see
Limitations.

**G2/G3/G4** — a synthetic peer (independent Python reimplementation of the
wire protocol, not a reuse of `daim_peer_protocol.c`) sent scripted
sequences directly to a running node and the node's own JSONL log recorded
exactly the expected classification sequence:
`[new, duplicate]`, `[new, stale_rejected]`, `[new, stale_rejected, updated]`.

**G5** — after `kill -9` on the node that authored a cached remote record:
(a) unrelated traffic between two surviving nodes still passed end-to-end
(0% loss); (b) the surviving node holding that cached record still made a
correct forwarding decision and installed a real OVS flow for it, with the
existing flow explicitly deleted first to force a genuine table-miss (not
a hit on an already-installed rule). End-to-end delivery through the dead
node's own switch was not asserted — that switch's own controller is gone,
a separate and expected limitation, not a distributed-state failure.

**G6** — an `iptables` rule (not an application-level pause hook) dropped
all traffic to the pure-acceptor node's peer port. Detection took 3.28 s,
matching the configured `TCP_USER_TIMEOUT` (3000 ms) added specifically
because this milestone has no application-level heartbeat (see
`daim_peer_transport.c`). The target node did not falsely believe it knew
about traffic generated during the partition. After the rule was removed,
reconnection and a full symmetric snapshot exchange completed in 0.164 s,
and the target correctly learned what it had missed.

**G7** (`scaling_prototype_gate.py`, run `paper2-g7-scaling-20260810T095229Z`) — the same
mechanism as G1, generalized: for each N in {2, 4, 8, 16, 32}, a fresh N-node chain is built, the
cluster brought up under the same non-redundant `i<j` full-mesh policy as G1–G6 (so N=32 means
node 1 alone dials 31 peers — the reason `MAX_CONFIGURED_PEERS` was raised from 16 to 31 in
`daim_peer_transport.c` before this run, with a new boundary test in `test_peer_transport.c`
covering the cap itself), then the highest-numbered host pings host 1 repeatedly to measure
propagation. Every size passed: full mesh connectivity reached, and propagation observed on every
node that should have seen it. Convergence time scaled from 1.09 s (N=2) to 20.84 s (N=32);
connections observed matched the expected `N×(N-1)` exactly at every size (2, 12, 56, 240, 992);
propagation latency stayed at/below measurement resolution throughout, same as G1. This is a
characterization gate, not a single pass/fail claim — the scaling curve itself is the result.

**G8** (`gate_g8()` in `distributed_prototype_gate.py`, opt-in via `--include-g8`, run
`paper2-g1-g8-20260810T101545Z`) — a real duplicate-MAC scenario, not a synthetic protocol
message: `h1` and `h4`, opposite ends of the chain, are both given the same MAC address (an
operator/config-error scenario), and each independently triggers real traffic so its own node
learns and disseminates that MAC as locally owned via the existing, already-correct `host_port`
local-learn path (bug 2 above) — node 1's claim arrives first, node 4's second. All four nodes
correctly classified node 4's claim as `ownership_conflict` and rejected it: node 1's original
record was never overwritten, no two OpenFlow flow entries for the contested MAC on any switch
ever disagreed about which port to route it toward (i.e. no node's forwarding table was corrupted
by the losing claim), any pre-existing flow decision was preserved unchanged after the rejected
claim arrived, and unrelated traffic between the two uninvolved hosts stayed at 0% loss throughout.
This exercises the `DAIM_HOST_APPLY_OWNERSHIP_CONFLICT` path `daim_distributed_state.h` documents
as detect-only by design — G8 proves the detection is correct and safe, not that conflicts are
resolved, which stays explicitly out of scope. `test_distributed_state.c` also gained a second
`OWNERSHIP_CONFLICT` case covering the snapshot-import path (`daim_host_import_snapshot_entry`),
alongside the pre-existing live-update case, since both funnel through the same `apply_locked()`
but weren't both directly exercised before.

## Design decisions worth recording

- **Non-redundant mesh, not full mesh.** Node *i* dials node *j* only for
  *i* < *j*, so every pair has exactly one connection. An earlier
  every-node-dials-every-node design gave every pair two redundant
  connections — harmless for G1–G5, but it made G6's OS-level partition
  test ill-defined (blocking one node's listen port would leave its own
  outbound-dialed connections, on ephemeral source ports, untouched). With
  the ordering rule, the highest-numbered node only ever accepts inbound
  connections, so a single `iptables` rule on its listen port is a
  complete, correct partition.
- **Symmetric snapshot exchange.** Both sides of every connection request
  and serve a snapshot, not just the dialer. With the non-redundant mesh
  above, the highest-numbered node never dials anyone; if only dialers
  pulled snapshots (the original design), that node could never
  resynchronise after a reconnect.
- **`TCP_USER_TIMEOUT`, not a wire heartbeat**, bounds partition-detection
  time for this milestone; `DAIM_PEER_MSG_HEARTBEAT` is defined and
  decoded but unused. `SIGPIPE` is ignored process-wide so a send to a
  dead/partitioned peer reports failure through `send_all()`'s return
  value instead of terminating the node process.
- **Cluster start-up retry.** Bringing up several eventlet+native-pthread
  processes plus Mininet/OVS simultaneously was observed to occasionally
  stall at N=4 (confirmed via isolated single-node testing to otherwise
  succeed in ~5 s) — resource contention, not a reproducible deadlock.
  `start_cluster()`/`build_cluster()` retry up to 3 times with full cleanup
  between attempts, and every failed attempt is recorded as evidence, not
  discarded.
- **`eventlet.patcher.original(...)` for anything a native pthread's ctypes
  callback touches.** Bug 3 above is the general case: under Os-Ken's
  `eventlet.monkey_patch(thread=True)`, any `threading`/`queue` primitive
  reachable from `daim_peer_transport.c`'s own threads (dial, accept,
  connection-lifetime) rather than from Python-spawned/eventlet-managed
  code must use the pre-patch original, not the patched one. `_print_lock`
  was the only such site as of this session — grepped for and confirmed;
  worth re-checking if this callback surface grows.

## Limitations (explicitly not claimed)

- **Statistical campaign and baselines: closed, see `PAPER2_EVIDENCE_CAMPAIGN.md`.** Every
  condition above has since been repeated 30 times independently (150 for G7 — 30 × 5 sizes), with
  median/p95/p99/max/bootstrap-95%-CI aggregation, plus no-exchange and Single-Core Paper 1
  baselines, closing the master plan's Paper 2 evidence-gate statistical and baseline requirements.
  The single-trial results in this document remain the mechanism-level reference; the campaign
  document is the statistical evidence layered on top, including a fourth bug that campaign itself
  found (a test-timing race in `gate_g6`, not a product defect).
- **G8 tests detection under one specific scenario** (two hosts at opposite
  ends of the chain, MACs set before any traffic, one claim landing before
  the other) — not every possible race (e.g. near-simultaneous claims, or a
  conflict arriving mid-snapshot-exchange rather than as a live update, the
  latter covered only at the unit level in `test_distributed_state.c`, not
  end-to-end). `OWNERSHIP_CONFLICT` remains detect-only by design —
  resolution (deciding which claim should ultimately win) stays out of
  scope, as it was throughout diagnosing bug 2 above.
- **No causal ordering between origins, no vector clock.** The
  `(origin_node_id, owner_epoch, sequence)` scheme totally orders one
  origin's own updates; it says nothing about ordering between different
  origins. See `daim_distributed_state.h` for the full statement of this
  boundary.
- **`owner_epoch` fencing is an operator convention in this milestone**
  (a fixed value per run), not self-managed durable storage. Nothing here
  prevents a restarted node from reusing or lowering its own epoch.
- **Linear topology only.** The diamond topology and arbitrary routing
  named in the original spec are follow-up work.
- **Single VM, single host.** All processes and Mininet — up to 32 for
  G7 — share one VM's kernel and `CLOCK_MONOTONIC` domain; cross-host clock
  synchronisation has not been exercised. G7's connection/message-volume
  figures are event counts observed via each node's own JSONL log, not
  byte-level throughput from the transport's own stats
  (`daim_peer_transport_get_stats` exists in C but isn't wired through the
  ctypes boundary in `daim_distributed_node.py`).

## Raw evidence

G1–G6/G8 (4 nodes): `raw_events_20260810T101545Z.jsonl`,
`gate_results_20260810T101545Z.json`, `manifest_20260810T101545Z.json`.
G7 (N=2/4/8/16/32): `raw_events_g7_20260810T095229Z.jsonl`,
`gate_results_g7_20260810T095229Z.json`,
`scaling_summary_g7_20260810T095229Z.{json,csv}`,
`manifest_g7_20260810T095229Z.json`. Every event from every node in every
run is retained unfiltered, including failed attempts and the earlier
bugs' symptomatic `OWNERSHIP_CONFLICT` cascades — nothing is deleted from
this directory's other timestamped files.
