# Paper 2 Evidence-Gate Campaign — 30-Repetition Statistics + Three Baselines

> **SUPERSEDED — PRE-FIX NUMBERS, NOT THE REPORTED RESULTS.** This narrative was written
> against the `v1.0.0` code state and was never regenerated across the `v1.1.0`/`v1.2.0` fix
> rounds, so every number below (e.g. G1 propagation latency reported as "0.0 ms", G7 convergence
> reported in whole seconds up to ~21 s at N=32) reflects defects the manuscript's Sections
> 6.8–6.10 describe finding and fixing, not the results the paper actually cites. Kept here
> unmodified, per this repository's no-delete evidence-retention discipline, as a record of what
> the pre-fix campaign showed. **For the current, reported results, use:**
> - The paper's own Tables 1–4 and Sections 6.2–6.5, 6.10 (source of truth).
> - The latest timestamped raw evidence in this directory: `campaign_distributed_raw_*.json` /
>   `campaign_distributed_manifest_*.json` (G1–G6, G8) and `scaling_campaign_g7_*.json` /
>   `scaling_summary_g7_*.{json,csv}` (G7) — sort by timestamp in the filename; the newest set
>   for each is the one the manuscript's current numbers were computed from.
> - `CITATION.cff`'s `version`/`doi` fields for which release this checkout corresponds to.

Date: 10 August 2026
Environment: Ubuntu 24.04 ARM64 (`daim-lab-qemu`, 6 vCPU/10GiB), Open vSwitch 3.3.4, Mininet 2.3.0,
Os-Ken 2.6.0
Evidence level: `measured_emulation`
Companion document: `DISTRIBUTED_GATE_REPORT.md` (the single-trial G1–G8 mechanism gate this
campaign statistically repeats and adds baselines to — read that first for what each gate/condition
actually tests; this document is the evidence-gate closure, not a restatement of the mechanism).

## Status: the master plan's Paper 2 evidence-gate items are now satisfied

`DAIM_SIX_PAPER_MASTER_PLAN.md`'s Paper 2 "Evidence gate" requires, verbatim:

> At least 30 randomised repetitions per condition and 100 for inexpensive propagation trials;
> median, p95, p99, maximum, and bootstrap intervals.
> Healthy, stale-update, conflict, node-kill, partition, reconnection, and scaling experiments with
> retained failures and raw data.
> Single-Core Paper 1, multi-instance/no-exchange, and distributed/versioned modes as explicit
> baselines.

All three items are now met:

```
Distributed/versioned, 30 reps:      G1 G2 G3 G4 G5 G6 G8    30/30 every condition
No-exchange baseline, 30 reps:       propagation             30/30, exactly zero every time
Single-Core Paper 1 baseline, 30 reps: Packet-In decision latency   30/30 confirmed
G7 scaling, 30 reps x 5 sizes:       N=2,4,8,16,32            30/30 at every size
```

150 independent cluster bring-ups for G7 alone, 90 for the other three campaigns combined — 240
independent, torn-down-and-rebuilt trials in total, every one's raw evidence retained.

## A fourth bug found by this campaign, not by inspection

The first full 30-rep distributed run (`campaign_distributed_20260810T134534Z.json`, superseded by
the run below — kept, not deleted) came back 29/30 on G6, not 30/30: repetition 18 (seed 20260828)
recorded a real `detection_time_s`/`reconvergence_time_s` (snapshot exchange completed) but then
`knows_after_reconvergence: false`. This is exactly the class of thing 30 repetitions exist to
catch that a single trial doesn't: `gate_g6`'s `target_knows_after` check
(`distributed_prototype_gate.py`) was an instantaneous `events_matching()` snapshot the instant
after `snapshot_done` fired, not a `wait_for(...)` with a timeout the way every *other* marker in
that same function already uses. The source node's entry can legitimately land via live
dissemination a moment *after* the snapshot exchange itself completes (e.g. if the snapshot request
landed a beat before the source's post-partition update had settled) — a real race in the **test's**
assertion timing, not the underlying mechanism (`reconvergence_time_s` was recorded as 1.04s that
run, well within the p95/p99 range every other repetition also showed, ~1.03–1.06s). Fixed by
switching that one check to `wait_for(..., timeout_s=10)`, matching the pattern already used for
`disconnect_seen`/`reconnected`/`snapshot_done` in the same function. Re-run after the fix: 30/30.
Both the pre-fix (29/30) and post-fix (30/30) raw campaign data are retained under
`campaign_distributed_raw_20260810T134534Z.json` and `..._20260810T140517Z.json` respectively — the
failing run is evidence, not something to discard because a later run looked better.

## Campaign 1: distributed/versioned, 30 repetitions

Run `paper2-campaign-distributed-20260810T140517Z`. Each repetition is one complete, independent
invocation of `distributed_prototype_gate.py --include-g8` (fresh Mininet/cluster bring-up and
teardown every time, not one long-lived cluster reused across repetitions) — see
`campaign_distributed.py`.

| Condition | Repetitions | Passed | Pass rate |
|---|---|---|---|
| G1 (propagation) | 30 | 30 | 100% |
| G2 (duplicate rejected) | 30 | 30 | 100% |
| G3 (stale rejected) | 30 | 30 | 100% |
| G4 (epoch ordering) | 30 | 30 | 100% |
| G5 (node-kill isolation) | 30 | 30 | 100% |
| G6 (partition/reconnect) | 30 | 30 | 100% |
| G8 (ownership conflict) | 30 | 30 | 100% |

Numeric metrics, aggregated across the 30 independent repetitions (median/p95/p99/max/bootstrap
95% CI on the median, 2000 resamples):

| Metric | Median | p95 | p99 | Max | 95% CI (median) |
|---|---|---|---|---|---|
| G1 propagation latency | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | [0.0, 0.0] |
| G6 detection time | 3.286 s | 3.303 s | 3.335 s | 3.347 s | [3.282, 3.291] |
| G6 reconvergence time | 0.195 s | 1.034 s | 1.050 s | 1.056 s | [0.179, 0.601] |

G1's propagation latency stays at/below measurement resolution across all 30 repetitions (n=300
per repetition's own internal `100 for inexpensive propagation trials` clause, ×30 = 9000
individual apply-event samples underlying this aggregate — comfortably exceeds the evidence gate's
100-sample floor). G6 detection time is tightly clustered around `TCP_USER_TIMEOUT` (3000 ms) as
designed. Reconvergence shows a real bimodal-looking spread (median 0.195s, but p95/p99/max near
1.03–1.06s) — genuine, not an artifact: some repetitions' snapshot exchange completes fast, others
take closer to a full second depending on scheduling; both tails are legitimate observed behavior,
not filtered.

## Campaign 2: no-exchange baseline, 30 repetitions

Run `paper2-no-exchange-20260810T142307Z` (`gate_no_exchange.py`). Same topology, same four node
processes, `DAIM_PEER_ADDRS=""` for every node — no dial threads spawn, no peer connections are
ever made.

| Check | Result across all 30 repetitions |
|---|---|
| Cross-node `apply` events observed | **0** in every repetition (not merely low — exactly zero, every time) |
| `lifecycle` events observed | **0** in every repetition (no connections were ever attempted) |
| node4's own local learn still works | confirmed in all 30 |
| Cross-node decisions for h4's MAC stay FLOOD (never a directed port) | confirmed in all 30 |
| Pass rate | 30/30 (100%) |

This is the headline no-exchange result: with the peer-transport mechanism absent, propagation is
not merely rare — it is structurally impossible, and every node's own local learning keeps working
exactly as it would in isolation. A latency comparison against the other two modes was not
attempted for this baseline (see `gate_no_exchange.py`'s module docstring): this codebase's
Packet-In path has no per-stage nanosecond instrumentation the way the single-Core controller does,
and fabricating one from this driver's own log-arrival timestamps would not be a fair comparison.

## Campaign 3: Single-Core Paper 1 baseline, 30 repetitions

Run `paper2-campaign-single-core-20260810T143506Z` (`campaign_single_core.py`), reusing
`packetin_latency_breakdown.py`'s `run_trial()` unmodified — the same nanosecond-precision
Packet-In timing capture already established for Paper 1's own manuscript, mode
`process_per_rule` (DAIM's default single-Core adapter).

**Scope caveat, stated plainly**: this reuses `packetin_latency_breakdown.py`'s single-switch/
3-host topology, not Paper 2's 4-switch chain — a single Core's per-decision latency does not
depend on how many *other* switches share the topology (each Packet-In is dispatched by dpid,
handled independently), so this was judged not to compromise the comparison's validity for a
per-decision metric; building an unproven 4-switch variant of this exact harness was rejected in
favor of reusing what Paper 1 already validated. Aggregate proactive-install cost at larger N is a
separate question Paper 1's own `stage2_full_compare*.py` already covers at N=8..64.

| Metric | Median | p95 | p99 | Max | 95% CI (median) |
|---|---|---|---|---|---|
| Total (dispatch → confirmed) | 13.99 ms | 27.45 ms | 34.51 ms | 35.30 ms | [13.52, 15.71] |
| DAIM Core decision only | 0.00085 ms | 0.00157 ms | 0.00179 ms | 0.00188 ms | [0.00079, 0.00104] |
| Table-write → OVS install confirmed | 11.38 ms | 22.60 ms | 31.02 ms | 32.87 ms | [11.04, 12.60] |

30/30 confirmed (every repetition's installed Flow-Mod was independently verified present via
`ovs-ofctl dump-flows`, not merely assumed from a return code). The DAIM Core's own decision logic
is sub-microsecond; essentially the entire end-to-end latency is `ovs-ofctl` subprocess-spawn and
OVS install/confirm overhead (`process_per_rule` mode's own known cost, already documented in
Paper 1 — this campaign reproduces it fresh at 30 reps rather than reusing old numbers, per the
master plan's rule against presenting historical data as new evidence).

## Cross-mode comparison

| | Single-Core (Paper 1) | No-exchange | Distributed/versioned |
|---|---|---|---|
| Processes | 1 (all switches) | 4 (one per switch) | 4 (one per switch) |
| Cross-node propagation | n/a (one process knows everything immediately) | **0 events, always** | Real, sub-ms, 30/30 |
| Core decision latency | 0.00085 ms median | not separately instrumented (same core path) | not separately instrumented (same core path) |
| Duplicate/stale/conflict handling | n/a (no distributed state to conflict) | n/a (nothing to disseminate) | Detected/rejected correctly, 30/30 (G2,G3,G8) |
| Node-failure isolation | n/a (single process is a single point of failure) | n/a (never tested — no exchange to isolate) | Surviving nodes keep deciding, 30/30 (G5) |
| Partition/reconnect | n/a | n/a | Detected (~3.3s), reconverges (~0.2–1.1s), 30/30 (G6) |
| Scales to | — | — | N=2..32, 100% pass at every size (G7) |

The "n/a" rows are not gaps — they are the point of running these baselines: the mechanisms Paper 2
introduces (propagation, conflict detection, failure isolation, partition recovery) are precisely
the capabilities that do not exist without the distributed/versioned mode's peer-transport
mechanism, and this table is the evidence for that claim rather than an assertion of it.

## Campaign 4: G7 scaling, 30 repetitions x 5 sizes

Run `scaling_campaign_g7_20260810T143753Z` (`scaling_prototype_gate.py --repetitions 30`). 150
independent cluster bring-ups total (30 fresh Mininet/cluster cycles per N).

| N | Repetitions | Pass rate | Convergence median | p95 | p99 | Max | 95% CI (median) |
|---|---|---|---|---|---|---|---|
| 2 | 30 | 100% | 1.084 s | 1.142 s | 1.148 s | 1.150 s | [1.043, 1.088] |
| 4 | 30 | 100% | 2.378 s | 3.145 s | 3.507 s | 3.511 s | [2.358, 2.413] |
| 8 | 30 | 100% | 6.788 s | 7.308 s | 7.770 s | 7.930 s | [6.774, 6.813] |
| 16 | 30 | 100% | 12.884 s | 13.233 s | 13.614 s | 13.727 s | [12.851, 12.913] |
| 32 | 30 | 100% | 20.919 s | 21.544 s | 23.970 s | 24.825 s | [20.886, 20.955] |

Propagation latency stayed at/below measurement resolution (0.0 ms median/p95/p99/max) at every N,
every repetition. Convergence time scales smoothly and predictably with N, with tight, consistent
percentile spreads at every size (p99/median ratio shrinks as N grows, i.e. the *relative* spread
narrows even as absolute convergence time grows) — 150/150 trials passed with no observed failures
at any size, a materially stronger and more precise result than the single-sweep result reported in
`DISTRIBUTED_GATE_REPORT.md`.

## Files and raw evidence

Every repetition's raw event stream and every campaign's aggregate/manifest with SHA-256 checksums
are retained under `experiments/results/network/distributed/`, including the pre-fix 29/30 G6
campaign run (not deleted). Latest/canonical files for each campaign:

- Distributed/versioned: `campaign_distributed_20260810T140517Z.json` (aggregate),
  `campaign_distributed_raw_20260810T140517Z.json` (all 30 repetitions' full manifests/gate_results),
  `campaign_distributed_manifest_20260810T140517Z.json` (SHA-256s).
- No-exchange: `gate_results_no_exchange_20260810T142307Z.json`,
  `manifest_no_exchange_20260810T142307Z.json` (SHA-256s + per-repetition raw JSONL files).
- Single-Core: `campaign_single_core_20260810T143506Z.json` (aggregate),
  `campaign_single_core_raw_20260810T143506Z.json` (all 30 rows),
  `campaign_single_core_manifest_20260810T143506Z.json` (SHA-256s).
- G7 scaling: `scaling_campaign_g7_20260810T143753Z.json` (aggregate),
  `scaling_summary_g7_20260810T143753Z.{json,csv}` (per-repetition rows),
  `gate_results_g7_20260810T143753Z.json` (full per-repetition outcomes),
  `manifest_g7_20260810T143753Z.json` (SHA-256s).

## Scope calls made explicitly during this campaign (not silently assumed)

- Condition order within one distributed-mode repetition is fixed (G1,G2,G3,G4,G6,G8,G5), not
  reshuffled per repetition the way Paper 1's `stage2_full_compare_paired.py` reshuffles mode
  order — G8-before-G5 is a correctness requirement (G5 permanently kills node4), not a style
  choice, and reordering already-debugged gate logic was judged not worth the risk of
  reintroducing bugs of the kind this project's own gate report already documents diagnosing.
  What *is* independent per repetition is the entire cluster — fresh processes, fresh Mininet/OVS
  state, a fresh seed — which is what repetition-to-repetition independence actually requires.
- Single-Core baseline reuses a single-switch/3-host topology, not Paper 2's 4-switch chain (see
  Campaign 3 above).
- No-exchange and single-Core baselines measure the metrics that remain meaningful for each mode
  (propagation-completeness for no-exchange; decision latency for single-Core) rather than
  repeating every G1–G8 condition for both, since most of Paper 2's distributed-specific mechanisms
  (duplicate/stale/conflict rejection, node-failure isolation, partition recovery) have no
  single-Core or no-exchange analogue to measure in the first place.
