# DAIM Core + Switch Adapter MVP Test Report

Date: 18 July 2026  
Host: macOS ARM64  
Compiler: Apple Clang 21.0.0

## Scope completed

- In-memory, mutex-protected implementation of the five writable DAIM tables.
- Public API operations: initialise, quit, add, delete, sequential read,
  filtered read, rewind, and signal registration.
- Internal signal emission and generation/count monitoring hooks.
- Generic switch-adapter interface.
- Mock adapter supporting port buffers, flow recording, and counters.
- OVS flow add/delete adapter using an injected executor.
- Production OVS executor uses `posix_spawnp` and does not invoke a shell.

## Tests executed

| Test executable | Coverage | Result |
|---|---|---|
| `test_core` | validation, add/read/rewind/filter/delete, generation, callback | PASS |
| `test_adapters` | mock read/write/flow statistics, OVS argv generation, invalid input | PASS |
| `test_concurrency` | four writer threads, 1,000 entries each, count and generation | PASS |

All three tests passed under both the normal strict build and an
AddressSanitizer + UndefinedBehaviorSanitizer build. No sanitizer finding was
reported.

## Interpretation boundary

This establishes an executable Core MVP and adapter contract. A subsequent
Linux integration smoke test used the adapter to install OpenFlow 1.3 rules in
a real OVS/Mininet topology; that evidence is documented separately under
`results/network`. `port_read`, `port_write`, and `switch_ioctl` remain
unsupported in the OVS adapter until their Linux semantics are implemented. No
latency, throughput, recovery, scalability, or Q1/Q2 claim follows from these
unit tests.

## Next implementation increment

1. Linux integration environment with OVS and Mininet.
2. OVS bridge discovery and port-state mapping.
3. Translation from `packet_forwarding_table_entry` to validated OpenFlow
   matches/actions.
4. Packet-In event receiver and Core callback emission.
5. Integration test: two hosts, one bridge, one installed DAIM flow.

## Addendum: 19 July 2026 (stage-latency instrumentation)

Adding `struct daim_learning_app_timing`/`daim_learning_app_last_timing()`
(nanosecond `CLOCK_MONOTONIC` boundaries around the NO_RULE decision path,
for the Packet-In stage-latency benchmark under `network/`) and linking
`ovs_persistent_adapter.o` into `libdaim_core.so` prompted a first run of the
existing ASan/UBSan build on the target Ubuntu 24.04 ARM64 host rather than
only macOS ARM64, where two pre-existing, unrelated defects were found and
fixed:

- `ovs_persistent_adapter.c`: `OFPXMC_OPENFLOW_BASIC` (`0x8000`) was a plain
  signed `int`; `<< 16` overflowed it (UBSan: undefined-behaviour left
  shift). Fixed by giving the macro an unsigned literal (`0x8000u`).
- `tests/test_learning_app.c`: the mock adapter created by
  `daim_mock_adapter_create` was never destroyed before `daim_quit()`
  (LeakSanitizer, Linux-only, so invisible on the prior macOS-only runs).
  Fixed by calling `adapter.ops->destroy(&adapter)` before exit.

All five test executables (`test_core`, `test_adapters`, `test_concurrency`,
`test_learning_app`, `test_persistent_adapter`) now pass under both the
normal build and ASan+UBSan+LeakSanitizer on Ubuntu 24.04 ARM64, in addition
to the macOS ARM64 runs above.

## Addendum: 20 July 2026 (wire-size helper for control-traffic accounting)

`daim_ovs_wire_flow_mod_size()` was added to `ovs_persistent_adapter.c`/
`.h`: a pure function reusing the existing `parse_flow_string`/
`build_match`/`build_actions`/`build_flow_mod` encoder to compute the
exact on-wire `OFPT_FLOW_MOD` byte length for a flow string without
opening a connection, for the sustained-load control-plane profile
experiment's control-byte accounting (`network/control_plane_load_
profile.py`, `results/network/CONTROL_PLANE_LOAD_PROFILE_REPORT.md`). All
five test executables, including `test_persistent_adapter`, continue to
pass under both the normal build and ASan+UBSan on Ubuntu 24.04 ARM64
with this addition.

## Addendum: 9 August 2026 (ThreadSanitizer and expanded concurrency)

The concurrency test now runs four writers (4,000 insertions), three readers
performing repeated rewind/read/free traversals with count/generation checks, a
deleter removing 2,000 entries, and four signal emitters producing 8,000
callbacks. Exact final counts, generations, and callback totals are asserted.
All five executables passed under Clang 18.1.3 ThreadSanitizer on Ubuntu 24.04
ARM64 with no race report. The complete console evidence is archived in
`logs/linux_tsan_concurrency.log`.
