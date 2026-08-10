# DAIM Core + Switch Adapter MVP

This is the first executable implementation layer beneath the published
DAIM-OS v1.0.0 C interface specification.

Implemented now:

- thread-safe in-memory storage for the five writable DAIM tables;
- `daim_init`, `daim_quit`, `daim_table_write`, `daim_table_read`, and
  `daim_table_rewind`;
- signal-handler registration and an internal emission hook;
- a generic switch-adapter contract;
- a deterministic mock switch adapter;
- an OVS command adapter with an injectable executor for dry-run tests;
- a DAIM learning application (`src/daim_learning_app.c`) that handles the
  `NO_RULE` signal, writes `DAIM_PACKET_FORWARDING_TABLE`, and installs real
  OVS flows for learned L2 destinations, driven by a real OpenFlow 1.3
  Packet-In through a ctypes bridge (`network/daim_core_bridge.py` and
  `network/daim_bridge_controller.py`); see
  `results/network/STAGE_PACKETIN_BRIDGE_REPORT.md`;
- unit and integration tests.

Not implemented yet:

- L3/L4 packet matching (the learning app only matches `in_port`/`mac_dst`;
  `packet_forwarding_table_entry`'s IP/port/VLAN fields are unused);
- a DAIM Core that itself terminates an OpenFlow session (Python/Os-Ken still
  owns the wire protocol and the buffered-packet PacketOut, since
  `port_write` remains unsupported below);
- a persistent store;
- OVSDB topology/state discovery;
- cloud protocol transport;
- distributed DAIM agents or reconciliation (see Stage 3 for the current
  scripted, non-autonomous link-recovery intervention);
- authentication, authorisation, and production lifecycle handling.

Build and test:

```sh
make clean
make check
```

The OVS adapter tests do not require Open vSwitch. They verify the generated
argument vector using a fake executor. A Linux integration test will be added
when `ovs-vsctl` and `ovs-ofctl` are available.

