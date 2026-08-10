"""ctypes binding to build/libdaim_core.so.

This is the Packet-In -> DAIM Core bridge: a real OpenFlow controller
(daim_bridge_controller.py) calls DaimCoreBridge.packet_in() for every
Packet-In it receives. That call crosses into C and drives
daim_core_emit(NO_RULE, ...), the DAIM Core forwarding table, and the OVS
switch adapter exactly as a native DAIM application would, per
daim_os_api.h. Python's own job is limited to what DAIM Core does not
implement yet (see implementation/README.md): reading packets off the wire
and sending the buffered first packet back out, since port_read/port_write
are unsupported in the OVS adapter.
"""
import ctypes
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
LIB_PATH = ROOT / "implementation" / "build" / "libdaim_core.so"

PORT_FLOOD = 0xFFFB
PORT_NONE = 0xFFFE

MAC_ADDR_LEN = 6


class NoRulePacketInfo(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("in_port", ctypes.c_uint16),
        ("mac_src", ctypes.c_uint8 * MAC_ADDR_LEN),
        ("mac_dst", ctypes.c_uint8 * MAC_ADDR_LEN),
        ("ethernet_type", ctypes.c_uint16),
        ("ip_source", ctypes.c_uint32),
        ("ip_destination", ctypes.c_uint32),
        ("ip_netmask_source", ctypes.c_uint8),
        ("ip_netmask_destination", ctypes.c_uint8),
        ("ip_port_source", ctypes.c_uint16),
        ("tp_port_destination", ctypes.c_uint16),
        ("ip_proto", ctypes.c_uint8),
        ("vlan_id", ctypes.c_uint16),
        ("vlan_pcp", ctypes.c_uint8),
        ("ip_tos", ctypes.c_uint8),
    ]


assert ctypes.sizeof(NoRulePacketInfo) == 35, ctypes.sizeof(NoRulePacketInfo)


class PersistentAdapterStats(ctypes.Structure):
    """Mirrors struct daim_persistent_adapter_stats (ovs_persistent_adapter.h)."""
    _fields_ = [
        ("flow_mods_sent", ctypes.c_uint64),
        ("bytes_sent", ctypes.c_uint64),
        ("echo_replies_sent", ctypes.c_uint64),
    ]


class LearningAppTiming(ctypes.Structure):
    """Mirrors struct daim_learning_app_timing (daim_learning_app.h); natural
    (unpacked) alignment on both sides, matching the C header."""
    _fields_ = [
        ("entry_ns", ctypes.c_uint64),
        ("decision_done_ns", ctypes.c_uint64),
        ("table_write_done_ns", ctypes.c_uint64),
        ("install_done_ns", ctypes.c_uint64),
        ("exit_ns", ctypes.c_uint64),
        ("installed", ctypes.c_int),
    ]


class DaimOvsExecutorFn(object):
    """Keeps the ctypes CFUNCTYPE callback alive for the adapter's lifetime."""


EXECUTOR_CFUNC = ctypes.CFUNCTYPE(
    ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p)
)


class DaimCoreBridge:
    def __init__(self, executor=None, mode="process_per_rule",
                 persistent_port=None, persistent_timeout=15):
        """`executor(argv: list[bytes]) -> int` runs one ovs-ofctl command
        (e.g. via subprocess) and returns its exit status, mirroring
        daim_ovs_executor in ovs_switch_adapter.h. Required when
        mode="process_per_rule" (the default); ignored otherwise.

        mode="persistent" instead binds the from-scratch OpenFlow 1.3
        adapter (ovs_persistent_adapter.c): this blocks the calling thread
        until a switch connects to `persistent_port` and completes the
        OFPT_HELLO handshake, or `persistent_timeout` seconds elapse."""
        if not LIB_PATH.exists():
            raise FileNotFoundError(
                f"{LIB_PATH} not found; run `make all` in implementation/ first"
            )
        self.lib = ctypes.CDLL(str(LIB_PATH))
        self._configure_signatures()

        self.adapter = SwitchAdapter()
        if mode == "process_per_rule":
            if executor is None:
                raise ValueError("executor is required for mode=process_per_rule")
            self._executor_cb = self._wrap_executor(executor)
            rc = self.lib.daim_ovs_adapter_create(
                ctypes.byref(self.adapter), self._executor_cb, None
            )
            if rc != 0:
                raise RuntimeError("daim_ovs_adapter_create failed")
        elif mode == "persistent":
            if persistent_port is None:
                raise ValueError("persistent_port is required for mode=persistent")
            rc = self.lib.daim_ovs_persistent_adapter_create(
                ctypes.byref(self.adapter), persistent_port, persistent_timeout
            )
            if rc != 0:
                raise RuntimeError("daim_ovs_persistent_adapter_create failed "
                                    f"(port={persistent_port}, timeout={persistent_timeout}s)")
        else:
            raise ValueError(f"unknown mode: {mode!r}")

        if self.lib.daim_init() != 0:
            raise RuntimeError("daim_init failed")
        if self.lib.daim_learning_app_init(ctypes.byref(self.adapter)) != 0:
            raise RuntimeError("daim_learning_app_init failed")

    def _configure_signatures(self):
        self.lib.daim_init.restype = ctypes.c_uint16
        self.lib.daim_ovs_adapter_create.restype = ctypes.c_int
        self.lib.daim_ovs_adapter_create.argtypes = [
            ctypes.POINTER(SwitchAdapter),
            EXECUTOR_CFUNC,
            ctypes.c_void_p,
        ]
        self.lib.daim_ovs_persistent_adapter_create.restype = ctypes.c_int
        self.lib.daim_ovs_persistent_adapter_create.argtypes = [
            ctypes.POINTER(SwitchAdapter),
            ctypes.c_int,
            ctypes.c_int,
        ]
        self.lib.daim_learning_app_init.restype = ctypes.c_int
        self.lib.daim_learning_app_init.argtypes = [ctypes.POINTER(SwitchAdapter)]
        self.lib.daim_learning_app_packet_in.restype = ctypes.c_uint16
        self.lib.daim_learning_app_packet_in.argtypes = [
            ctypes.c_char_p,
            ctypes.POINTER(NoRulePacketInfo),
        ]
        self.lib.daim_learning_app_stats.argtypes = [
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        self.lib.daim_learning_app_last_timing.argtypes = [
            ctypes.POINTER(LearningAppTiming),
        ]
        self.lib.daim_ovs_persistent_adapter_get_stats.restype = ctypes.c_int
        self.lib.daim_ovs_persistent_adapter_get_stats.argtypes = [
            ctypes.POINTER(SwitchAdapter),
            ctypes.POINTER(PersistentAdapterStats),
        ]
        self.lib.daim_ovs_wire_flow_mod_size.restype = ctypes.c_size_t
        self.lib.daim_ovs_wire_flow_mod_size.argtypes = [ctypes.c_char_p, ctypes.c_int]

    def _wrap_executor(self, executor):
        def _callback(_context, argv):
            args = []
            i = 0
            while argv[i] is not None:
                args.append(argv[i])
                i += 1
            return executor(args)

        return EXECUTOR_CFUNC(_callback)

    def packet_in(self, bridge, in_port, mac_src, mac_dst, ethernet_type=0):
        info = NoRulePacketInfo()
        info.in_port = in_port
        info.mac_src = (ctypes.c_uint8 * 6)(*mac_src)
        info.mac_dst = (ctypes.c_uint8 * 6)(*mac_dst)
        info.ethernet_type = ethernet_type
        out_port = self.lib.daim_learning_app_packet_in(
            bridge.encode("ascii"), ctypes.byref(info)
        )
        return out_port

    def stats(self):
        no_rule_events = ctypes.c_uint64()
        flows_installed = ctypes.c_uint64()
        table_count = ctypes.c_size_t()
        self.lib.daim_learning_app_stats(
            ctypes.byref(no_rule_events),
            ctypes.byref(flows_installed),
            ctypes.byref(table_count),
        )
        return {
            "no_rule_events": no_rule_events.value,
            "flows_installed": flows_installed.value,
            "forwarding_table_rows": table_count.value,
        }

    def persistent_stats(self):
        """Cumulative real byte/message counters from the persistent
        adapter's own socket sends (ovs_persistent_adapter.c). Only valid
        when this bridge was constructed with mode="persistent"."""
        stats = PersistentAdapterStats()
        rc = self.lib.daim_ovs_persistent_adapter_get_stats(
            ctypes.byref(self.adapter), ctypes.byref(stats)
        )
        if rc != 0:
            raise RuntimeError("daim_ovs_persistent_adapter_get_stats failed "
                                "(not a persistent-mode adapter?)")
        return {
            "flow_mods_sent": stats.flow_mods_sent,
            "bytes_sent": stats.bytes_sent,
            "echo_replies_sent": stats.echo_replies_sent,
        }

    def wire_flow_mod_bytes(self, flow, is_delete=False):
        """Exact OpenFlow 1.3 OFPT_FLOW_MOD wire length for `flow`, computed
        by the same encoder the persistent adapter uses to send real
        messages (see daim_ovs_wire_flow_mod_size). Independent of adapter
        mode: the wire format for a given match/action shape does not
        depend on which channel (TCP or ovs-ofctl's local connection)
        carries it, so this is usable to size process_per_rule's
        installs too, which have no equivalent in-process byte counter."""
        n = self.lib.daim_ovs_wire_flow_mod_size(flow.encode("ascii"), int(is_delete))
        if n == 0:
            raise ValueError(f"malformed flow string: {flow!r}")
        return n

    def last_timing(self):
        """Nanosecond CLOCK_MONOTONIC boundaries from the most recent
        packet_in() call, comparable against this process's own
        time.perf_counter_ns() timestamps (same clock on Linux)."""
        t = LearningAppTiming()
        self.lib.daim_learning_app_last_timing(ctypes.byref(t))
        return {
            "entry_ns": t.entry_ns,
            "decision_done_ns": t.decision_done_ns,
            "table_write_done_ns": t.table_write_done_ns,
            "install_done_ns": t.install_done_ns,
            "exit_ns": t.exit_ns,
            "installed": bool(t.installed),
        }


class SwitchAdapterOps(ctypes.Structure):
    _fields_ = [
        ("port_read", ctypes.c_void_p),
        ("port_write", ctypes.c_void_p),
        ("switch_ioctl", ctypes.c_void_p),
        ("flow_add", ctypes.c_void_p),
        ("flow_delete", ctypes.c_void_p),
        ("destroy", ctypes.c_void_p),
    ]


class SwitchAdapter(ctypes.Structure):
    _fields_ = [
        ("ops", ctypes.POINTER(SwitchAdapterOps)),
        ("context", ctypes.c_void_p),
    ]
