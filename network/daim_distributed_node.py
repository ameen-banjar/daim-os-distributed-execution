"""One DAIM distributed node: a real OpenFlow 1.3 controller process for
exactly one switch, backed by daim_distributed_learning_app (libdaim_
distributed.so) instead of the single-Core daim_learning_app used by Paper
1's daim_bridge_controller.py. Each node is its own OS process with its own
memory -- see daim_distributed_learning_app.h and daim_peer_transport.h for
why that gives real isolation without touching daim_core.c.

Configuration is via environment variables (not argv, which osken-manager
itself parses), matching daim_bridge_controller.py's convention:

  DAIM_NODE_ID            This node's ID (uint64), used as origin_node_id
                           for everything it learns locally.
  DAIM_OWNER_EPOCH         This node's epoch for this run (see daim_
                           distributed_state.h's fencing-assumption note:
                           operator-supplied, e.g. the current unix time,
                           not a self-managed durable counter).
  DAIM_CHAIN_ORDER         Comma-separated node IDs in this run's linear
                           topology order, e.g. "1,2,3,4".
  DAIM_PORT_TOWARD_LOWER   OVS port number facing the lower-indexed
                           neighbour in DAIM_CHAIN_ORDER.
  DAIM_PORT_TOWARD_HIGHER  OVS port number facing the higher-indexed
                           neighbour.
  DAIM_PEER_LISTEN_PORT    TCP port this node's peer transport listens on.
  DAIM_PEER_ADDRS          Comma-separated host:port peers to dial (may be
                           empty for a single-node run).

This module is deliberately Os-Ken-free (no OpenFlow/Mininet dependency),
so it can be imported and its transport/ctypes layer exercised directly
(e.g. against daim_synthetic_peer.py) without Os-Ken installed. See
daim_distributed_controller.py for the Os-Ken app that wires this into a
real Packet-In path, and its docstring for the environment-variable
configuration.

Emits JSONL events to stdout via emit(): "apply" for every daim_host_apply_
remote/snapshot-entry result (from the transport's on_apply callback);
"lifecycle" for connection events. daim_distributed_controller.py adds
"ready" and "no_rule" on top of these.
"""
import ctypes
import json
import pathlib
import subprocess
import threading

ROOT = pathlib.Path(__file__).resolve().parents[1]
LIB_PATH = ROOT / "implementation" / "build" / "libdaim_distributed.so"
MAC_ADDR_LEN = 6
PORT_FLOOD = 0xFFFB
PORT_NONE = 0xFFFE

APPLY_RESULT_NAMES = ["new", "updated", "duplicate", "stale_rejected", "ownership_conflict", "invalid"]

# Under Os-Ken, eventlet.monkey_patch(thread=True) (os_ken/lib/hub.py) makes
# threading.Lock a greenlet-cooperative lock, not a real OS mutex. emit() is
# called both from the main eventlet-managed greenlet (via daim_distributed_
# controller.py) and directly from the C transport's own native pthreads
# (_on_apply/_on_lifecycle below, invoked by fire_lifecycle/on_apply in
# daim_peer_transport.c) -- a foreign OS thread acquiring a green lock is
# unsafe (confirmed via strace/gdb: it lands in greenlet's g_switch/
# inner_bootstrap machinery and can stall indefinitely, worse as peer count
# grows). eventlet.patcher.original("threading") -- the same idiom os_ken/
# lib/hub.py itself uses for a related reason -- returns the true pre-patch
# module, giving a real mutex safe for both callers. Falls back to plain
# threading.Lock() when eventlet isn't installed, per this module's own
# no-Os-Ken-required design (see module docstring).
try:
    import eventlet.patcher
    _print_lock = eventlet.patcher.original("threading").Lock()
except ImportError:
    _print_lock = threading.Lock()


def emit(event, **fields):
    with _print_lock:
        print(json.dumps({"event": event, **fields}), flush=True)


def mac_bytes_to_str(mac_bytes):
    return ":".join("%02x" % b for b in mac_bytes)


def mac_str_to_bytes(mac):
    return [int(part, 16) for part in mac.split(":")]


def run_ovs_command(argv):
    args = [a.decode() if isinstance(a, bytes) else a for a in argv]
    result = subprocess.run(args, capture_output=True, text=True)
    return result.returncode


class HostLocation(ctypes.Structure):
    """Mirrors struct daim_host_location (daim_distributed_state.h)."""
    _fields_ = [
        ("mac", ctypes.c_uint8 * MAC_ADDR_LEN),
        ("origin_node_id", ctypes.c_uint64),
        ("owner_dpid", ctypes.c_uint64),
        ("owner_port", ctypes.c_uint32),
        ("owner_epoch", ctypes.c_uint64),
        ("sequence", ctypes.c_uint64),
        ("is_local", ctypes.c_uint8),
        ("learned_at_ns", ctypes.c_uint64),
        ("applied_at_ns", ctypes.c_uint64),
    ]

    def to_dict(self):
        return {
            "mac": mac_bytes_to_str(bytes(self.mac)),
            "origin_node_id": self.origin_node_id,
            "owner_dpid": self.owner_dpid,
            "owner_port": self.owner_port,
            "owner_epoch": self.owner_epoch,
            "sequence": self.sequence,
            "is_local": bool(self.is_local),
            "learned_at_ns": self.learned_at_ns,
            "applied_at_ns": self.applied_at_ns,
        }


class NoRulePacketInfo(ctypes.Structure):
    """Mirrors struct no_rule_packet_info (daim_os_api.h); same layout
    daim_core_bridge.py uses for the Paper 1 bridge."""
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


EXECUTOR_CFUNC = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p))
APPLY_EVENT_CFUNC = ctypes.CFUNCTYPE(
    None, ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(HostLocation), ctypes.c_int
)
LIFECYCLE_EVENT_CFUNC = ctypes.CFUNCTYPE(
    None, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint64, ctypes.c_uint64
)


class TransportCallbacks(ctypes.Structure):
    _fields_ = [
        ("on_apply", APPLY_EVENT_CFUNC),
        ("on_lifecycle", LIFECYCLE_EVENT_CFUNC),
        ("context", ctypes.c_void_p),
    ]


HOST_PORT = 1  # every switch in the gate topology has its own host on port 1


class DistributedNode:
    def __init__(self, node_id, owner_epoch, chain_order, port_toward_lower, port_toward_higher,
                 peer_listen_port, host_port=HOST_PORT):
        if not LIB_PATH.exists():
            raise FileNotFoundError(f"{LIB_PATH} not found; run `make all` in implementation/ first")
        self.lib = ctypes.CDLL(str(LIB_PATH))
        self._configure_signatures()

        if self.lib.daim_host_state_init(ctypes.c_uint64(node_id), ctypes.c_uint64(owner_epoch)) != 0:
            raise RuntimeError("daim_host_state_init failed")

        # daim_distributed_learning_app_init only registers this process's
        # NO_RULE handler (daim_signal) -- it does not itself bring up DAIM
        # Core, exactly like Paper 1's daim_learning_app_init(). Paper 1's
        # daim_core_bridge.py calls daim_init() before daim_learning_app_
        # init() for this reason; this had been missed here, so daim_core_
        # emit(NO_RULE, ...) was silently a no-op -- on_no_rule never ran,
        # so no local learn or dissemination ever happened, while G2-G4
        # still passed because the synthetic peer drives daim_host_apply_
        # remote directly and never goes through this path. Confirmed via
        # a local two-node repro before this fix (decided_port was always
        # its zero-initialised default, never PORT_FLOOD or a real port).
        if self.lib.daim_init() != 0:
            raise RuntimeError("daim_init failed")

        self._on_apply_cb = APPLY_EVENT_CFUNC(self._on_apply)
        self._on_lifecycle_cb = LIFECYCLE_EVENT_CFUNC(self._on_lifecycle)
        callbacks = TransportCallbacks(self._on_apply_cb, self._on_lifecycle_cb, None)
        self.transport = self.lib.daim_peer_transport_create(
            ctypes.c_uint64(node_id), ctypes.c_uint64(owner_epoch), ctypes.c_int(peer_listen_port),
            ctypes.byref(callbacks),
        )
        if not self.transport:
            raise RuntimeError(f"daim_peer_transport_create failed (port={peer_listen_port})")
        self._callbacks_keepalive = callbacks  # ctypes does not keep this alive on its own

        self.executor_cb = EXECUTOR_CFUNC(self._executor)
        self.adapter = SwitchAdapter()
        if self.lib.daim_ovs_adapter_create(ctypes.byref(self.adapter), self.executor_cb, None) != 0:
            raise RuntimeError("daim_ovs_adapter_create failed")

        chain_array = (ctypes.c_uint64 * len(chain_order))(*chain_order)
        rc = self.lib.daim_distributed_learning_app_init(
            ctypes.byref(self.adapter), ctypes.c_uint64(node_id), ctypes.c_uint64(0),  # dpid set later
            ctypes.c_uint16(host_port),
            chain_array, ctypes.c_size_t(len(chain_order)),
            ctypes.c_uint16(port_toward_lower), ctypes.c_uint16(port_toward_higher),
            self.transport,
        )
        if rc != 0:
            raise RuntimeError("daim_distributed_learning_app_init failed")

    def _configure_signatures(self):
        self.lib.daim_init.restype = ctypes.c_uint16
        self.lib.daim_host_state_init.restype = ctypes.c_int
        self.lib.daim_host_state_init.argtypes = [ctypes.c_uint64, ctypes.c_uint64]

        self.lib.daim_peer_transport_create.restype = ctypes.c_void_p
        self.lib.daim_peer_transport_create.argtypes = [
            ctypes.c_uint64, ctypes.c_uint64, ctypes.c_int, ctypes.POINTER(TransportCallbacks),
        ]
        self.lib.daim_peer_transport_add_peer.restype = ctypes.c_int
        self.lib.daim_peer_transport_add_peer.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int]
        self.lib.daim_peer_transport_destroy.argtypes = [ctypes.c_void_p]

        self.lib.daim_ovs_adapter_create.restype = ctypes.c_int
        self.lib.daim_ovs_adapter_create.argtypes = [
            ctypes.POINTER(SwitchAdapter), EXECUTOR_CFUNC, ctypes.c_void_p,
        ]

        self.lib.daim_distributed_learning_app_init.restype = ctypes.c_int
        self.lib.daim_distributed_learning_app_init.argtypes = [
            ctypes.POINTER(SwitchAdapter), ctypes.c_uint64, ctypes.c_uint64, ctypes.c_uint16,
            ctypes.POINTER(ctypes.c_uint64), ctypes.c_size_t,
            ctypes.c_uint16, ctypes.c_uint16, ctypes.c_void_p,
        ]
        self.lib.daim_distributed_learning_app_packet_in.restype = ctypes.c_uint16
        self.lib.daim_distributed_learning_app_packet_in.argtypes = [
            ctypes.c_char_p, ctypes.POINTER(NoRulePacketInfo),
        ]

    def _executor(self, _context, argv):
        args = []
        i = 0
        while argv[i] is not None:
            args.append(argv[i])
            i += 1
        return run_ovs_command(args)

    def _on_apply(self, _context, from_peer_node_id, loc_ptr, result):
        loc = loc_ptr.contents
        emit("apply", from_peer_node_id=from_peer_node_id,
             result=APPLY_RESULT_NAMES[result], **loc.to_dict())

    def _on_lifecycle(self, _context, event_name, peer_node_id, detail):
        emit("lifecycle", name=event_name.decode(), peer_node_id=peer_node_id, detail=detail)

    def add_peer(self, host, port):
        if self.lib.daim_peer_transport_add_peer(self.transport, host.encode("ascii"), port) != 0:
            raise RuntimeError(f"daim_peer_transport_add_peer failed for {host}:{port}")

    def packet_in(self, bridge, in_port, mac_src, mac_dst, ethernet_type=0):
        info = NoRulePacketInfo()
        info.in_port = in_port
        info.mac_src = (ctypes.c_uint8 * MAC_ADDR_LEN)(*mac_src)
        info.mac_dst = (ctypes.c_uint8 * MAC_ADDR_LEN)(*mac_dst)
        info.ethernet_type = ethernet_type
        return self.lib.daim_distributed_learning_app_packet_in(bridge.encode("ascii"), ctypes.byref(info))

