"""Os-Ken controller wrapper around daim_distributed_node.DistributedNode --
mirrors the daim_core_bridge.py (ctypes) / daim_bridge_controller.py
(Os-Ken app) split from Paper 1, so the ctypes/transport layer stays
importable and testable without Os-Ken installed.

Configuration is via environment variables (not argv, which osken-manager
itself parses):

  DAIM_NODE_ID, DAIM_OWNER_EPOCH, DAIM_CHAIN_ORDER, DAIM_PORT_TOWARD_LOWER,
  DAIM_PORT_TOWARD_HIGHER, DAIM_PEER_LISTEN_PORT, DAIM_PEER_ADDRS -- see
  daim_distributed_node.py's docstring for exact semantics.
  DAIM_PEER_BARRIER_FILE (optional) -- path to a file that does not exist
  yet; peer dialing (add_peer for every DAIM_PEER_ADDRS entry) is deferred
  until it appears, instead of starting immediately at construction. Unset
  by default (immediate dialing, matching G1-G6/G8's fixed-N=4 gates);
  scaling_prototype_gate.py sets it so all N nodes in a G7 run start dialing
  at the same barrier-release instant the harness begins timing from, not
  whenever each node process happens to finish its own staged launch.

Emits JSONL events to stdout: "ready" once the switch's table-miss rule is
installed; "apply"/"lifecycle" (relayed from DistributedNode's transport
callbacks); "no_rule" per Packet-In decision.
"""
import os
import pathlib
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from os_ken.base import app_manager
from os_ken.controller import ofp_event
from os_ken.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from os_ken.ofproto import ofproto_v1_3
from os_ken.lib.packet import packet, ethernet

from daim_distributed_node import DistributedNode, PORT_FLOOD, PORT_NONE, emit, mac_str_to_bytes

NODE_ID = int(os.environ["DAIM_NODE_ID"])
OWNER_EPOCH = int(os.environ["DAIM_OWNER_EPOCH"])
CHAIN_ORDER = [int(x) for x in os.environ["DAIM_CHAIN_ORDER"].split(",")]
PORT_TOWARD_LOWER = int(os.environ.get("DAIM_PORT_TOWARD_LOWER", "0"))
PORT_TOWARD_HIGHER = int(os.environ.get("DAIM_PORT_TOWARD_HIGHER", "0"))
PEER_LISTEN_PORT = int(os.environ["DAIM_PEER_LISTEN_PORT"])
PEER_ADDRS = [a for a in os.environ.get("DAIM_PEER_ADDRS", "").split(",") if a]
# Optional: path to a file the harness creates only once every node in the
# cluster is already OpenFlow-ready. If set, peer dialing (add_peer) is held
# back until that file appears, instead of starting the instant this
# controller app is constructed. Without this, on a staged multi-node launch
# (scaling_prototype_gate.py's build_cluster, one node spawned every
# SPAWN_STAGGER_S) an early-spawned node can dial and fully converge with an
# already-running peer well before the harness's own "every node is ready"
# checkpoint, so a convergence measurement that starts its clock at that
# checkpoint would silently exclude real protocol work that already
# happened -- this barrier makes "every node dials for the first time" and
# "the harness starts timing" the same instant instead of two unrelated
# ones. Unset (the default) preserves the original immediate-dial behaviour
# G1-G6/G8 rely on, at fixed N=4 where this race is not being measured.
PEER_BARRIER_FILE = os.environ.get("DAIM_PEER_BARRIER_FILE")
PEER_BARRIER_POLL_S = 0.01


class DistributedNodeController(app_manager.OSKenApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = DistributedNode(NODE_ID, OWNER_EPOCH, CHAIN_ORDER,
                                     PORT_TOWARD_LOWER, PORT_TOWARD_HIGHER, PEER_LISTEN_PORT)
        if PEER_BARRIER_FILE:
            threading.Thread(target=self._dial_peers_after_barrier, daemon=True).start()
        else:
            for addr in PEER_ADDRS:
                host, port = addr.split(":")
                self.node.add_peer(host, int(port))
        self.dpid_to_bridge = {}
        self._table_miss_installed = threading.Event()
        threading.Thread(target=self._announce_when_ready, daemon=True).start()

    def _dial_peers_after_barrier(self):
        while not os.path.exists(PEER_BARRIER_FILE):
            time.sleep(PEER_BARRIER_POLL_S)
        emit("lifecycle", name="peer_dial_released", peer_node_id=0, detail=NODE_ID)
        for addr in PEER_ADDRS:
            host, port = addr.split(":")
            self.node.add_peer(host, int(port))

    def _announce_when_ready(self):
        self._table_miss_installed.wait()
        emit("ready", node_id=NODE_ID)

    def _resolve_bridge_name(self, dpid):
        name = self.dpid_to_bridge.get(dpid)
        if name:
            return name
        datapath_id = "%016x" % dpid
        result = subprocess.run(
            ["ovs-vsctl", "--bare", "--columns=name", "find", "bridge",
             'datapath_id="%s"' % datapath_id],
            capture_output=True, text=True,
        )
        name = result.stdout.strip().strip('"')
        if name:
            self.dpid_to_bridge[dpid] = name
        return name or datapath_id

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def features(self, ev):
        dp = ev.msg.datapath
        p = dp.ofproto_parser
        o = dp.ofproto
        self._resolve_bridge_name(dp.id)
        match = p.OFPMatch()
        actions = [p.OFPActionOutput(o.OFPP_CONTROLLER, o.OFPCML_NO_BUFFER)]
        dp.send_msg(p.OFPFlowMod(
            datapath=dp, priority=0, match=match,
            instructions=[p.OFPInstructionActions(o.OFPIT_APPLY_ACTIONS, actions)],
        ))
        self._table_miss_installed.set()

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in(self, ev):
        msg = ev.msg
        dp = msg.datapath
        p = dp.ofproto_parser
        o = dp.ofproto
        eth = packet.Packet(msg.data).get_protocol(ethernet.ethernet)
        if not eth:
            return
        bridge = self._resolve_bridge_name(dp.id)
        in_port = msg.match["in_port"]

        out_port = self.node.packet_in(
            bridge, in_port, mac_str_to_bytes(eth.src), mac_str_to_bytes(eth.dst), eth.ethertype,
        )
        emit("no_rule", bridge=bridge, in_port=in_port, mac_src=eth.src, mac_dst=eth.dst,
             out_port=out_port)

        actions = [p.OFPActionOutput(o.OFPP_FLOOD if out_port in (PORT_FLOOD, PORT_NONE) else out_port)]
        dp.send_msg(p.OFPPacketOut(
            datapath=dp, buffer_id=msg.buffer_id, in_port=in_port,
            actions=actions,
            data=msg.data if msg.buffer_id == o.OFP_NO_BUFFER else None,
        ))
