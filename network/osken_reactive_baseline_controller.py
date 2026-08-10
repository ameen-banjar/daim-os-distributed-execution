"""Matched DAIM-free reactive learning-switch baseline for Paper 1.

The controller performs the same L2 learning task and observes the same
completion boundary as the DAIM Packet-In benchmark: a first Packet-In for a
known destination causes a priority-100 (in_port, eth_dst) Flow-Mod, and the
reported total ends when the rule is observable in ``ovs-ofctl dump-flows``.
No DAIM Core, ctypes call, or DAIM switch adapter is involved.
"""
import json
import os
import subprocess
import time

from os_ken.base import app_manager
from os_ken.controller import ofp_event
from os_ken.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from os_ken.lib.packet import ethernet, ether_types, ipv4, packet
from os_ken.ofproto import ofproto_v1_3


TARGET_IP = os.environ.get("DAIM_TARGET_IP")
CONFIRM_POLL_INTERVAL_S = 0.0005
CONFIRM_TIMEOUT_S = 3.0


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


class MatchedReactiveBaselineController(app_manager.OSKenApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.captured = False
        self.dpid_to_bridge = {}

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
        actions = [p.OFPActionOutput(o.OFPP_CONTROLLER, o.OFPCML_NO_BUFFER)]
        dp.send_msg(p.OFPFlowMod(
            datapath=dp,
            priority=0,
            match=p.OFPMatch(),
            instructions=[p.OFPInstructionActions(o.OFPIT_APPLY_ACTIONS, actions)],
        ))
        emit("ready", mode="reactive_osken")

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in(self, ev):
        t_dispatch_enter = time.perf_counter_ns()
        msg = ev.msg
        dp = msg.datapath
        p = dp.ofproto_parser
        o = dp.ofproto
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if not eth:
            return
        ip_pkt = pkt.get_protocol(ipv4.ipv4) if eth.ethertype == ether_types.ETH_TYPE_IP else None
        t_parse_done = time.perf_counter_ns()

        dpid = dp.id
        in_port = msg.match["in_port"]
        table = self.mac_to_port.setdefault(dpid, {})
        table[eth.src] = in_port
        t_state_update_done = time.perf_counter_ns()
        out_port = table.get(eth.dst, o.OFPP_FLOOD)
        t_decision_done = time.perf_counter_ns()
        actions = [p.OFPActionOutput(out_port)]

        capture = (
            not self.captured
            and TARGET_IP is not None
            and ip_pkt is not None
            and ip_pkt.dst == TARGET_IP
            and out_port != o.OFPP_FLOOD
        )
        t_flowmod_sent = None
        if out_port != o.OFPP_FLOOD:
            flow_mod = p.OFPFlowMod(
                datapath=dp,
                priority=100,
                match=p.OFPMatch(in_port=in_port, eth_dst=eth.dst),
                instructions=[p.OFPInstructionActions(o.OFPIT_APPLY_ACTIONS, actions)],
            )
            dp.send_msg(flow_mod)
            t_flowmod_sent = time.perf_counter_ns()
            if capture:
                self.captured = True

        dp.send_msg(p.OFPPacketOut(
            datapath=dp,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=msg.data if msg.buffer_id == o.OFP_NO_BUFFER else None,
        ))
        t_packetout_sent = time.perf_counter_ns()
        if capture:
            bridge = self._resolve_bridge_name(dp.id)
            needle = f"dl_dst={eth.dst}"
            t_confirmed = None
            deadline = time.perf_counter() + CONFIRM_TIMEOUT_S
            while time.perf_counter() < deadline:
                result = subprocess.run(
                    ["ovs-ofctl", "-O", "OpenFlow13", "dump-flows", bridge],
                    capture_output=True, text=True,
                )
                if needle in result.stdout:
                    t_confirmed = time.perf_counter_ns()
                    break
                time.sleep(CONFIRM_POLL_INTERVAL_S)
            emit(
                "timing",
                mode="reactive_osken",
                t_dispatch_enter_ns=t_dispatch_enter,
                t_parse_done_ns=t_parse_done,
                t_state_update_done_ns=t_state_update_done,
                t_decision_done_ns=t_decision_done,
                t_flowmod_sent_ns=t_flowmod_sent,
                t_packetout_sent_ns=t_packetout_sent,
                t_confirmed_ns=t_confirmed,
                installed=True,
                confirmed=t_confirmed is not None,
            )
