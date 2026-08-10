"""Real OpenFlow 1.3 controller whose Packet-In handling is delegated to the
DAIM Core NO_RULE callback via daim_core_bridge.DaimCoreBridge, instead of a
Python dict (contrast with osken_learning_controller.py). The controller
still owns: the OpenFlow session, the table-miss rule, dpid<->bridge name
resolution, and sending the buffered first packet back out (PacketOut) --
DAIM Core owns the MAC-learning decision, the forwarding-table record, and
installing the persistent OVS flow for learned destinations.

Adapter mode and stage-latency capture are controlled by environment
variables (not argv, which osken-manager itself parses):

  DAIM_ADAPTER_MODE     "process_per_rule" (default) or "persistent".
  DAIM_PERSISTENT_PORT  Required when mode="persistent": the port this
                         process listens on for the switch's auxiliary
                         OpenFlow connection (see ovs_persistent_adapter.h).
                         Only one bridge may be bound to a persistent
                         adapter instance at a time, so this mode requires
                         a single-switch topology.
  DAIM_TARGET_IP        If set, enables stage-latency capture: the first
                         IPv4 Packet-In whose parsed destination equals
                         this address is timed end-to-end (dispatch entry
                         through switch-side flow-install confirmation) and
                         reported as one {"event": "timing", ...} JSON line
                         on stdout, in addition to the {"event": "ready"}
                         line printed once the controller can accept
                         traffic. All other Packet-Ins (including the ARP
                         traffic that necessarily precedes it) are handled
                         normally but not timed.
  DAIM_LOAD_PROFILE_SIGNAL_IP
                         If set, enables sustained-load profiling instead
                         of (and mutually exclusive with) single-event
                         capture. The harness sends its real measured
                         traffic first, then one packet SOURCED from this
                         IP as an explicit end-of-window marker (source, not
                         destination, so it cannot collide with the shared
                         ping destination every sender already targets).
                         On seeing it, process resource usage
                         (resource.getrusage, covering both this Python
                         process and DAIM Core, which runs in-process via
                         ctypes) and adapter-specific control-traffic
                         counters (see daim_core_bridge) are reported as
                         one {"event": "load_profile", ...} line. A count-
                         based threshold was tried first and rejected: each
                         real ping generates an a-priori-uncertain number of
                         NO_RULE events (ARP request, ARP reply, ICMP
                         request and/or reply, depending on which
                         direction's flow is already installed), so a fixed
                         event count cannot reliably be mapped back to "all
                         intended senders have gone through" -- an explicit
                         marker sidesteps that entirely.
"""
import json
import os
import resource
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from os_ken.base import app_manager
from os_ken.controller import ofp_event
from os_ken.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from os_ken.ofproto import ofproto_v1_3
from os_ken.lib.packet import packet, ethernet, ipv4, ether_types

from daim_core_bridge import DaimCoreBridge, PORT_FLOOD

ADAPTER_MODE = os.environ.get("DAIM_ADAPTER_MODE", "process_per_rule")
PERSISTENT_PORT = os.environ.get("DAIM_PERSISTENT_PORT")
TARGET_IP = os.environ.get("DAIM_TARGET_IP")
LOAD_PROFILE_SIGNAL_IP = os.environ.get("DAIM_LOAD_PROFILE_SIGNAL_IP")
CONFIRM_POLL_INTERVAL_S = 0.0005
CONFIRM_TIMEOUT_S = 3.0
# Fixed representative flow string: same field shape (priority, in_port,
# dl_dst, actions=output:N) that install_flow() always emits (daim_learning_
# app.c), so its wire size is identical to every real installed flow's,
# regardless of the specific port/MAC values.
SAMPLE_FLOW = "priority=100,in_port=1,dl_dst=00:00:00:00:00:01,actions=output:1"


def run_ovs_command(argv):
    args = [a.decode() if isinstance(a, bytes) else a for a in argv]
    result = subprocess.run(args, capture_output=True, text=True)
    return result.returncode


def mac_str_to_bytes(mac):
    return [int(part, 16) for part in mac.split(":")]


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


class DaimBridgeController(app_manager.OSKenApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dpid_to_bridge = {}
        self.bridge = None
        self._captured = False
        self._table_miss_installed = threading.Event()
        self._adapter_ready = threading.Event()
        self._adapter_error = None
        self._load_profile_signal_ip = LOAD_PROFILE_SIGNAL_IP
        self._load_profile_start_usage = None
        self._load_profile_start_ns = None
        self._load_profile_reported = False

        if ADAPTER_MODE == "process_per_rule":
            self.bridge = DaimCoreBridge(executor=run_ovs_command, mode="process_per_rule")
            self._adapter_ready.set()
        elif ADAPTER_MODE == "persistent":
            if not PERSISTENT_PORT:
                raise RuntimeError("DAIM_PERSISTENT_PORT is required for mode=persistent")
            threading.Thread(target=self._build_persistent_adapter, daemon=True).start()
        else:
            raise RuntimeError(f"unknown DAIM_ADAPTER_MODE: {ADAPTER_MODE!r}")

        threading.Thread(target=self._announce_when_ready, daemon=True).start()

    def _build_persistent_adapter(self):
        try:
            self.bridge = DaimCoreBridge(mode="persistent", persistent_port=int(PERSISTENT_PORT))
        except Exception as exc:  # noqa: BLE001 -- reported to the driver, not swallowed
            self._adapter_error = str(exc)
        finally:
            self._adapter_ready.set()

    def _announce_when_ready(self):
        self._adapter_ready.wait()
        if self._adapter_error:
            emit("adapter_error", detail=self._adapter_error)
            return
        self._table_miss_installed.wait()
        if self._load_profile_signal_ip:
            self._load_profile_start_usage = resource.getrusage(resource.RUSAGE_SELF)
            self._load_profile_start_ns = time.perf_counter_ns()
        emit("ready", mode=ADAPTER_MODE)

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
        t_dispatch_enter = time.perf_counter_ns()
        msg = ev.msg
        dp = msg.datapath
        p = dp.ofproto_parser
        o = dp.ofproto
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if not eth:
            return

        bridge = self._resolve_bridge_name(dp.id)
        in_port = msg.match["in_port"]

        ip_pkt = (
            pkt.get_protocol(ipv4.ipv4)
            if eth.ethertype == ether_types.ETH_TYPE_IP else None
        )

        capture = (
            not self._captured and TARGET_IP is not None
            and ip_pkt is not None and ip_pkt.dst == TARGET_IP
        )
        if capture:
            self._captured = True

        is_load_profile_signal = (
            self._load_profile_signal_ip is not None and not self._load_profile_reported
            and ip_pkt is not None and ip_pkt.src == self._load_profile_signal_ip
        )

        t_pre_ctypes = time.perf_counter_ns()
        out_port = self.bridge.packet_in(
            bridge, in_port,
            mac_str_to_bytes(eth.src), mac_str_to_bytes(eth.dst),
            eth.ethertype,
        )
        t_post_ctypes = time.perf_counter_ns()

        actions = [p.OFPActionOutput(o.OFPP_FLOOD if out_port == PORT_FLOOD else out_port)]
        dp.send_msg(p.OFPPacketOut(
            datapath=dp, buffer_id=msg.buffer_id, in_port=in_port,
            actions=actions,
            data=msg.data if msg.buffer_id == o.OFP_NO_BUFFER else None,
        ))
        t_packetout_sent = time.perf_counter_ns()

        if capture:
            self._report_timing(bridge, eth.dst, t_dispatch_enter, t_pre_ctypes,
                                 t_post_ctypes, t_packetout_sent)

        if is_load_profile_signal:
            self._report_load_profile(t_packetout_sent)

    def _report_load_profile(self, t_end_ns):
        self._load_profile_reported = True
        stats = self.bridge.stats()
        end_usage = resource.getrusage(resource.RUSAGE_SELF)
        start_usage = self._load_profile_start_usage
        elapsed_s = (t_end_ns - self._load_profile_start_ns) / 1e9

        if ADAPTER_MODE == "persistent":
            adapter_stats = self.bridge.persistent_stats()
            control_bytes_total = adapter_stats["bytes_sent"]
            control_messages_total = adapter_stats["flow_mods_sent"] + adapter_stats["echo_replies_sent"]
            control_bytes_source = "measured"
            echo_replies_sent = adapter_stats["echo_replies_sent"]
        else:
            per_flow_bytes = self.bridge.wire_flow_mod_bytes(SAMPLE_FLOW)
            control_bytes_total = stats["flows_installed"] * per_flow_bytes
            control_messages_total = stats["flows_installed"]
            control_bytes_source = "computed_wire_format"
            echo_replies_sent = None

        emit(
            "load_profile",
            mode=ADAPTER_MODE,
            no_rule_events=stats["no_rule_events"],
            flows_installed=stats["flows_installed"],
            elapsed_s=elapsed_s,
            throughput_installs_per_s=stats["flows_installed"] / elapsed_s if elapsed_s > 0 else None,
            cpu_s=(end_usage.ru_utime + end_usage.ru_stime)
                  - (start_usage.ru_utime + start_usage.ru_stime),
            max_rss_kib=end_usage.ru_maxrss,
            control_bytes_total=control_bytes_total,
            control_messages_total=control_messages_total,
            control_bytes_source=control_bytes_source,
            echo_replies_sent=echo_replies_sent,
        )

    def _report_timing(self, bridge, dst_mac, t_dispatch_enter, t_pre_ctypes,
                        t_post_ctypes, t_packetout_sent):
        c = self.bridge.last_timing()
        t_confirmed = None
        deadline = time.perf_counter() + CONFIRM_TIMEOUT_S
        needle = f"dl_dst={dst_mac}"
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
            mode=ADAPTER_MODE,
            t_dispatch_enter_ns=t_dispatch_enter,
            t_pre_ctypes_ns=t_pre_ctypes,
            c_entry_ns=c["entry_ns"],
            c_decision_done_ns=c["decision_done_ns"],
            c_table_write_done_ns=c["table_write_done_ns"],
            c_install_done_ns=c["install_done_ns"],
            c_exit_ns=c["exit_ns"],
            t_post_ctypes_ns=t_post_ctypes,
            t_packetout_sent_ns=t_packetout_sent,
            t_confirmed_ns=t_confirmed,
            installed=c["installed"],
            confirmed=t_confirmed is not None,
        )
