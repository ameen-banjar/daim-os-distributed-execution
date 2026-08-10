"""A minimal, independent Python implementation of the DAIM peer wire
protocol (mirrors daim_peer_protocol.c's byte layout exactly, reimplemented
rather than imported so it can act as a deterministic, scriptable
adversarial/synthetic peer for the gate's G2-G4 tests: dial a real running
node and send a controlled, out-of-order sequence of HOST_LOCATION_UPDATE
messages -- something the real node's own transport, which only ever sends
its own genuinely increasing local sequence, cannot be scripted to do).

This intentionally reuses none of daim_peer_protocol.c's code, so a
successful round trip (a crafted message a real node accepts and correctly
reports back via its own JSONL "apply" events) is also an independent
cross-check of the wire format, in the same spirit as Paper 1's persistent
adapter being verified against live, unmodified OVS."""
import socket
import struct
import time

PROTOCOL_VERSION = 1
HEADER_SIZE = 30
HOST_LOCATION_WIRE_SIZE = 59

MSG_HELLO = 1
MSG_HEARTBEAT = 2
MSG_HOST_LOCATION_UPDATE = 3
MSG_STATE_SNAPSHOT_REQUEST = 4
MSG_STATE_SNAPSHOT_BEGIN = 5
MSG_STATE_SNAPSHOT_ENTRY = 6
MSG_STATE_SNAPSHOT_END = 7
MSG_ACK = 8
MSG_ERROR = 9

_HEADER_FMT = ">BBQQIQ"  # version, type, origin_node_id, message_id, payload_length, send_time_ns
_LOC_TAIL_FMT = ">QQIQQBQQ"  # origin_node_id, owner_dpid, owner_port, owner_epoch, sequence, is_local, learned_at_ns, applied_at_ns


def _now_ns():
    return time.monotonic_ns()


def encode_host_location(mac_bytes, origin_node_id, owner_dpid, owner_port,
                          owner_epoch, sequence, is_local, learned_at_ns, applied_at_ns=0):
    assert len(mac_bytes) == 6
    return mac_bytes + struct.pack(
        _LOC_TAIL_FMT, origin_node_id, owner_dpid, owner_port, owner_epoch,
        sequence, 1 if is_local else 0, learned_at_ns, applied_at_ns,
    )


def decode_host_location(payload):
    mac_bytes = payload[:6]
    (origin_node_id, owner_dpid, owner_port, owner_epoch, sequence,
     is_local, learned_at_ns, applied_at_ns) = struct.unpack(_LOC_TAIL_FMT, payload[6:HOST_LOCATION_WIRE_SIZE])
    return {
        "mac": ":".join("%02x" % b for b in mac_bytes),
        "origin_node_id": origin_node_id,
        "owner_dpid": owner_dpid,
        "owner_port": owner_port,
        "owner_epoch": owner_epoch,
        "sequence": sequence,
        "is_local": bool(is_local),
        "learned_at_ns": learned_at_ns,
        "applied_at_ns": applied_at_ns,
    }


class SyntheticPeer:
    def __init__(self, node_id, owner_epoch):
        self.node_id = node_id
        self.owner_epoch = owner_epoch
        self.sock = None
        self._next_message_id = 1

    def connect(self, host, port, timeout_s=5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout_s)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def _build_frame(self, message_type, payload=b"", protocol_version=PROTOCOL_VERSION):
        message_id = self._next_message_id
        self._next_message_id += 1
        header = struct.pack(_HEADER_FMT, protocol_version, message_type,
                              self.node_id, message_id, len(payload), _now_ns())
        return header + payload

    def _send(self, message_type, payload=b""):
        self.sock.sendall(self._build_frame(message_type, payload))

    def _send_fragmented(self, message_type, payload=b"", fragment_sizes=(1, 2, 3, 5, 8)):
        """Send one valid frame in deliberately small TCP writes, crossing
        both header and payload boundaries."""
        frame = self._build_frame(message_type, payload)
        offset = 0
        index = 0
        while offset < len(frame):
            size = fragment_sizes[index % len(fragment_sizes)]
            self.sock.sendall(frame[offset:offset + size])
            offset += size
            index += 1

    def _recv_exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("peer closed during recv")
            buf += chunk
        return buf

    def _recv_message(self):
        header = self._recv_exact(HEADER_SIZE)
        version, message_type, origin_node_id, message_id, payload_length, send_time_ns = struct.unpack(
            _HEADER_FMT, header)
        assert version == PROTOCOL_VERSION
        payload = self._recv_exact(payload_length) if payload_length else b""
        return {
            "message_type": message_type, "origin_node_id": origin_node_id,
            "message_id": message_id, "send_time_ns": send_time_ns, "payload": payload,
        }

    def handshake_and_drain_snapshot(self):
        """Sends HELLO, reads the peer's HELLO, requests its snapshot, and
        discards every entry until STATE_SNAPSHOT_END. Because the C
        transport requests a snapshot symmetrically, this peer also serves
        that request with its own empty snapshot. Returns the number of
        entries received from the C peer."""
        self._send(MSG_HELLO, struct.pack(">Q", self.owner_epoch))
        hello = self._recv_message()
        assert hello["message_type"] == MSG_HELLO

        self._send(MSG_STATE_SNAPSHOT_REQUEST)
        count = 0
        own_snapshot_done = False
        peer_request_served = False
        while not (own_snapshot_done and peer_request_served):
            msg = self._recv_message()
            if msg["message_type"] == MSG_STATE_SNAPSHOT_REQUEST:
                self._send(MSG_STATE_SNAPSHOT_END)
                peer_request_served = True
            elif msg["message_type"] == MSG_STATE_SNAPSHOT_ENTRY:
                count += 1
            elif msg["message_type"] == MSG_STATE_SNAPSHOT_END:
                own_snapshot_done = True
        return count

    def send_host_location_update(self, mac_bytes, origin_node_id, owner_dpid, owner_port,
                                   owner_epoch, sequence, is_local=False, learned_at_ns=None):
        if learned_at_ns is None:
            learned_at_ns = _now_ns()
        payload = encode_host_location(mac_bytes, origin_node_id, owner_dpid, owner_port,
                                        owner_epoch, sequence, is_local, learned_at_ns)
        self._send(MSG_HOST_LOCATION_UPDATE, payload)

    def send_host_location_update_fragmented(
            self, mac_bytes, origin_node_id, owner_dpid, owner_port,
            owner_epoch, sequence, is_local=False, learned_at_ns=None):
        if learned_at_ns is None:
            learned_at_ns = _now_ns()
        payload = encode_host_location(mac_bytes, origin_node_id, owner_dpid, owner_port,
                                       owner_epoch, sequence, is_local, learned_at_ns)
        self._send_fragmented(MSG_HOST_LOCATION_UPDATE, payload)

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None
