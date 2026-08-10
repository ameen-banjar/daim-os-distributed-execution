#ifndef DAIM_PEER_PROTOCOL_H
#define DAIM_PEER_PROTOCOL_H

#include "daim_distributed_state.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Wire protocol for DAIM peer state exchange: length-prefixed binary
   messages (the fixed header below carries payload_length, which the
   transport reads before reading the payload), network byte order,
   bounded size. Pure encode/decode here -- no sockets, no I/O -- so the
   wire format is independently unit-testable, mirroring the separation
   ovs_persistent_adapter.c uses between build_flow_mod() and the socket
   code around it. */

#define DAIM_PEER_PROTOCOL_VERSION 1
#define DAIM_PEER_HEADER_SIZE 30
#define DAIM_PEER_HOST_LOCATION_WIRE_SIZE 59
#define DAIM_PEER_MAX_PAYLOAD_SIZE 4096
#define DAIM_PEER_MAX_ERROR_TEXT 128

enum daim_peer_message_type {
    DAIM_PEER_MSG_HELLO = 1,
    DAIM_PEER_MSG_HEARTBEAT = 2,
    DAIM_PEER_MSG_HOST_LOCATION_UPDATE = 3,
    DAIM_PEER_MSG_STATE_SNAPSHOT_REQUEST = 4,
    DAIM_PEER_MSG_STATE_SNAPSHOT_BEGIN = 5,
    DAIM_PEER_MSG_STATE_SNAPSHOT_ENTRY = 6,
    DAIM_PEER_MSG_STATE_SNAPSHOT_END = 7,
    DAIM_PEER_MSG_ACK = 8,
    DAIM_PEER_MSG_ERROR = 9,
};

struct daim_peer_header {
    uint8_t protocol_version;
    uint8_t message_type;
    uint64_t origin_node_id;
    uint64_t message_id;
    uint32_t payload_length;
    uint64_t send_time_ns;
};

/* Always writes exactly DAIM_PEER_HEADER_SIZE bytes to buf (caller-sized). */
void daim_peer_encode_header(uint8_t *buf, const struct daim_peer_header *hdr);

/* Returns 0 and fills *out on success. Returns -1 if len is too small or
   protocol_version does not match, -2 if the declared payload_length
   exceeds DAIM_PEER_MAX_PAYLOAD_SIZE (oversized-message rejection). */
int daim_peer_decode_header(const uint8_t *buf, size_t len, struct daim_peer_header *out);

/* Always writes exactly DAIM_PEER_HOST_LOCATION_WIRE_SIZE bytes. */
void daim_peer_encode_host_location(uint8_t *buf, const struct daim_host_location *loc);

/* Returns 0 on success, -1 unless len is exactly the fixed wire size. */
int daim_peer_decode_host_location(const uint8_t *buf, size_t len, struct daim_host_location *out);

/* HELLO payload: this node's current owner_epoch, 8 bytes. */
void daim_peer_encode_hello(uint8_t *buf, uint64_t owner_epoch);
int daim_peer_decode_hello(const uint8_t *buf, size_t len, uint64_t *owner_epoch);

/* ACK payload: the message_id being acknowledged, 8 bytes. */
void daim_peer_encode_ack(uint8_t *buf, uint64_t acked_message_id);
int daim_peer_decode_ack(const uint8_t *buf, size_t len, uint64_t *acked_message_id);

/* STATE_SNAPSHOT_BEGIN payload: expected entry count, 4 bytes. */
void daim_peer_encode_snapshot_begin(uint8_t *buf, uint32_t entry_count);
int daim_peer_decode_snapshot_begin(const uint8_t *buf, size_t len, uint32_t *entry_count);

/* ERROR payload: u16 code + u16 text length + up to DAIM_PEER_MAX_ERROR_TEXT
   bytes of text (not NUL-terminated on the wire). Returns the number of
   payload bytes written (4 + text length), or 0 if message is too long. */
size_t daim_peer_encode_error(uint8_t *buf, size_t buf_cap, uint16_t code, const char *message);

/* message_out_cap must be > 0; the decoded text is NUL-terminated within
   that capacity (truncated if necessary). Returns 0 on success, -1 on a
   malformed/short payload. */
int daim_peer_decode_error(const uint8_t *buf, size_t len, uint16_t *code,
                            char *message_out, size_t message_out_cap);

#ifdef __cplusplus
}
#endif

#endif
