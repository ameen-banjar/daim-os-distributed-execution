#include "daim_peer_protocol.h"

#include <string.h>

/* Manual big-endian packing (not htons/htobe64): those come from different
   headers on macOS vs Linux, and this module has no need for any system
   byte-order header at all if it just shifts bytes itself -- portable by
   construction across both build hosts this project uses. */

static void put_u8(uint8_t *buf, size_t *off, uint8_t v)
{
    buf[(*off)++] = v;
}

static void put_u16(uint8_t *buf, size_t *off, uint16_t v)
{
    buf[(*off)++] = (uint8_t)(v >> 8);
    buf[(*off)++] = (uint8_t)(v & 0xff);
}

static void put_u32(uint8_t *buf, size_t *off, uint32_t v)
{
    buf[(*off)++] = (uint8_t)(v >> 24);
    buf[(*off)++] = (uint8_t)(v >> 16);
    buf[(*off)++] = (uint8_t)(v >> 8);
    buf[(*off)++] = (uint8_t)(v & 0xff);
}

static void put_u64(uint8_t *buf, size_t *off, uint64_t v)
{
    int shift;
    for (shift = 56; shift >= 0; shift -= 8) {
        buf[(*off)++] = (uint8_t)(v >> shift);
    }
}

static uint8_t get_u8(const uint8_t *buf, size_t *off)
{
    return buf[(*off)++];
}

static uint16_t get_u16(const uint8_t *buf, size_t *off)
{
    uint16_t v = (uint16_t)((buf[*off] << 8) | buf[*off + 1]);
    *off += 2;
    return v;
}

static uint32_t get_u32(const uint8_t *buf, size_t *off)
{
    uint32_t v = ((uint32_t)buf[*off] << 24) | ((uint32_t)buf[*off + 1] << 16) |
                 ((uint32_t)buf[*off + 2] << 8) | (uint32_t)buf[*off + 3];
    *off += 4;
    return v;
}

static uint64_t get_u64(const uint8_t *buf, size_t *off)
{
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; ++i) {
        v = (v << 8) | buf[*off + i];
    }
    *off += 8;
    return v;
}

void daim_peer_encode_header(uint8_t *buf, const struct daim_peer_header *hdr)
{
    size_t off = 0;
    put_u8(buf, &off, hdr->protocol_version);
    put_u8(buf, &off, hdr->message_type);
    put_u64(buf, &off, hdr->origin_node_id);
    put_u64(buf, &off, hdr->message_id);
    put_u32(buf, &off, hdr->payload_length);
    put_u64(buf, &off, hdr->send_time_ns);
}

int daim_peer_decode_header(const uint8_t *buf, size_t len, struct daim_peer_header *out)
{
    size_t off = 0;
    if (len < DAIM_PEER_HEADER_SIZE) {
        return -1;
    }
    out->protocol_version = get_u8(buf, &off);
    if (out->protocol_version != DAIM_PEER_PROTOCOL_VERSION) {
        return -1;
    }
    out->message_type = get_u8(buf, &off);
    out->origin_node_id = get_u64(buf, &off);
    out->message_id = get_u64(buf, &off);
    out->payload_length = get_u32(buf, &off);
    out->send_time_ns = get_u64(buf, &off);
    if (out->payload_length > DAIM_PEER_MAX_PAYLOAD_SIZE) {
        return -2;
    }
    return 0;
}

void daim_peer_encode_host_location(uint8_t *buf, const struct daim_host_location *loc)
{
    size_t off = 0;
    memcpy(buf + off, loc->mac, MAC_ADDR_LEN);
    off += MAC_ADDR_LEN;
    put_u64(buf, &off, loc->origin_node_id);
    put_u64(buf, &off, loc->owner_dpid);
    put_u32(buf, &off, loc->owner_port);
    put_u64(buf, &off, loc->owner_epoch);
    put_u64(buf, &off, loc->sequence);
    put_u8(buf, &off, loc->is_local);
    put_u64(buf, &off, loc->learned_at_ns);
    put_u64(buf, &off, loc->applied_at_ns);
}

int daim_peer_decode_host_location(const uint8_t *buf, size_t len, struct daim_host_location *out)
{
    size_t off = 0;
    if (len != DAIM_PEER_HOST_LOCATION_WIRE_SIZE) {
        return -1;
    }
    memcpy(out->mac, buf + off, MAC_ADDR_LEN);
    off += MAC_ADDR_LEN;
    out->origin_node_id = get_u64(buf, &off);
    out->owner_dpid = get_u64(buf, &off);
    out->owner_port = get_u32(buf, &off);
    out->owner_epoch = get_u64(buf, &off);
    out->sequence = get_u64(buf, &off);
    out->is_local = get_u8(buf, &off);
    out->learned_at_ns = get_u64(buf, &off);
    out->applied_at_ns = get_u64(buf, &off);
    return 0;
}

void daim_peer_encode_hello(uint8_t *buf, uint64_t owner_epoch)
{
    size_t off = 0;
    put_u64(buf, &off, owner_epoch);
}

int daim_peer_decode_hello(const uint8_t *buf, size_t len, uint64_t *owner_epoch)
{
    size_t off = 0;
    if (len != 8) {
        return -1;
    }
    *owner_epoch = get_u64(buf, &off);
    return 0;
}

void daim_peer_encode_ack(uint8_t *buf, uint64_t acked_message_id)
{
    size_t off = 0;
    put_u64(buf, &off, acked_message_id);
}

int daim_peer_decode_ack(const uint8_t *buf, size_t len, uint64_t *acked_message_id)
{
    size_t off = 0;
    if (len != 8) {
        return -1;
    }
    *acked_message_id = get_u64(buf, &off);
    return 0;
}

void daim_peer_encode_snapshot_begin(uint8_t *buf, uint32_t entry_count)
{
    size_t off = 0;
    put_u32(buf, &off, entry_count);
}

int daim_peer_decode_snapshot_begin(const uint8_t *buf, size_t len, uint32_t *entry_count)
{
    size_t off = 0;
    if (len != 4) {
        return -1;
    }
    *entry_count = get_u32(buf, &off);
    return 0;
}

size_t daim_peer_encode_error(uint8_t *buf, size_t buf_cap, uint16_t code, const char *message)
{
    size_t off = 0;
    size_t text_len = message ? strlen(message) : 0;
    if (text_len > DAIM_PEER_MAX_ERROR_TEXT) {
        return 0;
    }
    if (buf_cap < 4 + text_len) {
        return 0;
    }
    put_u16(buf, &off, code);
    put_u16(buf, &off, (uint16_t)text_len);
    if (text_len) {
        memcpy(buf + off, message, text_len);
        off += text_len;
    }
    return off;
}

int daim_peer_decode_error(const uint8_t *buf, size_t len, uint16_t *code,
                            char *message_out, size_t message_out_cap)
{
    size_t off = 0;
    uint16_t text_len;
    size_t copy_len;
    if (len < 4 || message_out_cap == 0) {
        return -1;
    }
    *code = get_u16(buf, &off);
    text_len = get_u16(buf, &off);
    if (len < (size_t)(4 + text_len)) {
        return -1;
    }
    copy_len = text_len < message_out_cap - 1 ? text_len : message_out_cap - 1;
    memcpy(message_out, buf + off, copy_len);
    message_out[copy_len] = '\0';
    return 0;
}
