#include "daim_peer_protocol.h"

#include <assert.h>
#include <string.h>

int main(void)
{
    uint8_t buf[512];

    /* Header round-trip. */
    {
        struct daim_peer_header hdr, decoded;
        memset(&hdr, 0, sizeof(hdr));
        hdr.protocol_version = DAIM_PEER_PROTOCOL_VERSION;
        hdr.message_type = DAIM_PEER_MSG_HOST_LOCATION_UPDATE;
        hdr.origin_node_id = 4;
        hdr.message_id = 12345;
        hdr.payload_length = DAIM_PEER_HOST_LOCATION_WIRE_SIZE;
        hdr.send_time_ns = 9876543210ULL;
        daim_peer_encode_header(buf, &hdr);
        assert(daim_peer_decode_header(buf, DAIM_PEER_HEADER_SIZE, &decoded) == 0);
        assert(decoded.message_type == DAIM_PEER_MSG_HOST_LOCATION_UPDATE);
        assert(decoded.origin_node_id == 4);
        assert(decoded.message_id == 12345);
        assert(decoded.payload_length == DAIM_PEER_HOST_LOCATION_WIRE_SIZE);
        assert(decoded.send_time_ns == 9876543210ULL);

        /* Truncated header: partial read must fail cleanly, not overrun. */
        assert(daim_peer_decode_header(buf, DAIM_PEER_HEADER_SIZE - 1, &decoded) == -1);

        /* Invalid protocol version. */
        buf[0] = DAIM_PEER_PROTOCOL_VERSION + 1;
        assert(daim_peer_decode_header(buf, DAIM_PEER_HEADER_SIZE, &decoded) == -1);
        buf[0] = DAIM_PEER_PROTOCOL_VERSION;

        /* Oversized payload_length is rejected even though the header itself parses. */
        hdr.payload_length = DAIM_PEER_MAX_PAYLOAD_SIZE + 1;
        daim_peer_encode_header(buf, &hdr);
        assert(daim_peer_decode_header(buf, DAIM_PEER_HEADER_SIZE, &decoded) == -2);
    }

    /* host_location round-trip. */
    {
        struct daim_host_location loc, decoded;
        memset(&loc, 0, sizeof(loc));
        memset(&decoded, 0, sizeof(decoded));
        loc.mac[0] = 0xaa; loc.mac[5] = 0xbb;
        loc.origin_node_id = 7;
        loc.owner_dpid = 42;
        loc.owner_port = 3;
        loc.owner_epoch = 2;
        loc.sequence = 99;
        loc.is_local = 1;
        loc.learned_at_ns = 111;
        loc.applied_at_ns = 222;
        daim_peer_encode_host_location(buf, &loc);
        assert(daim_peer_decode_host_location(buf, DAIM_PEER_HOST_LOCATION_WIRE_SIZE, &decoded) == 0);
        assert(memcmp(&loc, &decoded, sizeof(loc)) == 0);

        /* Partial read of a multi-field payload must fail, not silently
           decode a truncated struct. */
        assert(daim_peer_decode_host_location(buf, DAIM_PEER_HOST_LOCATION_WIRE_SIZE - 1, &decoded) == -1);
        assert(daim_peer_decode_host_location(buf, DAIM_PEER_HOST_LOCATION_WIRE_SIZE + 1, &decoded) == -1);
    }

    /* hello round-trip. */
    {
        uint64_t epoch;
        daim_peer_encode_hello(buf, 55);
        assert(daim_peer_decode_hello(buf, 8, &epoch) == 0);
        assert(epoch == 55);
        assert(daim_peer_decode_hello(buf, 7, &epoch) == -1);
        assert(daim_peer_decode_hello(buf, 9, &epoch) == -1);
    }

    /* ack round-trip. */
    {
        uint64_t acked;
        daim_peer_encode_ack(buf, 777);
        assert(daim_peer_decode_ack(buf, 8, &acked) == 0);
        assert(acked == 777);
    }

    /* snapshot_begin round-trip. */
    {
        uint32_t count;
        daim_peer_encode_snapshot_begin(buf, 4096);
        assert(daim_peer_decode_snapshot_begin(buf, 4, &count) == 0);
        assert(count == 4096);
    }

    /* error round-trip, including bounded text length. */
    {
        uint16_t code;
        char text[64];
        size_t n = daim_peer_encode_error(buf, sizeof(buf), 42, "stale sequence");
        assert(n == 4 + strlen("stale sequence"));
        assert(daim_peer_decode_error(buf, n, &code, text, sizeof(text)) == 0);
        assert(code == 42);
        assert(strcmp(text, "stale sequence") == 0);

        /* Oversized error text is rejected at encode time (bounded allocation). */
        {
            char long_text[DAIM_PEER_MAX_ERROR_TEXT + 2];
            memset(long_text, 'x', sizeof(long_text) - 1);
            long_text[sizeof(long_text) - 1] = '\0';
            assert(daim_peer_encode_error(buf, sizeof(buf), 1, long_text) == 0);
        }

        /* Truncated error payload (claims more text than is present). */
        n = daim_peer_encode_error(buf, sizeof(buf), 42, "short");
        assert(daim_peer_decode_error(buf, n - 1, &code, text, sizeof(text)) == -1);

        /* Decoded text truncates safely into a small output buffer. */
        {
            char small[4];
            assert(daim_peer_decode_error(buf, n, &code, small, sizeof(small)) == 0);
            assert(strlen(small) == 3);
        }
    }

    return 0;
}
