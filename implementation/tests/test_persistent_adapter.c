#define _POSIX_C_SOURCE 200809L

#include "ovs_persistent_adapter.h"
#include "daim_switch_adapter.h"

#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define TEST_PORT 16653

struct creator_args {
    struct daim_switch_adapter *adapter;
    int result;
};

static void *creator_thread(void *arg) {
    struct creator_args *a = arg;
    a->result = daim_ovs_persistent_adapter_create(a->adapter, TEST_PORT, 5);
    return NULL;
}

/* Acts as the "switch" side: connects, completes the OF1.3 hello handshake,
   and returns the connected fd. */
static int connect_as_switch(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(TEST_PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    int connected = -1;
    for (int attempt = 0; attempt < 500; attempt++) {
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) { connected = 0; break; }
        poll(NULL, 0, 20); /* portable 20ms sleep */
    }
    assert(connected == 0);

    /* Read the adapter's Hello. */
    uint8_t header[8];
    assert(recv(fd, header, 8, MSG_WAITALL) == 8);
    assert(header[0] == 0x04); /* OFP_VERSION_13 */
    assert(header[1] == 0);    /* OFPT_HELLO */

    /* Send our own Hello. */
    uint8_t hello[8] = {0x04, 0, 0, 8, 0, 0, 0, 1};
    assert(send(fd, hello, 8, 0) == 8);
    return fd;
}

static void read_message(int fd, uint8_t *buf, size_t bufsize, size_t *out_len) {
    uint8_t header[8];
    assert(recv(fd, header, 8, MSG_WAITALL) == 8);
    memcpy(buf, header, 8);
    uint16_t len = ntohs(*(uint16_t *)(header + 2));
    assert(len <= bufsize);
    if (len > 8) {
        assert(recv(fd, buf + 8, len - 8, MSG_WAITALL) == (ssize_t)(len - 8));
    }
    *out_len = len;
}

static uint16_t be16(const uint8_t *p) { return (uint16_t)((p[0] << 8) | p[1]); }
static uint32_t be32(const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }

int main(void) {
    struct daim_switch_adapter adapter = {0};
    struct creator_args args = {&adapter, -1};
    pthread_t t;
    pthread_create(&t, NULL, creator_thread, &args);

    int switch_fd = connect_as_switch();

    pthread_join(t, NULL);
    assert(args.result == 0);
    assert(adapter.ops != NULL);

    /* --- flow_add: priority=100,in_port=2,dl_dst=00:11:22:33:44:55,actions=output:3 --- */
    assert(adapter.ops->flow_add(&adapter, "s1",
        "priority=100,in_port=2,dl_dst=00:11:22:33:44:55,actions=output:3") == 0);

    uint8_t msg[256];
    size_t len;
    read_message(switch_fd, msg, sizeof(msg), &len);

    assert(msg[0] == 0x04);          /* version */
    assert(msg[1] == 14);            /* OFPT_FLOW_MOD */
    /* xid at [4..8) not checked precisely, just non-zero-length message */
    assert(len >= 56);               /* fixed flow_mod header without match/actions */

    /* command is byte 17 (offset: 8 header + 8 cookie + 8 cookie_mask + 1 table_id) */
    assert(msg[8 + 8 + 8 + 1] == 0); /* OFPFC_ADD */

    /* priority is at offset 8+8+8+1+1+2+2 = 30, 2 bytes */
    uint16_t priority = be16(msg + 30);
    assert(priority == 100);

    /* match starts at offset 8(header)+8(cookie)+8(cookiemask)+1(table)+1(cmd)
       +2(idle)+2(hard)+2(prio)+4(buffer)+4(outport)+4(outgroup)+2(flags)+2(pad) = 48 */
    size_t match_off = 48;
    uint16_t match_type = be16(msg + match_off);
    uint16_t match_len = be16(msg + match_off + 2);
    assert(match_type == 1); /* OFPMT_OXM */

    /* Walk the two expected OXM TLVs: IN_PORT (class/field/len header 4 bytes
       + 4 bytes value) then ETH_DST (4 bytes header + 6 bytes value). */
    size_t oxm_off = match_off + 4;
    uint32_t oxm1 = be32(msg + oxm_off);
    uint16_t oxm1_class = (uint16_t)(oxm1 >> 16);
    uint8_t oxm1_field = (uint8_t)((oxm1 >> 9) & 0x7f);
    uint8_t oxm1_len = (uint8_t)(oxm1 & 0xff);
    assert(oxm1_class == 0x8000);
    assert(oxm1_field == 0); /* OXM_OF_IN_PORT */
    assert(oxm1_len == 4);
    uint32_t in_port_value = be32(msg + oxm_off + 4);
    assert(in_port_value == 2);

    size_t oxm2_off = oxm_off + 4 + 4;
    uint32_t oxm2 = be32(msg + oxm2_off);
    uint8_t oxm2_field = (uint8_t)((oxm2 >> 9) & 0x7f);
    uint8_t oxm2_len = (uint8_t)(oxm2 & 0xff);
    assert(oxm2_field == 3); /* OXM_OF_ETH_DST */
    assert(oxm2_len == 6);
    uint8_t expected_mac[6] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55};
    assert(memcmp(msg + oxm2_off + 4, expected_mac, 6) == 0);

    (void)match_len;

    /* Actions: OFPIT_APPLY_ACTIONS with one OFPAT_OUTPUT to port 3.
       match_len covers class(2)+len(2)+8(in_port TLV)+10(eth_dst TLV)=22,
       padded to 24; instructions follow immediately after. */
    size_t match_total_padded = (match_len + 7u) & ~7u;
    size_t instr_off = match_off + match_total_padded;
    uint16_t instr_type = be16(msg + instr_off);
    assert(instr_type == 4); /* OFPIT_APPLY_ACTIONS */
    size_t action_off = instr_off + 8; /* instruction header is 8 bytes */
    uint16_t action_type = be16(msg + action_off);
    uint32_t action_port = be32(msg + action_off + 4);
    assert(action_type == 0); /* OFPAT_OUTPUT */
    assert(action_port == 3);

    /* --- flow_add with ip+normal (Stage 2 style) --- */
    assert(adapter.ops->flow_add(&adapter, "s1", "priority=100,ip,actions=normal") == 0);
    read_message(switch_fd, msg, sizeof(msg), &len);
    assert(msg[1] == 14);

    /* --- flow_delete --- */
    assert(adapter.ops->flow_delete(&adapter, "s1", "in_port=2") == 0);
    read_message(switch_fd, msg, sizeof(msg), &len);
    assert(msg[1] == 14);
    assert(msg[8 + 8 + 8 + 1] == 3); /* OFPFC_DELETE */

    /* --- echo request/reply keepalive, handled by the reader thread --- */
    uint8_t echo_req[8] = {0x04, 2, 0, 8, 0, 0, 0, 42};
    assert(send(switch_fd, echo_req, 8, 0) == 8);
    uint8_t echo_reply[8];
    assert(recv(switch_fd, echo_reply, 8, MSG_WAITALL) == 8);
    assert(echo_reply[1] == 3); /* OFPT_ECHO_REPLY */
    assert(be32(echo_reply + 4) == 42);

    struct daim_persistent_adapter_stats stats;
    assert(daim_ovs_persistent_adapter_get_stats(&adapter, &stats) == 0);
    assert(stats.flow_mods_sent == 3);
    assert(stats.echo_replies_sent == 1);

    adapter.ops->destroy(&adapter);
    close(switch_fd);

    return 0;
}
