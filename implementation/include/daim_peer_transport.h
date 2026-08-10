#ifndef DAIM_PEER_TRANSPORT_H
#define DAIM_PEER_TRANSPORT_H

#include "daim_distributed_state.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Peer-to-peer state dissemination transport: one persistent TCP connection
   per configured peer (dialed, reconnected with bounded exponential backoff
   on drop) plus a listener accepting the reciprocal connections dialed by
   those same peers.  Which endpoint dials is a deployment policy: the
   four-node prototype gate configures exactly one full-duplex connection
   per unordered node pair (lower node ID dials higher node ID), while the
   transport also tolerates reciprocal connections because remote apply is
   idempotent.

   Partitioning for E6 is done at the OS level (e.g. iptables on the peer
   port) by the test harness, not inside this module -- this transport only
   detects it via a read failure/EOF on the affected connection(s) and
   reconnects once the harness restores the path. */

struct daim_peer_transport;

struct daim_peer_transport_stats {
    uint64_t active_connections;
    uint64_t configured_peers;
    uint64_t messages_sent;
    uint64_t bytes_sent;
    uint64_t messages_received;
    uint64_t bytes_received;
    uint64_t reconnect_attempts;
    uint64_t snapshot_entries_sent;
    uint64_t snapshot_entries_received;
};

/* Fired for every apply attempt (local snapshot entries during resync, and
   steady-state HOST_LOCATION_UPDATE messages) with the fully decoded record
   and the result daim_host_apply_remote/daim_host_import_snapshot_entry
   returned -- loc->learned_at_ns and loc->applied_at_ns are exactly Tlearn
   (at the owning node) and Tapply (here), giving propagation latency
   directly. May be called concurrently from different connections'
   threads; the callback must be thread-safe or do its own queuing. */
typedef void (*daim_peer_apply_event_fn)(void *context, uint64_t from_peer_node_id,
                                          const struct daim_host_location *loc,
                                          enum daim_host_apply_result result);

/* Fired for connection lifecycle events: "connected", "disconnected",
   "disconnect_reason", "reconnecting", "snapshot_sent",
   "snapshot_received". detail is event-specific (errno for disconnected;
   0 clean, 1 HELLO send, 2 HELLO receive/protocol, 3 snapshot, or 4 steady
   receive/framing for disconnect_reason; attempt count for reconnecting;
   entry count for snapshot events). */
typedef void (*daim_peer_lifecycle_event_fn)(void *context, const char *event_name,
                                              uint64_t peer_node_id, uint64_t detail);

struct daim_peer_transport_callbacks {
    daim_peer_apply_event_fn on_apply;
    daim_peer_lifecycle_event_fn on_lifecycle;
    void *context;
};

/* Starts listening on listen_port immediately (accepting connections in a
   background thread). Does not block. owner_epoch is this node's current
   epoch, sent in every HELLO. daim_host_state_init() must already have been
   called with the same node_id before this. */
struct daim_peer_transport *daim_peer_transport_create(
    uint64_t node_id, uint64_t owner_epoch, int listen_port,
    const struct daim_peer_transport_callbacks *callbacks);

/* Registers a peer to dial; spawns a dedicated connect/maintain thread that
   retries with bounded exponential backoff (base 100ms, cap 5s) until
   daim_peer_transport_destroy. Safe to call any time after create. */
int daim_peer_transport_add_peer(struct daim_peer_transport *t, const char *host, int port);

/* Sends loc as a HOST_LOCATION_UPDATE on every currently active connection
   (outbound and inbound). Returns the number of connections it was sent
   on. Does not wait for the peer to apply or acknowledge it. */
int daim_peer_transport_disseminate(struct daim_peer_transport *t, const struct daim_host_location *loc);

void daim_peer_transport_get_stats(struct daim_peer_transport *t, struct daim_peer_transport_stats *out);

/* Stops all threads, closes all connections (including the listener), and
   frees t. Blocks until every thread has exited. */
void daim_peer_transport_destroy(struct daim_peer_transport *t);

#ifdef __cplusplus
}
#endif

#endif
