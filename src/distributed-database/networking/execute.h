#include <stdint.h>

#include "networking/msg.h"

/**
 * Execute a message
 * @param msg the message to execute
 * @param senderId the id of the node that sent the message
 * @param dequeuedAtNs monotonic time this message was popped off its
 * peer's job queue, i.e. right before processing started - used by
 * APPEND_ENTRIES_RESPONSE to split RPC latency into queue/network time vs
 * time spent waiting to acquire the raft node lock
 */
extern void execute(Msg msg, int senderId, uint64_t dequeuedAtNs);
