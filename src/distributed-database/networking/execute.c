#include "execute.h"

#include "networking/rpc.h"
#include "raft/callbacks.h"

// Identify is handled directly by the network handler so can be ignored
// here
void execute(Msg msg, int senderId) {
    switch (msg->type) {
        // Included to stop the compiler complaining
        case CLIENT_IDENTIFY:
        case SERVER_IDENTIFY:
            break;
        case PING:
            processPing(senderId);
            break;
        case REQUEST_VOTE:
            handleRequestVote(senderId, msg->data.requestVote.senderTerm,
                              msg->data.requestVote.lastLogIndex,
                              msg->data.requestVote.lastLogTerm);
            break;
        case REQUEST_VOTE_RESPONSE:
            handleRequestVoteResponse(
                senderId, msg->data.requestVoteResponse.term,
                msg->data.requestVoteResponse.voteGranted);
            break;
        case APPEND_ENTRIES:
            handleAppendEntries(senderId, msg->data.appendEntries.term,
                                msg->data.appendEntries.prevLogIndex,
                                msg->data.appendEntries.prevLogTerm,
                                msg->data.appendEntries.leaderCommit,
                                msg->data.appendEntries.numEntries,
                                msg->data.appendEntries.entries);
            break;
        case APPEND_ENTRIES_RESPONSE:
            handleAppendEntriesResponse(
                senderId, msg->data.appendEntriesResponse.prevLogIndex,
                msg->data.appendEntriesResponse.numEntries,
                msg->data.appendEntriesResponse.term,
                msg->data.appendEntriesResponse.success);
            break;
    }
}
