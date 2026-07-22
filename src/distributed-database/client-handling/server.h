#ifndef CLIENT_HANDLING_SERVER_H
#define CLIENT_HANDLING_SERVER_H

extern void *runClientHandlingServer(void *arg);

/**
 * Wakes this node's client-handling event loop to re-check pending writes
 * immediately. Safe to call from any thread; a no-op before startServer()
 * has run.
 */
extern void notifyWriteProgress(void);

#endif  // CLIENT_HANDLING_SERVER_H
