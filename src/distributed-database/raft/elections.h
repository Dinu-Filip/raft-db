#ifndef ELECTIONS_H
#define ELECTIONS_H

#include <stdbool.h>

extern void setElectionTimeout(void);

/**
 * Increment current term, set state to CANDIDATE, vote for itself,
 * issue RequestVote RPCs to all other nodes
 */
extern void commenceElection(void);

/**
 * Set state to leader and initialise other leader properties
 */
extern void electionWon(void);

/**
 * Check if the election has been won
 * @return bool that is true iff the current node has won the election
 */
extern bool checkElectionWon(void);

/**
 * Check if the current node should call an election
 * i.e. the election timeout has been exceeded
 * @return bool that is true iff the current node should call an election
 */
extern bool shouldCallElection(void);

#endif  // ELECTIONS_H
