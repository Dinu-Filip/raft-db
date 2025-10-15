# Distributed DBMS with RAFT

This is a fault-tolerant distribute DBMS developed in collaboration with [Max Smith](https://github.com/Max-Sepp), [Vladimir Filip](https://github.com/vladimirfilip) and [Daniel Howard](https://github.com/danielnhoward). In contrast to traditional ACID database systems, this system follows the BASE (Basically Available, Soft State, Eventually Consistent) transaction model, suitable for situtations in which stale data is permitted given that a consistent state will eventually be reached. 

We implemented the RAFT consensus algorithm for a cluster of servers, in which each node holds a log of transactions and nodes vote for a leader which acts as a central source of truth. If the leader fails, elections are held to select a new leader and synchronise the logs between the leader and its followers. Our implementation follows that described in Diego Ongaro's and John Ousterhout's original paper:
[In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)

We also implemented our own networking protocols for transmitting logs and transactions between servers, and built the underlying database engine from scratch.

# Features
- Supports failure of up to $\lfloor n/2 \rfloor$ nodes
- Query data sent between client and server using HTTP as JSON
- Database supports all CRUD operations with wide range of fixed and variable-length data types

# Credits
The RAFT algorithm was implemented by Max Smith and Vladimir Filip.
The networking protocols, serialisation methods and RPCs between nodes were developed by Daniel Howard.
The database engine was designed by Constantin Filip.
