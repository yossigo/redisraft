Raft Persistence
================

Data is persisted to disk in order to make it possible to recover from a
process crash. We can assume all data that's in disk is also in memory.

Normally a node needs to persist data before producing a reply. In this
context we'll assume that this is configurable, the same way AOF is.

Events that trigger persistence include:
* Persist a new term
* Persist the vote given on a term
* Persist a log entry added to the log (received from user or from leader)
* Persist removal of a log entry from the end of the log

Another set of events are related to log compaction and snapshots. A Raft node
is allowed to remove elements from the log, as long as:
# They were applied locally to the FSM.
# The FSM is persisted, so on recovery it will be available along with the
  index that indicates which is the last applied entry.
# The node is able to deliver the FSM to another node as a *snapshot*, using a
  dedicated mechanism.


Reasons to rely on Redis persistence
------------------------------------

# Latency; using the same mechanism instead of writing (and possibly syncing)
  to a different file should be faster.
# Convenience; users can use the same configuration, same files/file formats
  as with any other Redis server.


Reasons not to rely on Redis persistence
----------------------------------------

# RedisModule_Replicate() cannot be called on a Thread Safe context.


AOF Commands
------------

The Raft module uses AOF for persistence, and interleaves various
Raft-specific updates with other Redis updates.

When a new term or term/vote are persisted:
```
RAFT.VOTE <term> <vote>
```

When a new entry is appended to the log:
```
RAFT.ENTRY <idx> <entry data>
```

When removing an entry from the log (top or bottom):
```
RAFT.DELENTRY <idx>
```

When a committed entry is applied to the FSM (i.e. sent to Redis as a
`RedisModule_Call()`), the module first propagates a marker. We rely on Redis
to fsync the commands together so this will be as atomic as possible (at least
without extensive changes to Redis).
```
RAFT.APPLY <idx>
```

Ideally these commands should never be processed unless read from AOF.


Compaction and Snapshots
------------------------

Raft refers to compaction as a periodic process where logs are trimmed and the
FSM state is persisted. The persistent image can then be delivered on demand
to nodes that require missing log entries (or even nodes that are simply too
slow to process logs).

When compared to Redis with persistence enabled:
# There is no need to periodically create the snapshot, as it's always
  available on disk.
# The process of delivering a snapshot can be considered a lazy one, i.e.
  replication is initiated and a fresh RDB is created and delivered.

In order to properly deal with AOF rewrites, the Raft module needs to register
a custom data type with a single place-holder key.  When invoked, it will
persist the `RAFT.VOTE`, `RAFT.APPLY` (indicating latest applied entry) and
`RAFT.ENTRY` for all log entries not yet applied.
