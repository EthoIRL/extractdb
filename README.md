# Extract DB
A thread-safe, in-memory value store supporting concurrent reads and writes.<br/>
This is not a traditional kv-store, as it does not use any form of keys.

# Guarantees.
- Fetching is guaranteed persistent FIFO
- Fetching & Pushing is thread-safe

[//]: # (- Removing from permanent store is possible but heavily degraded during fetch/push operations.)

[//]: # (# Usage)

[//]: # (The intended use of this database is to be a permanent read-only memory database. )

[//]: # (Once an item has been pushed you are heavily incentivized to never remove it. All item fetching is a clone of the original data)