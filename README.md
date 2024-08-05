# xdb
xdb is an ACID compliant database and storage engine written purely in go. As of now only the storage engine part is complete. Full database with query parser will be implemented soon.

### Implementation
* B+trees for storing data
* Copy-on-write mechanism for atomic updates
* mmap to map disk pages to memory
* fsync to flush mapped pages to durable storage

### Features
* Atomic updates (either full transaction or none at all)
* Durable to process crashes and power failures (Not so sure about disk failures, uses fsync)
* Point queries in O(logn) time
* Range queries in O(k * logn) time
* Secondary Indexes
* Transactions

### Contributing
The code contains many bugs, and there is much room for optimization, so any contributions are welcome.\
[Todo](https://github.com/vanshjangir/xdb/issues/1)

### Useful
* [build-your-own-db](https://build-your-own.org/database/00a_overview)
* [mmap-tsoding](https://www.youtube.com/watch?v=sFYFuBzu9Ow)
* [B+tree visualization](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)
