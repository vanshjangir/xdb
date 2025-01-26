# xdb
xdb is an ACID compliant database and storage engine written purely in go.

### Implementation
* B+trees
* Copy-on-write mechanism
* mmap and fsync

### Features
* Atomic updates (either full transaction or none at all)
* Durable to process crashes and power failures (Not so sure about disk failures, uses fsync)
* Point queries in O(logn) time
* Range queries in O(k * logn) time
* Secondary Indexes
* Transactions

### Usage
Currently working on more complex sql queries, check docs [here](https://github.com/vanshjangir/xdb/blob/master/DOCS.md).

### Contributing
The code contains many bugs, and there is much room for optimization, so any contributions are welcome.\
[Todo](https://github.com/vanshjangir/xdb/issues/1)

### References
* [build-your-own-db](https://build-your-own.org/database/00a_overview)
* [mmap-tsoding](https://www.youtube.com/watch?v=sFYFuBzu9Ow)
* [B+tree visualization](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)
