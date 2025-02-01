# xdb
xdb is an ACID compliant database and storage engine written purely in go.\
Testing is still going on, may contain bugs.

### Features
* Atomic updates (testing ongoing)
* Durable to process crashes and power failures (testing ongoing)
* Point queries in O(logn) time
* Range queries in O(k * logn) time
* Secondary Indexes
* Transactions

### Implementation
* B+trees
* Copy-on-write
* Mmap and fsync

### Installation
```
git clone https://github.com/vanshjangir/xdb
cd xdb
go build -o xdb
./xdb
```

### Contributing
Currently working on more complex sql queries, check docs [here](https://github.com/vanshjangir/xdb/blob/master/DOCS.md).\
The code contains many bugs, and there is much room for optimization, so any contributions are welcome.\
[Todo](https://github.com/vanshjangir/xdb/issues/1)

### References
* [Build-your-own-db](https://build-your-own.org/database/00a_overview)
* [What is mmap - tsoding](https://www.youtube.com/watch?v=sFYFuBzu9Ow)
* [B+tree visualization](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)
