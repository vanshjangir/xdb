# xdb
xdb is an ACID compliant database and storage engine written purely in go.

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

### Usage
* Create a new database (will be created in the home directory)
```
db create <database_name>
```
* Initialize a database (always initialize before doing anything)
```
db use <database_name>
```
* Start a transaction (a transaction is necessary to do anything)
```
db begin
```
* Commit or Rollback a transaction
```
db commit/rollback
```
* Table related queries are in SQL
  - [x] create
  - [x] delete
  - [x] select (show whole table)
  - [ ] update
  - [ ] select (with complex conditions)
  - [ ] alter

### Contributing
The code contains many bugs, and there is much room for optimization, so any contributions are welcome.\
[Todo](https://github.com/vanshjangir/xdb/issues/1)

### Useful
* [build-your-own-db](https://build-your-own.org/database/00a_overview)
* [mmap-tsoding](https://www.youtube.com/watch?v=sFYFuBzu9Ow)
* [B+tree visualization](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)
