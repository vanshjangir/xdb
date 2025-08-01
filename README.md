# Xdb
Xdb is a B+Tree based storage engine with a partially implemented query executor.

### Features
* Atomic updates
* Durable to process crashes and power failures
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

### Usage (full of bugs)
| Command | Description |
|--------|-------------|
| db create <database_name> | Create new database |
| db ls | List all databases |
| db show | Show all tables |
| db use <database_name> | Initialize/select database |
| db begin | Start transaction |
| db commit | Commit transaction |
| db rollback | Rollback transaction |

### References
* [Build-your-own-db](https://build-your-own.org/database/00a_overview)
* [What is mmap](https://www.youtube.com/watch?v=sFYFuBzu9Ow)
* [B+tree visualization](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)
