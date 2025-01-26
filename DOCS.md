### Basic commands
* Create a new database (will be created in the home directory)
```
db create <database_name>
```
* List all databases
```
db ls
```
* Show current database
```
db show
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
* Table related queries are in SQL(currently supports only basic where clauses)
  - [x] create
  - [x] delete
  - [x] select (show whole table)
  - [ ] update
  - [ ] select (with complex conditions)
  - [ ] alter
