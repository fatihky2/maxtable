# Introduction #
  * Sharding include table sharding and tablet sharding. Table sharding can only be on the table without index and it will scan all the tablet and split those tablets that have at least two rows. tablet sharding can split the tablet user specified.
  * Generally, manual sharding will be followed by one rebalance operation that will rebalance the new tablets raised by the sharding.

# Usage #
```
# ./imql
Please type "help" if you need some further information.

/* Sharding the whole table and it will return success if the table has no index. */
IMQL:sharding table table_name

/* Sharding the tablet user specified. You can use the mcc checktable table_name to get the tablet information. */
IMQL:sharding tablet table_name (tablet_name)
```