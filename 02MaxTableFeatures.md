  * **Scalability**: New ranger nodes can be added as storage needs increase; the system automatically adapts to the new nodes while running the loadbalance.

  * **SSTable Map**: This feature will reduce the data consistency control and improve the performance of data write, and we use a innovative method to solve the conflicts between writes so that it doesn't need any lock mutation for multi-writes.

  * **Data writes**: When an application insert a data, writes can be cached at the Ranger server, periodically, the cache is flushed, for consistency, applications will force one data log to be flushed to the disk.

  * **Re-balancing**: Using the administrator tool to rebalance the tablets amongst Rangerservers. This is done to help with balancing the workload amongst nodes.

  * **Index**: Maxtable will automatically build one unique index for each table and support secondary index on any columns user specified.

  * **Recovery**: Maxtable implements the write ahead logging (WAL) to make sure this writing is safe. It can recover the crash server by replaying its log.

  * **Failover**: Metaserver maintains a heartbeat with each rangerserver, while the metaserver detects that the range server is unreachable, it will fail-over the data service locating on the crash rangerserver to another rangerserver and continue the service for this range.

  * **Metadata Consistency Checking (MCC)**: Data checking tools to ensure the data consistency between on the metaserver and rangerserver.

  * **Range Query**: It will support the range query by any fields, and support the condition clause. Split the query work over all the range nodes in a cluster.

  * **Aggregation**: Support some basic aggregation computing, such as the sum and count computing.

  * **Sharding**: Automatic sharding support, distributing data range over range servers. Manually sharding support, it will scan all the data range and split those ranges containing two blocks of data. If customers want better scaling, they can do so manually by sharding ranges. Generally, manually sharding will be followed by one rebalance operation that will rebalance the ranges because sharding may raise some new range.