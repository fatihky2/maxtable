1. Manual
  * Connect to the server using IMQL
```
./imql 
```
  * Run the rebalancer on the target table
```
IMQL:rebalance table_name
```

2. Overview

![http://m3.img.libdd.com/farm2/93/AC3FD13D38F9DD0EBE00429733240B5D_513_125.jpg](http://m3.img.libdd.com/farm2/93/AC3FD13D38F9DD0EBE00429733240B5D_513_125.jpg)
  * Rebalancer send the request to metaserver that will driver data rebalancing
  * Metaserver count the average number of all the tablet locating at all the ranger servers and choose the ranger owning the max number of tablet and the one with minimal number of tablet.
  * Rebalance the data from the one with the max tablets to the one with the minimal tablets.