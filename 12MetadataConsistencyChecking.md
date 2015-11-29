1. Manual

  * Connect to the server using IMQL
```
./imql 
```
  * Run the rebalancer on the target table
```
IMQL:rebalance table_name
```
  * Check the ranger server
```
IMQL:mcc checkranger
```
  * Check the metadata of user table
```
IMQL:mcc checktable table_name
```
  * Check the user data of user table
```
./Administrator/mcc checksstab table_name
```
2. Overview

![http://m1.img.libdd.com/farm2/84/1CA3A734F40A28C9759FA33A6366E054_421_156.jpg](http://m1.img.libdd.com/farm2/84/1CA3A734F40A28C9759FA33A6366E054_421_156.jpg)
  * MCC send the request to metaserver that will driver the metadata consistency checking.
  * MCC will check the following area for metadata.
    * tabletscheme table.
    * tablet files.
    * table index consistency.
    * Checking if ranger server is on-line or off-line.
    * data row formate and block management data.
    * data order.

3. Future

MCC should include the deep scan and fast scan. Deep scan will check the metadata and ranger data, fast scan will only scan the data locating at the metadata server.