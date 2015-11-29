  * Enter into the stress directory
```
cd ./stresstest
```
  * Create a test table
```
./crt_table
```
  * Insert data into this table using 5 clients that everyone has 10,000 data
```
./insert.sh > insert_result.out
```
  * Select all the inserted data
```
./select.sh > select_result.out
```