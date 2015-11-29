MQL is based on the style of traditional relational query lanmaxtabage.
  * **CREATE TABLE**.....create table table\_name (column1 type1, ...,cloumnx type x)
  * **INSERT**....................insert into table\_name (column1\_value,...columnx\_value)
  * **DELETE**..................delete table\_name (column1\_value)
  * **DELETEWHERE**.........deletewhere table\_name where columnX\_name(columnX\_value1, columnX\_value2)
  * **SELECT**..................select table\_name (column1\_value)
  * **SELECTRANGE**......selectrange table\_name (column1\_value1, column1\_value2)
  * **SELECTWHERE**......selectwhere table\_name where columnX\_name(columnX\_value1, columnX\_value2) and columnY\_name(columnY\_value1, columnY\_value2)
  * **SELECTSUM**..........selectsum (column\_name) table\_name where columnX\_name(columnX\_value1, columnX\_value2) and columnY\_name(columnY\_value1, columnY\_value2)
  * **SELECTCOUNT**......selectcount table\_name where columnX\_name(columnX\_value1, columnX\_value2) and columnY\_name(columnY\_value1, columnY\_value2)
  * **UPDATE**...........update set columnX\_name(columnX\_value) maxtab where columnX\_name(columnX\_value1, columnX\_value2)
  * **CREATEINDEX**......create index index\_name on table\_name (columnX\_name)
  * **DROPINDEX**........drop index index\_name on table\_name
  * **DROP**.....................drop table\_name

**Examples**
  * create table
```
create table maxtab(id1 varchar, id2 varchar,id3 int, id4 varchar,id5 varchar,id6 varchar,id7 varchar,id8 varchar,id9 varchar)
return: success or fail
```
  * insert data
```
insert into maxtab(aaaa1, bbbb1, 1, cccccc1, dddddd1, eeeeee1, ffffff1, gggggg1, hhhhhh1)
...
insert into maxtab(aaaa9999, bbbb9999, 9999, cccccc9999, dddddd9999, eeeeee9999, ffffff9999, gggggg9999, hhhhhh9999)
return: success or fail
```
  * select data by the default key column
```
select maxtab (aaaa1)
return: 
aaaa1bbbb1cccccc1dddddd1eeeeee1ffffff1gggggg1hhhhhh1A
```
  * select data by the range user specified
```
selectrange maxtab(*, *)
return:
aaaa1bbbb1cccccc1dddddd1eeeeee1ffffff1gggggg1hhhhhh1A
...
```
  * select data by the WHERE clause
```
selectwhere maxtab where id1(aaaa1, *) and id2(bbbb35, bbbb46)
return:
aaaa35bbbb35cccccc35dddddd35eeeeee35ffffff35gggggg35hhhhhh35A
...
aaaa46bbbb46cccccc46dddddd46eeeeee46ffffff46gggggg46hhhhhh46A
```
  * selectsum data by the WHERE clause
```
selectsum (id3) maxtab where id1(aaaa38, aaaa47) and id2(bbbb39, bbbb40)
return:
The total value is 398978
```
  * selectcount data by the WHERE clause
```
selectcount maxtab where id1(aaaa38, aaaa47) and id2(bbbb35, bbbb46)
return:
The total row # is 890
```
  * update data by the WHERE clasue
```
update set id2(aaaa60) maxtab where id2(bbbb20, bbbb20)
```
  * create index against table
```
create index idx1 on maxtab (id2)
```
  * drop index against table
```
drop index idx1 on maxtab
```
  * delete data by the index against the first column
```
delete maxtab (aaaa1)
return: success or fail
```
  * delete data by the WHERE clause
```
deletewhere maxtab where id1(aaaa20, aaaa30)
```
  * drop table
```
drop maxtab
return: success or fail
```