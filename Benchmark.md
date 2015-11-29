**Introduction**

This post is not the official benchmark for Maxtable, and its performance should be tested on your application to find bottleneck and do the performance tuning.

**Background**

  1. CPU information
```
model name	: Intel(R) Xeon(R) CPU           E5506  @ 2.13GHz
cpu MHz		: 2127.683
cache size	: 4096 KB
```
  1. Data scheme
```
create table maxtab(id1 varchar, id2 varchar,id3 int, id4 varchar,id5 varchar,
                    id6 varchar,id7 varchar,id8 varchar,id9 varchar)
```
  1. One machine, one Meta server, one Ranger server (one thread), one Client
  1. DEBUG Binary (Meta & Ranger Server)

**Performance
```
1. insert into table without secondary index
  Client 1: insert into maxtab(aaaa1, bbbb1, 1, cccccc1, dddddd1, eeeeee1, ffffff1, gggggg1, hhhhhh1)
  ....
  Client 1: insert into maxtab(aaaa4999998, bbbb4999998, 4999998, cccccc4999998, dddddd4999998, eeeeee4999998, ffffff4999998, gggggg4999998, hhhhhh4999998)
  Client 1: insert into maxtab(aaaa4999999, bbbb4999999, 4999999, cccccc4999999, dddddd4999999, eeeeee4999999, ffffff4999999, gggggg4999999, hhhhhh4999999)
  time cost(s): 3984.861084

2. selectwhere on table without secondary index
  Client 1: selectwhere maxtab where id2(bbbb6000, bbbb6000)
  col: ffffff6000gggggg6000hhhhhh6000V
  time cost(s): 2.634655

3. selectcount on table without secondary index
  Client 1: selectcount maxtab where id2(bbbb5, bbbb8)
  The total row # is 333334
  time cost(s): 2.552949

4. selectsum on table without secondary index
  Client 1: selectsum (id3) maxtab where id2(bbbb38, bbbb47)
  The total value is -1834702716
  time cost(s): 2.595446

5. crtindex
  Client 1: create index idx1 on maxtab (id2)
  time cost(s): 3652.883545

6. selectwhere on table with sencondary index
  Client 1: selectwhere maxtab where id2(bbbb6000, bbbb6000)
  col: ffffff6000gggggg6000hhhhhh6000V
  time cost(s): 0.004635

7. selectsum on table with sencondary index
  Client 1: selectsum (id3) maxtab where id2(bbbb38, bbbb47)
  The total value is -1834702716
  time cost(s): 154.166534

8. selectcount on table with sencondary index
  Client 1: selectcount maxtab where id2(bbbb5, bbbb8)
  The total row # is 333334
  time cost(s): 55.726135

9. insert into table with one secondary index and one cluster index
Client 1: insert into maxtab(aaaa2001, bbbb2001, 2001, cccccc2001, dddddd2001, eeeeee2001, ffffff2001, gggggg2001, hhhhhh2001)
....
Client 1: insert into maxtab(aaaa5001998, bbbb5001998, 5001998, cccccc5001998, dddddd5001998, eeeeee5001998, ffffff5001998, gggggg5001998, hhhhhh5001998)
Client 1: insert into maxtab(aaaa5001999, bbbb5001999, 5001999, cccccc5001999, dddddd5001999, eeeeee5001999, ffffff5001999, gggggg5001999, hhhhhh5001999)
time cost(s): 46435.878906
```**


_Work Continued..._