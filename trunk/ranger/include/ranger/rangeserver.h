/*
** rangeserver.h 2010-06-21 xueyingfei
**
** Copyright flying/xueyingfei.
**
** This file is part of MaxTable.
**
** Licensed under the Apache License, Version 2.0
** (the "License"); you may not use this file except in compliance with
** the License. You may obtain a copy of the License at
**
** http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
** implied. See the License for the specific language governing
** permissions and limitations under the License.
*/


#ifndef RANGESERVER_H_
#define RANGESERVER_H_

#ifdef MAXTABLE_BENCH_TEST

#define MT_RANGE_TABLE	"./rg_table"

#else

#define MT_RANGE_TABLE	"/mnt/ranger/rg_table"

#endif

typedef struct rg_loginfo
{
	int	logfd;
	int	logoffset;
	char	logdir[256];
}RG_LOGINFO;

typedef struct sstab_scancontext
{
	int		rminlen;
	int		curblk;		/* Current working block, the start 
					** number is the 0 even if its true
					** number is the bp->bblk->blkno.
					*/
	int		currow;		/* Current row numbet, start from 0. */
	int		endrow;		/* For the index case, it can be used 
					** for the index range.
					*/
	int		pad;
	int		stat;
	ORANDPLAN	*orplan;
	ORANDPLAN	*andplan;
	char		*sstab;		/* Ptr to the data in block. */
	RANGE_QUERYCTX	*rgsel;
	int		rowcnt;		/* For the selectcount. */
	int		sum_colval;	/* For the selectsum. */
	int		querytype;
	int		sum_coloff;	/* For the selectsum. */
	int		sstab_id;	/* The id of current sstable. */
	int		ridnum;		/* For the index case, it should save
					** the ridnum of index row.
					*/
}SSTAB_SCANCTX;

/* 
** Following definition id for the stat in SSTAB_SCANCTX, 
** return stat in the SSTable scan. 
*/
#define	SSTABSCAN_HIT_ROW	0x0001	/* Hit one row at least. */
#define	SSTABSCAN_BLK_IS_FULL	0x0002	/* The block hit the issue of overload. */


/* Defines for the function rg_get_sstab_tablet(). */
#define	RG_TABLET_1ST_SSTAB		0x0001
#define	RG_TABLET_LAST_SSTAB		0x0002
#define	RG_TABLET_ANYONE_SSTAB		0x0004
#define	RG_TABLET_LEFT_BOUND		0x0008
#define RG_TABLET_RIGHT_BOUND		0x0010

typedef struct tablet_scancontext
{
	int		stat;
	int		querytype;	/* Query type. */
	ORANDPLAN	*orplan;	/* Ptr to the parser tree of OR plan. */
	ORANDPLAN	*andplan;	/* Ptr to the parser tree of AND plan. */
	char		*tablet;	/* Ptr to the first block of tablet. */
	char		*tabdir;	/* Table name full path. */
	int		rminlen;	/* The minimal length of row. */
	int		keycolid;	/* The id of key column in the table, 
					** its value is '1' generally. 
					*/
	int		tabid;		/* Table id. */
	int		connfd;		/* Socket id of connection to the 
					** bigdata port.
					*/
	int		rowcnt;		/* For the selectcount. */
	int		sum_colval;	/* For the selectsum (col). */
	int		sum_coloff;	/* For the selectsum (col). */
	int		pad;
}TABLET_SCANCTX;

#define	SCANCTX_HIT_END		0x0001


#endif 
