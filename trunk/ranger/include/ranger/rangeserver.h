/*
** Copyright (C) 2011 Xue Yingfei
**
** This file is part of MaxTable.
**
** Maxtable is free software: you can redistribute it and/or modify
** it under the terms of the GNU General Public License as published by
** the Free Software Foundation, either version 3 of the License, or
** (at your option) any later version.
**
** Maxtable is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with Maxtable. If not, see <http://www.gnu.org/licenses/>.
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
