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
	int		curblk;		
	int		currow;		
	int		endrow;		
	int		pad;
	int		stat;
	ORANDPLAN	*orplan;
	ORANDPLAN	*andplan;
	char		*sstab;		
	RANGE_QUERYCTX	*rgsel;
	char		*tabdir;	
	int		rowcnt;		
	int		sum_colval;	
	int		querytype;
	int		sum_coloff;	
	int		sstab_id;	
	int		ridnum;		
}SSTAB_SCANCTX;


#define	SSTABSCAN_HIT_ROW	0x0001	
#define	SSTABSCAN_BLK_IS_FULL	0x0002	
#define	SSTABSCAN_HIT_BOUND	0x0004	



#define	RG_TABLET_1ST_SSTAB		0x0001
#define	RG_TABLET_LAST_SSTAB		0x0002
#define	RG_TABLET_ANYONE_SSTAB		0x0004
#define	RG_TABLET_LEFT_BOUND		0x0008
#define RG_TABLET_RIGHT_BOUND		0x0010

typedef struct tablet_scancontext
{
	int		stat;
	int		querytype;	
	ORANDPLAN	*orplan;	
	ORANDPLAN	*andplan;	
	char		*tablet;	
	char		*tabdir;	
	int		rminlen;	
	int		keycolid;	
	int		tabid;		
	int		tabletid;	
	int		totcol;		
	int		connfd;		
	int		rowcnt;		
	int		sum_colval;	
	int		sum_coloff;	
	int		has_index;	
	COLINFO		*colinfo;	
}TABLET_SCANCTX;

#define	SCANCTX_HIT_END		0x0001


#endif 
