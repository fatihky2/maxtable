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

#ifdef MAXTABLE_BENCH_TEST

#define	MT_RANGE_STATE	"./rg_state"

#else

#define	MT_RANGE_STATE	"/mnt/ranger/rg_state"

#endif


typedef struct sstab_split_info
{
	int	sstab_state;
	int	tab_name_len;
	char	tab_name[256];
	char	newsstabname[256];
	int	sstab_id;	/* New sstable id. */
	int	split_ts;	/* Time stamp, it can be used for the 
				** duplicated recovery.
				*/
	int	split_sstabid;	/* Old sstable id. */
	int	sstab_keylen;
	char	*sstab_key;	/* Ptr to the sstable key. */	
}SSTAB_SPLIT_INFO;

/* Following definition is for the sstab_state. */
#define	RG_SSTABLE_DELETED	0x0001	/* Flag that the tablet split is successful in meta server. */
#define	RG_SSTABLE_RECOVERED	0x0002	/* Flag this Add SStable has been recovered. */

typedef struct rg_state
{
	int	sstab_split_num;/* The # of splitted sstab in the file rg_state. */
	int	offset;		/* The offset of the Add SStable row. */
	char	tabletname[256];/* This tablet to be splitted in the Add SStable. */
	char	sstab_state[SSTABLE_MAX_COUNT * 1024];/* Save the SSTAB_SPLIT_INFO. */
}RG_STATE;

#define	RG_STATE_HEADER		(sizeof(int) *2 + 256)
#define	SSTAB_SPLIT_INFO_HEADER	(sizeof (int) * 6 + 512)


#define	SSTAB_SPLIT_INFO_INIT(split_info, sstabstate, tabname, tabname_len, newsstab_name,	\
		sstabid, splitts, splitsstabid, sstabkeylen, sstabkey)				\
do{												\
	(split_info)->sstab_state = sstabstate;							\
	MEMCPY((split_info)->tab_name, tabname, tabname_len);					\
	(split_info)->tab_name_len = tabname_len;						\
	MEMCPY((split_info)->newsstabname, newsstab_name, 256);					\
	(split_info)->sstab_id = sstabid;							\
	(split_info)->split_ts = splitts;							\
	(split_info)->split_sstabid = splitsstabid;						\
	(split_info)->sstab_keylen = sstabkeylen;						\
	(split_info)->sstab_key = sstabkey;							\
}while (0)

/* In-memory structure. */
typedef struct rg_systable
{
	int		tabnum;		/* The number of table current system owning. */
	char		rg_tabdir[TAB_MAX_NUM][256];
	char		rg_sstab[TAB_MAX_NUM][256];
	int		pad;
}RG_SYSTABLE;

#define	RANGE_CONF_PATH_MAX_LEN	64


/* This struct will also be used at cli side */
typedef struct rg_info
{
	char		conf_path[RANGE_CONF_PATH_MAX_LEN];
	char		rg_meta_ip[RANGE_ADDR_MAX_LEN];
	int     	rg_meta_port;
	char		rg_ip[RANGE_ADDR_MAX_LEN];
	int		port;
	int		bigdataport;
	int		flush_check_interval;
	char		rglogfiledir[256];
	char		rgbackup[256];
	char		rgstatefile[256];
	int		rginsdellognum;		/* the log number. */
	RG_SYSTABLE	rg_systab;
	META_SYSINDEX	*rg_meta_sysindex;	/* It loads the meta_sysindex 
						** file into ranger context.
						*/
}RANGEINFO;

int
ri_rgstat_putdata(char *statefile, char *new_sstab, int state, SSTAB_SPLIT_INFO *split_value);

int
ri_rgstat_deldata(char * statefile,char *new_sstab);

SSTAB_SPLIT_INFO *
ri_rgstat_getdata(RG_STATE *rgstate, char *new_sstab, int state);

int
ri_get_rgstate(char *rgstate, char *rgip, int rgport);

