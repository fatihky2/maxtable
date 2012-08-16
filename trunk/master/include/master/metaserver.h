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

#ifndef METASERVER_H_
#define METASERVER_H_
#include <pthread.h>
#include "global.h"
#include "conf.h"
#include "netconn.h"
#include "spinlock.h"

/*
** 256MB * 1024 * 1024 = 256TB
** 256MB * (4*4 * 4*4*4) * (4*4*4 *4*4)
*/
/* 
MetaServer Behavior
------------------
1. metaserver setup(first setup):

                 maxtable/tableid -- allocation of table id 
                 maxtable/table   -- save the metadata of user table 
       file:     maxtable/rg_server -- save the addr of range server

 2. create table:

                 maxtable/table/table_name -- save the tablet of user table
        file:   maxtable/table/table_name/table_name -- save the tablet information
                               | header (fixed size) | tablet row1 |tablet  row2  |  tablet row...|
                 maxtable/table/table_name/table_col -- save the information of column, 
                                                        such as the name /offset of column 

 3. insert data(first data of table):

                  maxtable/table/table_name/tablet0 -- as the root storage unit of table
                                | header (fixed size)| sstable row1 | sstable row2  |  sstable row...|
                  

 RangeServer Behavior
 -------------------
 1. metaserver setup:(first setup)        No
 2. create table:		              No
 3. insert data:(first data of table)	-> just call the API from GFS	
  
                  


		< 1st Version of Design>
                            =================
metaserver:
                          row header,  fixed col,    var-col,            var-col2
	table ( <row header, tablet0,  key of 1st row, key of last row>, <r_hd,tablet1..>, <r_hd, tablet2...>)

	tablet( <r_hd, sstable0,  key of 1st row, key of last row, range IP, range port>, <>)

range server

	sstable(block0, block1, block2....)
	             |
	           save the block index (<block1, key of 1st row, key of last row >)  




		<2nd Version of Design>
                             ==================
metaserver:
                          
	table (tab_hdr,  <r_hd, tablet0, key of first row,  <r_hd,tablet1,  key of first row>, <tablet2...>)

	tablet( tablet hdr, <r_hd, sstable0,  range addr, key of 1st row>, <>)

range server

	sstable(block0, block1, block2....)
	             |
	           save the block index (<block1, key of 1st row>)  


		<3rd Version of Design> -- Current Implementation
                             ==================
metaserver:
	1. table scheme  files, as follows:
		a. table header file
		b. sysobject file
		c. syscolumns file

	table header file manage some table scheme information, such as the tablet #,...
		
	2. tablet files, as follows:
		tablet scheme file which is a table in fact, its each row stores the corresponding tablet information,
			such as tablet name, tablet's 1st key, sstable# of this tablet.
		tablet files. There are many tablet files, its name like tablet0, tablet1,tablet2,....tabletn.
	
                          
	tablet_scheme (<r_hd, tablet0, sstable #, key of first row>, <r_hd,tablet1, sstable #, key of first row>, <tablet2...>)

	tablet( <r_hd, sstable0,  range addr, key of 1st row>, <>)

range server

	sstable(block0, block1, block2....)
	              
*/

struct srch_info;
struct buf;
struct block_row_info;


#define RANGE_ADDR_MAX_LEN      32
#define RANGE_PORT_MAX_LEN	4
#define RANGE_SERVER_TEST       "127.0.0.1\0"
#define RANGE_PORT_TEST         1949
#define SSTABLE_NAME_MAX_LEN    128
#define TABLE_NAME_MAX_LEN      256
#define TABLET_NAME_MAX_LEN	32
#define RANGER_MAX_NUM		64


struct stat st;

#ifdef MAXTABLE_BENCH_TEST

#define MT_META_TABLE   "./meta_table"
#define MT_META_REGION  "./rg_server"
#define MT_META_INDEX   "./index"	
#define	MT_META_BACKUP	"./meta_tablet_backup"	/* save the tablet file for the 
						** splitted tablet.
						*/
#else

#define MT_META_TABLE   "/mnt/metaserver/meta_table"
#define MT_META_REGION  "/mnt/metaserver/rg_server"
#define MT_META_INDEX   "/mnt/metaserver/index"	
#define	MT_META_BACKUP	"/mnt/metaserver/meta_tablet_backup"



#endif


/*
Rid structure
*/
typedef struct rid
{
	int	sstable_id;	/* SStable number. */
	int	block_id;	/* Block number. */
	int	roffset;	/* Row offset in one block. */
	int	pad;
}RID;


/*
** sycolumn scheme:
**             1.  each table has one syscolumn
**             2. Clo_id (int) | Col_name (char 64) | Col_length (int)| Col_offset (int) |Col_type (int) 
*/
typedef struct col_info
{
        int     col_id;
        char    col_name[64];
        int     col_len;
        int     col_offset;
        int     col_type;
} COLINFO;


typedef struct table_hdr
{
	int     tab_id;
	char    tab_name[128];
	int	tab_tablet;		/* # of tablet. It stands for the # of
					** tablet row.
					*/
	int	tab_sstab;		/* # of sstable of the whole TABLE, 
					** not the TABLET. 
					*/
	int	tab_row_minlen;		
	int	tab_key_colid;		/* column id of key column */
	int	tab_key_coloff;		/* the offset of key column that data
					** store according to it.
					*/
	int	tab_key_coltype;	/* Column type of key column */
	int	tab_col;		/* # of column */
	int	tab_varcol;		/* # of var-column */
	int	tab_stat;		/* the state of table */
	int 	offset_c1;		/* the column of tablet name */
	int 	offset_c2;		/* the column of key */
	int	index_ts;		/* The timestamp of index against the
					** table, It must be keep consistency 
					** with 'idx_ver' in sysindex.
					*/
	int	has_index;		/* Flag if table has at least one index. */
} TABLEHDR;

/*
** INT 32 bit
**	31 <----- 0    column number
** 00000000  00000000  00000000  00000000
**
** NOTE: the id of column start from the '1' (column id of cluster key)
*/
#define	TAB_COL_IS_INDEX(index_map, col_num)	((index_map >> (col_num - 1)) & 0x1)
#define	TAB_COL_SET_INDEX(index_map, col_num)	(index_map |= (1 << (col_num - 1)))
#define	TAB_COL_CLEAR_INDEX(index_map, col_num)	(index_map &= ~(1 << (col_num - 1)))



/* Get the column beased on index and col_idx must start from '0'. */
#define INDEX_MAP_GET_COLUMN_NUM(col_map, col_idx)	\
	int	tmp_map = col_map;			\
							\
	do{						\
		if (tmp_map & 0x01)			\
		{					\
			break;				\
		}					\
							\
		tmp_map >>= 1;				\
		col_idx++;				\
							\
		if (col_idx > COL_MAX_NUM)		\
		{					\
			col_idx = -1;			\
			break;				\
		}					\
	}while (tmp_map)				\


typedef struct rg_prof
{
	char		rg_addr[RANGE_ADDR_MAX_LEN];
	int		rg_port;
	
	int		rg_stat;
	int		rg_tablet_num;	/* Only used by Rebalancer. */
	int		rg_index;
	char		rg_statefile[TABLE_NAME_MAX_LEN];
					/* Save the rgstatefile for the range 
					** state containning the sstable split
					** information.
					*/

	pthread_t	tid;
} RANGE_PROF;

/* Following definition is for the rg_stat. */
#define RANGER_IS_ONLINE	0x0001
#define RANGER_IS_OFFLINE	0x0002
#define	RANGER_NEED_RECOVERY	0x0004
#define RANGER_IS_SUSPECT	0x0008
#define RANGER_RESTART		0x0010

typedef union infor_hdr
{
	RANGE_PROF	rg_info;
	char		magic[RPC_MAGIC_MAX_LEN];
} INFOR_HDR;

#define SSTAB_MAP_ITEM	(1024 * 1024)

typedef struct sstab_infor
{
	int		sstab_stat;	/* sstable state. */	
	unsigned int	split_ts;	/* Split TS the current sstable,
					** we only use the low part of TS.
					*/
} SSTAB_INFOR;

#define SSTAB_MAP_SIZE	(SSTAB_MAP_ITEM * sizeof(SSTAB_INFOR))

/* Structure for the sstable map. */
typedef struct tab_sstab_map
{
	int		tabid;
	int		stat;
	char		sstabmap_path[TABLE_NAME_MAX_LEN];
	struct tab_sstab_map 
			*nexttabmap;
	SSTAB_INFOR	sstab_map[SSTAB_MAP_ITEM];
	
}TAB_SSTAB_MAP;


#define SSTABMAP_CHG	0x0001


/* Following is for the ranger server saving. */
#define SVR_IDX_FILE_HDR	16
#define SVR_IDX_FILE_BLK	((sizeof(int)*3 + sizeof(pthread_t) + TABLE_NAME_MAX_LEN + RANGE_PORT_MAX_LEN + RANGE_ADDR_MAX_LEN) * 16)
#define SVR_IDX_FILE_SIZE	(SVR_IDX_FILE_HDR + SVR_IDX_FILE_BLK)
typedef	struct svr_idx_file
{
	int		nextrno;	/* if data, next free row number */	
	int		freeoff; 	/* first unused byte */
	short		stat;
	char		pad2[6];
	char		data[SVR_IDX_FILE_BLK];	/* actual useful data */
}SVR_IDX_FILE;

/* Following definition is for the stat. */
#define	SVR_IS_BAD	0x0001


/* This structure should be corresponding to the formate of metaserver returns. */
typedef struct insert_meta
{
	union infor_hdr	i_hdr;
	int		sstab_id;
	int		res_sstab_id;
	unsigned int	ts_low;		/*SSTable's split TS. */		
	char    	sstab_name[SSTABLE_NAME_MAX_LEN];
	int		status;
	int		col_num;	/* # of column in the table */
	int		varcol_num;	/* # of var-length column */
	int		row_minlen;
	int		tabletid;	/* The id of tablet to be inserted. */
	
} INSMETA;

/* following definition is for the status field of insert_meta. */
#define	INS_META_1ST	0x0001		/* Falg if this insert is first row 
					** in this sstable file.
					*/
typedef struct select_range
{
	INSMETA		left_range;
	INSMETA		right_range;	
} SELRANGE;


/*
** These meta information will be stored to the disk.
** The tuple of data in the file sysindex.
*/
typedef struct idxmeta
{
	char	magic[RPC_MAGIC_MAX_LEN];
	int	idx_tabid;	/* The id of table which the index is based on.  */
	int	idx_id; 	/* The id of index. */
	int	idx_stat;	/* The status of index. */		
	int	idx_col_map;	/* Flag this index is created on which columns.*/
	char	idxname[TABLE_NAME_MAX_LEN];
}IDXMETA;

/* Following definition is for the idx_stat. */
#define	IDX_IN_CREATE	0x0001	/* Index is creating. */
#define	IDX_IN_WORKING	0x0002	/* Index can be access. */
#define	IDX_IN_DROP	0x0004	/* Index is in drop processing. */
#define	IDX_IN_SUSPECT	0x0008	/* Index is in the suspect and needs to
				** check further. 
				*/


typedef struct idx_root_split
{
	int	idx_srcroot_id;
	int	idx_destroot_id;
	int	idx_tabid;
	int	idx_ts;		/* Index version. */
	char	idx_tabname[TABLE_NAME_MAX_LEN];
}IDX_ROOT_SPLIT;

typedef struct select_where
{
	char		magic[RPC_MAGIC_MAX_LEN];
	int		leftnamelen;
	int		rightnamelen;
	char		lefttabletname[128];
	char		righttabletname[128];
	int		use_idxmeta;		/* True if the query will use the index on this clumn. */
	int		pad;
	IDXMETA		idxmeta;
}SELWHERE;

typedef union context
{	
	struct select_range	*selrg;
	struct select_where	*selwh;
}CONTEXT;

/* Following definition is for the stat. */
#define	SELECT_RANGE_OP		0x0001
#define	SELECT_WHERE_OP		0x0002

typedef struct select_context
{
	int			stat;
	struct svr_idx_file	rglist;
	union context		ctx;
}SELCTX;


typedef struct insert_ranger
{
	char		old_sstab_name[SSTABLE_NAME_MAX_LEN];	
	char		new_sstab_name[SSTABLE_NAME_MAX_LEN];
	int		old_keylen;
	int		new_keylen;
	char		*old_sstab_key; 
	char		*new_sstab_key;
}INSRG;


#define META_CONF_PATH_MAX_LEN   64

#define	COL_MAX_NUM	32
#define	TAB_MAX_NUM	64

/* Temp solution is to support the index on one column. */
#define	IDX_MAX_NUM	(TAB_MAX_NUM * COL_MAX_NUM)

/* In-memory structure. */
typedef struct meta_systable
{
	int		last_tabid;	/* Max table id. */
	int		update_ts;	/* The TS latest update. */
	int		tabnum;		/* The number of table current system owning. */
	int		tab_stat[TAB_MAX_NUM];
	char		meta_tabdir[TAB_MAX_NUM][TABLE_NAME_MAX_LEN];
	int		pad;
}META_SYSTABLE;

/* Following definition is for the tab_stat. */
#define	TAB_1ST_INSERT		0x0001	/* No insertion since the table was created. */


typedef struct meta_syscolumn
{
	int		colnum[TAB_MAX_NUM];
	COLINFO		colinfor[TAB_MAX_NUM][COL_MAX_NUM];
}META_SYSCOLUMN;


typedef struct meta_sysobject
{
	TABLEHDR	sysobject[TAB_MAX_NUM];
}META_SYSOBJECT;

typedef struct meta_sysindex
{
	char		idx_magic[8];
	int		idx_num;	/* The total number of index in system. */
	int		idx_ver;	/* Index version that it will keep the 
					** consistency between the metaserver
					** and ranger server.
					*/
	IDXMETA		idx_meta[IDX_MAX_NUM];	
}META_SYSINDEX;


/* 
** minlen is needed in the insert case and this value will be saved into disk. 
** In the search case, we can skip this value or use the invalid value.
*/
#define TABINFO_INIT(tabinfo, sstab_name, tab_name, tab_namelen, srch_info, minlen, status, tabid, sstabid)	\
do {											\
		(tabinfo)->t_dold = (tabinfo)->t_dnew = (BUF *)(tabinfo);		\
		(tabinfo)->t_sstab_name = sstab_name;					\
		(tabinfo)->t_tab_name = tab_name;					\
		(tabinfo)->t_tab_namelen = tab_namelen;					\
		(tabinfo)->t_row_minlen = minlen;					\
		(tabinfo)->t_sinfo = srch_info;						\
		(tabinfo)->t_stat|= status;						\
		(tabinfo)->t_tabid = tabid;						\
		(tabinfo)->t_sstab_id = sstabid;					\
}while (0)


int
meta_save_sysobj(char *tab_dir, char *tab_hdr);


#endif 
