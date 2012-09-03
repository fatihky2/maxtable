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
#define	MT_META_BACKUP	"./meta_tablet_backup"	
#else

#define MT_META_TABLE   "/mnt/metaserver/meta_table"
#define MT_META_REGION  "/mnt/metaserver/rg_server"
#define MT_META_INDEX   "/mnt/metaserver/index"	
#define	MT_META_BACKUP	"/mnt/metaserver/meta_tablet_backup"



#endif



typedef struct rid
{
	int	sstable_id;	
	int	block_id;	
	int	roffset;	
	int	pad;
}RID;



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
	int	tab_tablet;		
	int	tab_sstab;		
	int	tab_row_minlen;		
	int	tab_key_colid;		
	int	tab_key_coloff;		
	int	tab_key_coltype;	
	int	tab_col;		
	int	tab_varcol;		
	int	tab_stat;		
	int 	offset_c1;		
	int 	offset_c2;		
	int	index_ts;		
	int	has_index;		
} TABLEHDR;


#define	TAB_COL_IS_INDEX(index_map, col_num)	((index_map >> (col_num - 1)) & 0x1)
#define	TAB_COL_SET_INDEX(index_map, col_num)	(index_map |= (1 << (col_num - 1)))
#define	TAB_COL_CLEAR_INDEX(index_map, col_num)	(index_map &= ~(1 << (col_num - 1)))




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
	int		rg_tablet_num;	
	int		rg_index;
	char		rg_statefile[TABLE_NAME_MAX_LEN];
					

	pthread_t	tid;
} RANGE_PROF;


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
	int		sstab_stat;		
	unsigned int	split_ts;	
} SSTAB_INFOR;

#define SSTAB_MAP_SIZE	(SSTAB_MAP_ITEM * sizeof(SSTAB_INFOR))


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



#define SVR_IDX_FILE_HDR	16
#define SVR_IDX_FILE_BLK	((sizeof(int)*3 + sizeof(pthread_t) + TABLE_NAME_MAX_LEN + RANGE_PORT_MAX_LEN + RANGE_ADDR_MAX_LEN) * 16)
#define SVR_IDX_FILE_SIZE	(SVR_IDX_FILE_HDR + SVR_IDX_FILE_BLK)
typedef	struct svr_idx_file
{
	int		nextrno;		
	int		freeoff; 	
	short		stat;
	char		pad2[6];
	char		data[SVR_IDX_FILE_BLK];	
}SVR_IDX_FILE;


#define	SVR_IS_BAD	0x0001



typedef struct insert_meta
{
	union infor_hdr	i_hdr;
	int		sstab_id;
	int		res_sstab_id;
	unsigned int	ts_low;				
	char    	sstab_name[SSTABLE_NAME_MAX_LEN];
	int		status;
	int		col_num;	
	int		varcol_num;	
	int		row_minlen;
	int		tabletid;	
	
} INSMETA;


#define	INS_META_1ST	0x0001		
typedef struct select_range
{
	INSMETA		left_range;
	INSMETA		right_range;	
} SELRANGE;



typedef struct idxmeta
{
	char	magic[RPC_MAGIC_MAX_LEN];
	int	idx_tabid;	
	int	idx_id; 	
	int	idx_stat;			
	int	idx_col_map;	
	char	idxname[TABLE_NAME_MAX_LEN];
}IDXMETA;


#define	IDX_IN_CREATE	0x0001	
#define	IDX_IN_WORKING	0x0002	
#define	IDX_IN_DROP	0x0004	
#define	IDX_IN_SUSPECT	0x0008	


typedef struct idx_root_split
{
	int	idx_srcroot_id;
	int	idx_destroot_id;
	int	idx_tabid;
	int	idx_ts;		
	char	idx_tabname[TABLE_NAME_MAX_LEN];
}IDX_ROOT_SPLIT;

typedef struct select_where
{
	char		magic[RPC_MAGIC_MAX_LEN];
	int		leftnamelen;
	int		rightnamelen;
	char		lefttabletname[128];
	char		righttabletname[128];
	int		use_idxmeta;		
	int		pad;
	IDXMETA		idxmeta;
}SELWHERE;

typedef union context
{	
	struct select_range	*selrg;
	struct select_where	*selwh;
}CONTEXT;


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


#define	IDX_MAX_NUM	(TAB_MAX_NUM * COL_MAX_NUM)


typedef struct meta_systable
{
	int		last_tabid;	
	int		update_ts;	
	int		tabnum;		
	int		tab_stat[TAB_MAX_NUM];
	char		meta_tabdir[TAB_MAX_NUM][TABLE_NAME_MAX_LEN];
	int		pad;
}META_SYSTABLE;


#define	TAB_1ST_INSERT		0x0001	


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
	int		idx_num;	
	int		idx_ver;	
	IDXMETA		idx_meta[IDX_MAX_NUM];	
}META_SYSINDEX;



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
