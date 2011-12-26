/*
** metaserver.h 2010-06-15 xueyingfei
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

#else

#define MT_META_TABLE   "/mnt/metaserver/meta_table"
#define MT_META_REGION  "/mnt/metaserver/rg_server"
#define MT_META_INDEX   "/mnt/metaserver/index"	

#endif


typedef struct key_col
{
	int	col_offset;	
} KEYCOL;


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
} TABLEHDR;


#define	TAB_DROPPED	0x0001		


typedef struct tablet_hdr
{
	char	firstkey[256];
	char    tblet_name[128];
	int	tblet_sstab;		
	int	offset_c1;		
	int	offset_c2;		
	int	offset_c3;		
	int	tabletid;	
}TABLETHDR;


typedef struct col_info
{
        int     col_id;
        char    col_name[64];
        int     col_len;
        int     col_offset;
        int     col_type;
} COLINFO;


typedef struct sstable
{
	char    sstab_name[128];
}SSTABLE;


typedef struct rg_prof
{
	char		rg_addr[RANGE_ADDR_MAX_LEN];
	int		rg_port;
	int		rg_stat;
	int		rg_tablet_num;	
	int		rg_index;
	pthread_t	tid;
} RANGE_PROF;


#define RANGER_IS_ONLINE	0x0001
#define RANGER_IS_OFFLINE	0x0002
#define	RANGER_NEED_RECOVERY	0x0004
#define RANGER_IS_SUSPECT	0x0008
#define RANGER_RESTART		0x0010


typedef union infor_hdr
{
	struct rg_prof	rg_info;
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
#define SVR_IDX_FILE_BLK	((sizeof(int)*2 + sizeof(pthread_t) + RANGE_PORT_MAX_LEN + RANGE_ADDR_MAX_LEN) * 1024)
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
	int		pad;
	
} INSMETA;


#define	INS_META_1ST	0x0001		

typedef struct select_range
{
	INSMETA		left_range;
	INSMETA		right_range;	
} SELRANGE;

typedef struct select_where
{
	char		magic[RPC_MAGIC_MAX_LEN];
	int		leftnamelen;
	int		rightnamelen;
	char		lefttabletname[128];
	char		righttabletname[128];
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

typedef struct tab_info
{
	struct buf	*t_dnew;		 
	struct buf	*t_dold;	 
//	TABLEHDR	*t_tabhdr;
	int		t_tabid;
	int		t_key_colid;
	int		t_key_coltype;
	int		t_key_coloff;
	struct tab_info	*t_nexttab;	
	TABLETHDR	*t_tablethdr;
	COLINFO		*t_colinfo;
	INSMETA		*t_insmeta;
	SELRANGE	*t_selrg;
	int		t_row_minlen;
	int		t_stat;
	int		t_sstab_id;	
	char		*t_sstab_name;	
	int		t_split_tabletid;
	struct block_row_info 
			*t_rowinfo;
	struct srch_info	
			*t_sinfo;
	struct buf	*t_keptbuf;
	struct buf	*t_resbuf;
	INSRG		*t_insrg;
	unsigned int	t_insdel_old_ts_lo;		
	unsigned int    t_insdel_new_ts_lo;
	char		*t_cur_rowp;
	int		t_cur_rowlen;
} TABINFO;



#define TAB_META_SYSTAB		0x00000001
#define TAB_SCHM_SRCH		0x00000002	
#define TAB_CRT_NEW_FILE	0x00000004
#define	TAB_SRCH_DATA		0x00000008	
#define TAB_SSTAB_SPLIT		0x00000010	 
#define TAB_SSTAB_1ST_ROW_CHG	0x00000020
#define TAB_KEPT_BUF_VALID	0x00000040	
#define TAB_INS_DATA		0x00000080
#define TAB_SCHM_INS		0x00000100	
#define TAB_GET_RES_SSTAB	0x00000200	
#define TAB_TABLET_SPLIT	0x00000400
#define TAB_TABLET_CRT_NEW	0x00000800	
#define TAB_TABLET_KEYROW_CHG	0x00001000	
#define TAB_DEL_DATA		0x00002000	
#define	TAB_RETRY_LOOKUP	0x00004000	
#define	TAB_DO_SPLIT		0x00008000
#define TAB_RESERV_BUF		0x00010000
#define TAB_INS_SPLITING_SSTAB	0x00020000
#define	TAB_LOG_SKIP_LOG	0x00040000


#define TAB_IS_SYSTAB(tabinfo)	(tabinfo->t_stat & TAB_META_SYSTAB)


#define META_CONF_PATH_MAX_LEN   64

#define	COL_MAX_NUM	16
#define	TAB_MAX_NUM	64


#define MAX_RANGER_NUM	1024
#define HB_DATA_SIZE	128

typedef struct hb_data
{
	int	hb_stat;
	char	recv_data[HB_DATA_SIZE];
}HB_DATA;

/* Place holder: following definition is for the hb_stat. */
#define	HB_IS_OFF	0x0000		/* heartbeat is down. */
#define	HB_IS_ON	0x0001		/* heartbeat setup. */

#define HB_RANGER_IS_ON(hb_data)	((hb_data)->hb_stat & HB_IS_ON)

#define HB_SET_RANGER_ON(hb_data)	((hb_data)->hb_stat = (((hb_data)->hb_stat & ~HB_IS_OFF) | HB_IS_ON))

#define HB_SET_RANGER_OFF(hb_data)	((hb_data)->hb_stat = (((hb_data)->hb_stat & ~HB_IS_ON) | HB_IS_OFF))

/* In-memory structure. */
typedef struct meta_systable
{
	int		last_tabid;	/* Max table id. */
	int		update_ts;	/* The TS latest update. */
	int		tabnum;		/* The number of table current system owning. */
	int		tab_stat[TAB_MAX_NUM];
	char		meta_tabdir[TAB_MAX_NUM][256];
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

typedef struct master_infor
{
	char		conf_path[META_CONF_PATH_MAX_LEN];
	int		port;
	int		last_tabid;
	META_SYSTABLE	*meta_systab;
	META_SYSOBJECT	*meta_sysobj;
	META_SYSCOLUMN	*meta_syscol;	
	LOCKATTR 	mutexattr;
	SPINLOCK	rglist_spinlock;	
	SVR_IDX_FILE	rg_list;
	HB_DATA		heart_beat_data[MAX_RANGER_NUM];
}MASTER_INFOR;


#define TABINFO_INIT(tabinfo, sstab_name, srch_info, minlen, status, tabid, sstabid)	\
do {											\
		(tabinfo)->t_dold = (tabinfo)->t_dnew = (BUF *)(tabinfo);		\
		(tabinfo)->t_sstab_name = sstab_name;					\
		(tabinfo)->t_row_minlen = minlen;					\
		(tabinfo)->t_sinfo = srch_info;						\
		(tabinfo)->t_stat|= status;						\
		(tabinfo)->t_tabid = tabid;						\
		(tabinfo)->t_sstab_id = sstabid;					\
}while (0)


#endif 
