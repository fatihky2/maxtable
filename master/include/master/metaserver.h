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
	int		pad;
	pthread_t	tid;
} RANGE_PROF;


#define RANGER_IS_ONLINE	0x0001
#define RANGER_IS_OFFLINE	0x0002


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

typedef struct select_range
{
	INSMETA		left_range;
	INSMETA		right_range;	
} SELRANGE;



#define	INS_META_1ST	0x0001	

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
	INSRG		*t_insrg;
} TABINFO;



#define TAB_META_SYSTAB		0x0001
#define TAB_SCHM_SRCH		0x0002	
#define TAB_CRT_NEW_FILE	0x0004
#define	TAB_SRCH_DATA		0x0008	
#define TAB_SSTAB_SPLIT		0x0010  
#define TAB_SSTAB_1ST_ROW_CHG	0x0020
#define TAB_KEPT_BUF_VALID	0x0040	
#define TAB_INS_DATA		0x0080
#define TAB_SCHM_INS		0x0100	
#define TAB_GET_RES_SSTAB	0x0200	
#define TAB_TABLET_SPLIT	0x0400
#define TAB_TABLET_CRT_NEW	0x0800	
#define TAB_TABLET_KEYROW_CHG	0x1000	
#define TAB_DEL_DATA		0x2000	
#define	TAB_RETRY_LOOKUP	0x4000	/* Retry to lookup the metadata. */
#define	TAB_DO_SPLIT		0x8000


#define TAB_IS_SYSTAB(tabinfo)	(tabinfo->t_stat & TAB_META_SYSTAB)



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


#define META_CONF_PATH_MAX_LEN   64
#define DEFAULT_DUPLICATE_NUM 1
#define MIN_REGION_AVAILABLE_SIZE 100 //Unit is MB
#define DEFAULT_MASTER_FLUSH_CHECK_INTERVAL 600 //10Min


typedef struct master_infor
{
	char		conf_path[META_CONF_PATH_MAX_LEN];
	int		port;
	SVR_IDX_FILE	rg_list;
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
