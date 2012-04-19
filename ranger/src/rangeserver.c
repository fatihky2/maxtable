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

#include "global.h"
#include "utils.h"
#include "list.h"
#include "conf.h"
#include "netconn.h"
#include "memcom.h"
#include "master/metaserver.h"
#include "tabinfo.h"
#include "parser.h"
#include "file_op.h"
#include "row.h"
#include "token.h"
#include "tss.h"
#include "hkgc.h"
#include "strings.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "metadata.h"
#include "cache.h"
#include "exception.h"
#include "type.h"
#include "trace.h"
#include "session.h"
#include "tablet.h"
#include "rebalancer.h"
#include "sstab.h"
#include "checktable.h"
#include "compact.h"
#include "b_search.h"
#include "log.h"
#include "m_socket.h"
#include "rginfo.h"
#include "interface.h"
#include "ranger/rangeserver.h"
#include "index.h"


extern TSS	*Tss;
extern KERNEL	*Kernel;
extern	char	Kfsserver[32];
extern	int	Kfsport;


extern	RG_LOGINFO	*Rg_loginfo;
extern	RANGEINFO	*Range_infor;

extern int	sstab_split_cnt;
extern int	sstabsplit_idx_upd_cnt;



pthread_mutex_t port_mutex = PTHREAD_MUTEX_INITIALIZER;

int mapred_data_port = 40000;
int max_data_port = 40100;
int init_data_port = 40000;

typedef struct _mapred_arg
{
	char table_name[128];
	char tablet_name[128];
	int data_port;
	int pad;
}mapred_arg;


static int
rg_fill_resd(TREE *command, COLINFO *colinfor, int totcol);

static void 
rg_regist();

static char *
rg_rebalancer(REBALANCE_DATA * rbd);

static char *
rg_compact_sstab_by_tablet(COMPACT_DATA *cpctdata);

static char *
rg_check_sstab_by_tablet(CHECKTABLE_DATA *chkdata);

static int
rg_tabletscan(TABLET_SCANCTX *scanctx);

static int
rg_sstabscan(SSTAB_SCANCTX *scanctx);

static char *
rg_recovery(char *rgip, int rgport);


static int
rg_table_is_exist(char *tabname);


static int
rg_table_regist(char *tabname);

static int
rg_table_unregist(char *tabname);

static int
rg_sstable_is_exist(char *sstabname, int tabidx);

static int
rg_sstable_regist(char *sstabname, int tabidx);

static int
rg__selrangetab(TABINFO *tabinfo, char *sstab_left, char *sstab_right, int connfd,
		char *key_left, int keylen_left, char *key_right, int keylen_right,
		int left_expand, int right_expand, char *tabdir, int *data_cont);

static int
rg_get_sstab_tablet(char *tabletbp, char *key, int keylen, char **sstabname, 
				int *namelen, int *sstabid, int flag);


static int
rg_count_data_tablet(TABLET_SCANCTX *scanctx);

static int
rg_count_data_sstable(SSTAB_SCANCTX *scanctx);

static char *
rg_mapred_setup(char * req_buf);

static int
rg_crtidx_tablet(TABLET_SCANCTX *scanctx, IDXBLD *idxbld);

static int
rg_crtidx_sstab(SSTAB_SCANCTX *scanctx, IDXBLD *idxbld);

static void *
rg_mapred_process(void *args);

static char *
rg_idxroot_split(IDX_ROOT_SPLIT *idx_root_split);



char *
rg_droptab(TREE *command)
{
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	char		*resp;


	Assert(command);

	rtn_stat = FALSE;
	
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;


	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	if (STAT(tab_dir, &st) != 0)
	{
		rtn_stat = TRUE;
		goto exit;		
	}

#ifdef MT_KFS_BACKEND
	int	status;
	RMDIR(status, tab_dir);
	if(!status)

#else
	char	cmd_str[TABLE_NAME_MAX_LEN];

	MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
	
	sprintf(cmd_str, "rm -rf %s", tab_dir);
	
	if (!system(cmd_str))
#endif
	{
		rtn_stat = TRUE;
	}

	rg_table_unregist(tab_dir);

exit:
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;
}


char *
rg_dropidx(TREE *command)
{
	char		*tab_name;
	int		tab_name_len;
	char		*idx_name;
	int		idx_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	char		*resp;
	int		tabidx;


	Assert(command);

	rtn_stat = FALSE;
	

	/* Index name*/
	idx_name = command->sym.command.tabname;
	idx_name_len = command->sym.command.tabname_len;

	/* Table name locate at the sub-command ON. */
	tab_name = (command->right)->sym.command.tabname;
	tab_name_len = (command->right)->sym.command.tabname_len;

	/* The full path of meta table. */
	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	/* The full path of ranger table. */
	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	
	/* Check if table is exist. */
	if ((tabidx = rg_table_is_exist(tab_dir)) == -1)
	{
		if (STAT(tab_dir, &st) != 0)
		{
			traceprint("Table %s is not exist.\n", tab_name);
			goto exit;
		}

		/* If hit the error, it has printed the information in caller. */
		tabidx = rg_table_regist(tab_dir);
	}


	char	tab_meta_dir[TABLE_NAME_MAX_LEN];

	/* Build the full path for the tabletscheme file. */
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', idx_name);
	
	/* Make the directory to save the state of ranger servers. */
	if (STAT(tab_meta_dir, &st) != 0)
	{
		traceprint("Index %s on table %s is not exist.\n", idx_name, tab_name);
		goto exit;
	}

#ifdef MT_KFS_BACKEND
	int	status;
	RMDIR(status, tab_meta_dir);
	if(!status)

#else
	char	cmd_str[TABLE_NAME_MAX_LEN];

	MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
	
	sprintf(cmd_str, "rm -rf %s", tab_meta_dir);
	
	if (!system(cmd_str))
#endif
	{
		rtn_stat = TRUE;
	}

exit:
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;
}

char *
rg_instab(TREE *command, TABINFO *tabinfo)
{
	LOCALTSS(tss);
	char		*sstable;
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		rg_tab_dir[TABLE_NAME_MAX_LEN];
	int		status;
	int		rtn_stat;
	int		sstab_rlen;
	int		sstab_idx;
	char	 	*resp;
	char		*rp;
	int		rp_idx;
	int		rlen;
	char		col_off_tab[COL_OFFTAB_MAX_SIZE];	/* Max of var-column is 16 */
	int		col_off_idx;
	int		col_offset;
	char		*col_val;
	int		col_len;
	int		col_num;
	INSMETA 	*ins_meta;
	COLINFO 	*col_info;
	char		*resp_buf;
	int		resp_len;
	int		buf_spin;
	int		tabidx;
	IDXBLD		idxbld;


	Assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	ins_meta = tabinfo->t_insmeta;
	col_info = tabinfo->t_colinfo;
	buf_spin = FALSE;
	resp_len = 0;
	resp_buf = NULL;
	rp = NULL;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	if ((tabidx = rg_table_is_exist(tab_dir)) == -1)
	{
		if (STAT(tab_dir, &st) != 0)
		{
			MKDIR(status, tab_dir, 0755);

			if (status < 0)
			{
				goto exit;
			}
		}

		tabidx = rg_table_regist(tab_dir);
	}

	
	if (DEBUG_TEST(tss))
	{
		/* 
		** sstable name = "tablet name _ sstable_id", so sstable name
		** is unique in one table. 
		*/
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}

	MEMSET(rg_tab_dir,TABLE_NAME_MAX_LEN);
	MEMCPY(rg_tab_dir,tab_dir,STRLEN(tab_dir));
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

	/* sstable checking is for the key propagation to the tablet. */
	if (!rg_sstable_is_exist(tab_dir, tabidx))
	{
		if (STAT(tab_dir, &st) != 0)
		{
			/* Flag if it's the first insertion. */
			ins_meta->status |= INS_META_1ST;
		}
		else
		{
			rg_sstable_regist(tab_dir, tabidx);
		}
	}
	
	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
		traceprint("tab_dir =%s \n", tab_dir);
	}

	rlen = BLOCKSIZE - BLK_TAILSIZE - BLKHEADERSIZE - sizeof(int);
	rp = MEMALLOCHEAP(rlen);

	/* Begin to build row. */
	row_build_hdr(rp, 0, 0, ins_meta->varcol_num);

	col_offset = sizeof(ROWFMT);
	rp_idx = sizeof(ROWFMT);
	col_num = ins_meta->col_num;
	col_off_idx = COL_OFFTAB_MAX_SIZE;

	rg_fill_resd(command, col_info, col_num);

	while(col_num)
	{
		col_val = par_get_colval_by_coloff(command, col_offset,	
							&col_len);

		if (col_offset == tabinfo->t_key_coloff)
		{
			/* 
			** Fill search information for the searching in the 
			** block.
			*/
			tabinfo->t_sinfo->sicolval = col_val;
			tabinfo->t_sinfo->sicollen = col_len;
			tabinfo->t_sinfo->sicolid = tabinfo->t_key_colid;
			tabinfo->t_sinfo->sicoltype = tabinfo->t_key_coltype;
			tabinfo->t_sinfo->sicoloff = tabinfo->t_key_coloff;
		}
		
		if(col_val)
		{
			if (col_offset < 0)
			{
				col_off_idx -= sizeof(int);
				*((int *)(col_off_tab + col_off_idx)) = rp_idx;
			}

			if ((rp_idx + col_len) > rlen)
			{
				traceprint("The row to be inserted expand the max size %d of one row.\n", rlen);
				goto exit;
			}
			
			PUT_TO_BUFFER(rp, rp_idx, col_val, col_len);
			if (col_offset > 0)
			{
				col_offset += col_len;
			}
			else
			{				
				col_offset--;
			}

			col_num--;
		}
		else
		{
			Assert(col_offset > 0);

			if (!(col_offset > 0))
			{
				traceprint("Hit a row error!\n");
				MEMFREEHEAP(rp);
				ex_raise(EX_ANY);
			}
			
			/* 
			** col_offset will increase until it's an invalid one,
			** and then we start to parse the var-col case.
			*/
			
			/* Var-col case. */	

			if (col_num > 0)
			{
				/* Row length */
				rp_idx += sizeof(int);
				col_offset = -1;
			}
			
			Assert(ins_meta->varcol_num == col_num);

			if (ins_meta->varcol_num != col_num)
			{
				traceprint("Hit a row error!\n");
				MEMFREEHEAP(rp);
				ex_raise(EX_ANY);
			}
		}		
		
	}

	if (COL_OFFTAB_MAX_SIZE > col_off_idx)
	{
		if ((rp_idx + (COL_OFFTAB_MAX_SIZE - col_off_idx)) > rlen)
		{
			traceprint("The row to be inserted expand the max size %d of one row.\n", rlen);
			goto exit;
		}
		
		PUT_TO_BUFFER(rp, rp_idx, (col_off_tab + col_off_idx), 
					(COL_OFFTAB_MAX_SIZE - col_off_idx));
		*(int *)(rp + ins_meta->row_minlen) = rp_idx;
	}

	P_SPINLOCK(BUF_SPIN);
	buf_spin = TRUE;

	rtn_stat = blkins(tabinfo, rp);

	/* Update index. */
	if (rtn_stat && (tabinfo->t_has_index))
	{
		MEMSET(&idxbld, sizeof(IDXBLD));

		idxbld.idx_tab_name = rg_tab_dir;

		idxbld.idx_stat = (ins_meta->status & INS_META_1ST) 
				? IDXBLD_FIRST_DATAROW_IN_TABLET : 0;

		idxbld.idx_root_sstab = tabinfo->t_tablet_id;

		if (tabinfo->t_index_ts 
			!= Range_infor->rg_meta_sysindex->idx_ver)
		{
			meta_load_sysindex(
				(char *)Range_infor->rg_meta_sysindex);
		}
				
		rtn_stat = index_insert(&idxbld, tabinfo, 
					Range_infor->rg_meta_sysindex);
		
	}

exit:

	
	
	if (rtn_stat && (tabinfo->t_stat & TAB_SSTAB_SPLIT))
	{
		resp_len = tabinfo->t_insrg->new_keylen + SSTABLE_NAME_MAX_LEN
					+ 3 * sizeof(int);
		
		resp_buf = (char *)MEMALLOCHEAP(resp_len);

		MEMSET(resp_buf, resp_len);

		int i = 0;

		if (DEBUG_TEST(tss))
		{
			traceprint("tabinfo->t_insrg->new_sstab_name = %s \n", tabinfo->t_insrg->new_sstab_name);
		}
		
		PUT_TO_BUFFER(resp_buf, i, tabinfo->t_insrg->new_sstab_name, 
				SSTABLE_NAME_MAX_LEN);

		PUT_TO_BUFFER(resp_buf, i, &tabinfo->t_insmeta->res_sstab_id,
				sizeof(int));
		
		/* Brifly solution for the unsigned int. */
		PUT_TO_BUFFER(resp_buf, i, &tabinfo->t_insmeta->ts_low,
				sizeof(int));
		
		/* split sstable id. */
		PUT_TO_BUFFER(resp_buf, i, &tabinfo->t_insmeta->sstab_id,
				sizeof(int));
		
		PUT_TO_BUFFER(resp_buf, i, tabinfo->t_insrg->new_sstab_key, 
				tabinfo->t_insrg->new_keylen);
		
//		printf("tabinfo->t_insmeta->sstab_id = %d,  tabinfo->t_insmeta->res_sstab_id = %d\n", tabinfo->t_insmeta->sstab_id, tabinfo->t_insmeta->res_sstab_id);
		Assert(resp_len == i);
	}
	
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, resp_len, resp_buf);
	}
	else
	{
		if(tabinfo->t_stat & TAB_RETRY_LOOKUP)
        	{
	                resp = conn_build_resp_byte(RPC_RETRY, 0, NULL);
        	}
        	else
        	{
			resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
        	}
	}

	if (buf_spin)
	{
		V_SPINLOCK(BUF_SPIN);
	}
	
	if (resp_buf)
	{
		MEMFREEHEAP(resp_buf);
	}

	if (rp)
	{
		MEMFREEHEAP(rp);
	}
	
	return resp;

}


char *
rg_seldeltab(TREE *command, TABINFO *tabinfo)
{
	LOCALTSS(tss);
	char		*sstable;
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	int		sstab_rlen;
	int		sstab_idx;
	char		*resp;
	INSMETA 	*ins_meta;
	COLINFO 	*col_info;
	BUF		*bp;
	char		*keycol;
	int		keycolen;
	int		rnum;
	char   		*col_buf;
	int 		rlen;
	int		buf_spin;
	int		tabidx;


	Assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	col_buf = NULL;
	bp = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	ins_meta = tabinfo->t_insmeta;
	col_info = tabinfo->t_colinfo;
	buf_spin = FALSE;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	if ((tabidx = rg_table_is_exist(tab_dir)) == -1)
	{
		if (STAT(tab_dir, &st) != 0)
		{
			traceprint("Table %s is not exist.\n", tab_name);
			goto exit;
		}

		tabidx = rg_table_regist(tab_dir);
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

	if (!rg_sstable_is_exist(tab_dir, tabidx))
	{
		if (STAT(tab_dir, &st) != 0)
		{
			goto exit; 
		}

		rg_sstable_regist(tab_dir, tabidx);
	}
	
	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
		traceprint("tab_dir =%s \n", tab_dir);
	}
	
	keycol = par_get_colval_by_colid(command, tabinfo->t_key_colid, &keycolen);

	TABINFO_INIT(tabinfo, ins_meta->sstab_name, tab_name, tab_name_len,
			tabinfo->t_sinfo, tabinfo->t_row_minlen, 0,
			tabinfo->t_tabid, tabinfo->t_sstab_id);
	SRCH_INFO_INIT(tabinfo->t_sinfo, keycol, keycolen, 1, VARCHAR, -1);

	/* Case delete data.*/
	if (tabinfo->t_stat & TAB_DEL_DATA)
	{
		P_SPINLOCK(BUF_SPIN);
		buf_spin = TRUE;
		
		rtn_stat = blkdel(tabinfo);
		
		if (rtn_stat && (tabinfo->t_has_index))
		{
			IDXBLD	idxbld;
			char	rg_tab_dir[TABLE_NAME_MAX_LEN];


			MEMSET(&idxbld, sizeof(IDXBLD));

			MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(rg_tab_dir, MT_RANGE_TABLE,
					STRLEN(MT_RANGE_TABLE));			
			str1_to_str2(rg_tab_dir, '/', tab_name);
	
			idxbld.idx_tab_name = rg_tab_dir;
	
			idxbld.idx_stat = 0;
	
			idxbld.idx_root_sstab = tabinfo->t_tablet_id;
	
			if (tabinfo->t_index_ts 
				!= Range_infor->rg_meta_sysindex->idx_ver)
			{
				meta_load_sysindex(
					(char *)Range_infor->rg_meta_sysindex);
			}
					
			rtn_stat = index_delete(&idxbld, tabinfo, 
						Range_infor->rg_meta_sysindex);
			
		}

		goto exit;		
	}
	else
	{
		/* Case select data. */
		bp = blkget(tabinfo);
//		offset = blksrch(tabinfo, bp);

		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			goto exit;
		}

		Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
		Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);

		if ((tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
		    || (tabinfo->t_rowinfo->rsstabid != bp->bsstab->bsstabid))
		{
			traceprint("Hit a buffer error!\n");
			ex_raise(EX_ANY);
		}
		
		rnum = tabinfo->t_rowinfo->rnum;
	}
	
	/* The selecting value is not exist. */
	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		/* Just return a NULL sucess. */
		rtn_stat = TRUE;
		goto exit;
	}

	if (!(tabinfo->t_stat & TAB_DEL_DATA))
	{
		/* TODO: rp, rlen just be the future work setting. */
		char *rp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, rnum);
		
		// char	*filename = meta_get_coldata(bp, offset, sizeof(ROWFMT));

		rlen = ROW_GET_LENGTH(rp, bp->bblk->bminlen);
		
		//value = row_locate_col(rp, -1, bp->bblk->bminlen, &rlen);

		/* Building the response information. */
		col_buf = MEMALLOCHEAP(rlen);
		
		MEMSET(col_buf, rlen);

		MEMCPY(col_buf, rp, rlen);
		
	}
	
	rtn_stat = TRUE;

exit:
	if (bp)
	{
		bufunkeep(bp->bsstab);
	}
	
	if (rtn_stat)
	{		
		if (tabinfo->t_sinfo->sistate & SI_NODATA)
		{
			resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
		}
		else if (tabinfo->t_stat & TAB_DEL_DATA)
		{
			/* Just return a SUCCESS. */
			resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
		}
		else
		{
			/* Send to client, just send the sstable name. */
			resp = conn_build_resp_byte(RPC_SUCCESS, rlen, col_buf);
		}
	}
	else
	{
		if(tabinfo->t_stat & TAB_RETRY_LOOKUP)
        	{
	                resp = conn_build_resp_byte(RPC_RETRY, 0, NULL);
        	}
        	else
        	{
			resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
        	}
	}

	if (buf_spin)
	{
		V_SPINLOCK(BUF_SPIN);
	}
	
	if (col_buf != NULL)
	{
		MEMFREEHEAP(col_buf);
	}
	
	return resp;

}


/* Single node operation for the whole range query. */
char *
rg_selrangetab(TREE *command, TABINFO *tabinfo, int fd)
{
	LOCALTSS(tss);
	char		*sstable;
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	int		sstab_rlen;
	int		sstab_idx;
	char		*resp;
	int		resp_size;
	INSMETA 	*ins_meta;
	COLINFO 	*col_info;
	BUF		*bp;
	char		*keycol;
	int		keycolen;
	int		rnum;
	char		last_sstab[SSTABLE_NAME_MAX_LEN];
	int		left_expand;
	int		right_expand;
	B_SRCHINFO	srchinfo;
	int		tabidx;


	Assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	bp = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	ins_meta = tabinfo->t_insmeta;
	col_info = tabinfo->t_colinfo;
	left_expand = right_expand = FALSE;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	if ((tabidx = rg_table_is_exist(tab_dir)) == -1)
	{
		if (STAT(tab_dir, &st) != 0)
		{
			traceprint("Table %s is not exist.\n", tab_name);
			goto exit;
		}

		tabidx = rg_table_regist(tab_dir);
	}
	
	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

	if (!rg_sstable_is_exist(tab_dir, tabidx))
	{
		if (STAT(tab_dir, &st) != 0)
		{
			goto exit; 
		}

		rg_sstable_regist(tab_dir, tabidx);
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
		traceprint("tab_dir =%s \n", tab_dir);
	}

	/* Left key ID is the '1' that's not the true column ID. */
	keycol = par_get_colval_by_colid(command, 1, &keycolen);

	if ((keycolen == 1) && (!strncasecmp("*", keycol, keycolen)))
	{
		left_expand = TRUE;
	}
	
	char	*right_rangekey;
	int	right_keylen;

	/* Right key ID is the '2'. */
	right_rangekey = par_get_colval_by_colid(command, 2, &right_keylen);

	if ((right_keylen == 1) && (!strncasecmp("*", right_rangekey, 
							right_keylen)))
	{
		right_expand = TRUE;
	}
	
	TABINFO_INIT(tabinfo, ins_meta->sstab_name, tab_name, tab_name_len,
			tabinfo->t_sinfo, tabinfo->t_row_minlen, 0, 
			tabinfo->t_tabid, tabinfo->t_sstab_id);
	SRCH_INFO_INIT(tabinfo->t_sinfo, keycol, keycolen, 1, VARCHAR, -1);

	if (left_expand)
	{
		bp = blk_getsstable(tabinfo);
	}
	else
	{
		bp = blkget(tabinfo);
	
	
		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			goto exit;
		}
	}
	
	int listenfd = conn_socket_open(Range_infor->bigdataport);

	if (!listenfd)
	{
		goto exit;
	}

	if (left_expand)
	{
		rnum = 0;
	}
	else
	{
		Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
		Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);

		if ((tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
		    || (tabinfo->t_rowinfo->rsstabid != bp->bsstab->bsstabid))
		{
			traceprint("Hit a buffer error!\n");
			bufunkeep(bp->bsstab);
			ex_raise(EX_ANY);
		}
		
		rnum = tabinfo->t_rowinfo->rnum;

		Assert(rnum != -1);
	}

	RANGE_QUERYCTX	rgsel_cont;

	char	resp_cli[8];

	rgsel_cont.first_rowpos = rnum;

	Assert(rnum < bp->bblk->bnextrno);


	int n;
	
	signal (SIGPIPE,SIG_IGN);
	
	resp = conn_build_resp_byte(RPC_BIGDATA_CONN, sizeof(int),
					(char *)(&(Range_infor->bigdataport)));
	resp_size = conn_get_resp_size((RPCRESP *)resp);	
	tcp_put_data(fd, resp, resp_size);
	conn_destroy_resp_byte(resp);

	int	connfd;
	int	data_cont = FALSE;
	
	connfd = conn_socket_accept(listenfd);

	if (connfd < 0)
	{
		printf("hit accept issue\n");
		goto exit;
	}
		
	while (TRUE)
	{		
		if (right_expand)
		{
			rgsel_cont.rowminlen = bp->bblk->bminlen;
			
			rgsel_cont.cur_rowpos = rgsel_cont.first_rowpos;
			rgsel_cont.end_rowpos = bp->bblk->bnextrno - 1;
			rgsel_cont.status = DATA_CONT;
			data_cont = TRUE;
		}
		else
		{	
			TABINFO_INIT(tabinfo, tabinfo->t_sstab_name,
					tab_name, tab_name_len,	tabinfo->t_sinfo,
					tabinfo->t_row_minlen, 0,
					tabinfo->t_tabid, tabinfo->t_sstab_id);
			
			SRCH_INFO_INIT(tabinfo->t_sinfo, right_rangekey, 
					right_keylen, 1, VARCHAR, -1);			

			MEMSET(&srchinfo, sizeof(B_SRCHINFO));
			SRCHINFO_INIT((&srchinfo), 0, 
					BLK_GET_NEXT_ROWNO(bp->bblk) - 1, 
					BLK_GET_NEXT_ROWNO(bp->bblk), LE);

			b_srch_block(tabinfo, bp, &srchinfo);

			rgsel_cont.rowminlen = bp->bblk->bminlen;
			
			rgsel_cont.cur_rowpos = rgsel_cont.first_rowpos;
	
			/* Stamp the status code. */
			if (srchinfo.brownum < (bp->bblk->bnextrno - 1))
			{
				rgsel_cont.end_rowpos = srchinfo.brownum;
				rgsel_cont.status = DATA_DONE;
				data_cont = FALSE;
			}
			else
			{
				Assert(srchinfo.brownum == (bp->bblk->bnextrno - 1));

				if (srchinfo.bcomp == GR)
				{
					rgsel_cont.end_rowpos = srchinfo.brownum;
					rgsel_cont.status = DATA_CONT;
					data_cont = TRUE;
				}
				else
				{
					/*
					** If there's only one row with DELETE 
					** flag in one sstable. the deleted row 
					** will not return to client. 
					*/
					if (srchinfo.bcomp == LE)
					{
						rgsel_cont.end_rowpos = 
							srchinfo.brownum - 1;
					}
					else
					{
						rgsel_cont.end_rowpos = 
							srchinfo.brownum;
					}
					
					rgsel_cont.status = DATA_DONE;
					data_cont = FALSE;
				}
			}

		}

		MEMCPY(rgsel_cont.data, (char *)(bp->bblk), BLOCKSIZE);
	
	 	resp = conn_build_resp_byte(RPC_SUCCESS, sizeof(RANGE_QUERYCTX), 
						(char *)&rgsel_cont);
		
		resp_size = conn_get_resp_size((RPCRESP *)resp);

		tcp_put_data(connfd, resp, resp_size);

		conn_destroy_resp_byte(resp);	

		
		/* TODO: placeholder for the TCP/IP check. */
		MEMSET(resp_cli, 8);
		n = conn_socket_read(connfd,resp_cli, 8);

		if (n != 8)
		{
			traceprint("Socket read error 6.\n");
			goto exit;
		}

		if (!data_cont)
		{
			/* We already hit all the data. */
			break;
		}
		
nextblk:			
		if (bp->bblk->bnextblkno != -1)
		{
			bp++;
		}
		else if (bp->bsstab->bblk->bnextsstabnum != -1)
		{
			MEMSET(last_sstab, SSTABLE_NAME_MAX_LEN);
			MEMCPY(last_sstab, tabinfo->t_sstab_name, 
					STRLEN(tabinfo->t_sstab_name));
			
			MEMSET(tabinfo->t_sstab_name, SSTABLE_NAME_MAX_LEN);			
			
			sstab_namebyid(last_sstab, tabinfo->t_sstab_name, 
						bp->bsstab->bblk->bnextsstabnum);

			tabinfo->t_sstab_id = bp->bsstab->bblk->bnextsstabnum;

			bufunkeep(bp->bsstab);
			
			bp = blk_getsstable(tabinfo);			
		}
		else
		{
			rgsel_cont.status = DATA_EMPTY;
			resp = conn_build_resp_byte(RPC_SUCCESS, 
						sizeof(RANGE_QUERYCTX),
						(char *)&rgsel_cont);
		
			resp_size = conn_get_resp_size((RPCRESP *)resp);
			  
			tcp_put_data(connfd, resp, resp_size);			

			conn_destroy_resp_byte(resp);	

			/* TODO: placeholder for the TCP/IP check. */
			MEMSET(resp_cli, 8);
			n = conn_socket_read(connfd,resp_cli, 8);

			if (n != 8)
			{
				traceprint("Socket read error 5.\n");
				goto exit;
			}

			break;
		}

		if (bp->bblk->bfreeoff > BLKHEADERSIZE)
		{
			rgsel_cont.first_rowpos = 0;
		}
		else
		{
			goto nextblk;
		}		
			
	}
	rtn_stat = TRUE;

exit:
	if (bp)
	{
		bufunkeep(bp->bsstab);
	}
	
	conn_socket_close(connfd);

	conn_socket_close(listenfd);
	if (rtn_stat)
	{
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;

}




/*
** This routine will execeute the range query from the sstab_left to the 
** sstab_right.
**
** Parameters:
**	tabinfo		- (IN) table information.
**	sstab_left		- (IN) the sstable that's the left bound of 
**			         range query.
**	sstab_right	- (IN) the sstable that's the right bound of range
**			         query.
**	connfd		- (IN) socket id of the connection to transfer the 
**			-        big data.
**	key_left		- (IN) the left key of this range user specified.
**	keylen_left	- (IN) the length of the left key.
**	key_right		- (IN) the right key of this range user specified.
**	keylen_right	- (IN) the length of right key.
**	left_expand	- (IN) flag if the left bound is *.
**	right_expand	- (IN) flag if the right bound is *. 
**	tabdir		- (IN) full path of table in the ranger server.
**	data_cont	- (OUT) flag if it hit the last sstable.
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None
** 
*/
static int
rg__selrangetab(TABINFO *tabinfo, char *sstab_left, char *sstab_right, int connfd,
		char *key_left, int keylen_left, char *key_right, int keylen_right,
		int left_expand, int right_expand, char *tabdir, int *data_cont)
{
	char		*resp;
	int		resp_size;
	BUF		*bp;
	int		rnum;
	char		last_sstab[SSTABLE_NAME_MAX_LEN];
	B_SRCHINFO	srchinfo;
	char		tab_left_sstab_dir[TABLE_NAME_MAX_LEN];
	char		tab_right_sstab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	int 		*offtab;
	char		*keyleft;
	int		keyleftlen;


	bp = NULL;
	rtn_stat = TRUE;

	
	/* Get the full path for the left bound. */
	MEMSET(tab_left_sstab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_left_sstab_dir, tabdir, STRLEN(tabdir));
	str1_to_str2(tab_left_sstab_dir, '/', sstab_left);

	/* Get the full path for the right bound. */
	MEMSET(tab_right_sstab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_right_sstab_dir, tabdir, STRLEN(tabdir));
	str1_to_str2(tab_right_sstab_dir, '/', sstab_right);	
	keyleft = key_left;
	keyleftlen = keylen_left;


	TABINFO_INIT(tabinfo, tab_left_sstab_dir, tabinfo->t_tab_name,
			tabinfo->t_tab_namelen, tabinfo->t_sinfo, 
			tabinfo->t_row_minlen, 0, tabinfo->t_tabid, 
			tabinfo->t_sstab_id);
	SRCH_INFO_INIT(tabinfo->t_sinfo, keyleft, keyleftlen, 1, VARCHAR, -1);

	if (left_expand)
	{
		bp = blk_getsstable(tabinfo);

		rnum = 0;
	}
	else
	{
		bp = blkget(tabinfo);
	
	
		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			return FALSE;
		}

		Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
		Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);

		if ((tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
		    || (tabinfo->t_rowinfo->rsstabid != bp->bsstab->bsstabid))
		{
			traceprint("Hit a buffer error!\n");
			bufunkeep(bp->bsstab);
			ex_raise(EX_ANY);
		}
		
		rnum = tabinfo->t_rowinfo->rnum;

		Assert(rnum != -1);
	}
	

	offtab = ROW_OFFSET_PTR(bp->bblk);
	

	RANGE_QUERYCTX	rgsel_cont;

	char	resp_cli[8];	
	

	/* Get the 1st left boud for this tablet range. */
	rgsel_cont.first_rowpos = rnum;

	Assert(rnum < bp->bblk->bnextrno);

	int	n;
	

	/* Working for the range query. */
	while (TRUE)
	{		
		if (right_expand)
		{
			rgsel_cont.rowminlen = bp->bblk->bminlen;
			
			rgsel_cont.cur_rowpos = rgsel_cont.first_rowpos;
			rgsel_cont.end_rowpos = bp->bblk->bnextrno - 1;
			rgsel_cont.status = DATA_CONT;
			*data_cont = TRUE;
		}
		else
		{	
			TABINFO_INIT(tabinfo, tabinfo->t_sstab_name,
					tabinfo->t_tab_name, 
					tabinfo->t_tab_namelen, 
					tabinfo->t_sinfo, tabinfo->t_row_minlen, 
					0, tabinfo->t_tabid, tabinfo->t_sstab_id);
			
			SRCH_INFO_INIT(tabinfo->t_sinfo, key_right, 
					keylen_right, 1, VARCHAR, -1);			

			MEMSET(&srchinfo, sizeof(B_SRCHINFO));
			SRCHINFO_INIT((&srchinfo), 0, 
					BLK_GET_NEXT_ROWNO(bp->bblk) - 1, 
					BLK_GET_NEXT_ROWNO(bp->bblk), LE);

			b_srch_block(tabinfo, bp, &srchinfo);

			rgsel_cont.rowminlen = bp->bblk->bminlen;
			
			rgsel_cont.cur_rowpos = rgsel_cont.first_rowpos;
	
			/* Stamp the status code. */
			if (srchinfo.brownum < (bp->bblk->bnextrno - 1))
			{
				rgsel_cont.end_rowpos = srchinfo.brownum;
				rgsel_cont.status = DATA_CONT;
				*data_cont = FALSE;
			}
			else
			{
				Assert(srchinfo.brownum == (bp->bblk->bnextrno - 1));

				if (srchinfo.bcomp == GR)
				{
					rgsel_cont.end_rowpos = srchinfo.brownum;
					rgsel_cont.status = DATA_CONT;
					*data_cont = TRUE;
				}
				else
				{
					if (srchinfo.bcomp == LE)
					{
						rgsel_cont.end_rowpos = 
							srchinfo.brownum - 1;
					}
					else
					{
						rgsel_cont.end_rowpos = 
							srchinfo.brownum;
					}
					
					rgsel_cont.status = DATA_CONT;
					*data_cont = FALSE;
				}
			}

		}

		MEMCPY(rgsel_cont.data, (char *)(bp->bblk), BLOCKSIZE);
	
	 	resp = conn_build_resp_byte(RPC_SUCCESS, sizeof(RANGE_QUERYCTX), 
						(char *)&rgsel_cont);
		
		resp_size = conn_get_resp_size((RPCRESP *)resp);

		/* Send the result to the client. */
		tcp_put_data(connfd, resp, resp_size);

		conn_destroy_resp_byte(resp);	

		
		/* TODO: placeholder for the TCP/IP check. */
		MEMSET(resp_cli, 8);
		n = conn_socket_read(connfd,resp_cli, 8);

		if (n != 8)
		{
			traceprint("Socket read error 4.\n");
			rtn_stat = FALSE;
			goto done;
		}

		if (!(*data_cont))
		{
			/* Data is DATA_DONE and we already hit all the data. */
			goto done;
		}
		
nextblk:			
		if (bp->bblk->bnextblkno != -1)
		{
			bp++;
		}
		else if (bp->bsstab->bblk->bnextsstabnum != -1)
		{
			/* Hit the right bound. */
			if (!row_col_compare(VARCHAR,tabinfo->t_sstab_name,
						STRLEN(tabinfo->t_sstab_name),
						tab_right_sstab_dir,
						STRLEN(tab_right_sstab_dir)))
			{
				goto done;
			}

			
			
			MEMSET(last_sstab, SSTABLE_NAME_MAX_LEN);
			MEMCPY(last_sstab, tabinfo->t_sstab_name, 
					STRLEN(tabinfo->t_sstab_name));
			
			MEMSET(tabinfo->t_sstab_name, SSTABLE_NAME_MAX_LEN);			
			
			sstab_namebyid(last_sstab, tabinfo->t_sstab_name, 
						bp->bsstab->bblk->bnextsstabnum);

			tabinfo->t_sstab_id = bp->bsstab->bblk->bnextsstabnum;

			bufunkeep(bp->bsstab);
			
			bp = blk_getsstable(tabinfo);			
		}
		else
		{
			goto done;
		}

		if (bp->bblk->bfreeoff > BLKHEADERSIZE)
		{
			rgsel_cont.first_rowpos = 0;
		}
		else
		{
			goto nextblk;
		}		
			
	}

done:
	if (bp)
	{
		bufunkeep(bp->bsstab);
	}
	
	return rtn_stat;
}


/* 
** This routine will execute the query with some conditions, such as the where
** clause, range query (key range). 
**
** Parameters:
** 	command	- (IN) the command user specified.
**	selwhere		- (IN) the meta information from metaserver
**	tab_hdr		- (IN) table header information read from 
**			         the sysobjects.
**	colinfo		- (IN) column information.
**	fd		- (IN) the socket id of the connection between 
**			         the ranger and client.
** 
** Returns:
**	the ptr to the response information.
** 
** Side Effects:
**	None.
** 
*/
char *
rg_selwheretab(TREE *command, SELWHERE *selwhere, TABLEHDR *tab_hdr, COLINFO *colinfo,int fd)
{
	char		*tab_name;	/* Ptr to the table name. */
	int		tab_name_len;	/* Name length. */
	char		tab_dir[TABLE_NAME_MAX_LEN];
					/* The full path of table in the meta
					** server.
					*/
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
					/* Save the full path of table meta. */
	int		fd1;		/* The id of openning file. */
	int		rtn_stat;	/* Return state. */
		
	char		*resp;		/* Response information. */
	int		resp_size;	/* The size of response information*/
	char		*rp;		/* Ptr to the row. */
	int		namelen;	/* Name length. */
	char		*rg_addr;	/* The address of ranger server. */
	int		rg_port;	/* Ranger port. */
	TABLET_SCANCTX	*tablet_scanctx;/* The context of tablet scanning. */
	RANGE_QUERYCTX	rgsel_cont;	/* The context of range query. */
	char		rg_tab_dir[TABLE_NAME_MAX_LEN];
					/* The full path of table in the ranger 
					** server.
					*/
	int		tabidx;		/* The index of the table information 
					** in the Ranger_infor.
					*/
	int		querytype;


	Assert(command);

	/* Initialization. */
	rtn_stat	= FALSE;
	tablet_scanctx 	= NULL;
	tab_name	= command->sym.command.tabname;
	tab_name_len 	= command->sym.command.tabname_len;
	querytype	= command->sym.command.querytype;

	/* The full path of meta table. */
	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	/* The full path of ranger table. */
	MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(rg_tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
	str1_to_str2(rg_tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	/* Check if table is exist. */
	if ((tabidx = rg_table_is_exist(rg_tab_dir)) == -1)
	{
		if (STAT(rg_tab_dir, &st) != 0)
		{
			traceprint("Table %s range data is not exist.\n", tab_name);
			goto exit;
		}

		/* If hit the error, it has printed the information in caller. */
		tabidx = rg_table_regist(rg_tab_dir);
	}


	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", tab_name);
		goto exit;
	}

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		goto exit;
	}

	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table should have one tablet at least! \n");
		goto exit;
	}
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");
		

	/* Read the file tabletscheme. */
	char	*tablet_schm_bp = NULL;
	
	OPEN(fd1, tab_meta_dir, (O_RDONLY));

	if (fd1 < 0)
	{
	       printf("Table is not exist! \n");
	       goto exit;
	}

	tablet_schm_bp = (char *)MEMALLOCHEAP(SSTABLE_SIZE);
	MEMSET(tablet_schm_bp, SSTABLE_SIZE);

	READ(fd1, tablet_schm_bp, SSTABLE_SIZE); 

	CLOSE(fd1);
	
	BLOCK		*blk;
	int		i = 0, rowno;
	int		*offset;
	int		result;
	char		*tabletname;
	char		tab_tablet_dir[TABLE_NAME_MAX_LEN];
	char		tab_rg_dir[TABLE_NAME_MAX_LEN];
	int		ign;
	char		tablet_bp[SSTABLE_SIZE];	

	/* The tablet to be interested. */
	char		*wanted_tablet;

	int		wanted_tablet_len;

	/* Check if we got the first tablet. */
	int		leftbegin = FALSE;
	/* Check if we got the last tablet. */	
	int 		rightend = FALSE;

	/* For selectrange. */
	int		left_expand;
	int		right_expand; 
	char		*key_left;
	char		*key_right;
	int		keycolen_left;
	int		keycolen_right;

	/* Create the socket fot the transferring of bigdata. */
	int listenfd = conn_socket_open(Range_infor->bigdataport);

	if (!listenfd)
	{
		goto exit;
	}

	signal (SIGPIPE,SIG_IGN);

	resp = conn_build_resp_byte(RPC_BIGDATA_CONN, sizeof(int),
					(char *)(&(Range_infor->bigdataport)));
	resp_size = conn_get_resp_size((RPCRESP *)resp);

	/* Send the connection information to the client. */
	tcp_put_data(fd, resp, resp_size);
	conn_destroy_resp_byte(resp);

	int	connfd;

	connfd = conn_socket_accept(listenfd);

	if (connfd < 0)
	{
		traceprint("hit socket accept issue\n");
		goto exit;
	}
		
	if (querytype == SELECTWHERE)
	{
		if (!par_fill_colinfo(tab_hdr->tab_col, colinfo, command))
		{
			goto exit;
		}

		/* Initialize the context of tablet scanning. */
		tablet_scanctx = (TABLET_SCANCTX *)MEMALLOCHEAP(sizeof(TABLET_SCANCTX));
		MEMSET(tablet_scanctx, sizeof(TABLET_SCANCTX));

		tablet_scanctx->andplan = par_get_andplan(command);
		tablet_scanctx->orplan = par_get_orplan(command);
		tablet_scanctx->connfd = connfd;
		tablet_scanctx->querytype = querytype;
	}
	else if (querytype == SELECTRANGE)
	{
		left_expand = right_expand = FALSE;

		/*
		** Left range: left key ID is the '1' that's not the true 
		** column ID, it's just a index of column in the query clause.
		*/
		key_left = par_get_colval_by_colid(command, 1, &keycolen_left);

		if ((keycolen_left == 1) && (!strncasecmp("*", key_left, keycolen_left)))
		{
			left_expand = TRUE;
		}
		
		
		/* Right range: right key ID is the '2'. */
		key_right = par_get_colval_by_colid(command, 2, &keycolen_right);

		if ((keycolen_right == 1) && (!strncasecmp("*", key_right, 
								keycolen_right)))
		{
			right_expand = TRUE;
		}

		
		MEMSET(tab_rg_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_rg_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
	
		
		str1_to_str2(tab_rg_dir, '/', tab_name);
	}

	/* Working for the query. */
	while (TRUE)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);


		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			       rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;

			assert(*offset < blk->bfreeoff);

			tabletname = row_locate_col(rp, 
					TABLETSCHM_TABLETNAME_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);

			/* Get the left or right range of key. */
			if (!leftbegin)
			{
				wanted_tablet = selwhere->lefttabletname;
				wanted_tablet_len = selwhere->leftnamelen;
			}
			else
			{
				wanted_tablet = selwhere->righttabletname;
				wanted_tablet_len = selwhere->rightnamelen;
			}

			/* Check if it got the left or right tablet. */
			result = row_col_compare(VARCHAR, tabletname, 
						STRLEN(tabletname), 
						wanted_tablet,
						wanted_tablet_len);
			
			/* 
			** Continue to the scan if it still not hit the first 
			** tablet that locates at the range of key. 
			*/
			if ((result != EQ) && (leftbegin == FALSE))
			{
				continue;
			}
			
			if (result == EQ)
			{	
				if (leftbegin == TRUE)
				{
					/* 
					** Hit the right side of range, that's
					** to say, hit the last tablet. 
					*/
					rightend = TRUE;
				}
				else if (leftbegin == FALSE)
				{
					/*
					** Hit the left side of the range, 
					** that's to say, hit the fisrt tablet.
					*/
					leftbegin = TRUE;
				}
			}

			/* Check if this tablet locates at this ranger server. */
			rg_addr = row_locate_col(rp, 
					TABLETSCHM_RGADDR_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &ign);

			result = row_col_compare(VARCHAR, Range_infor->rg_ip,
						STRLEN(Range_infor->rg_ip), 
						rg_addr, STRLEN(rg_addr));
			
			
			if (result == EQ)
			{
				rg_port = *(int *)row_locate_col(rp, 
						TABLETSCHM_RGPORT_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLETSCHM, &ign);

				result = (Range_infor->port == rg_port) ? EQ : GR;
			}
					
			if (result == EQ)
			{
				if (selwhere->use_idxmeta)
				{
					Assert(querytype == SELECTWHERE);

					int		tablet_id;

					tablet_scanctx->tabdir = rg_tab_dir;
					tablet_scanctx->tabid = tab_hdr->tab_id;
					tablet_scanctx->rminlen = 
							tab_hdr->tab_row_minlen;

					/* The id of tablet based on index. */
					tablet_id = *(int *)row_locate_col(rp, 
						TABLETSCHM_TABLETID_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLETSCHM, &ign);

					index_range_sstab_scan(tablet_scanctx,
							&(selwhere->idxmeta),
							tab_name, tab_name_len,
							tablet_id);

					goto next_tablet;
				}
							
				/* 
				** We got the tablet that locates at the current
				** ranger server.
				*/
				MEMSET(tab_tablet_dir, TABLE_NAME_MAX_LEN);
				MEMCPY(tab_tablet_dir, tab_dir, STRLEN(tab_dir));
				str1_to_str2(tab_tablet_dir, '/', tabletname);  
			
				
				/* 
				** Read the file tablet and get the information
				** of each row.
				*/
				OPEN(fd1, tab_tablet_dir, (O_RDONLY));

				if (fd1 < 0)
				{
				       traceprint("Tablet %s is not exist! \n", tab_tablet_dir);
				       goto exit;
				}

				READ(fd1, tablet_bp, SSTABLE_SIZE); 

				CLOSE(fd1);

				if (querytype == SELECTWHERE)
				{
					tablet_scanctx->tablet = tablet_bp;
					tablet_scanctx->keycolid = 
							tab_hdr->tab_key_colid;
					tablet_scanctx->rminlen = 
							tab_hdr->tab_row_minlen;
					tablet_scanctx->tabdir = rg_tab_dir;
					tablet_scanctx->tabid = tab_hdr->tab_id;

					/* 
					** Scan the tablet and get the sstabname
					** for the every row to be wanted. 
					*/
					if (!rg_tabletscan(tablet_scanctx))
					{
						goto exit;
					}
				}
				else if (querytype == SELECTRANGE)
				{
					char	*sstab;
					int 	sstabid;
					int	flag;
					

					flag = (left_expand)? RG_TABLET_1ST_SSTAB 
							: RG_TABLET_ANYONE_SSTAB;

					flag |= RG_TABLET_LEFT_BOUND;
					
					/* Check if left key is in this scope. */
					if (!rg_get_sstab_tablet(tablet_bp, key_left, 
							keycolen_left, &sstab, &namelen,
							&sstabid, flag))
					{
						goto exit;
					}

					
					char	sstab_left[TABLE_NAME_MAX_LEN];
					char	sstab_right[TABLE_NAME_MAX_LEN];
					TABINFO tabinfo;
					SINFO	psinfo;
					BLK_ROWINFO blk_rowinfo;
					
					
					MEMSET(sstab_left, TABLE_NAME_MAX_LEN);
					MEMSET(sstab_right, TABLE_NAME_MAX_LEN);
					MEMSET(&tabinfo,sizeof(TABINFO));
					MEMSET(&psinfo, sizeof(SINFO));

					/* TODO: it needs to re-get the length of fixed column. */
					MEMCPY(sstab_left, sstab, STRLEN(sstab));
					
					tabinfo.t_tabid = tab_hdr->tab_id;
					tabinfo.t_row_minlen = tab_hdr->tab_row_minlen;
					tabinfo.t_sstab_id = sstabid;
					tabinfo.t_sinfo = &psinfo;
					tabinfo.t_rowinfo = &blk_rowinfo;
					tabinfo.t_stat |= TAB_SRCH_RANGE;
					MEMSET(tabinfo.t_rowinfo, sizeof(BLK_ROWINFO));
					

					flag = (right_expand)? RG_TABLET_LAST_SSTAB 
							: RG_TABLET_ANYONE_SSTAB;

					flag |= RG_TABLET_RIGHT_BOUND;

					/* It must get the one right sstab. */
					if (!rg_get_sstab_tablet(tablet_bp, key_right, 
						keycolen_right, &sstab, &namelen, &sstabid,
						flag))
					{
						goto exit;
					}
					
					MEMCPY(sstab_right, sstab, STRLEN(sstab));				

					int data_cont = TRUE;
					if (!rg__selrangetab(&tabinfo, sstab_left,
							sstab_right, connfd,key_left, 
							keycolen_left, key_right,
							keycolen_right, left_expand,
							right_expand, tab_rg_dir, 
							&data_cont))
					{
						goto exit;
					}
					
					if (!data_cont)
					{
						goto finish;
					}

				}
				
			}
next_tablet:
			if (rightend)
			{
				goto finish;
			}		       
		}

		if (blk->bnextblkno != -1)
		{
			i = blk->bnextblkno;
		}
		else
		{
			break;
		}
	}

finish:
	
	rgsel_cont.status = DATA_EMPTY;

 	resp = conn_build_resp_byte(RPC_SUCCESS, sizeof(RANGE_QUERYCTX) - 
					BLOCKSIZE, (char *)&rgsel_cont);

	resp_size = conn_get_resp_size((RPCRESP *)resp);
	  
	tcp_put_data(connfd, resp, resp_size);			

	conn_destroy_resp_byte(resp);	

	char	resp_cli[8];
	MEMSET(resp_cli, 8);
	int n = conn_socket_read(connfd, resp_cli, 8);

	if (n != 8)
	{
		/* 
		** Here got the end of this session, so it should be a normal
		** case.
		*/
		rtn_stat = TRUE;
		
		traceprint("Socket read error 3.\n");
		goto exit;
	}
	
	rtn_stat = TRUE;

exit:
	conn_socket_close(connfd);

	conn_socket_close(listenfd);
	
	if (rtn_stat)
	{
		/* 
		** This case stands for the success for this session. The client
		** has been close the accept. 
		*/
		resp = conn_build_resp_byte(RPC_SKIP_SEND, 0, NULL);
	}
	else
	{
		/* 
		** Exption case need to be send to the client and the client
		** can accept this RPC and use it to make sure this session
		** is failed.
		*/
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	if (tablet_scanctx)
	{
		par_release_orandplan(tablet_scanctx->andplan);
		par_release_orandplan(tablet_scanctx->orplan);

		MEMFREEHEAP(tablet_scanctx);
	}

	if (tablet_schm_bp)
	{
		MEMFREEHEAP(tablet_schm_bp);
	}
	
	return resp;

}


/* Clone from rg_selwheretab(). */
char *
rg_selcounttab(TREE *command, SELWHERE *selwhere, TABLEHDR *tab_hdr, COLINFO *colinfo, int fd)
{
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	int		fd1;
	int		rtn_stat;	
	char		*resp;
	char		*rp;
	int		namelen;
	char		*rg_addr;
	int		rg_port;
	TABLET_SCANCTX	*tablet_scanctx;
	char		rg_tab_dir[TABLE_NAME_MAX_LEN];
	int		tabidx;
	int		querytype;
	int		rowcnt;		/* The # of row to be wanted. */
	int		sum_value;	/* For the SELECTSUM. */


	Assert(command);

	rtn_stat	= FALSE;
	tablet_scanctx 	= NULL;
	tab_name	= command->sym.command.tabname;
	tab_name_len 	= command->sym.command.tabname_len;
	querytype	= command->sym.command.querytype;
	rowcnt		= 0;
	sum_value	= 0;

	Assert((querytype == SELECTSUM) || (querytype == SELECTCOUNT));

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(rg_tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(rg_tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	if ((tabidx = rg_table_is_exist(rg_tab_dir)) == -1)
	{
		if (STAT(rg_tab_dir, &st) != 0)
		{
			traceprint("Table %s range data is not exist.\n", tab_name);
			goto exit;
		}

		tabidx = rg_table_regist(rg_tab_dir);
	}

	
	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", tab_name);
		goto exit;
	}

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		goto exit;
	}

	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table should have one tablet at least! \n");
		goto exit;
	}
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");
		


	char	*tablet_schm_bp = NULL;
	
	OPEN(fd1, tab_meta_dir, (O_RDONLY));

	if (fd1 < 0)
	{
	       printf("Table is not exist! \n");
	       goto exit;
	}

	tablet_schm_bp = (char *)MEMALLOCHEAP(SSTABLE_SIZE);
	MEMSET(tablet_schm_bp, SSTABLE_SIZE);

	READ(fd1, tablet_schm_bp, SSTABLE_SIZE); 

	CLOSE(fd1);
	
	BLOCK		*blk;
	int		i = 0, rowno;
	int		*offset;
	int		result;
	char		*tabletname;
	char		tab_tablet_dir[TABLE_NAME_MAX_LEN];
	int		ign;
	char		tablet_bp[SSTABLE_SIZE];	

	/* The tablet to be interested. */
	char		*wanted_tablet;
	int		wanted_tablet_len;

	/* Check if we got the first tablet. */
	int		leftbegin = FALSE;
	/* Check if we got the last tablet. */	
	int 		rightend = FALSE;

	/* For selectrange. */
		
	/* Initialization. */
	par_fill_colinfo(tab_hdr->tab_col, colinfo, command);

	tablet_scanctx = (TABLET_SCANCTX *)MEMALLOCHEAP(sizeof(TABLET_SCANCTX));
	MEMSET(tablet_scanctx, sizeof(TABLET_SCANCTX));

	tablet_scanctx->andplan = par_get_andplan(command);
	tablet_scanctx->orplan = par_get_orplan(command);
	tablet_scanctx->connfd = -1;
	tablet_scanctx->querytype = querytype;

	tablet_scanctx->sum_coloff = (querytype == SELECTSUM) ?
					command->left->sym.resdom.coloffset: 0;

	/* Working for the query. */
	while (TRUE)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);


		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			       rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;

			assert(*offset < blk->bfreeoff);

			tabletname = row_locate_col(rp, 
					TABLETSCHM_TABLETNAME_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);

			/* 
			** Note: use the original data (don't use the strlen()) 
			** from meta server because the tabletname string 
			** may be dirty by the net  connection, such as the
			** tail zero lost.
			*/
			if (!leftbegin)
			{
				wanted_tablet = selwhere->lefttabletname;
				wanted_tablet_len = selwhere->leftnamelen;
			}
			else
			{
				wanted_tablet = selwhere->righttabletname;
				wanted_tablet_len = selwhere->rightnamelen;
			}
			
			/* Check if it got the left or right tablet. */
			result = row_col_compare(VARCHAR, tabletname, 
						STRLEN(tabletname), 
						wanted_tablet,
						wanted_tablet_len);
			

			if ((result != EQ) && (leftbegin == FALSE))
			{
				continue;
			}
			
			if (result == EQ)
			{	
				if (leftbegin == TRUE)
				{
					/* 
					** Hit the right side of range, that's
					** to say, hit the last tablet. 
					*/
					rightend = TRUE;
				}
				else if (leftbegin == FALSE)
				{
					/*
					** Hit the left side of the range, 
					** that's to say, hit the fisrt tablet.
					*/
					leftbegin = TRUE;
				}
			}

			/* Check if this tablet locates at this range. */
			rg_addr = row_locate_col(rp, 
					TABLETSCHM_RGADDR_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &ign);

			result = row_col_compare(VARCHAR, Range_infor->rg_ip,
						STRLEN(Range_infor->rg_ip), 
						rg_addr, STRLEN(rg_addr));
			
			
			if (result == EQ)
			{
				rg_port = *(int *)row_locate_col(rp, 
						TABLETSCHM_RGPORT_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLETSCHM, &ign);

				result = (Range_infor->port == rg_port) ? EQ : GR;
			}
					
			if (result == EQ)
			{
				tablet_scanctx->keycolid = 
						tab_hdr->tab_key_colid;
				tablet_scanctx->rminlen = 
						tab_hdr->tab_row_minlen;
				tablet_scanctx->tabdir = rg_tab_dir;
				tablet_scanctx->tabid = tab_hdr->tab_id;
				tablet_scanctx->rowcnt = 0;
				tablet_scanctx->sum_colval = 0;
				
				if (selwhere->use_idxmeta)
				{
					int		tablet_id;

					/* The id of tablet based on index. */
					tablet_id = *(int *)row_locate_col(rp, 
						TABLETSCHM_TABLETID_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLETSCHM, &ign);

					index_range_sstab_scan(tablet_scanctx,
							&(selwhere->idxmeta),
							tab_name, tab_name_len,
							tablet_id);

					goto next_tablet;
				}
				
				/* 
				** We got the tablet that locates at the current
				** ranger server.
				*/
				MEMSET(tab_tablet_dir, TABLE_NAME_MAX_LEN);
				MEMCPY(tab_tablet_dir, tab_dir, STRLEN(tab_dir));
				str1_to_str2(tab_tablet_dir, '/', tabletname);  

				OPEN(fd1, tab_tablet_dir, (O_RDONLY));

				if (fd1 < 0)
				{
				       traceprint("Tablet %s is not exist! \n", tab_tablet_dir);
				       goto exit;
				}

				READ(fd1, tablet_bp, SSTABLE_SIZE); 

				CLOSE(fd1);
				
				tablet_scanctx->tablet = tablet_bp;
				
				/* 
				** Scan the tablet and get the sstabname
				** for the every row to be wanted. 
				*/
				if (!rg_count_data_tablet(tablet_scanctx))
				{
					goto exit;
				}

next_tablet:

				if (querytype == SELECTCOUNT)
				{
					rowcnt += tablet_scanctx->rowcnt;
				}
				else
				{
					sum_value += tablet_scanctx->sum_colval;
				}
								
			}

			if (rightend)
			{
				goto finish;
			}		       
		}

		if (blk->bnextblkno != -1)
		{
			i = blk->bnextblkno;
		}
		else
		{
			break;
		}
	}

finish:
	
	rtn_stat = TRUE;

exit:

	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, sizeof(int), 
				(querytype == SELECTCOUNT)? (char *)&rowcnt 
							: (char *)&sum_value);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	if (tablet_scanctx)
	{
		par_release_orandplan(tablet_scanctx->andplan);
		par_release_orandplan(tablet_scanctx->orplan);

		MEMFREEHEAP(tablet_scanctx);
	}

	if (tablet_schm_bp)
	{
		MEMFREEHEAP(tablet_schm_bp);
	}
	
	return resp;

}


/* Get the row for the OR and AND clause in the query plan. */
static int
rg_sstabscan(SSTAB_SCANCTX *scanctx)
{	
	BLOCK	*blk;
	BLOCK	*datablk;
	int     *offset;
	char    *rp;
	int	minrowlen;


	minrowlen= scanctx->rminlen;
	datablk= (BLOCK *)(scanctx->rgsel->data);	

	while (TRUE)
	{
		blk = (BLOCK *)(scanctx->sstab + scanctx->curblk * BLOCKSIZE);

		offset = ROW_OFFSET_PTR(blk);

		offset -= scanctx->currow;
			       
		for(; scanctx->currow < blk->bnextrno; (scanctx->currow)++, 
							offset--)
		{
			rp = (char *)blk + *offset;
			
                        if (ROW_IS_DELETED(rp))
                        {
                                continue;
                        }

			Assert(*offset < blk->bfreeoff);

			if (   par_process_orplan(scanctx->orplan, rp, minrowlen)
			    && par_process_andplan(scanctx->andplan, rp, 
			    				minrowlen))
			{
				if (!(scanctx->stat & SSTABSCAN_HIT_ROW))
				{
					scanctx->stat |= SSTABSCAN_HIT_ROW;
				}
				
				if (!(blk_appendrow(datablk, rp, 
						ROW_GET_LENGTH(rp, minrowlen))))
				{
					scanctx->stat |= SSTABSCAN_BLK_IS_FULL;
					goto finish;
				}
			}
		}

		if (blk->bnextblkno == -1)
		{
			scanctx->curblk = 0;
			scanctx->currow = 0;

			break;
		}

		(scanctx->curblk)++;
		scanctx->currow = 0;
	}

finish:	
	
	return TRUE;
}


/* Get the sstable name from the tablet file. */
static int
rg_get_sstab_tablet(char *tabletbp, char *key, int keylen, char **sstabname, 
				int *namelen, int *sstabid, int flag)
{
	BLOCK		*blk;
	BLOCK		*last_blk;
	int		rowno;
	int		*offset;
	char		*rp;
	int		i;
	char		*tabletkey;
	int		tabletkeyln;
	char		*lastrp;
	int		result;
	int		ign;
	

	lastrp = NULL;
	last_blk = NULL;
	i = 0;

	blk = (BLOCK *)tabletbp;
	
	if (blk->bfreeoff == BLKHEADERSIZE)
	{
		/* Error case 1. */
		return FALSE;
	}

	while (TRUE)
	{
		blk = (BLOCK *)(tabletbp + i * BLOCKSIZE);

			       
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			       rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;

			/* Skip the deleted row. */
			if (ROW_IS_DELETED(rp))
			{
				continue;
			}

			Assert(*offset < blk->bfreeoff);


			/* Get the first row of this tablet. */
			if (flag & RG_TABLET_1ST_SSTAB)
			{
				goto finish;
			}
			/* Get the last row of this tablet. */
			else if (flag & RG_TABLET_LAST_SSTAB)
			{
				break;;
			}		

			tabletkey = row_locate_col(rp, 
						TABLET_KEY_COLOFF_INROW,
						ROW_MINLEN_IN_TABLET, &tabletkeyln);

			result = row_col_compare(VARCHAR, tabletkey, tabletkeyln, key, keylen);

			if (result != LE)
			{
				/* Return for the case GR or EQ. */
				if (result == GR)
				{
					if (lastrp == NULL)
					{
						if (flag & RG_TABLET_RIGHT_BOUND)
						{
							/* 
							** Error case 2: if the 1st key 
							** is greater than the input key, 
							** it flags this tablet is not in this 
							** range. 
							*/
							return FALSE;
						}
						else
						{
							Assert(flag & RG_TABLET_LEFT_BOUND);

							/* Return the 1st sstable. */
							goto finish;
						}
					}

					rp = lastrp;
				}
				
				goto finish;
			}

			lastrp = rp;			
		}

		if (flag & RG_TABLET_LAST_SSTAB)
		{
			/* Hit the last block that has data. */
			if (blk->bfreeoff == BLKHEADERSIZE)
			{
				blk = last_blk;
				goto finish;
			}

			/* Save the previous block. */
			last_blk = blk;
		}
	
		if (blk->bnextblkno != -1)
		{
			/* Continue. */
			i = blk->bnextblkno;
		}
		else
		{
			break;
		}
		
	}

finish:

	if (flag & RG_TABLET_LAST_SSTAB)
	{
		/* 
		** Get the last row.
		** TODO: it should check if this row is deleted.
		*/
		Assert(blk != NULL);
		int *offtab = ROW_OFFSET_PTR(blk);
		rp = (char *)blk + offtab[-(blk->bnextrno - 1)];
	}

	*sstabname = row_locate_col(rp,
				TABLET_SSTABNAME_COLOFF_INROW, 
				ROW_MINLEN_IN_TABLET, namelen);

	*sstabid = *(int *)row_locate_col(rp, 
				TABLET_SSTABID_COLOFF_INROW,
				ROW_MINLEN_IN_TABLET, &ign);
	
	return TRUE;			
}


static int
rg_tabletscan(TABLET_SCANCTX *scanctx)
{
	
	BLOCK 		*blk;
	int		i = 0, rowno;
	int     	*offset;
	char    	*rp;
	char		*sstabname;
	int		sstabid;
	char		tab_sstab_dir[TABLE_NAME_MAX_LEN];
	int		namelen;
	int		ign;
	SSTAB_SCANCTX	sstab_scanctx;
	char		*resp;
	int		resp_size;
	char		resp_cli[8];

	RANGE_QUERYCTX	rgsel_cont;
	TABINFO		*tabinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		minrowlen;
	BUF		*bp;
	int		scan_result;
	BLOCK		*datablk;
				

	scan_result = FALSE;
	MEMSET(&sstab_scanctx, sizeof(SSTAB_SCANCTX));
	MEMSET(&rgsel_cont,sizeof(RANGE_QUERYCTX));
	MEMSET(&blk_rowinfo,sizeof(BLK_ROWINFO));
	
	sstab_scanctx.andplan	= scanctx->andplan;
	sstab_scanctx.orplan	= scanctx->orplan;
	sstab_scanctx.rminlen	= scanctx->rminlen;
	rgsel_cont.rowminlen	= scanctx->rminlen;
	sstab_scanctx.rgsel	= &rgsel_cont;
	sstab_scanctx.stat	= 0;
	bp = NULL;

	/* initialization of sending data buffer. */
	datablk= (BLOCK *)(sstab_scanctx.rgsel->data);
	datablk->bfreeoff = BLKHEADERSIZE;
	datablk->bnextrno = 0;
	datablk->bstat = 0;
	MEMSET(datablk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);
	

	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	

	tabinfo->t_rowinfo = &blk_rowinfo;

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);
	
	while (TRUE)
	{
		blk = (BLOCK *)(scanctx->tablet + i * BLOCKSIZE);

			       
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			       rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;

			/* Skip the deleted row. */
			if (ROW_IS_DELETED(rp))
			{
				continue;
			}

			assert(*offset < blk->bfreeoff);

			sstabname = row_locate_col(rp,
					TABLET_SSTABNAME_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLET, &namelen);

			sstabid = *(int *)row_locate_col(rp, 
						TABLET_SSTABID_COLOFF_INROW,
						ROW_MINLEN_IN_TABLET, &ign);

			
			MEMSET(tab_sstab_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(tab_sstab_dir, scanctx->tabdir, 
						STRLEN(scanctx->tabdir));
			str1_to_str2(tab_sstab_dir, '/', sstabname);	
			

			minrowlen = scanctx->rminlen;

			MEMSET(tabinfo->t_sinfo, sizeof(SINFO));
			MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));
			
			TABINFO_INIT(tabinfo, tab_sstab_dir, NULL, 0,
					tabinfo->t_sinfo, minrowlen, 
					0, scanctx->tabid, sstabid);
			
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0,
					scanctx->keycolid, VARCHAR, -1);
					
			bp = blk_getsstable(tabinfo);	

			sstab_scanctx.sstab = (char *)(bp->bblk);
			sstab_scanctx.curblk = 0;
			sstab_scanctx.currow = 0;

scan_cont:			
			rg_sstabscan(&sstab_scanctx);

			if (!(sstab_scanctx.stat & SSTABSCAN_BLK_IS_FULL))
			{
				/* Finding the next sstable to fill the sending block. */
				bufunkeep(bp->bsstab);
				bp = NULL;
				
				continue;
			}

			/* Fill the structure to sign the client. */
			rgsel_cont.cur_rowpos = 0;
			rgsel_cont.first_rowpos = 0;
			rgsel_cont.end_rowpos = BLK_GET_NEXT_ROWNO(datablk) - 1;
			rgsel_cont.status = DATA_CONT;

		 	resp = conn_build_resp_byte(RPC_SUCCESS, 
							sizeof(RANGE_QUERYCTX), 
							(char *)&rgsel_cont);
		
			resp_size = conn_get_resp_size((RPCRESP *)resp);

			/* Send the result to the client. */
			tcp_put_data(scanctx->connfd, resp, resp_size);			

			conn_destroy_resp_byte(resp);	

			
			/*
			** TODO: placeholder for the TCP/IP check.
			** Read the response from the client.
			*/
			MEMSET(resp_cli, 8);
			int n = conn_socket_read(scanctx->connfd, resp_cli, 8);

			if (n != 8)
			{
				traceprint("Socket read error 1.\n");
				goto exit;
			}

			Assert(sstab_scanctx.stat & SSTABSCAN_BLK_IS_FULL);

			/* 
			** It's a new round of sending data and continue to 
			** search and send data in the previous sstable. 
			*/
			sstab_scanctx.stat = 0;
			datablk->bfreeoff = BLKHEADERSIZE;
			datablk->bnextrno = 0;
			datablk->bstat = 0;
			MEMSET(datablk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);
			
			goto scan_cont;
		}

		if (blk->bnextblkno != -1)
		{
			i = blk->bnextblkno;
		}
		else
		{
			/* Sending the last block of data in one Tablet. */
			if (sstab_scanctx.stat & SSTABSCAN_HIT_ROW)
			{
				/* Fill the structure to sign the client. */
				rgsel_cont.cur_rowpos = 0;
				rgsel_cont.first_rowpos = 0;
				rgsel_cont.end_rowpos = BLK_GET_NEXT_ROWNO(datablk) - 1;
				rgsel_cont.status = DATA_CONT;

			 	resp = conn_build_resp_byte(RPC_SUCCESS, 
							sizeof(RANGE_QUERYCTX), 
							(char *)&rgsel_cont);
			
				resp_size = conn_get_resp_size((RPCRESP *)resp);

				/* Send the result to the client. */
				tcp_put_data(scanctx->connfd, resp, resp_size);			

				conn_destroy_resp_byte(resp);	

				
				/* TODO: placeholder for the TCP/IP check. */
				MEMSET(resp_cli, 8);
				int n = conn_socket_read(scanctx->connfd,
							resp_cli, 8);

				if (n != 8)
				{
					traceprint("Socket read error 2.\n");
					goto exit;
				}
			}
			
			break;
		}
	}

	scan_result = TRUE;
exit:
	if (bp)
	{
		bufunkeep(bp->bsstab);
	}
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();
			
	return scan_result;
}


/* Clone from the rg_tabletscan. */
static int
rg_crtidx_tablet(TABLET_SCANCTX *scanctx, IDXBLD *idxbld)
{
	
	BLOCK 		*blk;
	int		i = 0, rowno;
	int     	*offset;
	char    	*rp;
	char		*sstabname;
	int		sstabid;
	char		tab_sstab_dir[TABLE_NAME_MAX_LEN];
	int		namelen;
	int		ign;
	SSTAB_SCANCTX	sstab_scanctx;
	RANGE_QUERYCTX	rgsel_cont;
	TABINFO		*tabinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		minrowlen;
	BUF		*bp;
	int		scan_result;
	BLOCK		*datablk;
				

	scan_result = FALSE;
	MEMSET(&sstab_scanctx, sizeof(SSTAB_SCANCTX));
	MEMSET(&rgsel_cont,sizeof(RANGE_QUERYCTX));
	MEMSET(&blk_rowinfo,sizeof(BLK_ROWINFO));
	
	sstab_scanctx.andplan	= scanctx->andplan;
	sstab_scanctx.orplan	= scanctx->orplan;
	sstab_scanctx.rminlen	= scanctx->rminlen;
	rgsel_cont.rowminlen	= scanctx->rminlen;
	sstab_scanctx.rgsel	= &rgsel_cont;
	sstab_scanctx.stat	= 0;
	bp = NULL;

	/* initialization of sending data buffer. */
	datablk= (BLOCK *)(sstab_scanctx.rgsel->data);
	datablk->bfreeoff = BLKHEADERSIZE;
	datablk->bnextrno = 0;
	datablk->bstat = 0;
	MEMSET(datablk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);
	

	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	

	tabinfo->t_rowinfo = &blk_rowinfo;

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;



	tabinfo_push(tabinfo);
	
	while (TRUE)
	{
		blk = (BLOCK *)(scanctx->tablet + i * BLOCKSIZE);

			       
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			       rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;

			/* Skip the deleted row. */
			if (ROW_IS_DELETED(rp))
			{
				continue;
			}

			Assert(*offset < blk->bfreeoff);

			sstabname = row_locate_col(rp,
					TABLET_SSTABNAME_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLET, &namelen);

			sstabid = *(int *)row_locate_col(rp, 
						TABLET_SSTABID_COLOFF_INROW,
						ROW_MINLEN_IN_TABLET, &ign);

			
			MEMSET(tab_sstab_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(tab_sstab_dir, scanctx->tabdir, 
						STRLEN(scanctx->tabdir));
			str1_to_str2(tab_sstab_dir, '/', sstabname);	
			

			minrowlen = scanctx->rminlen;

			MEMSET(tabinfo->t_sinfo, sizeof(SINFO));
			MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));
			
			TABINFO_INIT(tabinfo, tab_sstab_dir, NULL, 0,
					tabinfo->t_sinfo, minrowlen, 
					0, scanctx->tabid, sstabid);
			
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0,
					scanctx->keycolid, VARCHAR, -1);

			/* Get the buffer for the data sstable. */
			bp = blk_getsstable(tabinfo);	

			sstab_scanctx.sstab = (char *)(bp->bblk);
			sstab_scanctx.curblk = 0;
			sstab_scanctx.currow = 0;
			sstab_scanctx.sstab_id = bp->bsstab->bsstabid;

	
			if (!rg_crtidx_sstab(&sstab_scanctx, idxbld))
			{
				goto exit;
			}

			bufunkeep(bp->bsstab);
			bp = NULL;

		
		}

		if (blk->bnextblkno != -1)
		{
			i = blk->bnextblkno;
		}
		else
		{		
			break;
		}
	}

	scan_result = TRUE;
exit:
	if (bp)
	{
		bufunkeep(bp->bsstab);
	}
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();
			
	return scan_result;
}



/* Clone from rg_sstabscan. */
static int
rg_crtidx_sstab(SSTAB_SCANCTX *scanctx, IDXBLD *idxbld)
{	
	LOCALTSS(tss);
	BLOCK		*blk;
	BLOCK		*datablk;
	int     	*offset;
	char    	*rp;
	int		minrowlen;
	char 		*index_rp;	/* Max row length. */
	int		index_rlen;
	int		col_idx;
	int		col_map;
	char		*keycol;
	int		keycolen;
	COLINFO		*col_info;
	int		coloffset;
	int		coltype;
	RID 		rid;
	

	minrowlen	= scanctx->rminlen;
	datablk		= (BLOCK *)(scanctx->rgsel->data);	
	col_idx		= 0;
	col_map		= idxbld->idx_meta->idx_col_map;
	col_info	= tss->tcol_info;

	/* Get the index of column based on index.*/
	INDEX_MAP_GET_COLUMN_NUM(col_map,col_idx);

	coloffset	= col_info[col_idx].col_offset;
	coltype		= col_info[col_idx].col_type;

	while (TRUE)
	{
		blk = (BLOCK *)(scanctx->sstab + scanctx->curblk * BLOCKSIZE);

		offset = ROW_OFFSET_PTR(blk);

		offset -= scanctx->currow;
			       
		for(; scanctx->currow < blk->bnextrno; (scanctx->currow)++, 
							offset--)
		{
			rp = (char *)blk + *offset;

			Assert(*offset < blk->bfreeoff);
			
			/* Column based on index. */
			keycol = row_locate_col(rp, coloffset, minrowlen, 
						&keycolen);

			if (TYPE_IS_FIXED(coltype))
			{
				keycolen = TYPE_GET_LEN(coltype);
			}
			
			/*
			** sizeof(ROWFMT) + 2 * sizeof(int) + keycolen + sizeof(RID) + 2 * COLOFFSETENTRYSIZE
			*/
			index_rlen = sizeof(ROWFMT) + 2 * sizeof(int) + keycolen
					+ sizeof(RID) + 2 * COLOFFSETENTRYSIZE;

			/* Build index row and insert into index sstable. */
			index_rp = MEMALLOCHEAP(index_rlen);

			Assert(index_rlen < BLOCKSIZE);

			rid.block_id = blk->bblkno;
			rid.roffset = *offset;
			rid.sstable_id = scanctx->sstab_id;
			
			index_bld_row(index_rp, index_rlen, &rid, keycol,
						keycolen, coltype);

			idxbld->idx_rp = index_rp;
			idxbld->idx_rlen = index_rlen;

			index_ins_row(idxbld);

			MEMFREEHEAP(index_rp);
		}

		if (blk->bnextblkno == -1)
		{
			scanctx->curblk = 0;
			scanctx->currow = 0;

			break;
		}

		(scanctx->curblk)++;
		scanctx->currow = 0;
	}

	return TRUE;
}


/* Clone from the rg_tabletscan(). */
static int
rg_count_data_tablet(TABLET_SCANCTX *scanctx)
{
	
	BLOCK 		*blk;
	int		i = 0, rowno;
	int     	*offset;
	char    	*rp;
	char		*sstabname;
	int		sstabid;
	char		tab_sstab_dir[TABLE_NAME_MAX_LEN];
	int		namelen;
	int		ign;
	SSTAB_SCANCTX	sstab_scanctx;
	RANGE_QUERYCTX	rgsel_cont;
	TABINFO		*tabinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		minrowlen;
	BUF		*bp;
	int		scan_result;
				

	scan_result = FALSE;
	MEMSET(&sstab_scanctx, sizeof(SSTAB_SCANCTX));
	MEMSET(&rgsel_cont,sizeof(RANGE_QUERYCTX));
	MEMSET(&blk_rowinfo,sizeof(BLK_ROWINFO));
	
	sstab_scanctx.querytype	= scanctx->querytype;
	sstab_scanctx.andplan	= scanctx->andplan;
	sstab_scanctx.orplan	= scanctx->orplan;
	sstab_scanctx.rminlen	= scanctx->rminlen;
	rgsel_cont.rowminlen	= scanctx->rminlen;
	sstab_scanctx.rgsel	= &rgsel_cont;
	sstab_scanctx.stat	= 0;
	sstab_scanctx.rowcnt	= 0;
	sstab_scanctx.sum_colval= 0;
	sstab_scanctx.sum_coloff= scanctx->sum_coloff;
	bp = NULL;

	/* Initialize the context of tabinfor. */
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);
	
	while (TRUE)
	{
		/* Skill to the next block of tablet. */
		blk = (BLOCK *)(scanctx->tablet + i * BLOCKSIZE);

		/* Do the processing row by row*/
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			       rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;

			/* Skip the deleted row. */
			if (ROW_IS_DELETED(rp))
			{
				continue;
			}

			Assert(*offset < blk->bfreeoff);

			/* Locate the information of sstab. */
			sstabname = row_locate_col(rp,
					TABLET_SSTABNAME_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLET, &namelen);

			sstabid = *(int *)row_locate_col(rp, 
						TABLET_SSTABID_COLOFF_INROW,
						ROW_MINLEN_IN_TABLET, &ign);

			
			MEMSET(tab_sstab_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(tab_sstab_dir, scanctx->tabdir, 
						STRLEN(scanctx->tabdir));
			str1_to_str2(tab_sstab_dir, '/', sstabname);	
			

			minrowlen = scanctx->rminlen;

			/* Initialize the searching context. */
			MEMSET(tabinfo->t_sinfo, sizeof(SINFO));
			MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));
			
			TABINFO_INIT(tabinfo, tab_sstab_dir, NULL, 0,
					tabinfo->t_sinfo, minrowlen, 
					0, scanctx->tabid, sstabid);
			
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0,
					scanctx->keycolid, VARCHAR, -1);
					
			bp = blk_getsstable(tabinfo);	

			sstab_scanctx.sstab = (char *)(bp->bblk);
			sstab_scanctx.curblk = 0;
			sstab_scanctx.currow = 0;

			/* Count the row in one sstable. */
			rg_count_data_sstable(&sstab_scanctx);

			/* Buffer unkept. */
			bufunkeep(bp->bsstab);
			bp = NULL;

		}

		if (blk->bnextblkno != -1)
		{
			i = blk->bnextblkno;
		}
		else
		{
			/* Sending the last block of data in one Tablet. */
			break;
		}
	}

	scan_result = TRUE;
	
	switch (scanctx->querytype)
	{
	    case SELECTCOUNT:
		scanctx->rowcnt += sstab_scanctx.rowcnt;
		break;

	    case SELECTSUM:
	    	scanctx->sum_colval += sstab_scanctx.sum_colval;
	    	break;

	    default:
	    	traceprint("No any counting.\n");
	    	break;
	}
	
	if (bp)
	{
		bufunkeep(bp->bsstab);
	}
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();
			
	return scan_result;
}



/* Clone from rg_sstabscan(). */
/* Get the row for the OR and AND clause in the query plan. */
static int
rg_count_data_sstable(SSTAB_SCANCTX *scanctx)
{	
	BLOCK	*blk;
	int     *offset;
	char    *rp;
	int	minrowlen;
	char	*colval;
	int	ign;


	minrowlen= scanctx->rminlen;

	while (TRUE)
	{
		/* Skill to the next block. */
		blk = (BLOCK *)(scanctx->sstab + scanctx->curblk * BLOCKSIZE);

		offset = ROW_OFFSET_PTR(blk);

		offset -= scanctx->currow;
			       
		for(; scanctx->currow < blk->bnextrno; (scanctx->currow)++, 
							offset--)
		{
			rp = (char *)blk + *offset;

			Assert(*offset < blk->bfreeoff);

			if (   par_process_orplan(scanctx->orplan, rp, minrowlen)
			    && par_process_andplan(scanctx->andplan, rp, 
			    				minrowlen))
			{
				switch (scanctx->querytype)
				{
				    case SELECTCOUNT:
				
					/* Count the row. */
					(scanctx->rowcnt)++;
					break;
					
				    case SELECTSUM:
				    	
				    	colval = row_locate_col(rp, scanctx->sum_coloff,
							minrowlen, &ign);
					
					scanctx->sum_colval += *(int *)colval;
				    	break;
					
				    default:
				    	traceprint("No any counting.\n");
				    	break;
				}
				
			}
		}

		if (blk->bnextblkno == -1)
		{
			scanctx->curblk = 0;
			scanctx->currow = 0;

			break;
		}

		(scanctx->curblk)++;
		scanctx->currow = 0;
	}

	return TRUE;
}


int
rg_get_meta(char *req_buf, INSMETA **ins_meta, SELRANGE **sel_rg, IDXMETA **idxmeta,
		SELWHERE **sel_where, TABLEHDR **tab_hdr, COLINFO **col_info)
{
	int	rtn_stat;

	
	if ((rtn_stat = conn_chk_reqmagic(req_buf)) == 0)
	{
		return rtn_stat;
	}

	if (rtn_stat & RPC_REQ_NORMAL_OP)
	{	
		*ins_meta = (INSMETA *)req_buf;
		req_buf += sizeof(INSMETA);

		*tab_hdr = (TABLEHDR *)req_buf;
		req_buf += sizeof(TABLEHDR);

		*col_info = (COLINFO *)req_buf;
	}

	if (rtn_stat & RPC_REQ_SELECTRANGE_OP)
	{	
		*sel_rg = (SELRANGE *)req_buf;
		req_buf += sizeof(SELRANGE);

		*tab_hdr = (TABLEHDR *)req_buf;
		req_buf += sizeof(TABLEHDR);

		*col_info = (COLINFO *)req_buf;
	}

	if (rtn_stat & RPC_REQ_SELECWHERE_OP)
	{
		*sel_where = (SELWHERE *)req_buf;
		req_buf += sizeof(SELWHERE);

		*tab_hdr = (TABLEHDR *)req_buf;
		req_buf += sizeof(TABLEHDR);

		*col_info = (COLINFO *)req_buf;
	}

	if (rtn_stat & RPC_REQ_CRT_IDX_OP)
	{
		*idxmeta = (IDXMETA *)req_buf;
		req_buf += sizeof(IDXMETA);

		*tab_hdr = (TABLEHDR *)req_buf;
		req_buf += sizeof(TABLEHDR);

		*col_info = (COLINFO *)req_buf;
	}

	if (rtn_stat & RPC_IDXROOT_SPLIT_OP)
	{
		*idxmeta = (IDXMETA *)req_buf;
	}
	
	return rtn_stat;	
}

static int
rg_fill_resd(TREE *command, COLINFO *colinfor, int totcol)
{
	COLINFO		*col_info;
	int		colid;

	while(command)
	{
		if (PAR_NODE_IS_RESDOM(command->type))
		{
			colid = command->sym.resdom.colid;
			col_info = meta_get_colinfor(colid, NULL, totcol, colinfor);

			Assert(col_info);

			command->sym.resdom.coloffset = col_info->col_offset;
			command->sym.resdom.coltype = col_info->col_type;
		}

		command = command->left;
	}

	return TRUE;

}

static int
rg_heartbeat(char * req_buf)
{
	if (!strncasecmp(RPC_MASTER2RG_HEARTBEAT, req_buf, 
			STRLEN(RPC_MASTER2RG_HEARTBEAT)))
	{
		//this is heart beat msg from meta server, just response.........
		return TRUE;
	}
	else
	{
		return FALSE;
	}

}

static int
rg_rsync(char * req_buf)
{
	if (!strncasecmp(RPC_MASTER2RG_NOTIFY, req_buf, 
			STRLEN(RPC_MASTER2RG_NOTIFY)))
	{
		//do ceph sync
		//to be fixed here!!
		return TRUE;
	}
	else
	{
		return FALSE;
	}

}


char *
rg_crtidx(TREE *command, IDXMETA *idxmeta, TABLEHDR *tab_hdr, COLINFO *colinfo, int fd)
{
	char		*tab_name;
	int		tab_name_len;
	char		*idx_name;
	int		idx_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		rg_tab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	char 		*resp;
	int		tabidx;
	LOGREC		*logrec;
	int		buf_spin;


	Assert(command);
	
	rtn_stat = FALSE;
	resp = NULL;
	logrec = NULL;
	buf_spin = FALSE;

	/* Index name*/
	idx_name = command->sym.command.tabname;
	idx_name_len = command->sym.command.tabname_len;

	/* Table name locate at the sub-command ON. */
	tab_name = (command->right)->sym.command.tabname;
	tab_name_len = (command->right)->sym.command.tabname_len;

	/* The full path of meta table. */
	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	/* The full path of ranger table. */
	MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(rg_tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
	str1_to_str2(rg_tab_dir, '/', tab_name);

	
	/* Check if table is exist. */
	if ((tabidx = rg_table_is_exist(rg_tab_dir)) == -1)
	{
		if (STAT(tab_dir, &st) != 0)
		{
			traceprint("Table %s is not exist.\n", tab_name);
			goto exit;
		}

		/* If hit the error, it has printed the information in caller. */
		tabidx = rg_table_regist(rg_tab_dir);
	}


	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", tab_name);
		goto exit;
	}

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		goto exit;
	}

	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table should have one tablet at least! \n");
		goto exit;
	}



	char	tab_meta_dir[TABLE_NAME_MAX_LEN];
	int	fd1;
	int	status;

	/* Build the full path for the tabletscheme file. */
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, rg_tab_dir, STRLEN(rg_tab_dir));
	str1_to_str2(tab_meta_dir, '/', idx_name);
	
	/* Make the directory to save the state of ranger servers. */
	if (STAT(tab_meta_dir, &st) == 0)
	{
		traceprint("Index %s on table %s is exist.\n", idx_name, tab_name);
		goto exit;
	}
	else
	{
		MKDIR(status, tab_meta_dir, 0755);
	}
	
	/* Build the full path for the tabletscheme file. */
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");
		

	/* Read the file tabletscheme. */
	char	*tablet_schm_bp = NULL;
	
	OPEN(fd1, tab_meta_dir, (O_RDONLY));

	if (fd1 < 0)
	{
	       printf("Table tabletscheme is not exist! \n");
	       goto exit;
	}

	tablet_schm_bp = (char *)MEMALLOCHEAP(SSTABLE_SIZE);
	MEMSET(tablet_schm_bp, SSTABLE_SIZE);

	READ(fd1, tablet_schm_bp, SSTABLE_SIZE); 

	CLOSE(fd1);
	
	BLOCK		*blk;
	int		i = 0, rowno;
	int		*offset;
	int		result;
	char		*tabletname;
	char		tab_tablet_dir[TABLE_NAME_MAX_LEN];
	int		ign;
	char		tablet_bp[SSTABLE_SIZE];	

	/* For selectrange. */
	char		*rp;
	char		*rg_addr;
	int		rg_port;
	int		namelen;
	TABLET_SCANCTX	*tablet_scanctx;
	IDXBLD		idxbld;

	P_SPINLOCK(BUF_SPIN);
	buf_spin = TRUE;
	
	tablet_scanctx = (TABLET_SCANCTX *)MEMALLOCHEAP(sizeof(TABLET_SCANCTX));
	MEMSET(tablet_scanctx, sizeof(TABLET_SCANCTX));

	/* Insert the begin log for this index creating. */
	

	/* Working for the query. */
	while (TRUE)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);


		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			       rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;

			assert(*offset < blk->bfreeoff);

			tabletname = row_locate_col(rp, 
					TABLETSCHM_TABLETNAME_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);

			

			/* Check if this tablet locates at this ranger server. */
			rg_addr = row_locate_col(rp, 
					TABLETSCHM_RGADDR_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &ign);

			result = row_col_compare(VARCHAR, Range_infor->rg_ip,
						STRLEN(Range_infor->rg_ip), 
						rg_addr, STRLEN(rg_addr));
			
			
			if (result == EQ)
			{
				rg_port = *(int *)row_locate_col(rp, 
						TABLETSCHM_RGPORT_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLETSCHM, &ign);

				result =  (Range_infor->port == rg_port) 
					? EQ : GR;
			}
					
			if (result == EQ)
			{
				/* 
				** We got the tablet that locates at the current
				** ranger server.
				*/
				MEMSET(tab_tablet_dir, TABLE_NAME_MAX_LEN);
				MEMCPY(tab_tablet_dir, tab_dir, STRLEN(tab_dir));
				str1_to_str2(tab_tablet_dir, '/', tabletname);	

				/* 
				** Read the file tablet and get the information
				** of each row.
				*/
				OPEN(fd1, tab_tablet_dir, (O_RDONLY));

				if (fd1 < 0)
				{
				       traceprint("Tablet %s is not exist! \n", tab_tablet_dir);
				       goto exit;
				}

				READ(fd1, tablet_bp, SSTABLE_SIZE); 

				CLOSE(fd1);

				tablet_scanctx->tablet = tablet_bp;
				tablet_scanctx->keycolid = 
						tab_hdr->tab_key_colid;
				tablet_scanctx->rminlen = 
						tab_hdr->tab_row_minlen;
				tablet_scanctx->tabdir = rg_tab_dir;
				tablet_scanctx->tabid = tab_hdr->tab_id;

				idxbld.idx_tab_name = rg_tab_dir;

				/* 
				** The root id of index on the tablet is same 
				** with the id of tablet. 
				*/
				idxbld.idx_root_sstab = *(int *)row_locate_col(rp, 
					TABLETSCHM_TABLETID_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);
				
				idxbld.idx_stat =  IDXBLD_FIRST_DATAROW_IN_TABLET
						 | IDXBLD_NOLOG;
				
				idxbld.idx_meta = idxmeta;
					
				rg_crtidx_tablet(tablet_scanctx, &idxbld);		
				
			}

		}

		if (blk->bnextblkno != -1)
		{
			i = blk->bnextblkno;
		}
		else
		{
			break;
		}
	}


	rtn_stat = TRUE;

exit:
	if (buf_spin)
	{
		V_SPINLOCK(BUF_SPIN);
	}
	
	if (tablet_scanctx != NULL)
	{
		MEMFREEHEAP(tablet_scanctx);
	}
	
	if (rtn_stat)
	{
		/* 
		** This case stands for the success for this session. The client
		** has been close the accept. 
		*/
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		/* 
		** Exption case need to be send to the client and the client
		** can accept this RPC and use it to make sure this session
		** is failed.
		*/
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}


	if (tablet_schm_bp)
	{
		MEMFREEHEAP(tablet_schm_bp);
	}
	
	return resp;
	
}


char *
rg_handler(char *req_buf, int fd)
{
	LOCALTSS(tss);
	TREE		*command;
	int		resp_buf_idx;
	int		resp_buf_size;
	char		*resp;
	INSMETA		*ins_meta;
	COLINFO 	*col_info;
	TABLEHDR	*tab_hdr;
	TABINFO		*tabinfo;
	int		req_op;
	BLK_ROWINFO	blk_rowinfo;
	SELRANGE	*sel_rg;
	SELWHERE	*sel_where;
	IDXMETA		*idxmeta;
	


	ins_meta	= NULL;
	sel_rg		= NULL;
	tab_hdr		= NULL;
	col_info	= NULL;
	resp 		= NULL;
	tss->rglogfile 	= Range_infor->rglogfiledir;
	tss->rgbackpfile= Range_infor->rgbackup;
	tss->rgstatefile= Range_infor->rgstatefile;
	
	if ((req_op = rg_get_meta(req_buf, &ins_meta, &sel_rg, &idxmeta,
				&sel_where, &tab_hdr, &col_info)) == 0)
	{
		return NULL;
	}

	/* Initialize the meta data for build RESDOM. */
	tss->tcol_info = col_info;
	tss->tmeta_hdr = ins_meta;
	tss->ttab_hdr = tab_hdr;
	
	/* 
	** For performance, We see the Drop Table as a special case to process it. 
	** Maybe there are still some better solution.
	*/
	if (req_op & RPC_REQ_DROPTAB_OP)
	{
		if (!parser_open(req_buf + RPC_MAGIC_MAX_LEN))
		{
			parser_close();
			tss->tstat |= TSS_PARSER_ERR;
			traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
			return NULL;
		}

		command = tss->tcmd_parser;

		Assert(command->sym.command.querytype == DROPTAB);

		resp = rg_droptab(command);

		goto finish;
	}
	else if (req_op & RPC_REQ_DROPIDX_OP)
	{
		if (!parser_open(req_buf + RPC_MAGIC_MAX_LEN))
		{
			parser_close();
			tss->tstat |= TSS_PARSER_ERR;
			traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
			return NULL;
		}

		command = tss->tcmd_parser;

		Assert(command->sym.command.querytype == DROPINDEX);

		resp = rg_dropidx(command);

		goto finish;
	}
	else if (req_op & RPC_REQ_SELECWHERE_OP)
	{
		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - SELECT WHERE/RANGE TABLE\n");
		}
		
		req_buf += sizeof(SELWHERE) + sizeof(TABLEHDR) + 
				tab_hdr->tab_col * sizeof(COLINFO);
		
		if (!parser_open(req_buf))
		{
			parser_close();
			tss->tstat |= TSS_PARSER_ERR;
			traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
			return NULL;
		}

		command = tss->tcmd_parser;

		if (   (command->sym.command.querytype == SELECTCOUNT)
		    || (command->sym.command.querytype == SELECTSUM))
		{
			resp = rg_selcounttab(command, sel_where, tab_hdr, col_info, fd);
		}
		else
		{
			Assert(   (command->sym.command.querytype == SELECTWHERE)
				||(command->sym.command.querytype == SELECTRANGE));

			resp = rg_selwheretab(command, sel_where, tab_hdr, col_info, fd);
		}

		
		goto finish;
	}
	/* Rebalancer case. */
	else if (req_op & RPC_REQ_REBALANCE_OP)
	{
		return rg_rebalancer((REBALANCE_DATA *)(req_buf - RPC_MAGIC_MAX_LEN));
	}
	else if (req_op & RPC_REQ_RECOVERY_RG_OP)
	{
		req_buf += RPC_MAGIC_MAX_LEN;
		return rg_recovery((char *)req_buf, *(int *)(req_buf + RANGE_ADDR_MAX_LEN));
	}
	/* process with heart beat */
	else if (req_op & RPC_REQ_M2RHEARTBEAT_OP)
	{
		traceprint("\n$$$$$$ rg recv heart beat. \n");
	
		Assert(rg_heartbeat(req_buf));
		/* 
		** to be fixed here
		** maybe more info will be added here in the future
		*/
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);

		traceprint("\n$$$$$$ rg sent heart beat. \n");

		return resp;
	}
	/* process with rsync notify */
	else if (req_op & RPC_REQ_M2RNOTIFY_OP)
	{
		Assert(rg_rsync(req_buf));

		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);

		return resp;
	}
	/* process with map reduce req */
	else if (req_op & RPC_REQ_MAPRED_GET_DATAPORT_OP)
	{
		return rg_mapred_setup(req_buf);
	}
	else if (req_op & RPC_REQ_CRT_IDX_OP)
	{
		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - CREATE INDEX\n");
		}
		
		req_buf += sizeof(IDXMETA) + sizeof(TABLEHDR) + 
				tab_hdr->tab_col * sizeof(COLINFO);
		
		if (!parser_open(req_buf))
		{
			parser_close();
			tss->tstat |= TSS_PARSER_ERR;
			traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
			return NULL;
		}

		command = tss->tcmd_parser;

		resp = rg_crtidx(command, idxmeta, tab_hdr, col_info, fd);
		
		goto finish;
	}
	else if (req_op & RPC_IDXROOT_SPLIT_OP)
	{
		tss->topid |= TSS_OP_IDXROOT_SPLIT;
		
		req_buf += sizeof(INSMETA);

		resp = rg_idxroot_split((IDX_ROOT_SPLIT *)req_buf);

		goto finish;
	}
	
	volatile struct
	{
		TABINFO	*tabinfo;
	} copy;

	copy.tabinfo = NULL;
	tabinfo = NULL;
	
	if(ex_handle(EX_ANY, yxue_handler))
	{
		tabinfo = copy.tabinfo;
		
		goto close;
	}

	if (req_op & RPC_REQ_SELECTRANGE_OP)
	{
		ins_meta = &(sel_rg->left_range);
		
		req_buf += sizeof(SELRANGE);
	}
	else
	{	
		req_buf += sizeof(INSMETA) + sizeof(TABLEHDR) + 
				ins_meta->col_num * sizeof(COLINFO);
	}

	if (!parser_open(req_buf))
	{
		parser_close();
		tss->tstat |= TSS_PARSER_ERR;
		traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
		return NULL;
	}

	command = tss->tcmd_parser;
	resp_buf_idx = 0;
	resp_buf_size = 0;

	copy.tabinfo= tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;
	tabinfo->t_colinfo = col_info;
	tabinfo->t_insmeta = ins_meta;
	tabinfo->t_selrg = sel_rg;

	tabinfo->t_has_index = tab_hdr->has_index;
	tabinfo->t_index_ts = tab_hdr->index_ts;		
	tabinfo->t_key_colid = tab_hdr->tab_key_colid;
	tabinfo->t_key_coltype = tab_hdr->tab_key_coltype;
	tabinfo->t_key_coloff = tab_hdr->tab_key_coloff;
	tabinfo->t_row_minlen = tab_hdr->tab_row_minlen;
	tabinfo->t_tabid = tab_hdr->tab_id;
	tabinfo->t_tablet_id = (ins_meta) ? ins_meta->tabletid : -1;

	tabinfo->t_sstab_id = ins_meta->sstab_id;
	tabinfo->t_sstab_name = ins_meta->sstab_name;

	tabinfo->t_tab_name = command->sym.command.tabname;
	tabinfo->t_tab_namelen = command->sym.command.tabname_len;
//	MEMCPY(tabinfo->t_sstab_name, ins_meta->sstab_name, STRLEN(ins_meta->sstab_name));

	tabinfo_push(tabinfo);

	switch(command->sym.command.querytype)
	{
	    case TABCREAT:

		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - CREATING TABLE\n");
		}
		
		break;

	    case INSERT:

		resp = rg_instab(command, tabinfo);

		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - INSERTING TABLE\n");
		}
		
	    	break;

	    case CRTINDEX:
	    	break;

	    case SELECT:
    		tabinfo->t_stat |= TAB_SRCH_DATA;
		resp = rg_seldeltab(command, tabinfo);

		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - SELECTING TABLE\n");
		}
		
	    	break;

	    case DELETE:
	    	tabinfo->t_stat |= TAB_DEL_DATA;
		resp = rg_seldeltab(command, tabinfo);

		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - DELETE TABLE\n");
		}
		
	    	break;

	    case SELECTRANGE:

		resp = rg_selrangetab(command, tabinfo, fd);

		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - SELECTRANGE TABLE\n");
		}
	    	break;
		
	    case DROPTAB:
	    	Assert(0);
	    	break;
	    case REBALANCE:
	    	break;

	    default:
	    	break;
	}


	session_close(tabinfo);

close:

	tabinfo_pop();
	
	if (tabinfo!= NULL)
	{
		MEMFREEHEAP(tabinfo->t_sinfo);

		if (tabinfo->t_insrg)
		{
			Assert(tabinfo->t_stat & TAB_SSTAB_SPLIT);

			if (tabinfo->t_insrg->new_sstab_key)
			{
				MEMFREEHEAP(tabinfo->t_insrg->new_sstab_key);
			}

			if (tabinfo->t_insrg->old_sstab_key)
			{
				MEMFREEHEAP(tabinfo->t_insrg->old_sstab_key);
			}

			MEMFREEHEAP(tabinfo->t_insrg);
		}
		
		MEMFREEHEAP(tabinfo);
//		tss->ttabinfo = NULL;
	}

finish:	
	parser_close();

	return resp;

}

static int
rg_crt_rg_insdel_logfile(char *rgip, int rgport)
{
	char	rglogfile[256];
	char	rgname[64];
	LOGFILE	*logfile;
	int	status;
	int	fd;


	MEMSET(rglogfile, 256);
	MEMCPY(rglogfile, LOG_FILE_DIR, STRLEN(LOG_FILE_DIR));

	MEMSET(rgname, 64);
	sprintf(rgname, "%s%d", rgip, rgport);

	str1_to_str2(rglogfile, '/', rgname);
	

	OPEN(fd, rglogfile, (O_CREAT|O_WRONLY|O_TRUNC));

	logfile = (LOGFILE *)MEMALLOCHEAP(sizeof(LOGFILE));
	MEMSET(logfile,sizeof(LOGFILE));

	logfile->magic[0] = 'm';
	logfile->magic[1] = 'a';
	logfile->magic[2] = 'x';
	logfile->magic[3] = 't';
	logfile->magic[4] = 'a';
	logfile->magic[5] = 'b';
	logfile->magic[6] = 'l';
	logfile->magic[7] = 'e';
	logfile->magic[8] = 'l';
	logfile->magic[9] = 'o';
	logfile->magic[10] = 'g';	

	WRITE(fd, logfile, sizeof(LOGFILE));

	CLOSE(fd);	


	MEMSET(rglogfile, 256);
	MEMCPY(rglogfile, BACKUP_DIR, STRLEN(BACKUP_DIR));

	str1_to_str2(rglogfile, '/', rgname);

	if (STAT(rglogfile, &st) != 0)
	{
		MKDIR(status, rglogfile, 0755);
	}

	return status;
}


void
rg_setup(char *conf_path)
{
	int	status;
	char	port[32];
	int	rg_port;
	char	metaport[32];
	int	fd;
	
	sstab_split_cnt = 0;
	sstabsplit_idx_upd_cnt = 0;

	Range_infor = malloc(sizeof(RANGEINFO));
	memset(Range_infor, 0, sizeof(RANGEINFO));

	Range_infor->rg_meta_sysindex = malloc(sizeof(META_SYSINDEX));
	memset(Range_infor->rg_meta_sysindex, 0, sizeof(META_SYSINDEX));

	Rg_loginfo = malloc(sizeof(RG_LOGINFO));
	memset(Rg_loginfo, 0, sizeof(RG_LOGINFO));
	
	MEMCPY(Range_infor->conf_path, conf_path, STRLEN(conf_path));

	MEMSET(port, 32);
	MEMSET(metaport, 32);

	conf_get_value_by_key(port, conf_path, CONF_RG_PORT);
	conf_get_value_by_key(Range_infor->rg_ip, conf_path, CONF_RG_IP);

	/* Get the IP and Port of metaserver. */
	conf_get_value_by_key(Range_infor->rg_meta_ip, conf_path, CONF_META_IP);
	conf_get_value_by_key(metaport, conf_path, CONF_META_PORT);
	Range_infor->rg_meta_port = m_atoi(metaport, STRLEN(metaport));

	conf_get_value_by_key(metaport, conf_path, CONF_BIGDATA_PORT);
	Range_infor->bigdataport = m_atoi(metaport, STRLEN(metaport));

	rg_port = m_atoi(port, STRLEN(port));
	if(rg_port != INDEFINITE)
	{
		Range_infor->port = rg_port;
	}
	else
	{
		Range_infor->port = RANGE_DEFAULT_PORT;
	}

#ifdef MT_KFS_BACKEND
	MEMSET(Kfsserver, 32);
	conf_get_value_by_key(Kfsserver, conf_path, CONF_KFS_IP);
	conf_get_value_by_key(port, conf_path, CONF_KFS_PORT);

	Kfsport = m_atoi(port, STRLEN(port));
#endif
	if (STAT(MT_RANGE_TABLE, &st) != 0)
	{
		MKDIR(status, MT_RANGE_TABLE, 0755);
	}

	/* Make the directory to save the state of ranger servers. */
	if (STAT(MT_RANGE_STATE, &st) != 0)
	{
		MKDIR(status, MT_RANGE_STATE, 0755);
	}

	rg_regist();

	char	rgname[64];

	/* Build the array for the range logging directory. */
	MEMSET(Range_infor->rglogfiledir, 256);
	MEMCPY(Range_infor->rglogfiledir, LOG_FILE_DIR, STRLEN(LOG_FILE_DIR));

	MEMSET(rgname, 64);
	sprintf(rgname, "%s%d", Range_infor->rg_ip, Range_infor->port);

	str1_to_str2(Range_infor->rglogfiledir, '/', rgname);

	str1_to_str2(Range_infor->rglogfiledir, '/', "log");

	if (!(STAT(Range_infor->rglogfiledir, &st) == 0))
	{
		traceprint("Log file %s is not exist.\n", Range_infor->rglogfiledir);
		return;
	}

	log_get_latest_rglogfile(Rg_loginfo->logdir, Range_infor->rg_ip,
					Range_infor->port);

	if (strcmp(Range_infor->rglogfiledir, Rg_loginfo->logdir) == 0)
	{
		sprintf(Rg_loginfo->logdir, "%s0", Rg_loginfo->logdir);

		/* Long open. */
		OPEN(fd, Rg_loginfo->logdir, (O_CREAT | O_APPEND | O_RDWR |O_TRUNC));
//		CLOSE(fd);
		Rg_loginfo->logfd = fd;
		Rg_loginfo->logoffset = 0;	
	}
	else
	{
		OPEN(fd, Rg_loginfo->logdir, (O_APPEND | O_RDWR));
//		CLOSE(fd);
		Rg_loginfo->logfd = fd;
		Rg_loginfo->logoffset = LSEEK(fd, 0, SEEK_END);		
	}

	int idxpos = str1nstr(Rg_loginfo->logdir, Range_infor->rglogfiledir, 
					STRLEN(Range_infor->rglogfiledir));

	Range_infor->rginsdellognum = m_atoi(Rg_loginfo->logdir + idxpos, 
					STRLEN(Rg_loginfo->logdir) - idxpos);

	/* Build the array for the ranger backup directory. */
	MEMSET(Range_infor->rgbackup, 256);
	MEMCPY(Range_infor->rgbackup, BACKUP_DIR, STRLEN(BACKUP_DIR));

	str1_to_str2(Range_infor->rgbackup, '/', rgname);

	if (STAT(Range_infor->rgbackup, &st) != 0)
	{
		traceprint("Backup file %s is not exist.\n", Range_infor->rgbackup);
		return;
	}

	/* Build the array for the range state directory. */
	if (!ri_get_rgstate(Range_infor->rgstatefile,Range_infor->rg_ip, 
				Range_infor->port))
	{
		return;
	}

	if (STAT(MT_META_INDEX, &st) != 0)
	{
		traceprint("Index directory (%s) is not exist!\n", MT_META_INDEX);
	}
	else
	{
		/* Load the index meta information into the ranger context. */
		meta_load_sysindex((char *)Range_infor->rg_meta_sysindex);
	}

	ca_setup_pool();	

	return;
}


void
rg_boot()
{
	startup(Range_infor->port, TSS_OP_RANGESERVER, rg_handler);
}

/* Regist its ranger information to the metaserver. */
static void
rg_regist()
{
	int	sockfd;
	RPCRESP	*resp;
	char	send_buf[2 * RPC_MAGIC_MAX_LEN + RANGE_ADDR_MAX_LEN + RANGE_PORT_MAX_LEN];
	
	
	sockfd = conn_open(Range_infor->rg_meta_ip, Range_infor->rg_meta_port);

	/* REQUEST magic number and RG2MASTER magic number. */
	MEMSET(send_buf, 2 * RPC_MAGIC_MAX_LEN + RANGE_ADDR_MAX_LEN + RANGE_PORT_MAX_LEN);

	int idx = 0;
	PUT_TO_BUFFER(send_buf, idx, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_buf, idx, RPC_RG2MASTER_REPORT, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_buf, idx, Range_infor->rg_ip, RANGE_ADDR_MAX_LEN);
	PUT_TO_BUFFER(send_buf, idx, &(Range_infor->port), RANGE_PORT_MAX_LEN);

	Assert(idx == (2 * RPC_MAGIC_MAX_LEN + RANGE_ADDR_MAX_LEN + RANGE_PORT_MAX_LEN));
	
	tcp_put_data(sockfd, send_buf, idx);

	resp = conn_recv_resp(sockfd);

	if ((resp == NULL) || (resp->status_code != RPC_SUCCESS))
	{
		traceprint("\n ERROR \n");
	}
	
	conn_close(sockfd, NULL, resp);
}

int
rg_rebalan_process_sender(REBALANCE_DATA * rbd, char *rg_addr, int port)
{
	int		sockfd;
	RPCRESP		*resp;
	int		rtn_stat;
	int		status;


	rtn_stat = TRUE;
	
	sockfd = conn_open(rg_addr, port);

	status = tcp_put_data(sockfd, (char *)rbd, sizeof(REBALANCE_DATA));

	Assert (status == sizeof(REBALANCE_DATA));

	resp = conn_recv_resp(sockfd);

	if ((resp == NULL) || (resp->status_code != RPC_SUCCESS))
	{
		rtn_stat = FALSE;
		traceprint("\n ERROR \n");
	}
	
	conn_close(sockfd, NULL, resp);

	return rtn_stat;

}


static char *
rg_rebalancer(REBALANCE_DATA * rbd)
{
	int		rtn_stat;
	char		*resp;	
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		status;
	int		namelen;
	char		tab_sstab_dir[TABLE_NAME_MAX_LEN];
	int		fd1;
	

	rtn_stat = FALSE;
	
	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', rbd->rbd_tabname);

	if (STAT(tab_dir, &st) != 0)
	{
		MKDIR(status, tab_dir, 0755);

		if (status < 0)
		{
			goto exit;
		}
	}
	

	if (rbd->rbd_opid == RBD_FILE_SENDER)
	{
		REBALANCE_DATA	*rbd_sstab;
		
		rbd_sstab = (REBALANCE_DATA *)MEMALLOCHEAP(sizeof(REBALANCE_DATA));
		MEMSET(rbd_sstab, sizeof(REBALANCE_DATA));

		MEMCPY(rbd_sstab->rbd_tabname, rbd->rbd_tabname, 
					STRLEN(rbd->rbd_tabname));
		
		BLOCK *blk;

		int i = 0, rowno;
		int	*offset;
		char 	*rp;
		char	*sstabname;
				
		while (TRUE)
		{
			blk = (BLOCK *)(rbd->rbd_data + i * BLOCKSIZE);

			
			for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
					rowno < blk->bnextrno; rowno++, offset--)
			{
				rp = (char *)blk + *offset;
			
				Assert(*offset < blk->bfreeoff);
			
				sstabname = row_locate_col(rp, 
						TABLET_SSTABNAME_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLET, &namelen);

				
				MEMSET(tab_sstab_dir, TABLE_NAME_MAX_LEN);
				MEMCPY(tab_sstab_dir, tab_dir, STRLEN(tab_dir));
				str1_to_str2(tab_sstab_dir, '/', sstabname);	
		
				OPEN(fd1, tab_sstab_dir, (O_RDONLY));
		
				if (fd1 < 0)
				{
					traceprint("Table is not exist! \n");
					goto exit;
				}
		
				READ(fd1, rbd_sstab->rbd_data, SSTABLE_SIZE); 
		
				MEMCPY(rbd_sstab->rbd_magic, RPC_RBD_MAGIC, 
						RPC_MAGIC_MAX_LEN);
				MEMCPY(rbd_sstab->rbd_magic_back, 
						RPC_RBD_MAGIC, RPC_MAGIC_MAX_LEN);
				
				rbd_sstab->rbd_opid = RBD_FILE_RECVER;

				MEMSET(rbd_sstab->rbd_sstabname,
						TABLE_NAME_MAX_LEN);
				MEMCPY(rbd_sstab->rbd_sstabname, sstabname, 
						STRLEN(sstabname));
		
				rtn_stat = rg_rebalan_process_sender(rbd_sstab, 
							rbd->rbd_min_tablet_rg,
							rbd->rbd_min_tablet_rgport);
				
				Assert(rtn_stat == TRUE);


				if (rtn_stat == TRUE)
				{
#ifdef MT_KFS_BACKEND
					RMFILE(status, tab_sstab_dir);
					if(!status)
#else
					char	cmd_str[TABLE_NAME_MAX_LEN];
					MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
	
					sprintf(cmd_str, "rm -rf %s", tab_sstab_dir);
	
					if (!system(cmd_str))
#endif
					{
						rtn_stat = TRUE;
					}
				}
				
				CLOSE(fd1);
					
				
			}

			i++;

			if (i > (BLK_CNT_IN_SSTABLE - 1))
			{
				break;
			}
			
		
		}

		MEMFREEHEAP(rbd_sstab);

		rtn_stat = TRUE;

	}
	else if (rbd->rbd_opid == RBD_FILE_RECVER)
	{
		MEMSET(tab_sstab_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_sstab_dir, tab_dir, STRLEN(tab_dir));
		str1_to_str2(tab_sstab_dir, '/', rbd->rbd_sstabname);	

		Assert(STAT(tab_sstab_dir, &st) != 0);

		OPEN(fd1, tab_sstab_dir, (O_CREAT|O_WRONLY|O_TRUNC));
		
		if (fd1 < 0)
		{
			traceprint("Table is not exist! \n");
			goto exit;
		}

		WRITE(fd1, rbd->rbd_data, SSTABLE_SIZE); 

		CLOSE(fd1);

		rtn_stat = TRUE;
	}
	else
	{
		rtn_stat = FALSE;
	}

exit:
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;

}

/* We should maintaince the log to recovery the exception during the index root split. */
static char *
rg_idxroot_split(IDX_ROOT_SPLIT *idx_root_split)
{
	char		idx_src_root_name[TABLE_NAME_MAX_LEN];
	char		idx_dest_root_name[TABLE_NAME_MAX_LEN];
	char		idx_src_root_dir[TABLE_NAME_MAX_LEN];
	char		idx_dest_root_dir[TABLE_NAME_MAX_LEN];
	char		tabname[TABLE_NAME_MAX_LEN];
	int		meta_num;
	IDXMETA		*idx_meta;
	META_SYSINDEX	*meta_sysidx;
	TABINFO		psrc_tabinfo;
	TABINFO		*src_tabinfo;
	SINFO		psrc_sinfo;
	TABINFO		pdest_tabinfo;
	TABINFO		*dest_tabinfo;
	SINFO		pdest_sinfo;
	BUF		*srcbp;
	BUF		*destbp;
	BLOCK		*srcblk;
	BLOCK		*destblk;
	int		rtn_stat;
	char		*resp;
	int		split_num;
	IDXBLD		idxbld;

	
	rtn_stat	= TRUE;
	split_num	= 0;
	meta_sysidx	= Range_infor->rg_meta_sysindex;
	idx_meta	= meta_sysidx->idx_meta;
	destblk		= NULL;

	MEMSET(tabname, TABLE_NAME_MAX_LEN);
	MEMCPY(tabname, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	str1_to_str2(tabname, '/', idx_root_split->idx_tabname);

	
	for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
	{
		if (idx_root_split->idx_tabid == idx_meta->idx_tabid)
		{
			MEMSET(&idxbld, sizeof(IDXBLD));

			idxbld.idx_meta = idx_meta;
			idxbld.idx_stat = IDXBLD_NOLOG;
			idxbld.idx_tab_name = tabname;

			index_bld_root_dir(idx_src_root_dir, tabname,
						idx_meta->idxname, 
						idx_root_split->idx_srcroot_id);
			
			index_bld_root_name(idx_src_root_name, tabname,
						idx_meta->idxname, 
						idx_root_split->idx_srcroot_id, 
						FALSE);

			
			MEMSET(&psrc_tabinfo,sizeof(TABINFO));
			MEMSET(&psrc_sinfo, sizeof(SINFO));
		
			src_tabinfo = &psrc_tabinfo;
			src_tabinfo->t_sinfo = &psrc_sinfo;			

			/* Get the src index root. */
			TABINFO_INIT(src_tabinfo, idx_src_root_name, 
						NULL, 0, 
						src_tabinfo->t_sinfo, -1, 0,
						idx_meta->idx_id,
						idx_root_split->idx_srcroot_id);

			/* Don't need the key if we just get the sstable. */
			SRCH_INFO_INIT(src_tabinfo->t_sinfo, NULL, 0, 1,
								VARCHAR, -1);

			srcbp = blk_getsstable(src_tabinfo);

					
			srcblk = srcbp->bsstab->bblk;

			index_bld_root_dir(idx_dest_root_dir, tabname,
						idx_meta->idxname, 
						idx_root_split->idx_destroot_id);
			
			index_bld_root_name(idx_dest_root_name, tabname,
						idx_meta->idxname, 
						idx_root_split->idx_destroot_id,
						FALSE);


			MEMSET(&pdest_tabinfo,sizeof(TABINFO));
			MEMSET(&pdest_sinfo, sizeof(SINFO));
		
			dest_tabinfo = &pdest_tabinfo;
			dest_tabinfo->t_sinfo = &pdest_sinfo;
			
			/* Get the dest index root. */
			TABINFO_INIT(dest_tabinfo, idx_dest_root_name,
						NULL, 0, 
						dest_tabinfo->t_sinfo, -1, 0,
						idx_meta->idx_id,
						idx_root_split->idx_destroot_id);

			/* Don't need the key if we just get the sstable. */
			SRCH_INFO_INIT(dest_tabinfo->t_sinfo, NULL, 0,	1,
								VARCHAR, -1);

			destbp = blk_getsstable(dest_tabinfo);

			destblk = destbp->bsstab->bblk;


			Assert(   (destblk->bstat & BLK_INDEX_ROOT) 
			       && (destblk->bstat & BLK_CRT_EMPTY));

			destblk->bstat &= ~BLK_CRT_EMPTY;

			idxbld.idx_stat |=  IDXBLD_IDXROOT_SPLIT
					 | IDXBLD_FIRST_DATAROW_IN_TABLET;

			index_root_move(&idxbld, srcblk, destblk,
					idx_meta->idx_id,
					idx_src_root_dir,
					idx_dest_root_dir,
					idx_root_split->idx_destroot_id);

			
		
			BLOCK	*metablk;
			
			metablk = destblk + (BLK_CNT_IN_SSTABLE / 2 - 1);
	
			while (metablk->bfreeoff > BLKHEADERSIZE)
			{
				metablk->bfreeoff = BLKHEADERSIZE;
				metablk->bnextrno = 0;
				metablk->bstat = 0;
	
				/* 
				** Just for testing to have a clear overview look, in production, 
				** we can omit it. 
				*/
				MEMSET(metablk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);
	
				if(metablk->bnextblkno == -1)
				{
					break;
				}
	
				metablk++;
			}
				
			
			bufunkeep(srcbp);
			bufunkeep(destbp);
			
		}

		idx_meta++;
	}	

	
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;
}


static char *
rg_recovery(char *rgip, int rgport)
{
	LOCALTSS(tss);
	int		rtn_stat;
	char		*resp;	
		

	rtn_stat = FALSE;
	tss->tstat |= TSS_OP_RECOVERY;

	rtn_stat = log_recov_rg(rgip, rgport);

	tss->tstat &= ~TSS_OP_RECOVERY;
	
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;

}

static char *
rg_check_sstab_by_tablet(CHECKTABLE_DATA *chkdata)
{
	int		rtn_stat;
	char		*resp;	
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		status;
	int		namelen;
	char		tab_sstab_dir[TABLE_NAME_MAX_LEN];
	

	rtn_stat = FALSE;
	
	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', chkdata->chktab_tabname);

	if (STAT(tab_dir, &st) != 0)
	{
		MKDIR(status, tab_dir, 0755);

		if (status < 0)
		{
			goto exit;
		}
	}

		
	BLOCK *blk;

	int	i = 0, rowno;
	int	*offset;
	char 	*rp;
	char	*sstabname;
	int	sstabid;
	char	*sstab_bp;

	int	ign;
	
	while (TRUE)
	{
		blk = (BLOCK *)(chkdata->chktab_data + i * BLOCKSIZE);

		
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
				rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;
		
			Assert(*offset < blk->bfreeoff);
		
			sstabname = row_locate_col(rp,
						TABLET_SSTABNAME_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLET, &namelen);

			sstabid = *(int *)row_locate_col(rp, 
						TABLET_SSTABID_COLOFF_INROW,
						ROW_MINLEN_IN_TABLET, &ign);

			
			MEMSET(tab_sstab_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(tab_sstab_dir, tab_dir, STRLEN(tab_dir));
			str1_to_str2(tab_sstab_dir, '/', sstabname);	

			TABINFO		*tabinfo;
			int		minrowlen;
			BLK_ROWINFO	blk_rowinfo;
			BUF		*bp;
			
			tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
			MEMSET(tabinfo, sizeof(TABINFO));
			tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
			MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

			tabinfo->t_rowinfo = &blk_rowinfo;
			MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

			tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

			tabinfo_push(tabinfo);

			minrowlen = chkdata->chktab_row_minlen;

			
			TABINFO_INIT(tabinfo, tab_sstab_dir, NULL, 0, 
					tabinfo->t_sinfo, minrowlen, 0,
					chkdata->chktab_tabid, sstabid);
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0,
					chkdata->chktab_key_colid, VARCHAR, -1);
					
			bp = blk_getsstable(tabinfo);

			sstab_bp = (char *)(bp->bsstab->bblk);

			BLOCK 	*sstab_blk;

			int	i = 0, rowno;
			int	result;
			int	*offset;
			char 	*rp;
			char	*key_in_blk;
			int	keylen_in_blk;
			char	*lastkey_in_blk;
			int	lastkeylen_in_blk;
			
			while (TRUE)
			{
				sstab_blk = (BLOCK *)(sstab_bp + i * BLOCKSIZE);

				
				for(rowno = 0, offset = ROW_OFFSET_PTR(sstab_blk); 
					rowno < blk->bnextrno; rowno++, offset--)
				{
					rp = (char *)sstab_blk + *offset;

					key_in_blk = row_locate_col(rp,
							TABLE_KEY_FAKE_COLOFF_INROW, 
							minrowlen, &keylen_in_blk);

					if (rowno > 0)
					{
						result = row_col_compare(VARCHAR, 
								key_in_blk, 
								keylen_in_blk,
								lastkey_in_blk, 
								lastkeylen_in_blk);
						
						if (result != GR)
						{
							traceprint("%s(%d): the %dth block hit index issue\n",sstabname, sstabid, sstab_blk->bblkno);
						}
					}
					
					lastkey_in_blk = key_in_blk;
					lastkeylen_in_blk = keylen_in_blk;
				
					Assert(*offset < sstab_blk->bfreeoff);
					
				}

				i++;

				if (i > (BLK_CNT_IN_SSTABLE - 1))
				{
					break;
				}			
			
			}

			bufunkeep(bp->bsstab);
			session_close(tabinfo);

			MEMFREEHEAP(tabinfo->t_sinfo);
			MEMFREEHEAP(tabinfo);

			tabinfo_pop();

		}


		i++;

		if (i > (BLK_CNT_IN_SSTABLE - 1))
		{
			break;
		}
	}


	rtn_stat = TRUE;


exit:
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;

}



static char *
rg_compact_sstab_by_tablet(COMPACT_DATA *cpctdata)
{
	int		rtn_stat;
	char		*resp;	
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		status;
	int		namelen;
	char		tab_sstab_dir[TABLE_NAME_MAX_LEN];
	

	rtn_stat = FALSE;
	
	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', cpctdata->compact_tabname);

	if (STAT(tab_dir, &st) != 0)
	{
		MKDIR(status, tab_dir, 0755);

		if (status < 0)
		{
			goto exit;
		}
	}

		
	BLOCK 	*blk;

	int	i = 0, rowno;
	int	*offset;
	char 	*rp;
	char	*sstabname;
	int	sstabid;
	char	*sstab_bp;
	int	ign;
	
	while (TRUE)
	{
		blk = (BLOCK *)(cpctdata->compact_data + i * BLOCKSIZE);

		
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
				rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;
		
			Assert(*offset < blk->bfreeoff);
		
			sstabname = row_locate_col(rp,
						TABLET_SSTABNAME_COLOFF_INROW,
						ROW_MINLEN_IN_TABLET, &namelen);

			sstabid = *(int *)row_locate_col(rp, 
						TABLET_SSTABID_COLID_INROW,
						ROW_MINLEN_IN_TABLET, &ign);

			
			MEMSET(tab_sstab_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(tab_sstab_dir, tab_dir, STRLEN(tab_dir));
			str1_to_str2(tab_sstab_dir, '/', sstabname);	

			TABINFO		*tabinfo;
			int		minrowlen;
			BLK_ROWINFO	blk_rowinfo;
			BUF		*bp;
			
			tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
			MEMSET(tabinfo, sizeof(TABINFO));
			tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
			MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

			tabinfo->t_rowinfo = &blk_rowinfo;
			MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

			tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

			tabinfo_push(tabinfo);

			minrowlen = cpctdata->compact_row_minlen;

			
			TABINFO_INIT(tabinfo, tab_sstab_dir, NULL, 0,
					tabinfo->t_sinfo, minrowlen, 
					0, cpctdata->compact_tabid, 
					sstabid);
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, 
					cpctdata->compact_key_colid, VARCHAR, 
					-1);
					
			bp = blk_getsstable(tabinfo);

			sstab_bp = (char *)(bp->bsstab->bblk);

			BLOCK 	*sstab_blk;
			BLOCK	*sstab_nxtblk;

			int	i = 0, rowno;
			float	freeoff;
			int	*offset;
			char 	*rp;
			int	rlen;
					
			while (TRUE)
			{
				sstab_blk = (BLOCK *)(sstab_bp + i * BLOCKSIZE);

				freeoff = (BLOCKSIZE - sstab_blk->bfreeoff - 
					ROW_OFFSET_ENTRYSIZE * sstab_blk->bnextrno) / BLOCKSIZE;
				
				if (freeoff > 0.1)
				{
					goto nextblk;
				}

				if ((i + 1) > (BLK_CNT_IN_SSTABLE - 1))
				{
					break;
				}

				Assert(sstab_blk->bnextblkno != -1);

				sstab_nxtblk = (BLOCK *)(sstab_bp + (i +1) * BLOCKSIZE);
				
				for(rowno = 0, offset = ROW_OFFSET_PTR(sstab_nxtblk); 
						rowno < sstab_nxtblk->bnextrno; 
						rowno++, offset--)
				{
					rp = (char *)sstab_nxtblk + *offset;

					Assert(*offset < sstab_nxtblk->bfreeoff);

					rlen = ROW_GET_LENGTH(rp, minrowlen);
					

					if ((sstab_blk->bfreeoff + rlen) > (BLOCKSIZE - BLK_TAILSIZE 
						- (ROW_OFFSET_ENTRYSIZE * (sstab_blk->bnextrno + 1))))
					{
						
						int *offtab = ROW_OFFSET_PTR(sstab_nxtblk);
						
						BACKMOVE(rp, sstab_nxtblk + BLKHEADERSIZE, 
							sstab_nxtblk->bfreeoff - *offset);

						int j,k = sstab_nxtblk->bnextrno - rowno;
						
						for (j = sstab_nxtblk->bnextrno; 
							(j > rowno) && (k > 0); j--,k--)
						{
							if (offtab[-(j-1)] < *offset)
							{
								break;
							}
						
							offtab[-(k-1)] = offtab[-(j-1)] - 
									*offset + BLKHEADERSIZE;						
						}

						Assert((k == 0) && (j == rowno));
					}


					PUT_TO_BUFFER(sstab_blk + sstab_blk->bfreeoff, 
							ign, rp, rlen);

					ROW_SET_OFFSET(sstab_blk, sstab_blk->bnextrno, 
							sstab_blk->bfreeoff);
					
					bp->bblk->bfreeoff += rlen;

					(sstab_blk->bnextrno)++;					
				}

nextblk:

				i++;

				if (i > (BLK_CNT_IN_SSTABLE - 1))
				{
					break;
				}			
			
			}

			bufunkeep(bp->bsstab);
			session_close(tabinfo);

			MEMFREEHEAP(tabinfo->t_sinfo);
			MEMFREEHEAP(tabinfo);

			tabinfo_pop();

		}


		i++;

		if (i > (BLK_CNT_IN_SSTABLE - 1))
		{
			break;
		}
	}


	rtn_stat = TRUE;


exit:
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;

}



static int
rg_table_is_exist(char *tabname)
{
	int	i;
	int	tabidx;


	tabidx = -1;

	for (i = 0; i < Range_infor->rg_systab.tabnum; i++)
	{
		if(strcmp(tabname, Range_infor->rg_systab.rg_tabdir[i])==0)
		{
			return i;
		}
	}

	return tabidx;
}


/* 
** SSTABLE exist checking just temp solution, because the insertion may not hit the same
** sstable in the batch inserting.
*/
static int
rg_sstable_is_exist(char *sstabname, int tabidx)
{
	if(strcmp(sstabname, Range_infor->rg_systab.rg_sstab[tabidx])== 0)
	{
		return TRUE;;
	}
	

	return FALSE;
}


static int
rg_sstable_regist(char *sstabname, int tabidx)
{
	if (tabidx > TAB_MAX_NUM)
	{
		return FALSE;
	}

	MEMSET(Range_infor->rg_systab.rg_sstab[tabidx], 256);
	
	MEMCPY(Range_infor->rg_systab.rg_sstab[tabidx], 
			sstabname, STRLEN(sstabname));

	return TRUE;
}

static int
rg_table_regist(char *tabname)
{
	int	tabidx;

	
	if ((Range_infor->rg_systab.tabnum + 1) > TAB_MAX_NUM)
	{
		traceprint("The number of table current system owning expand the limits.\n");
		return -1;
	}

	MEMCPY(Range_infor->rg_systab.rg_tabdir[Range_infor->rg_systab.tabnum], 
			tabname, STRLEN(tabname));

	tabidx = Range_infor->rg_systab.tabnum;
	
	(Range_infor->rg_systab.tabnum)++;

	return tabidx;
}

static int
rg_table_unregist(char *tabname)
{
	int	i;
	int	j;


	/* Table register is not start yet. */
	if (Range_infor->rg_systab.tabnum == 0)
	{
		return TRUE;
	}
	
	j = -1;
	
	for (i = 0; i < Range_infor->rg_systab.tabnum; i++)
	{
		if(strcmp(tabname, Range_infor->rg_systab.rg_tabdir[i])==0)
		{
			j = i;
			break;
		}
	}

	/* This table does not start to register. */
	if (j == -1)
	{
		return TRUE;
	}

	/* Here we have got this table in the register slot. */
	
	/* Foward the information of unregisted table. */
	for(i = j; (i + 1) < Range_infor->rg_systab.tabnum; i++)
	{
		MEMCPY(Range_infor->rg_systab.rg_tabdir[i], 
			Range_infor->rg_systab.rg_tabdir[i+1],
			STRLEN(Range_infor->rg_systab.rg_tabdir[i+1]));

		MEMCPY(Range_infor->rg_systab.rg_sstab[i], 
			Range_infor->rg_systab.rg_sstab[i+1],
			STRLEN(Range_infor->rg_systab.rg_sstab[i+1]));
	}

	/* Clearing the last place of rg_systab. */
	if (i == Range_infor->rg_systab.tabnum)
	{
		MEMSET(Range_infor->rg_systab.rg_tabdir[i - 1],
				TABLE_NAME_MAX_LEN);
		MEMSET(Range_infor->rg_systab.rg_sstab[i - 1],
				TABLE_NAME_MAX_LEN);
	}

	(Range_infor->rg_systab.tabnum)--;

	return TRUE;
}


static void *
rg_mapred_process(void *args)
{
	pthread_detach(pthread_self());

	mapred_arg * in = (mapred_arg *) args;
	char * table_name = in->table_name;
	char * tablet_name = in->tablet_name;
	int data_port = in->data_port;

	char tab_dir[TABLE_NAME_MAX_LEN];
	char rg_tab_dir[TABLE_NAME_MAX_LEN];
	char tab_meta_dir[TABLE_NAME_MAX_LEN];
	char tab_tablet_dir[TABLE_NAME_MAX_LEN];

	int fd1;

	TABLEHDR tab_hdr;

	char * tablet_bp = (char *)malloc(SSTABLE_SIZE);
	char * sstable_buf = (char *)malloc(SSTABLE_SIZE);

	int listenfd = 0;
	int connfd = 0;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', table_name);

	MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(rg_tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
	str1_to_str2(rg_tab_dir, '/', table_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "sysobjects");

	OPEN(fd1, tab_meta_dir, (O_RDONLY));
	if (fd1 < 0)
	{
		traceprint("Table is not exist! \n");
		goto exit;
	}
	READ(fd1, &tab_hdr, sizeof(TABLEHDR));	
	CLOSE(fd1);

	if (tab_hdr.tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", table_name);
		goto exit;
	}

	if (tab_hdr.tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		goto exit;
	}	
	
	
	MEMSET(tab_tablet_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_tablet_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_tablet_dir, '/', tablet_name);	
	
	OPEN(fd1, tab_tablet_dir, (O_RDONLY));
	if (fd1 < 0)
	{
		   traceprint("Tablet %s is not exist! \n", tab_tablet_dir);
		   goto exit;
	}
	READ(fd1, tablet_bp, SSTABLE_SIZE); 
	CLOSE(fd1);

	/* Create the socket fot the transferring of bigdata. */
	listenfd = conn_socket_open(data_port);
	if (!listenfd)
	{
		goto exit;
	}

	connfd = conn_socket_accept(listenfd);
	if (connfd < 0)
	{
		traceprint("hit accept issue\n");
		goto exit;
	}

	BLOCK *blk;
	int i, rowno;
	int *offset;
	char *rp;
	char tab_sstab_dir[TABLE_NAME_MAX_LEN];

	MT_BLOCK_CACHE *block_cache = (MT_BLOCK_CACHE *)malloc(sizeof(MT_BLOCK_CACHE));
	char data_req[RPC_MAGIC_MAX_LEN];
	
	for(i = 0; i < BLK_CNT_IN_SSTABLE; i ++)
	{
		blk = (BLOCK *)(tablet_bp + i * BLOCKSIZE);
			       
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			       rowno < blk->bnextrno; rowno++, offset--)
		{
			//process each sstable
			
			rp = (char *)blk + *offset;
			
			int sstabname_length;
			char * sstabname = row_locate_col(rp, TABLET_SSTABNAME_COLOFF_INROW, ROW_MINLEN_IN_TABLET, &sstabname_length);
			//int sstabid_length;
			//int sstabid = *(int *)row_locate_col(rp,TABLET_SSTABID_COLOFF_INROW,ROW_MINLEN_IN_TABLET,&sstabid_length);

			
			MEMSET(tab_sstab_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(tab_sstab_dir, rg_tab_dir, STRLEN(rg_tab_dir));
			str1_to_str2(tab_sstab_dir, '/', sstabname);
				
			OPEN(fd1, tab_sstab_dir, (O_RDONLY));
			if (fd1 < 0)
			{
				traceprint("sstable %s is not exist! \n", tab_sstab_dir);
				goto exit;
			}
			READ(fd1, sstable_buf, SSTABLE_SIZE); 
			CLOSE(fd1);

#if 0
			TABINFO tabinfo;
			SINFO	psinfo;
			BLK_ROWINFO blk_rowinfo;			
						
			MEMSET(&tabinfo,sizeof(TABINFO));
			MEMSET(&psinfo, sizeof(SINFO));
	
			tabinfo.t_tabid = tab_hdr.tab_id;
			tabinfo.t_row_minlen = tab_hdr.tab_row_minlen;
			tabinfo.t_sstab_id = sstabid;
			tabinfo.t_sinfo = &psinfo;
			tabinfo.t_rowinfo = &blk_rowinfo;
			MEMSET(tabinfo.t_rowinfo, sizeof(BLK_ROWINFO));

			MEMSET(tab_sstab_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(tab_sstab_dir, rg_tab_dir, STRLEN(rg_tab_dir));
			str1_to_str2(tab_sstab_dir, '/', sstabname);

			TABINFO_INIT(&tabinfo, tab_sstab_dir, tabinfo.t_sinfo, 
				tabinfo.t_row_minlen, 0, tabinfo.t_tabid, 
				tabinfo.t_sstab_id);
			SRCH_INFO_INIT(tabinfo.t_sinfo, NULL, 0, 1, VARCHAR, -1);

			BUF *sstable = blk_getsstable(&tabinfo);
			//int sstable_offset = BLKHEADERSIZE;
#endif
			char *sstable_bp = sstable_buf;//(char *)(sstable->bsstab->bblk);
			BLOCK * sstable_blk;
			int j;

			for(j = 0; j < BLK_CNT_IN_SSTABLE; j ++)
			{
				sstable_blk = (BLOCK *)(sstable_bp + j * BLOCKSIZE);

				if(!sstable_blk->bnextrno)
				{
					break;
				}

				//wait req
				MEMSET(data_req, RPC_MAGIC_MAX_LEN);
				int data_req_len = read(connfd, data_req, RPC_MAGIC_MAX_LEN);
				
				if (data_req_len != RPC_MAGIC_MAX_LEN)
				{
					traceprint("\n ERROR in response \n");
					goto exit;
				}

				//process one req
				if (!strncasecmp(RPC_MAPRED_GET_NEXT_VALUE, data_req, STRLEN(RPC_MAPRED_GET_NEXT_VALUE)))
				{
					MEMSET(block_cache, sizeof(MT_BLOCK_CACHE));
					MEMCPY(block_cache->data_cache, (char *)sstable_blk, BLOCKSIZE);
					MEMCPY(block_cache->current_sstable_name, sstabname, sstabname_length);
					block_cache->current_block_index = j;
					block_cache->cache_index = 0;
					block_cache->max_row_count = sstable_blk->bnextrno;
					block_cache->row_min_len = sstable_blk->bminlen;
					
					char * resp = conn_build_resp_byte(RPC_SUCCESS, sizeof(MT_BLOCK_CACHE), 
						(char *)block_cache);
					int resp_size = conn_get_resp_size((RPCRESP *)resp);
					/* Send the result to the client. */
					tcp_put_data(connfd, resp, resp_size);
					conn_destroy_resp_byte(resp);	
				}
				else
				{
					traceprint("\n the request can only be RPC_MAPRED_GET_NEXT_VALUE \n");
					Assert(0);
				}
			}

			//bufunkeep(sstable->bsstab);
			
		}
	}

	while(TRUE)
	{
		MEMSET(data_req, RPC_MAGIC_MAX_LEN);
		int data_req_len = read(connfd, data_req, RPC_MAGIC_MAX_LEN);
				
		if(data_req_len == 0)
		{
			traceprint("\n map reduce exit in this tablet \n");
			goto exit;
		}
		else if (data_req_len != RPC_MAGIC_MAX_LEN)
		{
			traceprint("\n ERROR in response \n");
			goto exit;
		}

		if (!strncasecmp(RPC_MAPRED_GET_NEXT_VALUE, data_req, STRLEN(RPC_MAPRED_GET_NEXT_VALUE)))
		{
			traceprint("\n no value left for value get in mapreduce \n");
			
			char * resp = conn_build_resp_byte(RPC_NO_VALUE, 0,	NULL);
			int resp_size = conn_get_resp_size((RPCRESP *)resp);
			/* Send the result to the client. */
			tcp_put_data(connfd, resp, resp_size);
			conn_destroy_resp_byte(resp);	
		}
		else if(!strncasecmp(RPC_MAPRED_EXIT, data_req, STRLEN(RPC_MAPRED_EXIT)))
		{
			traceprint("\n map reduce exit in this tablet \n");

			char * resp = conn_build_resp_byte(RPC_SUCCESS, 0,	NULL);
			int resp_size = conn_get_resp_size((RPCRESP *)resp);
			/* Send the result to the client. */
			tcp_put_data(connfd, resp, resp_size);
			conn_destroy_resp_byte(resp);
			
			goto exit;
		}
		else
		{
			traceprint("\n the request can only be RPC_MAPRED_GET_NEXT_VALUE or RPC_MAPRED_EXIT \n");
			Assert(0);
		}
	}
					

exit:
	if(connfd)
		conn_socket_close(connfd);
	if(listenfd)
		conn_socket_close(listenfd);

	MEMFREEHEAP(in);
	free(tablet_bp);
	free(sstable_buf);
	free(block_cache);	
	
	return NULL;	
}


//max 100 simultaneous map, the port is from 50000 to 50099
int rg_mapred_gen_port()
{
	pthread_mutex_lock(&port_mutex);
	int ret = mapred_data_port++;
	if(mapred_data_port == max_data_port)
	{
		mapred_data_port = init_data_port;
	}
	pthread_mutex_unlock(&port_mutex);
	return ret;
}

static char *
rg_mapred_setup(char * req_buf)
{
	char tab_dir[TABLE_NAME_MAX_LEN];
	char tablet_dir[TABLE_NAME_MAX_LEN];
	char *resp;

	int tabidx;
	int ret = TRUE;
	
	char * table_name = req_buf + RPC_MAGIC_MAX_LEN;
	char * tablet_name = req_buf + RPC_MAGIC_MAX_LEN + strlen(table_name) + 1;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', table_name);
	
	MEMSET(tablet_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tablet_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tablet_dir, '/', tablet_name);
	
	if ((tabidx = rg_table_is_exist(tab_dir)) == -1)
	{
		if (STAT(tab_dir, &st) != 0)
		{
			traceprint("Table %s is not exist.\n", table_name);
			ret = FALSE;
			goto exit;
		}
	
		tabidx = rg_table_regist(tab_dir);
	}

	if (STAT(tablet_dir, &st) != 0)
	{
		traceprint("Tablet %s is not exist.\n", tablet_name);
		ret = FALSE;
		goto exit;
	}

	pthread_t pthread_id;
	//int	*tmpid;

	int data_port = rg_mapred_gen_port();

	mapred_arg* args = MEMALLOCHEAP(sizeof(mapred_arg));
	//mapred_arg* args = malloc(sizeof(mapred_arg));
	MEMSET(args, sizeof(mapred_arg));
	MEMCPY(args->table_name, table_name, strlen(table_name));
	MEMCPY(args->tablet_name, tablet_name, strlen(tablet_name));
	args->data_port = data_port;

	pthread_create(&pthread_id, NULL, rg_mapred_process, (void *)args);	

exit:
	if(ret)
	{
		resp = conn_build_resp_byte(RPC_BIGDATA_CONN, sizeof(int),
						(char *)(&data_port));
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}
	return resp;
}

int 
main(int argc, char *argv[])
{
	char		*conf_path;
//	pthread_t	tid1, tid2;


	mem_init_alloc_regions();

	
	conf_path = RANGE_DEFAULT_CONF_PATH;
	
	conf_get_path(argc, argv, &conf_path);

	tss_setup(TSS_OP_RANGESERVER);

	rg_setup(conf_path);

	//Trace = MEM_USAGE;
	Trace = 0;
	rg_boot();
	return TRUE;
}
