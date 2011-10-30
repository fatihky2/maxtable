/*
** rangeserver.c 2010-06-21 xueyingfei
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


#include "global.h"
#include "utils.h"
#include "list.h"
#include "conf.h"
#include "netconn.h"
#include "ranger/rangeserver.h"
#include "memcom.h"
#include "master/metaserver.h"
#include "parser.h"
#include "file_op.h"
#include "row.h"
#include "token.h"
#include "tss.h"
#include "hkgc.h"
#include "strings.h"
#include "buffer.h"
#include "block.h"
#include "metadata.h"
#include "cache.h"
#include "exception.h"
#include "type.h"
#include "trace.h"
#include "session.h"
#include "tabinfo.h"
#include "tablet.h"
#include "rebalancer.h"
#include "sstab.h"
#include "checktable.h"
#include "compact.h"
#include "b_search.h"
#include "log.h"


extern TSS	*Tss;
extern KERNEL	*Kernel;
extern	char	Kfsserver[32];
extern	int	Kfsport;

#define	RANGE_CONF_PATH_MAX_LEN	64

char	*RgLogfile;
char	*RgBackup;


typedef struct rg_info
{
	char	conf_path[RANGE_CONF_PATH_MAX_LEN];
	char	rg_meta_ip[RANGE_ADDR_MAX_LEN];
	int     rg_meta_port;
	char	rg_ip[RANGE_ADDR_MAX_LEN];
	int	port;
	int	bigdataport;
	int	flush_check_interval;
	char	rglogfiledir[256];
	char	rgbackup[256];
}RANGEINFO;


typedef struct _range_query_contex
{
	int	status;
	int	first_rowpos;
	int	end_rowpos;
	int	cur_rowpos;
	int	rowminlen;
	char	data[BLOCKSIZE];
}range_query_contex;


RANGEINFO *Range_infor = NULL;


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



char *
rg_droptab(TREE *command)
{
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	char		cmd_str[TABLE_NAME_MAX_LEN];
	char		*resp;


	Assert(command);

	rtn_stat = FALSE;
	
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;


	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
	{
		rtn_stat = TRUE;
		goto exit;		
	}

	MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
	
	sprintf(cmd_str, "rm -rf %s", tab_dir);
	
	if (!system(cmd_str))
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
	int		status;
	int		rtn_stat;
	int		sstab_rlen;
	int		sstab_idx;
	char	 	*resp;
	char		rp[1024];
	int		rp_idx;
	char		col_off_tab[COL_OFFTAB_MAX_SIZE];
	int		col_off_idx;
	int		col_offset;
	char		*col_val;
	int		col_len;
	int		col_num;
	INSMETA 	*ins_meta;
	COLINFO 	*col_info;
	char		*resp_buf;
	int		resp_len;


	Assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	ins_meta = tabinfo->t_insmeta;
	col_info = tabinfo->t_colinfo;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
	{
		MKDIR(status, tab_dir, 0755);

		if (status < 0)
		{
			goto exit;
		}
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
	{
		ins_meta->status |= INS_META_1ST;
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
		traceprint("tab_dir =%s \n", tab_dir);
	}
	
	row_build_hdr(rp, 0, 0, ins_meta->varcol_num);

	col_offset = sizeof(ROWFMT);
	rp_idx = sizeof(ROWFMT);
	col_num = ins_meta->col_num;
	col_off_idx = COL_OFFTAB_MAX_SIZE;

	rg_fill_resd(command, col_info, col_num);

	while(col_num)
	{
		col_val = par_get_colval_by_coloff(command, col_offset,	&col_len);

		if (col_offset == tabinfo->t_key_coloff)
		{
			
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
				ex_raise(EX_ANY);
			}
			
			
			

			if (col_num > 0)
			{
				
				rp_idx += sizeof(int);
				col_offset = -1;
			}
			
			Assert(ins_meta->varcol_num == col_num);

			if (ins_meta->varcol_num != col_num)
			{
				traceprint("Hit a row error!\n");
				ex_raise(EX_ANY);
			}
		}		
		
	}

	if (COL_OFFTAB_MAX_SIZE > col_off_idx)
	{
		PUT_TO_BUFFER(rp, rp_idx, (col_off_tab + col_off_idx), 
					(COL_OFFTAB_MAX_SIZE - col_off_idx));
		*(int *)(rp + ins_meta->row_minlen) = rp_idx;
	}


	rtn_stat = blkins(tabinfo, rp);

exit:

	resp_len = 0;
	resp_buf = NULL;
	
	if (rtn_stat && (tabinfo->t_stat & TAB_SSTAB_SPLIT))
	{
		resp_len = tabinfo->t_insrg->new_keylen + SSTABLE_NAME_MAX_LEN + 3 * sizeof(int);
		resp_buf = (char *)MEMALLOCHEAP(resp_len);

		MEMSET(resp_buf, resp_len);

		int i = 0;

		if (DEBUG_TEST(tss))
		{
			traceprint("tabinfo->t_insrg->new_sstab_name = %s \n", tabinfo->t_insrg->new_sstab_name);
		}
		
		PUT_TO_BUFFER(resp_buf, i, tabinfo->t_insrg->new_sstab_name, SSTABLE_NAME_MAX_LEN);

		PUT_TO_BUFFER(resp_buf, i, &tabinfo->t_insmeta->res_sstab_id, sizeof(int));
		
		
		PUT_TO_BUFFER(resp_buf, i, &tabinfo->t_insmeta->ts_low, sizeof(int));
		
		
		PUT_TO_BUFFER(resp_buf, i, &tabinfo->t_insmeta->sstab_id, sizeof(int));
		
		PUT_TO_BUFFER(resp_buf, i, tabinfo->t_insrg->new_sstab_key, tabinfo->t_insrg->new_keylen);
		
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

	if (resp_buf)
	{
		MEMFREEHEAP(resp_buf);
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
	int		status;
	int		rtn_stat;
	int		sstab_rlen;
	int		sstab_idx;
	char		*resp;
	INSMETA 	*ins_meta;
	COLINFO 	*col_info;
	BUF		*bp;
	char		*keycol;
	int		keycolen;
	int		offset;
	char   		*col_buf;
	int 		rlen;


	Assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	col_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	ins_meta = tabinfo->t_insmeta;
	col_info = tabinfo->t_colinfo;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
	{
		MKDIR(status, tab_dir, 0755);

		if (status < 0)
		{
			goto exit;
		}
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
	{
		goto exit; 
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
		traceprint("tab_dir =%s \n", tab_dir);
	}
	
	keycol = par_get_colval_by_colid(command, tabinfo->t_key_colid, &keycolen);

	TABINFO_INIT(tabinfo, ins_meta->sstab_name, tabinfo->t_sinfo, tabinfo->t_row_minlen, 
			0, tabinfo->t_tabid, tabinfo->t_sstab_id);
	SRCH_INFO_INIT(tabinfo->t_sinfo, keycol, keycolen, 1, VARCHAR, -1);

	
	if (tabinfo->t_stat & TAB_DEL_DATA)
	{
		rtn_stat = blkdel(tabinfo);

		if (rtn_stat)
		{
			goto exit;
		}
	}
	else
	{
		
		bp = blkget(tabinfo);
//		offset = blksrch(tabinfo, bp);

		
		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			goto exit;
		}

		Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
		Assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);

		if ((tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
		    || (tabinfo->t_rowinfo->rsstabid != bp->bblk->bsstabid))
		{
			traceprint("Hit a buffer error!\n");
			ex_raise(EX_ANY);
		}
		
		offset = tabinfo->t_rowinfo->roffset;
	}
	
	
	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		goto exit;
	}

	if (!(tabinfo->t_stat & TAB_DEL_DATA))
	{
		
		char *rp = (char *)(bp->bblk) + offset;
		char *value;
		
		// char	*filename = meta_get_coldata(bp, offset, sizeof(ROWFMT));

		value = row_locate_col(rp, -2, bp->bblk->bminlen, &rlen);

		col_buf = MEMALLOCHEAP(rlen);
		MEMSET(col_buf, rlen);

		MEMCPY(col_buf, value, rlen);
		
	}
	
	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		if (tabinfo->t_stat & TAB_DEL_DATA)
		{
			
			resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
		}
		else
		{
			
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

	if (col_buf != NULL)
	{
		MEMFREEHEAP(col_buf);
	}
	
	return resp;

}


char *
rg_selrangetab(TREE *command, TABINFO *tabinfo)
{
	LOCALTSS(tss);
	char		*sstable;
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		status;
	int		rtn_stat;
	int		sstab_rlen;
	int		sstab_idx;
	char		*resp;
	INSMETA 	*ins_meta;
	COLINFO 	*col_info;
	BUF		*bp;
	char		*keycol;
	int		keycolen;
	int		offset;
	char   		*col_buf;
	int 		rlen;
	char		last_sstab[SSTABLE_NAME_MAX_LEN];


	Assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	col_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	ins_meta = tabinfo->t_insmeta;
	col_info = tabinfo->t_colinfo;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
	{
		MKDIR(status, tab_dir, 0755);

		if (status < 0)
		{
			goto exit;
		}
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
	{
		goto exit; 
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
		traceprint("tab_dir =%s \n", tab_dir);
	}
	
	keycol = par_get_colval_by_colid(command, 1, &keycolen);
	
	char	*right_rangekey;
	int	right_keylen;
	right_rangekey = par_get_colval_by_colid(command, 2, &right_keylen);
	
	TABINFO_INIT(tabinfo, ins_meta->sstab_name, tabinfo->t_sinfo, tabinfo->t_row_minlen, 
			0, tabinfo->t_tabid, tabinfo->t_sstab_id);
	SRCH_INFO_INIT(tabinfo->t_sinfo, keycol, keycolen, 1, VARCHAR, -1);


	/* Mallocate max buffer length. */
	
	col_buf = MEMALLOCHEAP(512);
	MEMSET(col_buf, 512);
	int	col_len = sizeof(int);
	int	rowcnt = 0;
	

	bp = blkget(tabinfo);

	
	if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
	{
		goto exit;
	}

	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);

	if ((tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
	    || (tabinfo->t_rowinfo->rsstabid != bp->bblk->bsstabid))
	{
		traceprint("Hit a buffer error!\n");
		ex_raise(EX_ANY);
	}
	
	offset = tabinfo->t_rowinfo->roffset;

	while (TRUE)
	{
		char *rp = (char *)(bp->bblk) + offset;
		
		rlen = ROW_GET_LENGTH(rp, bp->bblk->bminlen);

		char	*key_in_blk;
		int	keylen_in_blk;
		int 	result;
		
		key_in_blk = row_locate_col(rp, tabinfo->t_sinfo->sicoloff, bp->bblk->bminlen, 
						    &keylen_in_blk);
		
		result = row_col_compare(tabinfo->t_sinfo->sicoltype, key_in_blk, keylen_in_blk,
					right_rangekey, right_keylen);

		if (result == GR)
		{
			break;
		}
		
		if ((col_len + sizeof(int) + rlen - sizeof(ROWFMT)) < 512)
		{
			char	*data_start = meta_get_coldata(bp, offset, sizeof(ROWFMT));
			
			*(int *)(col_buf + col_len) = rlen - sizeof(ROWFMT);
			col_len += sizeof(int);
			MEMCPY(col_buf + col_len, data_start, rlen - sizeof(ROWFMT));
			col_len += rlen - sizeof(ROWFMT);
			rowcnt++;
		}
		else
		{
			*(int *)col_buf = rowcnt;
			break;
		}

		offset += rlen;

		if (bp->bblk->bfreeoff > offset)
		{	
			Assert(bp->bblk->bfreeoff > offset);
		}
		else
		{
			if (bp->bblk->bnextblkno != -1)
			{
				bp++;
				offset = BLKHEADERSIZE;
			}
			else if (bp->bsstab->bblk->bnextsstabnum != -1)
			{
				MEMSET(last_sstab, SSTABLE_NAME_MAX_LEN);
				MEMCPY(last_sstab, tabinfo->t_sstab_name, STRLEN(tabinfo->t_sstab_name));
				
				MEMSET(tabinfo->t_sstab_name, SSTABLE_NAME_MAX_LEN);			
				
				sstab_namebyid(last_sstab, tabinfo->t_sstab_name, 
							bp->bsstab->bblk->bnextsstabnum);

				bp = blk_getsstable(tabinfo);
				offset = BLKHEADERSIZE;
			}
			else
			{
				break;
			}
		}

		
	}
	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		if (tabinfo->t_stat & TAB_DEL_DATA)
		{
			
			resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
		}
		else
		{
			
			resp = conn_build_resp_byte(RPC_SUCCESS, 512, col_buf);
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

	if (col_buf != NULL)
	{
		MEMFREEHEAP(col_buf);
	}
	
	return resp;

}

/* Following define is for the status sending to the client. */
#define	DATA_CONT	0x0001	
#define DATA_DONE	0x0002
#define DATA_EMPTY	0x0004


char *
rg_selrange_tab(TREE *command, TABINFO *tabinfo, int fd)
{
	LOCALTSS(tss);
	char		*sstable;
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		status;
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
	int		offset;
	char		last_sstab[SSTABLE_NAME_MAX_LEN];
	int		left_expand;
	int		right_expand;
	B_SRCHINFO	srchinfo;


	Assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	ins_meta = tabinfo->t_insmeta;
	col_info = tabinfo->t_colinfo;
	left_expand = right_expand = FALSE;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
	{
		MKDIR(status, tab_dir, 0755);

		if (status < 0)
		{
			goto exit;
		}
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
	{
		goto exit; 
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
		traceprint("tab_dir =%s \n", tab_dir);
	}
	
	keycol = par_get_colval_by_colid(command, 1, &keycolen);

	if ((keycolen == 1) && (!strncasecmp("*", keycol, keycolen)))
	{
		left_expand = TRUE;
	}
	
	char	*right_rangekey;
	int	right_keylen;
	right_rangekey = par_get_colval_by_colid(command, 2, &right_keylen);

	if ((right_keylen == 1) && (!strncasecmp("*", right_rangekey, right_keylen)))
	{
		right_expand = TRUE;
	}
	
	TABINFO_INIT(tabinfo, ins_meta->sstab_name, tabinfo->t_sinfo, tabinfo->t_row_minlen, 
			0, tabinfo->t_tabid, tabinfo->t_sstab_id);
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
		offset = BLKHEADERSIZE;
	}
	else
	{
		Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
		Assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);

		if ((tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
		    || (tabinfo->t_rowinfo->rsstabid != bp->bblk->bsstabid))
		{
			traceprint("Hit a buffer error!\n");
			ex_raise(EX_ANY);
		}
		
		offset = tabinfo->t_rowinfo->roffset;
	}

	range_query_contex	rgsel_cont;

	char	resp_cli[8];
	int 	*offtab = ROW_OFFSET_PTR(bp->bblk);
	int	i;
		
	for (i = 0; i < bp->bblk->bnextrno; i++)
	{
		if (offtab[-i] == offset)
		{
			break;
		}
	}

	rgsel_cont.first_rowpos = i;

	Assert(i < bp->bblk->bnextrno);


	int n;

	resp = conn_build_resp_byte(RPC_BIGDATA_CONN, sizeof(int),
					(char *)(&(Range_infor->bigdataport)));
	resp_size = conn_get_resp_size((RPCRESP *)resp);	
	write(fd, resp, resp_size);
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
			TABINFO_INIT(tabinfo, tabinfo->t_sstab_name, tabinfo->t_sinfo, tabinfo->t_row_minlen, 
					0, tabinfo->t_tabid, tabinfo->t_sstab_id);
			SRCH_INFO_INIT(tabinfo->t_sinfo, right_rangekey, right_keylen, 1, VARCHAR, -1);			

			MEMSET(&srchinfo, sizeof(B_SRCHINFO));
			SRCHINFO_INIT((&srchinfo), 0, BLK_GET_NEXT_ROWNO(bp) - 1, BLK_GET_NEXT_ROWNO(bp), LE);

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
					if (srchinfo.bcomp == LE)
					{
						rgsel_cont.end_rowpos = srchinfo.brownum - 1;
					}
					else
					{
						rgsel_cont.end_rowpos = srchinfo.brownum;
					}
					
					rgsel_cont.status = DATA_DONE;
					data_cont = FALSE;
				}
			}

		}

		MEMCPY(rgsel_cont.data, (char *)(bp->bblk), BLOCKSIZE);
	
	 	resp = conn_build_resp_byte(RPC_SUCCESS, sizeof(range_query_contex), (char *)&rgsel_cont);
		
		resp_size = conn_get_resp_size((RPCRESP *)resp);
		  
		write(connfd, resp, resp_size);			

		conn_destroy_resp_byte(resp);	

		
		/* TODO: placeholder for the TCP/IP check. */
		MEMSET(resp_cli, 8);
		n = conn_socket_read(connfd,resp_cli, 8);

		if (n != 8)
		{
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
			MEMCPY(last_sstab, tabinfo->t_sstab_name, STRLEN(tabinfo->t_sstab_name));
			
			MEMSET(tabinfo->t_sstab_name, SSTABLE_NAME_MAX_LEN);			
			
			sstab_namebyid(last_sstab, tabinfo->t_sstab_name, 
						bp->bsstab->bblk->bnextsstabnum);

			tabinfo->t_sstab_id = bp->bsstab->bblk->bnextsstabnum;

			bp = blk_getsstable(tabinfo);			
		}
		else
		{
			rgsel_cont.status = DATA_EMPTY;
			resp = conn_build_resp_byte(RPC_SUCCESS, sizeof(range_query_contex), (char *)&rgsel_cont);
		
			resp_size = conn_get_resp_size((RPCRESP *)resp);
			  
			write(connfd, resp, resp_size);			

			conn_destroy_resp_byte(resp);	

			/* TODO: placeholder for the TCP/IP check. */
			MEMSET(resp_cli, 8);
			n = conn_socket_read(connfd,resp_cli, 8);

			if (n != 8)
			{
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



int
rg_get_meta(char *req_buf, INSMETA **ins_meta, SELRANGE **sel_rg,TABLEHDR **tab_hdr, COLINFO **col_info)
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
			col_info = meta_get_colinfor(colid, totcol, colinfor);

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
	if (!strncasecmp(RPC_MASTER2RG_HEARTBEAT, req_buf, STRLEN(RPC_MASTER2RG_HEARTBEAT)))
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
	if (!strncasecmp(RPC_MASTER2RG_NOTIFY, req_buf, STRLEN(RPC_MASTER2RG_NOTIFY)))
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
	


	ins_meta	= NULL;
	sel_rg		= NULL;
	tab_hdr		= NULL;
	col_info	= NULL;
	
	if ((req_op = rg_get_meta(req_buf, &ins_meta, &sel_rg, &tab_hdr, &col_info)) == 0)
	{
		return NULL;
	}

	
	if (req_op & RPC_REQ_DROP_OP)
	{
		if (!parser_open(req_buf + RPC_MAGIC_MAX_LEN))
		{
			parser_close();
			tss->tstat |= TSS_PARSER_ERR;
			traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
			return NULL;
		}

		command = tss->tcmd_parser;

		Assert(command->sym.command.querytype == DROP);

		return rg_droptab(command);
	}

	
	if (req_op & RPC_REQ_REBALANCE_OP)
	{
		return rg_rebalancer((REBALANCE_DATA *)(req_buf - RPC_MAGIC_MAX_LEN));
	}


	//process with heart beat
	if (req_op & RPC_REQ_M2RHEARTBEAT_OP)
	{
		traceprint("\n$$$$$$ rg recv heart beat. \n");
	
		Assert(rg_heartbeat(req_buf));
		//to be fixed here
		//maybe more info will be added here in the future
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);

		traceprint("\n$$$$$$ rg sent heart beat. \n");

		return resp;
	}

	//process with rsync notify
	if (req_op & RPC_REQ_M2RNOTIFY_OP)
	{
		Assert(rg_rsync(req_buf));

		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);

		return resp;
	}
	
	volatile struct
	{
		TABINFO	*tabinfo;
	} copy;

	copy.tabinfo = NULL;
	tabinfo = NULL;
	resp = NULL;

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
	
	tss->tcol_info = col_info;
	tss->tmeta_hdr = ins_meta;
	tss->rgbackpfile = RgBackup;
	tss->rglogfile = RgLogfile;

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

	tabinfo->t_key_colid = tab_hdr->tab_key_colid;
	tabinfo->t_key_coltype = tab_hdr->tab_key_coltype;
	tabinfo->t_key_coloff = tab_hdr->tab_key_coloff;
	tabinfo->t_row_minlen = tab_hdr->tab_row_minlen;
	tabinfo->t_tabid = tab_hdr->tab_id;

	tabinfo->t_sstab_id = ins_meta->sstab_id;
	tabinfo->t_sstab_name = ins_meta->sstab_name;
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
    		tabinfo->t_stat |= TAB_INS_DATA;
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

		resp = rg_selrange_tab(command, tabinfo, fd);

		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - SELECTRANGE TABLE\n");
		}
	    	break;
		
	    case DROP:
	    	Assert(0);
	    	break;
	    case REBALANCE:
	    	break;

	    default:
	    	break;
	}


	session_close(tabinfo);

	if (tabinfo->t_stat & TAB_SSTAB_SPLIT)
	{
		LOGREC	logrec;
		
		log_build(&logrec, LOG_END, 0, tabinfo->t_sstab_name, NULL);

		log_insert(RgLogfile, &logrec, SPLIT_LOG);
	}
       

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
	
	parser_close();

	return resp;

}

void
rg_setup(char *conf_path)
{
	int	status;
	char	port[32];
	int	rg_port;
	char	metaport[32];

	Range_infor = MEMALLOCHEAP(sizeof(RANGEINFO));
	MEMSET(Range_infor, sizeof(RANGEINFO));
	
	MEMCPY(Range_infor->conf_path, conf_path, STRLEN(conf_path));

	MEMSET(port, 32);
	MEMSET(metaport, 32);

	conf_get_value_by_key(port, conf_path, CONF_RG_PORT);
	conf_get_value_by_key(Range_infor->rg_ip, conf_path, CONF_RG_IP);

	
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
	
	if (!EXIST(MT_RANGE_TABLE))
#else	
	if (STAT(MT_RANGE_TABLE, &st) != 0)
#endif
	{
		MKDIR(status, MT_RANGE_TABLE, 0755);
	}	

	rg_regist();

	char	rgname[64];
	RgLogfile = Range_infor->rglogfiledir;
	
	MEMSET(RgLogfile, 256);
	MEMCPY(RgLogfile, LOG_FILE_DIR, STRLEN(LOG_FILE_DIR));

	MEMSET(rgname, 64);
	sprintf(rgname, "%s%d", Range_infor->rg_ip, Range_infor->port);

	str1_to_str2(RgLogfile, '/', rgname);

#ifdef MT_KFS_BACKEND
	if (!EXIST(RgLogfile))
#else
	if (!(STAT(RgLogfile, &st) == 0))
#endif
	{
		traceprint("Log file %s is not exist.\n", RgLogfile);
		return;
	}


	RgBackup = Range_infor->rgbackup;
	
	MEMSET(RgBackup, 256);
	MEMCPY(RgBackup, BACKUP_DIR, STRLEN(BACKUP_DIR));

	str1_to_str2(RgBackup, '/', rgname);

#ifdef MT_KFS_BACKEND
	if (!EXIST(RgBackup))
#else
	if (STAT(RgBackup, &st) != 0)
#endif
	{
		traceprint("Backup file %s is not exist.\n", RgBackup);
		return;
	}

	
	ca_setup_pool();

	

	return;
}


void
rg_boot()
{
	startup(Range_infor->port, TSS_OP_RANGESERVER, rg_handler);
}


static void
rg_regist()
{
	int	sockfd;
	RPCRESP	*resp;
	char	send_buf[2 * RPC_MAGIC_MAX_LEN + RANGE_ADDR_MAX_LEN + RANGE_PORT_MAX_LEN];
	
	
	sockfd = conn_open(Range_infor->rg_meta_ip, Range_infor->rg_meta_port);

	
	MEMSET(send_buf, 2 * RPC_MAGIC_MAX_LEN + RANGE_ADDR_MAX_LEN + RANGE_PORT_MAX_LEN);

	int idx = 0;
	PUT_TO_BUFFER(send_buf, idx, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_buf, idx, RPC_RG2MASTER_REPORT, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_buf, idx, Range_infor->rg_ip, RANGE_ADDR_MAX_LEN);
	PUT_TO_BUFFER(send_buf, idx, &(Range_infor->port), RANGE_PORT_MAX_LEN);

	Assert(idx == (2 * RPC_MAGIC_MAX_LEN + RANGE_ADDR_MAX_LEN + RANGE_PORT_MAX_LEN));
	
	write(sockfd, send_buf, idx);

	resp = conn_recv_resp(sockfd);

	if (resp->status_code != RPC_SUCCESS)
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

	status = write(sockfd, rbd, sizeof(REBALANCE_DATA));

	Assert (status == sizeof(REBALANCE_DATA));

	resp = conn_recv_resp(sockfd);

	if (resp->status_code != RPC_SUCCESS)
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

#ifdef MT_KFS_BACKEND
	if (!EXIST(tab_dir))
#else
	if (STAT(tab_dir, &st) != 0)
#endif
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

		MEMCPY(rbd_sstab->rbd_tabname, rbd->rbd_tabname, STRLEN(rbd->rbd_tabname));
		
		BLOCK *blk;

		int i = 0, rowno;
		int	*offset;
		char 	*rp;
		char	*sstabname;
		char	cmd_str[TABLE_NAME_MAX_LEN];
		
		while (TRUE)
		{
			blk = (BLOCK *)(rbd->rbd_data + i * BLOCKSIZE);

			
			for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
					rowno < blk->bnextrno; rowno++, offset--)
			{
				rp = (char *)blk + *offset;
			
				Assert(*offset < blk->bfreeoff);
			
				sstabname = row_locate_col(rp, TABLET_SSTABNAME_COLOFF_INROW, 
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
		
				MEMCPY(rbd_sstab->rbd_magic, RPC_RBD_MAGIC, RPC_MAGIC_MAX_LEN);
				MEMCPY(rbd_sstab->rbd_magic_back, RPC_RBD_MAGIC, RPC_MAGIC_MAX_LEN);
				
				rbd_sstab->rbd_opid = RBD_FILE_RECVER;

				MEMSET(rbd_sstab->rbd_sstabname,TABLE_NAME_MAX_LEN);
				MEMCPY(rbd_sstab->rbd_sstabname, sstabname, STRLEN(sstabname));
		
				rtn_stat = rg_rebalan_process_sender(rbd_sstab, rbd->rbd_min_tablet_rg,
								rbd->rbd_min_tablet_rgport);
				Assert(rtn_stat == TRUE);


				if (rtn_stat == TRUE)
				{
					MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
	
					sprintf(cmd_str, "rm -rf %s", tab_sstab_dir);
	
					if (system(cmd_str))
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

#ifdef MT_KFS_BACKEND
		Assert(!EXIST(tab_sstab_dir));
#else
		Assert(STAT(tab_sstab_dir, &st) != 0);
#endif		
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
		
			sstabname = row_locate_col(rp, TABLET_SSTABNAME_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLET, &namelen);

			sstabid = *(int *)row_locate_col(rp, TABLET_SSTABID_COLID_INROW,
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

			
			TABINFO_INIT(tabinfo, tab_sstab_dir, tabinfo->t_sinfo, minrowlen, 
					0, chkdata->chktab_tabid, sstabid);
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, chkdata->chktab_key_colid, 
				       VARCHAR, -1);
					
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

					key_in_blk = row_locate_col(rp, TABLE_KEY_FAKE_COLOFF_INROW, 
								minrowlen, &keylen_in_blk);

					if (rowno > 0)
					{
						result = row_col_compare(VARCHAR, key_in_blk, 
								keylen_in_blk, lastkey_in_blk, 
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
		
			sstabname = row_locate_col(rp, TABLET_SSTABNAME_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLET, &namelen);

			sstabid = *(int *)row_locate_col(rp, TABLET_SSTABID_COLID_INROW,
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

			
			TABINFO_INIT(tabinfo, tab_sstab_dir, tabinfo->t_sinfo, minrowlen, 
					0, cpctdata->compact_tabid, sstabid);
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, cpctdata->compact_key_colid, 
				       VARCHAR, -1);
					
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

				freeoff = (BLOCKSIZE - sstab_blk->bfreeoff - ROW_OFFSET_ENTRYSIZE * sstab_blk->bnextrno) / BLOCKSIZE;
				
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
						rowno < sstab_nxtblk->bnextrno; rowno++, offset--)
				{
					rp = (char *)sstab_nxtblk + *offset;

					Assert(*offset < sstab_nxtblk->bfreeoff);

					rlen = ROW_GET_LENGTH(rp, minrowlen);
					

					if ((sstab_blk->bfreeoff + rlen) > (BLOCKSIZE - BLK_TAILSIZE 
						- (ROW_OFFSET_ENTRYSIZE * (sstab_blk->bnextrno + 1))))
					{
						
						int *offtab = ROW_OFFSET_PTR(sstab_nxtblk);
						
						BACKMOVE(rp, sstab_nxtblk + BLKHEADERSIZE, sstab_nxtblk->bfreeoff - *offset);

						int j,k = sstab_nxtblk->bnextrno - rowno;
						
						for (j = sstab_nxtblk->bnextrno; (j > rowno) && (k > 0); j--,k--)
						{
							if (offtab[-(j-1)] < *offset)
							{
								break;
							}
						
							offtab[-(k-1)] = offtab[-(j-1)] - *offset + BLKHEADERSIZE;						
						}

						Assert((k == 0) && (j == rowno));
					}


					PUT_TO_BUFFER(sstab_blk + sstab_blk->bfreeoff, ign, rp, rlen);

					ROW_SET_OFFSET(sstab_blk, sstab_blk->bnextrno, sstab_blk->bfreeoff);
					
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


int 
main(int argc, char *argv[])
{
	char		*conf_path;
//	pthread_t	tid1, tid2;


	mem_init_alloc_regions();

	
	conf_path = RANGE_DEFAULT_CONF_PATH;
	
	conf_get_path(argc, argv, &conf_path);

	rg_setup(conf_path);

	//Trace = MEM_USAGE;
	Trace = 0;
	rg_boot();
	return TRUE;
}
