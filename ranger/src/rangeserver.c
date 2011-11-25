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
extern char	*RgInsdelLogfile;

/* This struct will also be used at cli side */
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
	char	rginsdellogfile[256];
	int	rginsdellognum;
}RANGEINFO;


typedef struct _range_query_contex
{
	int	status;
	int	first_rowpos;
	int	end_rowpos;
	int	cur_rowpos;
	int	rowminlen;
	char	data[BLOCKSIZE];
}RANGE_QUERYCTX;

typedef struct sstab_scancontext
{
	int		rminlen;
	int		curblk;
	int		currow;
	int		stat;
	ORANDPLAN	*orplan;
	ORANDPLAN	*andplan;
	char		*sstab;
	RANGE_QUERYCTX	*rgsel;
}SSTAB_SCANCTX;

/* 
** Following definition id for the stat in SSTAB_SCANCTX, 
** return stat in the SSTable scan. 
*/
#define	SSTABSCAN_HIT_ROW	0x0001	/* Hit one row at least. */
#define	SSTABSCAN_BLK_IS_FULL	0x0002	/* The block hit the issue of overload. */



typedef struct tablet_scancontext
{
	int		stat;
	ORANDPLAN	*orplan;
	ORANDPLAN	*andplan;
	char		*tablet;
	char		*tabdir;
	int		rminlen;
	int		keycolid;
	int		tabid;
	int		connfd;
}TABLET_SCANCTX;

#define	SCANCTX_HIT_END		0x0001


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

static int
rg_process_orplan(ORANDPLAN	*cmd, char *rp, int minrowlen);

static int
rg_process_andplan(ORANDPLAN *cmd, char *rp, int minrowlen);

static int
rg_tabletscan(TABLET_SCANCTX *scanctx);

static int
rg_sstabscan(SSTAB_SCANCTX *scanctx);


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

	if (STAT(tab_dir, &st) != 0)
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

	if (STAT(tab_dir, &st) != 0)
	{
		MKDIR(status, tab_dir, 0755);

		if (status < 0)
		{
			goto exit;
		}
	}

	if (DEBUG_TEST(tss))
	{
		/* 
		** sstable name = "tablet name _ sstable_id", so sstable name
		** is unique in one table. 
		*/
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

	if (STAT(tab_dir, &st) != 0)
	{
		/* Flag if it's the first insertion. */
		ins_meta->status |= INS_META_1ST;
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

		LOGREC	logrec;

		log_build(&logrec, LOG_INSERT, tabinfo->t_insdel_old_ts_lo,
					tabinfo->t_insdel_new_ts_lo,
					tabinfo->t_sstab_name, NULL, 
					tabinfo->t_row_minlen, tabinfo->t_tabid,
					tabinfo->t_sstab_id);
		
		log_insert_insdel(RgInsdelLogfile, &logrec, rp, rp_idx);
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
	int		offset;
	char   		*col_buf;
	int 		rlen;
	int		buf_spin;


	Assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	col_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	ins_meta = tabinfo->t_insmeta;
	col_info = tabinfo->t_colinfo;
	buf_spin = FALSE;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	if (STAT(tab_dir, &st) != 0)
	{
		goto exit;
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

	if (STAT(tab_dir, &st) != 0)
	{
		goto exit; 
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
		traceprint("tab_dir =%s \n", tab_dir);
	}
	
	keycol = par_get_colval_by_colid(command, tabinfo->t_key_colid, &keycolen);

	TABINFO_INIT(tabinfo, ins_meta->sstab_name, tabinfo->t_sinfo, 
			tabinfo->t_row_minlen, 0, tabinfo->t_tabid, 
			tabinfo->t_sstab_id);
	SRCH_INFO_INIT(tabinfo->t_sinfo, keycol, keycolen, 1, VARCHAR, -1);

	/* Case delete data.*/
	if (tabinfo->t_stat & TAB_DEL_DATA)
	{
		P_SPINLOCK(BUF_SPIN);
		buf_spin = TRUE;
		
		rtn_stat = blkdel(tabinfo);

		if (rtn_stat)
		{
			goto exit;
		}
	}
	else
	{
		/* Case select data. */
		bp = blkget(tabinfo);
//		offset = blksrch(tabinfo, bp);

		/* Just clear the bit 'BUF_KEPT'. */
		bufunkeep(bp->bsstab);
		
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
	
	/* The selecting value is not exist. */
	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		goto exit;
	}

	if (!(tabinfo->t_stat & TAB_DEL_DATA))
	{
		/* TODO: rp, rlen just be the future work setting. */
		char *rp = (char *)(bp->bblk) + offset;
		char *value;
		
		// char	*filename = meta_get_coldata(bp, offset, sizeof(ROWFMT));

		value = row_locate_col(rp, -2, bp->bblk->bminlen, &rlen);

		/* Building the response information. */
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
			/* Just return a SUCCESS. */
			resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);

			LOGREC	logrec;

			log_build(&logrec, LOG_DELETE, tabinfo->t_insdel_old_ts_lo, 
						tabinfo->t_insdel_new_ts_lo,
						tabinfo->t_sstab_name, NULL, 
						tabinfo->t_row_minlen, 
						tabinfo->t_tabid, 
						tabinfo->t_sstab_id);
			
			log_insert_insdel(RgInsdelLogfile, &logrec, 
						tabinfo->t_cur_rowp,
						tabinfo->t_cur_rowlen);

			MEMFREEHEAP(tabinfo->t_cur_rowp);
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


/* Following define is for the status sending to the client. */
#define	DATA_CONT	0x0001	
#define DATA_DONE	0x0002
#define DATA_EMPTY	0x0004


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
	int		offset;
	char		last_sstab[SSTABLE_NAME_MAX_LEN];
	int		left_expand;
	int		right_expand;
	B_SRCHINFO	srchinfo;


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

	if (STAT(tab_dir, &st) != 0)
	{
		goto exit;		
	}

	if (DEBUG_TEST(tss))
	{
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

	if (STAT(tab_dir, &st) != 0)
	{
		goto exit; 
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

	/* Same with the left key. */
	right_rangekey = par_get_colval_by_colid(command, 2, &right_keylen);

	if ((right_keylen == 1) && (!strncasecmp("*", right_rangekey, 
							right_keylen)))
	{
		right_expand = TRUE;
	}
	
	TABINFO_INIT(tabinfo, ins_meta->sstab_name, tabinfo->t_sinfo, 
			tabinfo->t_row_minlen, 0, tabinfo->t_tabid, 
			tabinfo->t_sstab_id);
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
			bufunkeep(bp->bsstab);
			ex_raise(EX_ANY);
		}
		
		offset = tabinfo->t_rowinfo->roffset;
	}

	RANGE_QUERYCTX	rgsel_cont;

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
			TABINFO_INIT(tabinfo, tabinfo->t_sstab_name,
					tabinfo->t_sinfo, tabinfo->t_row_minlen, 
					0, tabinfo->t_tabid, tabinfo->t_sstab_id);
			
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



char *
rg_selwheretab(TREE *command, SELWHERE *selwhere, int fd)
{
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	int		fd1;
	int		rtn_stat;
	TABLEHDR	tab_hdr;
	int		sstab_rlen;
	int		sstab_idx;
	char		*resp;
	int		resp_size;
	char		*rp;
	int		namelen;
	char		*rg_addr;
	int		rg_port;
	TABLET_SCANCTX	*tablet_scanctx;
	RANGE_QUERYCTX	rgsel_cont;
	char		rg_tab_dir[TABLE_NAME_MAX_LEN];


	Assert(command);

	rtn_stat	= FALSE;
	sstab_rlen 	= 0;
	sstab_idx 	= 0;
	tablet_scanctx 	= NULL;
	tab_name	= command->sym.command.tabname;
	tab_name_len 	= command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(rg_tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	
	str1_to_str2(rg_tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	if (!(STAT(tab_dir, &st) == 0))
	{
		traceprint("Table %s is not exist.\n", tab_name);
		goto exit;
	}
	
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
		traceprint("Table %s has no data.\n", tab_name);
		goto exit;
	}

	if (tab_hdr.tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		goto exit;
	}

	if (tab_hdr.tab_tablet == 0)
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

	/* Check if we got the first tablet. */
	int		leftbegin = FALSE;
	/* Check if we got the last tablet. */	
	int 		rightend = FALSE;

	int listenfd = conn_socket_open(Range_infor->bigdataport);

	if (!listenfd)
	{
		goto exit;
	}

	resp = conn_build_resp_byte(RPC_BIGDATA_CONN, sizeof(int),
					(char *)(&(Range_infor->bigdataport)));
	resp_size = conn_get_resp_size((RPCRESP *)resp);	
	write(fd, resp, resp_size);
	conn_destroy_resp_byte(resp);

	int	connfd;

	connfd = conn_socket_accept(listenfd);

	if (connfd < 0)
	{
		traceprint("hit accept issue\n");
		goto exit;
	}
		

	par_fill_colinfo(tab_dir, tab_hdr.tab_col, command);

	tablet_scanctx = (TABLET_SCANCTX *)MEMALLOCHEAP(sizeof(TABLET_SCANCTX));
	MEMSET(tablet_scanctx, sizeof(TABLET_SCANCTX));

	tablet_scanctx->andplan = par_get_andplan(command);
	tablet_scanctx->orplan = par_get_orplan(command);
	tablet_scanctx->connfd = connfd;
	

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

			wanted_tablet = (!leftbegin)?  selwhere->lefttabletname 
						   : selwhere->righttabletname;			

			result = row_col_compare(VARCHAR, tabletname, 
						STRLEN(tabletname), 
						wanted_tablet,
						STRLEN(wanted_tablet));
			

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
				tablet_scanctx->keycolid = tab_hdr.tab_key_colid;
				tablet_scanctx->rminlen = tab_hdr.tab_row_minlen;
				tablet_scanctx->tabdir = rg_tab_dir;
				tablet_scanctx->tabid = tab_hdr.tab_id;

				/* 
				** Scan the tablet and get the sstabname for the 
				** every row to be wanted. 
				*/
				if (!rg_tabletscan(tablet_scanctx))
				{
					goto exit;
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
	
	rgsel_cont.status = DATA_EMPTY;

 	resp = conn_build_resp_byte(RPC_SUCCESS, sizeof(RANGE_QUERYCTX), 
					(char *)&rgsel_cont);

	resp_size = conn_get_resp_size((RPCRESP *)resp);
	  
	write(connfd, resp, resp_size);			

	conn_destroy_resp_byte(resp);	

	
	char	resp_cli[8];
	MEMSET(resp_cli, 8);
	int n = conn_socket_read(connfd, resp_cli, 8);

	if (n != 8)
	{
		goto exit;
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


static int
rg_process_orplan(ORANDPLAN	*cmd, char *rp, int minrowlen)
{
	int		coloffset;
	int		length;
	char		*colp;
	char		*leftval;
	int		leftvallen;
	char		*rightval;
	int		rightvallen;
	int		result;
	int		rtn_stat;
	int		coltype;
	SRCHCLAUSE	*srchclause;

	
	if (cmd == NULL)
	{
		return TRUE;
	}

	rtn_stat = FALSE;
	
	while(cmd)
	{
		srchclause = &(cmd->orandsclause);
		
		coloffset = srchclause->scterms->left->sym.resdom.coloffset;
		coltype = srchclause->scterms->left->sym.resdom.coltype;

		colp = row_locate_col(rp, coloffset, minrowlen, &length);

		leftval = srchclause->scterms->left->right->sym.constant.value;
		leftvallen = srchclause->scterms->left->right->sym.constant.len;
		rightval = srchclause->scterms->left->right->sym.constant.rightval;
		rightvallen = srchclause->scterms->left->right->sym.constant.rightlen;

		result = row_col_compare(coltype, colp, length, leftval, 
					leftvallen);

		if (result == LE)
		{
			cmd = cmd->orandplnext;
			continue;
		}

		result = row_col_compare(coltype, colp, length, rightval, 
					rightvallen);

		if (result == GR)
		{
			cmd = cmd->orandplnext;
			continue;
		}

		
		rtn_stat = TRUE;

		break;
		
	}

	return rtn_stat;
}

static int
rg_process_andplan(ORANDPLAN *cmd, char *rp, int minrowlen)
{
	int		coloffset;
	int		length;
	char		*colp;
	char		*leftval;
	int		leftvallen;
	char		*rightval;
	int		rightvallen;
	int		result;
	int		rtn_stat;
	int		coltype;
	SRCHCLAUSE	*srchclause;

	
	if (cmd == NULL)
	{
		return TRUE;
	}

	rtn_stat = FALSE;
	
	while(cmd)
	{
		srchclause = &(cmd->orandsclause);
		
		coloffset = srchclause->scterms->left->sym.resdom.coloffset;
		coltype = srchclause->scterms->left->sym.resdom.coltype;

		colp = row_locate_col(rp, coloffset, minrowlen, &length);

		leftval = srchclause->scterms->left->right->sym.constant.value;
		leftvallen = srchclause->scterms->left->right->sym.constant.len;
		rightval = srchclause->scterms->left->right->sym.constant.rightval;
		rightvallen = srchclause->scterms->left->right->sym.constant.rightlen;

		result = row_col_compare(coltype, colp, length, leftval, 
					leftvallen);

		if (result == LE)
		{
			break;
		}

		result = row_col_compare(coltype, colp, length, rightval, 
					rightvallen);

		if (result == GR)
		{
			break;
		}

		cmd = cmd->orandplnext;
		
	}

	if (cmd == NULL)
	{
		rtn_stat = TRUE;
	}

	return rtn_stat;
}


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

			Assert(*offset < blk->bfreeoff);

			if (   rg_process_orplan(scanctx->orplan, rp, minrowlen)
			    && rg_process_andplan(scanctx->andplan, rp, 
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
			
			TABINFO_INIT(tabinfo, tab_sstab_dir, tabinfo->t_sinfo, 
					minrowlen, 0, scanctx->tabid, sstabid);
			
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0,
					scanctx->keycolid, VARCHAR, -1);
					
			bp = blk_getsstable(tabinfo);	

			sstab_scanctx.sstab = (char *)(bp->bblk);

scan_cont:			
			rg_sstabscan(&sstab_scanctx);

			if (!(sstab_scanctx.stat & SSTABSCAN_BLK_IS_FULL))
			{
				/* Finding the next sstable to fill the sending block. */
				bufunkeep(bp->bsstab);
				
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
			  
			write(scanctx->connfd, resp, resp_size);			

			conn_destroy_resp_byte(resp);	

			
			/* TODO: placeholder for the TCP/IP check. */
			MEMSET(resp_cli, 8);
			int n = conn_socket_read(scanctx->connfd, resp_cli, 8);

			if (n != 8)
			{
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
				  
				write(scanctx->connfd, resp, resp_size);			

				conn_destroy_resp_byte(resp);	

				
				/* TODO: placeholder for the TCP/IP check. */
				MEMSET(resp_cli, 8);
				int n = conn_socket_read(scanctx->connfd,
							resp_cli, 8);

				if (n != 8)
				{
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

int
rg_get_meta(char *req_buf, INSMETA **ins_meta, SELRANGE **sel_rg,
			TABLEHDR **tab_hdr, COLINFO **col_info)
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
	resp 		= NULL;
	
	if ((req_op = rg_get_meta(req_buf, &ins_meta, &sel_rg, &tab_hdr, 
					&col_info)) == 0)
	{
		return NULL;
	}

	/* 
	** For performance, We see the Drop Table as a special case to process it. 
	** Maybe there are still some better solution.
	*/
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

		resp = rg_droptab(command);

		goto finish;
	}

	if (req_op & RPC_REQ_SELECWHERE_OP)
	{
		if (!parser_open(req_buf + sizeof(SELWHERE)))
		{
			parser_close();
			tss->tstat |= TSS_PARSER_ERR;
			traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
			return NULL;
		}

		command = tss->tcmd_parser;

		Assert(command->sym.command.querytype == SELECTWHERE);

		resp = rg_selwheretab(command, (SELWHERE *)(req_buf), fd);

		goto finish;
	}

	/* Rebalancer case. */
	if (req_op & RPC_REQ_REBALANCE_OP)
	{
		return rg_rebalancer((REBALANCE_DATA *)(req_buf - RPC_MAGIC_MAX_LEN));
	}


	/* process with heart beat */
	if (req_op & RPC_REQ_M2RHEARTBEAT_OP)
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

	/* Initialize the meta data for build RESDOM. */
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

		resp = rg_selrangetab(command, tabinfo, fd);

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
		
		log_build(&logrec, LOG_END, 0, 0, tabinfo->t_sstab_name, NULL,
				0, 0, 0);

		log_insert_sstab_split(RgLogfile, &logrec, SPLIT_LOG);
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

	Range_infor = MEMALLOCHEAP(sizeof(RANGEINFO));
	MEMSET(Range_infor, sizeof(RANGEINFO));
	
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

	rg_regist();

	char	rgname[64];
	RgLogfile = Range_infor->rglogfiledir;
	
	MEMSET(RgLogfile, 256);
	MEMCPY(RgLogfile, LOG_FILE_DIR, STRLEN(LOG_FILE_DIR));

	MEMSET(rgname, 64);
	sprintf(rgname, "%s%d", Range_infor->rg_ip, Range_infor->port);

	str1_to_str2(RgLogfile, '/', rgname);

	str1_to_str2(RgLogfile, '/', "log");

	if (!(STAT(RgLogfile, &st) == 0))
	{
		traceprint("Log file %s is not exist.\n", RgLogfile);
		return;
	}

	RgInsdelLogfile = Range_infor->rginsdellogfile;
	log_get_latest_rginsedelfile(RgInsdelLogfile, Range_infor->rg_ip,
					Range_infor->port);

	if (strcmp(RgLogfile, RgInsdelLogfile) == 0)
	{
		sprintf(RgInsdelLogfile, "%s0", RgInsdelLogfile);

		OPEN(fd, RgInsdelLogfile, (O_CREAT|O_WRONLY|O_TRUNC));
		CLOSE(fd);
	}

	int idxpos = str1nstr(RgInsdelLogfile, RgLogfile, STRLEN(RgLogfile));

	Range_infor->rginsdellognum = m_atoi(RgInsdelLogfile + idxpos, 
					STRLEN(RgInsdelLogfile) - idxpos);

	RgBackup = Range_infor->rgbackup;
	
	MEMSET(RgBackup, 256);
	MEMCPY(RgBackup, BACKUP_DIR, STRLEN(BACKUP_DIR));

	str1_to_str2(RgBackup, '/', rgname);

	if (STAT(RgBackup, &st) != 0)
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
		char	cmd_str[TABLE_NAME_MAX_LEN];
		
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

			
			TABINFO_INIT(tabinfo, tab_sstab_dir, tabinfo->t_sinfo,
				minrowlen, 0, chkdata->chktab_tabid, sstabid);
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

			
			TABINFO_INIT(tabinfo, tab_sstab_dir, tabinfo->t_sinfo,
					minrowlen, 0, cpctdata->compact_tabid, 
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
