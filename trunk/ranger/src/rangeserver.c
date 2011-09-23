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



extern TSS	*Tss;
extern KERNEL	*Kernel;

#define DEFAULT_REGION_FLUSH_CHECK_INTERVAL 600 //10min

#ifdef MAXTABLE_BENCH_TEST

#define MT_RANGE_TABLE   "./rg_table"

#else

#define MT_RANGE_TABLE   "/mnt/ranger/rg_table"

#endif

#define	RANGE_CONF_PATH_MAX_LEN	64


typedef struct rg_info
{
	char	conf_path[RANGE_CONF_PATH_MAX_LEN];
	char	rg_meta_ip[RANGE_ADDR_MAX_LEN];
	int     rg_meta_port;
	char	rg_ip[RANGE_ADDR_MAX_LEN];
	int	port;
	int	flush_check_interval;
}RANGEINFO;

RANGEINFO *Range_infor = NULL;


static int
rg_fill_resd(TREE *command, COLINFO *colinfor, int totcol);

static void rg_regist();

static char *
rg_rebalancer(REBALANCE_DATA * rbd);



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
		traceprint("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	}
	
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

		
	if (STAT(tab_dir, &st) != 0)
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
		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			;
		}
		
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
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
		
		rlen = ROW_GET_LENGTH(rp, bp->bblk->bminlen);

		
		col_buf = MEMALLOCHEAP(rlen);
		MEMSET(col_buf, rlen);

		
		char	*filename = meta_get_coldata(bp, offset, sizeof(ROWFMT));
		MEMCPY(col_buf, filename, rlen - sizeof(ROWFMT));
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
			
			resp = conn_build_resp_byte(RPC_SUCCESS, rlen - sizeof(ROWFMT), col_buf);
		}
	}
	else
	{
		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			;
		}
		
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	if (col_buf != NULL)
	{
		MEMFREEHEAP(col_buf);
	}
	
	return resp;

}

int
rg_get_meta(char *req_buf, INSMETA **ins_meta, TABLEHDR **tab_hdr, COLINFO **col_info)
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

char *
rg_handler(char *req_buf)
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
	

	if ((req_op = rg_get_meta(req_buf, &ins_meta, &tab_hdr, &col_info)) == 0)
	{
		return NULL;
	}

	
	if (req_op & RPC_REQ_DROP_OP)
	{
		if (!parser_open(req_buf))
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
	
	req_buf += sizeof(INSMETA) + sizeof(TABLEHDR) + 
				ins_meta->col_num * sizeof(COLINFO);
	
	
	tss->tcol_info = col_info;
	tss->tmeta_hdr = ins_meta;

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
	    case DROP:
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
	MEMCPY(Range_infor->conf_path, conf_path, STRLEN(conf_path));

	MEMSET(port, 32);
	MEMSET(metaport, 32);

	conf_get_value_by_key(port, conf_path, CONF_RG_PORT);
	conf_get_value_by_key(Range_infor->rg_ip, conf_path, CONF_RG_IP);

	
	conf_get_value_by_key(Range_infor->rg_meta_ip, conf_path, CONF_META_IP);
	conf_get_value_by_key(metaport, conf_path, CONF_META_PORT);
	Range_infor->rg_meta_port = m_atoi(metaport, STRLEN(metaport));

	rg_port = m_atoi(port, STRLEN(port));
	if(rg_port != INDEFINITE)
	{
		Range_infor->port = rg_port;
	}
	else
	{
		Range_infor->port = RANGE_DEFAULT_PORT;
	}
	
	if (STAT(MT_RANGE_TABLE, &st) != 0)
	{
		MKDIR(status, MT_RANGE_TABLE, 0755);
	}	

	rg_regist();
	
	ca_setup_pool();
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

	status = WRITE(sockfd, rbd, sizeof(REBALANCE_DATA));

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
