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
#include "region/rangeserver.h"
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



extern TSS	*Tss;
extern KERNEL	*Kernel;

#define DEFAULT_REGION_FLUSH_CHECK_INTERVAL 600 //10min

#define MT_RANGE_TABLE   "./rg_table"

#define	RANGE_CONF_PATH_MAX_LEN	64


typedef struct rg_info
{
	char	conf_path[RANGE_CONF_PATH_MAX_LEN];
	int	port;
	int	flush_check_interval;
}RANGEINFO;

RANGEINFO *Range_infor = NULL;


static int
rg_fill_resd(TREE *command, COLINFO *colinfor, int totcol);


char *
rg_instab(TREE *command, TABINFO *tabinfo)
{
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
	char		col_off_idx;
	int		col_offset;
	char		*col_val;
	int		col_len;
	int		col_num;
	INSMETA 	*ins_meta;
	COLINFO 	*col_info;
	char		*resp_buf;
	int		resp_len;


	assert(command);

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

	
	printf("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

		
	if (STAT(tab_dir, &st) != 0)
	{
		ins_meta->status |= INS_META_1ST;
	}

	printf("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	printf("tab_dir =%s \n", tab_dir);


	
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
			assert(col_offset > 0);

			if (!(col_offset > 0))
			{
				ex_raise(EX_ANY);
			}
			
			
			

			if (col_num > 0)
			{
				
				rp_idx += sizeof(int);
				col_offset = -1;
			}
			
			assert(ins_meta->varcol_num == col_num);

			if (ins_meta->varcol_num != col_num)
			{
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

	blkins(tabinfo, rp);

	rtn_stat = TRUE;

exit:

	resp_len = 0;
	resp_buf = NULL;
	
	if (tabinfo->t_stat & TAB_SSTAB_SPLIT)
	{
		resp_len = tabinfo->t_insrg->new_keylen + SSTABLE_NAME_MAX_LEN + sizeof(int);
		resp_buf = (char *)MEMALLOCHEAP(resp_len);

		MEMSET(resp_buf, resp_len);

		int i = 0;

		printf("tabinfo->t_insrg->new_sstab_name = %s \n", tabinfo->t_insrg->new_sstab_name);
		//MEMCPY(resp_buf, tabinfo->t_insrg->new_sstab_name, STRLEN(tabinfo->t_insrg->new_sstab_name));
		PUT_TO_BUFFER(resp_buf, i, tabinfo->t_insrg->new_sstab_name, SSTABLE_NAME_MAX_LEN);
		//i += SSTABLE_NAME_MAX_LEN;
		PUT_TO_BUFFER(resp_buf, i, &tabinfo->t_insmeta->res_sstab_id, sizeof(int));
		PUT_TO_BUFFER(resp_buf, i, tabinfo->t_insrg->new_sstab_key, tabinfo->t_insrg->new_keylen);
		
//		printf("tabinfo->t_insmeta->sstab_id = %d,  tabinfo->t_insmeta->res_sstab_id = %d\n", tabinfo->t_insmeta->sstab_id, tabinfo->t_insmeta->res_sstab_id);
		assert(resp_len == i);
	}
	
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, resp_len, resp_buf);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;

}


char *
rg_seltab(TREE *command, TABINFO *tabinfo)
{
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


	assert(command);

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

	
	printf("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	sstable = ins_meta->sstab_name;

	str1_to_str2(tab_dir, '/', sstable);
	MEMSET(ins_meta->sstab_name, SSTABLE_NAME_MAX_LEN);
	MEMCPY(ins_meta->sstab_name, tab_dir, STRLEN(tab_dir));

	if (STAT(tab_dir, &st) != 0)
	{
		goto exit; 
	}

	printf("ins_meta->sstab_name =%s \n", ins_meta->sstab_name);
	printf("tab_dir =%s \n", tab_dir);

	keycol = par_get_colval_by_colid(command, tabinfo->t_key_colid, &keycolen);

	TABINFO_INIT(tabinfo, ins_meta->sstab_name, tabinfo->t_sinfo, tabinfo->t_row_minlen, 
			0, tabinfo->t_tabid, tabinfo->t_sstab_id);
	SRCH_INFO_INIT(tabinfo->t_sinfo, keycol, keycolen, 1, VARCHAR, -1);

	bp = blkget(tabinfo);
	offset = blksrch(tabinfo, bp);

	
	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		goto exit;
	}

	
	char *rp = (char *)(bp->bblk) + offset;
	int rlen = ROW_GET_LENGTH(rp, bp->bblk->bminlen);

	
	col_buf = MEMALLOCHEAP(rlen);
	MEMSET(col_buf, rlen);

	
	char	*filename = meta_get_coldata(bp, offset, sizeof(ROWFMT));
	MEMCPY(col_buf, filename, rlen - sizeof(ROWFMT));
	
	rtn_stat = TRUE;

	exit:
	if (rtn_stat)
	{
		resp = conn_build_resp_byte(RPC_SUCCESS, rlen - sizeof(ROWFMT), col_buf);
	}
	else
	{
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
	if (!conn_chk_reqmagic(req_buf))
	{
		return FALSE;
	}

	*ins_meta = (INSMETA *)req_buf;
	req_buf += sizeof(INSMETA);

	*tab_hdr = (TABLEHDR *)req_buf;
	req_buf += sizeof(TABLEHDR);

	*col_info = (COLINFO *)req_buf;

	return TRUE;	
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

			assert(col_info);

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
	

	if (!rg_get_meta(req_buf, &ins_meta, &tab_hdr, &col_info))
	{
		return NULL;
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
	
	
	
	tss->tcol_info = col_info;
	tss->tmeta_hdr = ins_meta;
	
	req_buf += sizeof(INSMETA) + sizeof(TABLEHDR) + 
				ins_meta->col_num * sizeof(COLINFO);
	
	parser_open(req_buf);

	command = tss->tcmd_parser;
	resp_buf_idx = 0;
	resp_buf_size = 0;

	copy.tabinfo= tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

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
	
	tabinfo_push(tabinfo);

	switch(command->sym.command.querytype)
	{
	    case TABCREAT:
				
			printf("I got here - CREATING TABLE\n");
			break;

	    case INSERT:
	    		tabinfo->t_stat |= TAB_INS_DATA;
			resp = rg_instab(command, tabinfo);
			printf("I got here - INSERTING TABLE\n");
	    	break;

	    case CRTINDEX:
	    	break;

	    case SELECT:
	    		tabinfo->t_stat |= TAB_SRCH_DATA;
			resp = rg_seltab(command, tabinfo);
			printf("I got here - SELECTING TABLE\n");
	    	break;

	    case DELETE:
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
			assert(tabinfo->t_stat & TAB_SSTAB_SPLIT);

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
	int	port;

	Range_infor = MEMALLOCHEAP(sizeof(RANGEINFO));
	MEMCPY(Range_infor->conf_path, conf_path, sizeof(conf_path));

	conf_get_value_by_key((char *)&port, conf_path, CONF_PORT_KEY);

	if(port != INDEFINITE)
	{
		Range_infor->port = atoi((char *)&port);
	}
	else
	{
		Range_infor->port = RANGE_DEFAULT_PORT;
	}
	
	if (STAT(MT_RANGE_TABLE, &st) != 0)
	{
		MKDIR(status, MT_RANGE_TABLE, 0755);
	}	

	ca_setup_pool();
}


void
rg_boot()
{
	startup(Range_infor->port, TSS_OP_RANGESERVER, rg_handler);
}


int 
main(int argc, char *argv[])
{
	char		*conf_path;
//	pthread_t	tid1, tid2;


	mem_init_alloc_regions();

	Trace = 0;

	conf_path = RANGE_DEFAULT_CONF_PATH;
	conf_get_path(argc, argv, &conf_path);

	rg_setup(conf_path);

	rg_boot();
	return TRUE;
}
