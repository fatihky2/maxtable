/*
** metaserver.c 2010-06-15 xueyingfei
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
#include "master/metaserver.h"
#include "region/rangeserver.h"
#include "conf.h"
#include "netconn.h"
#include "parser.h"
#include "tss.h"
#include "token.h"
#include "memcom.h"
#include "row.h"
#include "file_op.h"
#include "strings.h"
#include "type.h"
#include "buffer.h"
#include "block.h"
#include "cache.h"
#include "metadata.h"
#include "tablet.h"
#include "trace.h"
#include "session.h"
#include "tabinfo.h"
#include "sstab.h"


extern	TSS	*Tss;

#define META_CONF_PATH_MAX_LEN   64
#define DEFAULT_DUPLICATE_NUM 1
#define MIN_REGION_AVAILABLE_SIZE 100 //Unit is MB
#define DEFAULT_MASTER_FLUSH_CHECK_INTERVAL 600 //10Min

#define SSTAB_FREE	0
#define SSTAB_USED	1
#define SSTAB_RESERVED	2



int	*sstab_map;

TAB_SSTAB_MAP *tab_sstabmap;


#define SSTAB_MAP_SET(i, flag)	(sstab_map[i] = flag)

#define SSTAB_MAP_FREE(i)	(sstab_map[i] == SSTAB_FREE)
#define SSTAB_MAP_USED(i)	(sstab_map[i] == SSTAB_USED)
#define SSTAB_MAP_RESERV(i)	(sstab_map[i] == SSTAB_RESERVED)

typedef struct master_infor
{
	char		conf_path[META_CONF_PATH_MAX_LEN];
	int		port;
	SVR_IDX_FILE	rg_list;
}MASTER_INFOR;


MASTER_INFOR *Master_infor = NULL;

struct stat st;

#define MT_META_TABLE   "./meta_table"
#define MT_META_REGION  "./rg_server"
#define MT_META_INDEX   "./index"	


static void 
meta_bld_sysrow(char *rp, int rlen, int tabletid, int sstabnum);

static int
meta_get_free_sstab();

static void
meta_prt_sstabmap(int begin, int end);

static int
meta_collect_rg(char * req_buf);


void
meta_bld_rglist(char *filepath)
{
	;
}


void 
meta_server_setup(char *conf_path)
{
	int	status;
	char	port[32];
	char	rang_server[256];
	int	fd;
	SVR_IDX_FILE	*filebuf;


	MEMSET(port, 32);
	Master_infor = MEMALLOCHEAP(sizeof(MASTER_INFOR));
	MEMCPY(Master_infor->conf_path, conf_path, STRLEN(conf_path));

	conf_get_value_by_key(port, conf_path, CONF_PORT_KEY);

	if(port != INDEFINITE)
	{
		Master_infor->port = m_atoi(port, STRLEN(port));
	}
	else
	{
		Master_infor->port = META_DEFAULT_PORT;
	}

	if (STAT(MT_META_TABLE, &st) != 0)
	{
		MKDIR(status, MT_META_TABLE, 0755);
	}
	else
	{
		
		;
	}

	if (STAT(MT_META_REGION, &st) != 0)
	{
		MKDIR(status, MT_META_REGION, 0755); 

		MEMSET(rang_server, 256);
		MEMCPY(rang_server, MT_META_REGION, STRLEN(MT_META_REGION));
		str1_to_str2(rang_server, '/', "rangeserverlist");
	
		OPEN(fd, rang_server, (O_CREAT|O_WRONLY|O_TRUNC));
		
		
//		WRITE(fd, rang_server, STRLEN(rang_server));

		filebuf = (SVR_IDX_FILE *)MEMALLOCHEAP(SVR_IDX_FILE_SIZE);
		MEMSET(filebuf, SVR_IDX_FILE_SIZE);

		filebuf->freeoff = SVR_IDX_FILE_HDR;
		filebuf->nextrno = 0;
		filebuf->pad2[0] = 'r';
		filebuf->pad2[1] = 'g';
		filebuf->pad2[2] = 'l';
		filebuf->pad2[3] = 'i';
		filebuf->pad2[4] = 's';
		filebuf->pad2[5] = 't';
		
		filebuf->stat = 0;
	
		WRITE(fd, filebuf, SVR_IDX_FILE_SIZE);

		MEMFREEHEAP(filebuf);
		
		CLOSE(fd);		
	}
	else
	{
		
		MEMSET(rang_server, 256);
		MEMCPY(rang_server, MT_META_REGION, STRLEN(MT_META_REGION));
		str1_to_str2(rang_server, '/', "rangeserverlist");
	
		OPEN(fd, rang_server, (O_RDONLY));
		
		
//		WRITE(fd, rang_server, STRLEN(rang_server));

		MEMSET(&(Master_infor->rg_list), SVR_IDX_FILE_BLK);

		READ(fd, &(Master_infor->rg_list), SVR_IDX_FILE_BLK);

		
		CLOSE(fd);		
		
		
		;
	}

	if (STAT(MT_META_INDEX, &st) != 0)
	{
		MKDIR(status, MT_META_INDEX, 0755); 
	}

	tab_sstabmap = NULL;
	sstab_map = NULL;
	
	ca_setup_pool();

	return;
}


void
meta_add_server(TREE *command)
{
	char	rang_server[256];
	int		fd;
	SVR_IDX_FILE	*filebuf;
	
	MEMSET(rang_server, 256);
	MEMCPY(rang_server, MT_META_REGION, STRLEN(MT_META_REGION));
	str1_to_str2(rang_server, '/', "rangeserverlist");

	filebuf = (SVR_IDX_FILE *)MEMALLOCHEAP(SVR_IDX_FILE_BLK);
	
	OPEN(fd, rang_server, (O_CREAT|O_WRONLY|O_TRUNC));

	READ(fd,filebuf,SVR_IDX_FILE_BLK);
	

	PUT_TO_BUFFER(filebuf->data, filebuf->freeoff, command->sym.command.tabname,
					command->sym.command.tabname_len);
	PUT_TO_BUFFER(filebuf->data, filebuf->freeoff, command->left->right->sym.constant.value,
					command->left->right->sym.constant.len);

	WRITE(fd, filebuf, SVR_IDX_FILE_BLK);

	CLOSE(fd);	

	MEMFREEHEAP(filebuf);

	return;
}


char *
meta_crtab(TREE *command)
{
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[256];
	char    	tab_dir1[256];	
	int		status;
	int		fd;
	TREE		*col_tree;
	char		col_buf[256];	
	int		col_buf_idx;
	int		minlen;
	int		varcol;		
	int		colcnt;
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	int		tab_key_coloff;
	int		tab_key_colid;
	int		tab_key_coltype;
	COLINFO	col_info;
	char *resp;


	assert(command);
	
	rtn_stat = FALSE;
	resp = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	
	MEMSET(tab_dir, 256);
	MEMSET(tab_dir1, 256);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	if (STAT(tab_dir, &st) != 0)
	{
		MKDIR(status, tab_dir, 0755);        

		if (status < 0)
		{
			goto exit;
		}
	}
	
	
	MEMCPY(tab_dir1, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_dir1, '/', "syscolumns");

	OPEN(fd, tab_dir1, (O_CREAT|O_WRONLY|O_TRUNC));

	if (fd < 0)
	{
		goto exit;
	}

	
	//row_buf = MEMALLOCHEAP(4 * sizeof(int) + 64);
	col_buf_idx = 0;
	minlen = sizeof(ROWFMT);
	varcol = 0;
	colcnt = 0;
	col_tree = command->left;
	while (col_tree)
	{
	        MEMSET(&col_info, sizeof(COLINFO));

		
		//row_buf_idx = 0;

		col_info.col_id = col_tree->sym.resdom.colid;
		col_info.col_len = col_tree->sym.resdom.colen;
		MEMCPY(col_info.col_name, col_tree->sym.resdom.colname,
		STRLEN(col_tree->sym.resdom.colname));          
				
		
		if (col_tree->sym.resdom.colen > 0)
		{
			
			if (col_tree->sym.resdom.colid == 1)
			{
				tab_key_coloff = minlen;
				tab_key_colid = 1;
				tab_key_coltype = col_tree->sym.resdom.coltype;
			}
			
			
                        col_info.col_offset = minlen;
			
			minlen += col_tree->sym.resdom.colen;			
		}
		else
		{
			
			if (col_tree->sym.resdom.colid == 1)
			{
				tab_key_coloff = -(varcol+1);
				tab_key_colid = 1;
				tab_key_coltype = col_tree->sym.resdom.coltype;
			}
			
			
			col_info.col_offset = -(varcol+1);

			varcol++;
		}

		colcnt ++;

		col_info.col_type = col_tree->sym.resdom.coltype;
		
		
		PUT_TO_BUFFER(col_buf, col_buf_idx, &col_info, sizeof(COLINFO));
		
		col_tree = col_tree->left;
	}
	
        
	
        
	WRITE(fd, col_buf, col_buf_idx);

	CLOSE(fd);

	
	MEMSET(tab_dir1, 256);

	
	MEMCPY(tab_dir1, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_dir1, '/', "sysobjects");
	
	OPEN(fd, tab_dir1, (O_CREAT|O_WRONLY|O_TRUNC));

	if (fd < 0)
	{
		goto exit;
	}

	tab_hdr = MEMALLOCHEAP(sizeof(TABLEHDR));

	
	tab_hdr->tab_id = 1;
	MEMCPY(tab_hdr->tab_name, tab_name, tab_name_len);
	tab_hdr->tab_tablet = 0;
	tab_hdr->tab_sstab = 0;
	tab_hdr->tab_row_minlen = minlen;
	tab_hdr->tab_key_coloff = tab_key_coloff;
	tab_hdr->tab_key_colid = tab_key_colid;
	tab_hdr->tab_key_coltype = tab_key_coltype;
	tab_hdr->tab_col = colcnt;
	tab_hdr->tab_varcol = varcol;
	tab_hdr->offset_c1 = 0;
	tab_hdr->offset_c2 = -1;
	
	
	WRITE(fd, tab_hdr, sizeof(TABLEHDR));

	MEMFREEHEAP(tab_hdr);

	CLOSE(fd);


	

	int *sstab_map_tmp;
	
	
	sstab_map_tmp = (int *)malloc(SSTAB_MAP_SIZE);

	

	MEMSET(sstab_map_tmp, 1024 * 1024 * sizeof(int));
	
	MEMSET(tab_dir1, 256);

	
	MEMCPY(tab_dir1, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_dir1, '/', "sstabmap");
	
	OPEN(fd, tab_dir1, (O_CREAT|O_WRONLY|O_TRUNC));

	if (fd < 0)
	{
		goto exit;
	}
	
	
	WRITE(fd, sstab_map_tmp, SSTAB_MAP_SIZE);

	CLOSE(fd);

	free(sstab_map_tmp);
	
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


void
meta_ins_systab(char *systab, char *row)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	char		*key;
	int		ign;
	BLK_ROWINFO	blk_rowinfo;
	
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));
	
	tabinfo_push(tabinfo);

	minrowlen = sizeof(ROWFMT) + 3 * sizeof(int);

	
	key = row_locate_col(row, (sizeof(ROWFMT) + sizeof(int)), minrowlen, &ign);
	
	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, TAB_META_SYSTAB, 0 ,0);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, 4, 1, INT4, sizeof(ROWFMT) + sizeof(int));
			
	blkins(tabinfo, row);

	tabinfo_pop();
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
}




char *
meta_instab(TREE *command, TABINFO *tabinfo)
{
	
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	char		tab_tabletschm_dir[TABLE_NAME_MAX_LEN];
	int		fd1;
	int		rtn_stat;
	TABLEHDR	tab_hdr;
	char		*keycol;
	int		keycolen;
	int		sstab_idx;
	char		sstab_name[SSTABLE_NAME_MAX_LEN];
	int		sstab_namelen;
	char   		*col_buf;
	int		col_buf_idx;
	int		col_buf_len;
	char		*resp;
	char		tablet_name[32];
	int		tablet_min_rlen;
	int		namelen;
	char		*name;
	char		*rp;
	char		*tabletschm_rp;
	int		status;
	char		*rg_addr;
	int		rg_port;
	int		sstab_id;
	int		res_sstab_id;
	RANGE_PROF	*rg_prof;


	assert(command);

	rtn_stat = FALSE;
	sstab_idx = 0;
	col_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	assert(STAT(tab_dir, &st) == 0);
	
	str1_to_str2(tab_meta_dir, '/', "sysobjects");


	OPEN(fd1, tab_meta_dir, (O_RDWR));
	
	if (fd1 < 0)
	{
		goto exit;
	}

	status = READ(fd1, &tab_hdr, sizeof(TABLEHDR));	

	assert(status == sizeof(TABLEHDR));

	if (tab_hdr.tab_stat & TAB_DROPPED)
	{
		printf("This table has been dropped.\n");
		CLOSE(fd1);
		goto exit;
	}

	keycol = par_get_colval_by_colid(command, tab_hdr.tab_key_colid, &keycolen);

	MEMSET(sstab_name, SSTABLE_NAME_MAX_LEN);

	sstab_map = sstab_map_get(tab_hdr.tab_id, tab_dir, &tab_sstabmap);

	assert(sstab_map != NULL);

	if(Master_infor->rg_list.nextrno > 0)
	{
		
		rg_prof = (RANGE_PROF *)(Master_infor->rg_list.data);

		assert(rg_prof->rg_stat & RANGER_IS_ONLINE);
	}
	else
	{
		assert(0);
	}
	
	if (tab_hdr.tab_tablet > 0)
	{
		
		MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_tabletschm_dir, tab_dir, STRLEN(tab_dir));
		str1_to_str2(tab_tabletschm_dir, '/', "tabletscheme");

		tabletschm_rp = tablet_schm_srch_row(&tab_hdr, tab_hdr.tab_id, 0, tab_tabletschm_dir, keycol, keycolen);

		name = row_locate_col(tabletschm_rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
						&namelen);

		
		
		
		MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
		str1_to_str2(tab_meta_dir, '/', name);

		int tabletid;

		tabletid = *(int *)row_locate_col(tabletschm_rp, sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
						&namelen);

		
		tabinfo->t_stat &= ~TAB_TABLET_KEYROW_CHG;
		
		rp = tablet_srch_row(tabinfo, &tab_hdr, tab_hdr.tab_id, tabletid, tab_meta_dir, keycol, keycolen);

		
		name = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLET, &sstab_namelen);

		
		int ign;
		rg_addr = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT) + SSTABLE_NAME_MAX_LEN, 
					ROW_MINLEN_IN_TABLET, &ign);
		rg_port = rg_prof->rg_port;

				
		MEMCPY(sstab_name, name, STRLEN(name));

		sstab_id = *(int *)row_locate_col(rp, sizeof(ROWFMT), ROW_MINLEN_IN_TABLET, &namelen);

		char *testcol;
		testcol = row_locate_col(rp, sizeof(ROWFMT) + sizeof(int) + SSTABLE_NAME_MAX_LEN + RANGE_ADDR_MAX_LEN, 
						ROW_MINLEN_IN_TABLET, &namelen);
		
		res_sstab_id = *(int *)testcol;

		if((!SSTAB_MAP_RESERV(res_sstab_id)) || (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG))
		{
			int rlen, rlen_c5, sstab_res;

			sstab_res = FALSE;
			if (!SSTAB_MAP_RESERV(res_sstab_id))
			{
				assert(SSTAB_MAP_USED(res_sstab_id));

				res_sstab_id = meta_get_free_sstab();

				SSTAB_MAP_SET(res_sstab_id, SSTAB_RESERVED);

				sstab_res= TRUE;
			}

			if (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG)
			{
				
				rlen_c5 = ROW_MINLEN_IN_TABLET + sizeof(int) + keycolen + sizeof(int);
			}

			
			rlen = (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG) ? rlen_c5 : ROW_GET_LENGTH(rp, ROW_MINLEN_IN_TABLET);
			
			char	*newrp = (char *)MEMALLOCHEAP(rlen);

			if (sstab_res)
			{
				tablet_upd_col(newrp, rp, ROW_GET_LENGTH(rp, ROW_MINLEN_IN_TABLET), 4, (char *)(&res_sstab_id), 
						sizeof(int));
			}

			if (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG)
			{
				tablet_upd_col(newrp, rp, ROW_GET_LENGTH(rp, ROW_MINLEN_IN_TABLET), TABLET_KEYCOLID, keycol, keycolen);

								
			}
			
			tablet_del_row(&tab_hdr, tab_hdr.tab_id, tabletid, tab_meta_dir, rp, ROW_MINLEN_IN_TABLET);

			tablet_ins_row(&tab_hdr, tab_hdr.tab_id, tabletid, tab_meta_dir, newrp, ROW_MINLEN_IN_TABLET);

			MEMFREEHEAP(newrp);

			if (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG)
			{
				char	*tabletschm_newrp;
				int	rlen_c3;

				rlen_c3 = ROW_MINLEN_IN_TABLETSCHM + sizeof(int) + keycolen + sizeof(int);
				tabletschm_newrp = (char *)MEMALLOCHEAP(rlen_c3);

				
				tablet_schm_upd_col(tabletschm_newrp, tabletschm_rp, TABLET_SCHM_KEYCOLID, keycol, keycolen);
				
				tablet_schm_del_row(tab_hdr.tab_id, 0, tab_tabletschm_dir, tabletschm_rp);

				tablet_schm_ins_row(tab_hdr.tab_id, 0, tab_tabletschm_dir, tabletschm_newrp, tab_hdr.tab_tablet);

				tabinfo->t_stat &= ~TAB_TABLET_KEYROW_CHG;
				
				MEMFREEHEAP(tabletschm_newrp);
			}
			
		}

	}
	else if (tab_hdr.tab_tablet == 0)
	{
		MEMCPY(sstab_name, tab_name, tab_name_len);
		build_file_name("tablet", tablet_name, tab_hdr.tab_tablet);
		MEMCPY((sstab_name + tab_name_len), tablet_name, STRLEN(tablet_name));
		build_file_name("sstable", sstab_name + tab_name_len + STRLEN(tablet_name), 
				tab_hdr.tab_sstab);
	}
	else
	{
		assert(0);
	}
	
	
	if (tab_hdr.tab_tablet == 0)
	{
		char	*sstab_rp;
		int	sstab_rlen;

		
		sstab_rlen = ROW_MINLEN_IN_TABLET + keycolen + 4 + 4;

		sstab_rp = MEMALLOCHEAP(sstab_rlen);

		sstab_id = meta_get_free_sstab();
		SSTAB_MAP_SET(sstab_id, SSTAB_USED);

		res_sstab_id = meta_get_free_sstab();
		SSTAB_MAP_SET(res_sstab_id, SSTAB_RESERVED);
		
		
		tablet_min_rlen = tablet_bld_row(sstab_rp, sstab_rlen, tab_name, tab_name_len, 
						sstab_id, res_sstab_id, sstab_name, STRLEN(sstab_name), 
						rg_prof->rg_addr, keycol, keycolen, tab_hdr.tab_key_coltype);

		rg_addr = rg_prof->rg_addr;
		rg_port = rg_prof->rg_port;
		
		tablet_crt(&tab_hdr, tab_dir, sstab_rp, tablet_min_rlen);
		
		(tab_hdr.tab_tablet)++;
		(tab_hdr.tab_sstab)++;

		MEMFREEHEAP(sstab_rp);
	}

	LSEEK(fd1, 0, SEEK_SET);
	
	status = WRITE(fd1, &tab_hdr, sizeof(TABLEHDR));

	assert(status == sizeof(TABLEHDR));
	
	CLOSE(fd1);

	
	

	
	col_buf_len = sizeof(INSMETA) + sizeof(TABLEHDR) + tab_hdr.tab_col * (sizeof(COLINFO));
	col_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(col_buf, col_buf_len);


	
	col_buf_idx = 0;
		
	MEMCPY((col_buf + col_buf_idx), rg_addr, STRLEN(rg_addr));
	col_buf_idx += RANGE_ADDR_MAX_LEN;

	*(int *)(col_buf + col_buf_idx) = rg_port;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = RANGER_IS_ONLINE;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = sstab_id;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = res_sstab_id;
	col_buf_idx += sizeof(int);
	
	
	MEMCPY((col_buf + col_buf_idx), sstab_name, STRLEN(sstab_name));
	col_buf_idx += SSTABLE_NAME_MAX_LEN;

	
	col_buf_idx += sizeof(int);

	
        *(int *)(col_buf + col_buf_idx) = tab_hdr.tab_col;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr.tab_varcol;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr.tab_row_minlen;
	col_buf_idx += sizeof(int);

	
	MEMCPY((col_buf + col_buf_idx), &tab_hdr, sizeof(TABLEHDR));
	col_buf_idx += sizeof(TABLEHDR);
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "syscolumns");

	OPEN(fd1, tab_meta_dir, (O_RDONLY));

	if (fd1 < 0)
	{
		goto exit;
	}

	
	READ(fd1, (col_buf + col_buf_idx), tab_hdr.tab_col * sizeof(COLINFO));

	CLOSE(fd1);

	col_buf_idx += tab_hdr.tab_col * sizeof(COLINFO);
	rtn_stat = TRUE;

exit:

	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, col_buf_idx, col_buf);
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


char *
meta_droptab(TREE *command)
{
	
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	int		fd1;
	int		rtn_stat;
	TABLEHDR	tab_hdr;
	char		*resp;
	int		status;
	char   		*col_buf;
	int		col_buf_idx;
	int		col_buf_len;
	RANGE_PROF	*rg_prof;
	char		*rg_addr;
	int		rg_port;


	assert(command);

	rtn_stat = FALSE;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	assert(STAT(tab_dir, &st) == 0);
	
	str1_to_str2(tab_meta_dir, '/', "sysobjects");


	OPEN(fd1, tab_meta_dir, (O_RDWR));
	
	if (fd1 < 0)
	{
		goto exit;
	}

	status = READ(fd1, &tab_hdr, sizeof(TABLEHDR));	

	assert(status == sizeof(TABLEHDR));

	if (tab_hdr.tab_stat & TAB_DROPPED)
	{
		printf("This table has been dropped.\n");
		CLOSE(fd1);
		goto exit;
	}
		
	tab_hdr.tab_stat |= TAB_DROPPED;
	
	LSEEK(fd1, 0, SEEK_SET);
	
	status = WRITE(fd1, &tab_hdr, sizeof(TABLEHDR));

	assert(status == sizeof(TABLEHDR));
	
	CLOSE(fd1);

	if(Master_infor->rg_list.nextrno > 0)
	{
		
		rg_prof = (RANGE_PROF *)(Master_infor->rg_list.data);

		assert(rg_prof->rg_stat & RANGER_IS_ONLINE);
	}
	else
	{
		assert(0);
	}

	rg_addr = rg_prof->rg_addr;
	rg_port = rg_prof->rg_port;

	
	col_buf_len = RANGE_ADDR_MAX_LEN + sizeof(int);
	col_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(col_buf, col_buf_len);

	col_buf_idx = 0;
		
	MEMCPY((col_buf + col_buf_idx), rg_addr, STRLEN(rg_addr));
	col_buf_idx += RANGE_ADDR_MAX_LEN;

	*(int *)(col_buf + col_buf_idx) = rg_port;
	col_buf_idx += sizeof(int);

	
	rtn_stat = TRUE;

exit:

	
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, col_buf_idx, col_buf);
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


char *
meta_removtab(TREE *command)
{
	
	char	*tab_name;
	int	tab_name_len;
	char	tab_dir[TABLE_NAME_MAX_LEN];
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];
	char	cmd_str[TABLE_NAME_MAX_LEN];
	int	fd1;
	int	rtn_stat;
	TABLEHDR	tab_hdr;
	char	*resp;
	int	status;


	assert(command);

	rtn_stat = FALSE;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	assert(STAT(tab_dir, &st) == 0);
	
	str1_to_str2(tab_meta_dir, '/', "sysobjects");


	OPEN(fd1, tab_meta_dir, (O_RDONLY));
	
	if (fd1 < 0)
	{
		goto exit;
	}

	status = READ(fd1, &tab_hdr, sizeof(TABLEHDR));	

	assert(status == sizeof(TABLEHDR));
	
	assert(tab_hdr.tab_stat & TAB_DROPPED);
	
	CLOSE(fd1);

	MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
	
	sprintf(cmd_str, "rm -rf %s", tab_dir);
	
	if (system(cmd_str))
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
meta_seldeltab(TREE *command, TABINFO *tabinfo)
{
	
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	char   		*col_buf;
	int		col_buf_idx;
	int		col_buf_len;
	int		fd1;
	int		rtn_stat;
	TABLEHDR	tab_hdr;
	char		*keycol;
	int		keycolen;
	int		sstab_rlen;
	int		sstab_idx;
	char		*resp;
	char		*rp;
	char		*name;
	int		namelen;
	char		sstab_name[SSTABLE_NAME_MAX_LEN];
	int		sstab_id;
	int		res_sstab_id;
	RANGE_PROF	*rg_prof;
	char		*rg_addr;
	int		rg_port;


	assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	assert(STAT(tab_dir, &st) == 0);
	
	str1_to_str2(tab_meta_dir, '/', "sysobjects");

	OPEN(fd1, tab_meta_dir, (O_RDONLY));
	
	if (fd1 < 0)
	{
		printf("Table is not exist! \n");
		goto exit;
	}

	
	READ(fd1, &tab_hdr, sizeof(TABLEHDR));	

	CLOSE(fd1);

	
	
	assert(tab_hdr.tab_tablet > 0);

	if (tab_hdr.tab_stat & TAB_DROPPED)
	{
		printf("This table has been dropped.\n");
		goto exit;
	}

	if (tab_hdr.tab_tablet == 0)
	{
		printf("Table should have one tablet at least! \n");
		ex_raise(EX_ANY);
	}

	

	if(Master_infor->rg_list.nextrno > 0)
	{
		
		rg_prof = (RANGE_PROF *)(Master_infor->rg_list.data);

		assert(rg_prof->rg_stat & RANGER_IS_ONLINE);
	}
	else
	{
		assert(0);
	}

	keycol = par_get_colval_by_colid(command, tab_hdr.tab_key_colid, &keycolen);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	rp = tablet_schm_srch_row(&tab_hdr, tab_hdr.tab_id, 0, tab_meta_dir, keycol, keycolen);

	name = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
					&namelen);
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', name);

	int tabletid;

	tabletid = *(int *)row_locate_col(rp, sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
					&namelen);

	rp = tablet_srch_row(tabinfo, &tab_hdr, tab_hdr.tab_id, tabletid, tab_meta_dir, keycol, keycolen);

	
	name = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLET, &namelen);

	
	int ign;
	rg_addr = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT) + SSTABLE_NAME_MAX_LEN, 
				ROW_MINLEN_IN_TABLET, &ign);
	rg_port = rg_prof->rg_port;

	MEMSET(sstab_name, SSTABLE_NAME_MAX_LEN);
			
	MEMCPY(sstab_name, name, STRLEN(name));

	
	sstab_id = *(int *)row_locate_col(rp, sizeof(ROWFMT), ROW_MINLEN_IN_TABLET, &namelen);

	res_sstab_id = *(int *)row_locate_col(rp, sizeof(ROWFMT) + sizeof(int) + SSTABLE_NAME_MAX_LEN + RANGE_ADDR_MAX_LEN, 
						ROW_MINLEN_IN_TABLET, &namelen);
	
	

	
	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		goto exit;
	}

	

	
	col_buf_len = sizeof(INSMETA) + sizeof(TABLEHDR) + tab_hdr.tab_col * (sizeof(COLINFO));
	col_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(col_buf, col_buf_len);


	
	col_buf_idx = 0;
		
	MEMCPY((col_buf + col_buf_idx), rg_addr, STRLEN(rg_addr));
	col_buf_idx += RANGE_ADDR_MAX_LEN;

	*(int *)(col_buf + col_buf_idx) = rg_port;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = RANGER_IS_ONLINE;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = sstab_id;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = res_sstab_id;
	col_buf_idx += sizeof(int);

	
	MEMCPY((col_buf + col_buf_idx), sstab_name, SSTABLE_NAME_MAX_LEN);
	col_buf_idx += SSTABLE_NAME_MAX_LEN;

	
	col_buf_idx += sizeof(int);

	
        *(int *)(col_buf + col_buf_idx) = tab_hdr.tab_col;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr.tab_varcol;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr.tab_row_minlen;
	col_buf_idx += sizeof(int);

	
	MEMCPY((col_buf + col_buf_idx), &tab_hdr, sizeof(TABLEHDR));
	col_buf_idx += sizeof(TABLEHDR);
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "syscolumns");

	OPEN(fd1, tab_meta_dir, (O_RDONLY));

	if (fd1 < 0)
	{
		goto exit;
	}

	
	READ(fd1, (col_buf + col_buf_idx), tab_hdr.tab_col * sizeof(COLINFO));

	CLOSE(fd1);

	col_buf_idx += tab_hdr.tab_col * sizeof(COLINFO);
	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		
		resp = conn_build_resp_byte(RPC_SUCCESS, col_buf_idx, col_buf);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;
}



char *
meta_addsstab(TREE *command, TABINFO *tabinfo)
{
	
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	int		fd1;
	int		rtn_stat;
	TABLEHDR	tab_hdr;
	char		*keycol;
	int		keycolen;
	char		*resp;
	int		tablet_min_rlen;
	int		namelen;
	char		*name;
	char		*rp;
	int		status;
	char		*sstab_name;
	int		sstab_name_len;
	
	char		*sstab_rp;
	int		sstab_rlen;
	RANGE_PROF	*rg_prof;
	char		*rg_addr;


	assert(command);

	rtn_stat = FALSE;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	assert(STAT(tab_dir, &st) == 0);
	
	str1_to_str2(tab_meta_dir, '/', "sysobjects");


	OPEN(fd1, tab_meta_dir, (O_RDWR));
	
	if (fd1 < 0)
	{
		goto exit;
	}

	status = READ(fd1, &tab_hdr, sizeof(TABLEHDR));	

	assert(status == sizeof(TABLEHDR));
	assert(tab_hdr.tab_tablet > 0);


	if (tab_hdr.tab_stat & TAB_DROPPED)
	{
		printf("This table has been dropped.\n");
		CLOSE(fd1);
		goto exit;
	}
		
	keycol = par_get_colval_by_colid(command, 3, &keycolen);
	sstab_name= par_get_colval_by_colid(command, 1, &sstab_name_len);

	int colen;
	char *colptr = par_get_colval_by_colid(command, 2, &colen);

	sstab_map = sstab_map_get(tab_hdr.tab_id, tab_dir, &tab_sstabmap);
	
	int sstab_id;

	sstab_id = m_atoi(colptr, colen);

	SSTAB_MAP_SET(sstab_id, SSTAB_USED);

	if(Master_infor->rg_list.nextrno > 0)
	{
		
		rg_prof = (RANGE_PROF *)(Master_infor->rg_list.data);

		assert(rg_prof->rg_stat & RANGER_IS_ONLINE);
	}
	else
	{
		assert(0);
	}

	rg_addr = rg_prof->rg_addr;
	
	
	sstab_rlen = ROW_MINLEN_IN_TABLET + keycolen + 4 + 4;

	sstab_rp = MEMALLOCHEAP(sstab_rlen);



	int res_sstab_id;
	res_sstab_id = meta_get_free_sstab();
	SSTAB_MAP_SET(res_sstab_id, SSTAB_RESERVED);		

	
	tablet_min_rlen = tablet_bld_row(sstab_rp, sstab_rlen, tab_name, tab_name_len, 
					sstab_id, res_sstab_id,sstab_name, sstab_name_len,
					rg_addr, keycol, keycolen, tab_hdr.tab_key_coltype);


	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	
	rp = tablet_schm_srch_row(&tab_hdr, tab_hdr.tab_id, 0, tab_meta_dir, keycol, keycolen);

	name = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
					&namelen);
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', name);

	int tabletid;
	tabletid = *(int *)row_locate_col(rp, sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
					&namelen);

	tablet_ins_row(&tab_hdr, tab_hdr.tab_id, tabletid, tab_meta_dir, sstab_rp, ROW_MINLEN_IN_TABLET);

	MEMFREEHEAP(sstab_rp);

	(tab_hdr.tab_sstab)++;
	
	LSEEK(fd1, 0, SEEK_SET);
	
	
	status = WRITE(fd1, &tab_hdr, sizeof(TABLEHDR));

	assert(status == sizeof(TABLEHDR));
	
	CLOSE(fd1);

	
	

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
meta_get_free_sstab()
{
	int i = 0;

	while (i < SSTAB_MAP_SIZE)
	{
		if (SSTAB_MAP_FREE(i))
		{
			break;
		}

		i++;
	}

	return i;
}

static void
meta_prt_sstabmap(int begin, int end)
{
	while(begin < end)
	{
		printf("sstab_map[%d] == %d \n", begin, sstab_map[begin]);

		begin++;
	}
}


static int
meta_collect_rg(char * req_buf)
{
	char		*str;
	int		i;
	SVR_IDX_FILE	*rglist;
	int		found;
	RANGE_PROF	*rg_addr;


	
	if (!strncasecmp(RPC_RG2MASTER_REPORT, req_buf, STRLEN(RPC_RG2MASTER_REPORT)))
	{
		str = req_buf + RPC_MAGIC_MAX_LEN;

		rglist = &(Master_infor->rg_list);
		found = FALSE;
		rg_addr = (RANGE_PROF *)(rglist->data);

		for(i = 0; i < rglist->nextrno; i++)
		{
			if (   !strncasecmp(str, rg_addr[i].rg_addr, RANGE_ADDR_MAX_LEN)
			    && (rg_addr[i].rg_port == *(int *)(str + RANGE_ADDR_MAX_LEN))
			    )
			{
				found = TRUE;
				break;
			}
		}

		if (!found)
		{
			MEMCPY(rg_addr[i].rg_addr, str, RANGE_ADDR_MAX_LEN);
			rg_addr[i].rg_port = *(int *)(str + RANGE_ADDR_MAX_LEN);
			rg_addr[i].rg_stat = RANGER_IS_ONLINE;

			(rglist->nextrno)++;
		}
	}
	else
	{
		return FALSE;
	}

	return TRUE;
}

char *
meta_handler(char *req_buf)
{
	LOCALTSS(tss);
	TREE		*command;
	int		resp_buf_idx;
	int		resp_buf_size;
	char		*resp;
	char		*tmp_req_buf;
	char		crt_tab_cmd[256];
	TABINFO		*tabinfo;


	tmp_req_buf = req_buf;

	if (meta_collect_rg(req_buf))
	{		
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);

		return resp;
	}
	
parse_again:
	if (!parser_open(tmp_req_buf))
	{
		parser_close();
		tss->tstat |= TSS_PARSER_ERR;
		printf("PARSER ERR: Please input the command again by the 'help' signed.\n");
		return NULL;
	}
	

	command = tss->tcmd_parser;
	resp_buf_idx = 0;
	resp_buf_size = 0;
	resp = NULL;

	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;
	
	tabinfo_push(tabinfo);
	
	switch(command->sym.command.querytype)
	{
	    case ADDSERVER:
		meta_add_server(command);
		
		MEMSET(crt_tab_cmd, 256);
		MEMCPY(crt_tab_cmd, "create table", 12);
		str1_to_str2(crt_tab_cmd, ' ', command->sym.command.tabname);
		str1_to_str2(crt_tab_cmd, ' ', "(filename varchar, status int)");
		tmp_req_buf = crt_tab_cmd;
		parser_close();
		
		goto parse_again;
		
		break;
			
	    case TABCREAT:
		resp = meta_crtab(command);
						
		printf("I got here - CREATING TABLE\n");
		break;

	    case INSERT:
	    	resp = meta_instab(command, tabinfo);
		break;

	    case CRTINDEX:
	    	break;

	    case SELECT:
		resp = meta_seldeltab(command, tabinfo);
		printf("I got here - SELECTING TABLE\n");
	    	break;

	    case DELETE:
	    	resp = meta_seldeltab(command, tabinfo);
		printf("I got here - DELETE TABLE\n");
	    	break;
	    case ADDSSTAB:
	    	resp = meta_addsstab(command, tabinfo);
	    	break;
	    case DROP:
	    	resp = meta_droptab(command);
	    	break;
	    case REMOVE:
	    	resp = meta_removtab(command);
	    	break;

	    default:
	    	break;
	}

	session_close(tabinfo);

	tabinfo_pop();
	if (tabinfo!= NULL)
	{
		if (tabinfo->t_sinfo)
		{
			MEMFREEHEAP(tabinfo->t_sinfo);
		}
		MEMFREEHEAP(tabinfo);
	}
	parser_close();

	return resp;
}


static void 
meta_bld_sysrow(char *rp, int rlen, int tabletid, int sstabnum)
{
	int		rowidx;


	row_build_hdr(rp, 0, 0, 0);

	rowidx = sizeof(ROWFMT);

	*(int *)(rp + rowidx) = rlen;
	rowidx += sizeof(int);
	
	*(int *)(rp + rowidx) = tabletid;
	rowidx += sizeof(int);
	
	*(int *)(rp + rowidx) = sstabnum;
	rowidx += sizeof(int);

	assert(rowidx == rlen);
}

int main(int argc, char *argv[])
{
	char *conf_path;


	mem_init_alloc_regions();

	Trace = 0;
	conf_path = META_DEFAULT_CONF_PATH;
	conf_get_path(argc, argv, &conf_path);

	meta_server_setup(conf_path);

	startup(Master_infor->port, TSS_OP_METASERVER, meta_handler);

	return TRUE;
}
