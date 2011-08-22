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



extern	TSS	*Tss;

#define META_CONF_PATH_MAX_LEN   64
#define DEFAULT_DUPLICATE_NUM 1
#define MIN_REGION_AVAILABLE_SIZE 100 //Unit is MB
#define DEFAULT_MASTER_FLUSH_CHECK_INTERVAL 600 //10Min

#define SSTAB_MAP_SIZE	(1024 * 1024 * sizeof(int))
#define SSTAB_FREE	0
#define SSTAB_USED	1
#define SSTAB_RESERVED	2

/* SSTAB map */
int	*sstab_map;

#define SSTAB_MAP_SET(i, flag)	(sstab_map[i] = flag)

#define SSTAB_MAP_FREE(i)	(sstab_map[i] == SSTAB_FREE)
#define SSTAB_MAP_USED(i)	(sstab_map[i] == SSTAB_USED)
#define SSTAB_MAP_RESERV(i)	(sstab_map[i] == SSTAB_RESERVED)




typedef struct master_infor
{
	char	conf_path[META_CONF_PATH_MAX_LEN];
	int	port;
	
}MASTER_INFOR;

MASTER_INFOR *Master_infor = NULL;

struct stat st;

#define MT_META_TABLE   "./meta_table"
#define MT_META_REGION  "./rg_server"
#define MT_META_INDEX   "./index"	/* Delay this implementation */


static void 
meta_bld_sysrow(char *rp, int rlen, int tabletid, int sstabnum);

static int
meta_get_free_sstab();

static void
meta_prt_sstabmap(int begin, int end);



/** Make sure the conf path is valid **/
void 
meta_server_setup(char *conf_path)
{
	int	status;
	int	port;
	char	rang_server[256];
	int	fd;
	SVR_IDX_FILE	*filebuf;


	Master_infor = MEMALLOCHEAP(sizeof(MASTER_INFOR));
	MEMCPY(Master_infor->conf_path, conf_path, sizeof(conf_path));

	conf_get_value_by_key((char *)&port, conf_path, CONF_PORT_KEY);

	if(port != INDEFINITE)
	{
		Master_infor->port = atoi((char *)&port);
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
		/* Loading table infor. */
		;
	}

	if (STAT(MT_META_REGION, &st) != 0)
	{
		MKDIR(status, MT_META_REGION, 0755); 

		MEMSET(rang_server, 256);
		MEMCPY(rang_server, MT_META_REGION, STRLEN(MT_META_REGION));
		str1_to_str2(rang_server, '/', "rangeserverlist");
	
		OPEN(fd, rang_server, (O_CREAT|O_WRONLY|O_TRUNC));
		
		/* Scan the tree of command to get the column information. */
//		WRITE(fd, rang_server, STRLEN(rang_server));

		filebuf = (SVR_IDX_FILE *)MEMALLOCHEAP(SVR_IDX_FILE_BLK);
		MEMSET(filebuf, SVR_IDX_FILE_BLK);

		filebuf->freeoff = SVR_IDX_FILE_HDR;
	
		WRITE(fd, filebuf, SVR_IDX_FILE_BLK);

		MEMFREEHEAP(filebuf);
		
		CLOSE(fd);		
	}
	else
	{
		/* Checking the region server. */
		;
	}

	if (STAT(MT_META_INDEX, &st) != 0)
	{
		MKDIR(status, MT_META_INDEX, 0755); 
	}

	/* This map can contain 1M sstab file (1M * 1M = 1T) */
	sstab_map = (int *)malloc(SSTAB_MAP_SIZE);

	/* This Map has following flag:
	**
	**	0:	free
	**	1:	used
	**	-1:	reserved
	*/

	MEMSET(sstab_map, 1024 * 1024 * sizeof(int));
	
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
	/*
	** TODO:
	** if (filebuf->freeoff + )
	*/

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
	char    	tab_dir1[256];	/* For sysobject and syscolumn files */
	int		status;
	int		fd;
	TREE		*col_tree;
	char		col_buf[256];	/* The space for all the column rows in 
					** one table and sysobjects.
					*/
	int		col_buf_idx;
	int		minlen;
	int		varcol;		/* # of var-column */
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

	/* Create the file named table name. */
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
	
	/* Open syscolumn file to save the new table's column infor. */
	MEMCPY(tab_dir1, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_dir1, '/', "syscolumns");

	OPEN(fd, tab_dir1, (O_CREAT|O_WRONLY|O_TRUNC));

	if (fd < 0)
	{
		goto exit;
	}

	/* 
	** Clo_id (int) | Col_name (char 64) | Col_length (int)| 
	** Col_offset (int) |Col_type (int)
	** Table id is not needed, because this file locate at the current 
	** table dir. 
	*/
	//row_buf = MEMALLOCHEAP(4 * sizeof(int) + 64);
	col_buf_idx = 0;
	minlen = sizeof(ROWFMT);
	varcol = 0;
	colcnt = 0;
	col_tree = command->left;
	while (col_tree)
	{
	        MEMSET(&col_info, sizeof(COLINFO));

		/* 
		** Building a row for one column and insert it to the syscolumn.
		*/
		//row_buf_idx = 0;

		col_info.col_id = col_tree->sym.resdom.colid;
		col_info.col_len = col_tree->sym.resdom.colen;
		MEMCPY(col_info.col_name, col_tree->sym.resdom.colname,
		STRLEN(col_tree->sym.resdom.colname));          
				
		/*
		** Column length < 0 means this column is var-column. 
		** 
		** Building column offset...
		**
		*/
		if (col_tree->sym.resdom.colen > 0)
		{
			/* Using 1st column as the key defaultly. */
			if (col_tree->sym.resdom.colid == 1)
			{
				tab_key_coloff = minlen;
				tab_key_colid = 1;
				tab_key_coltype = col_tree->sym.resdom.coltype;
			}
			
			/* Offset for fixed column. */
                        col_info.col_offset = minlen;
			
			minlen += col_tree->sym.resdom.colen;			
		}
		else
		{
			/* Using 1st column as the key defaultly. */
			if (col_tree->sym.resdom.colid == 1)
			{
				tab_key_coloff = -(varcol+1);
				tab_key_colid = 1;
				tab_key_coltype = col_tree->sym.resdom.coltype;
			}
			
			/* 
			** The offset of var-column is -varcol, like -1, -2, -3... 
			*/
			col_info.col_offset = -(varcol+1);

			varcol++;
		}

		colcnt ++;

		col_info.col_type = col_tree->sym.resdom.coltype;
		
		/* Put the Col_infor buffer for the writting. */
		PUT_TO_BUFFER(col_buf, col_buf_idx, &col_info, sizeof(COLINFO));
		
		col_tree = col_tree->left;
	}
	
        /* No Need to append this file. */
	
        /* Scan the tree of command to get the column information. */
	WRITE(fd, col_buf, col_buf_idx);

	CLOSE(fd);

	/* Cleat this buffer. */
	MEMSET(tab_dir1, 256);

	/* 
	** Create the sysobject file for this table. It like the DES in the ASE. 
	*/
	MEMCPY(tab_dir1, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_dir1, '/', "sysobjects");
	
	OPEN(fd, tab_dir1, (O_CREAT|O_WRONLY|O_TRUNC));

	if (fd < 0)
	{
		goto exit;
	}

	tab_hdr = MEMALLOCHEAP(sizeof(TABLEHDR));

	/* TODO: Aquire the ID of table*/
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
	
	/* Scan the tree of command to get the column information. */
	WRITE(fd, tab_hdr, sizeof(TABLEHDR));

	MEMFREEHEAP(tab_hdr);

	CLOSE(fd);


	/* Create the sstable map file. */
	MEMSET(tab_dir1, 256);

	/* 
	** Create the sysobject file for this table. It like the DES in the ASE. 
	*/
	MEMCPY(tab_dir1, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_dir1, '/', "sstabmap");
	
	OPEN(fd, tab_dir1, (O_CREAT|O_WRONLY|O_TRUNC));

	if (fd < 0)
	{
		goto exit;
	}
	
	/* Scan the tree of command to get the column information. */
	WRITE(fd, sstab_map, 1024 * 1024 * sizeof(int));

	CLOSE(fd);
	
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
	TABINFO	*tabinfo;
	int	minrowlen;
	char	*key;
	int	ign;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo_push(tabinfo);

	minrowlen = sizeof(ROWFMT) + 3 * sizeof(int);

	/* TODO: coloffset should be the virtual value. */
	key = row_locate_col(row, (sizeof(ROWFMT) + sizeof(int)), minrowlen, &ign);
	
	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, TAB_META_SYSTAB, 0 ,0);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, 4, 1, INT4, sizeof(ROWFMT) + sizeof(int));
			
	blkins(tabinfo, row);

	tabinfo_pop();
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
}



/* 
** SYSTABLE tablethdr formate as follows:
**	| row header | tablet id | sstable # |
**
*/
char *
meta_instab(TREE *command, TABINFO *tabinfo)
{
	
	char	*tab_name;
	int	tab_name_len;
	char	tab_dir[TABLE_NAME_MAX_LEN];
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];/* For sysobject and  syscolumn files. */
	int	fd1;
	int	rtn_stat;
	TABLEHDR	tab_hdr;
	char	*keycol;
	int	keycolen;
	int	sstab_idx;
	char	sstab_name[SSTABLE_NAME_MAX_LEN];
	int	sstab_namelen;
	char   	*col_buf;
	int	col_buf_idx;
	int	col_buf_len;
	char	*resp;
	char	tablet_name[32];
	int	tablet_min_rlen;
	int	namelen;
	char	*name;
	char	*rp;
	int	status;
	char	rg_addr[RANGE_ADDR_MAX_LEN];
	int	sstab_id;
	int	res_sstab_id;


	assert(command);

	rtn_stat = FALSE;
	sstab_idx = 0;
	col_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	MEMSET(rg_addr, RANGE_ADDR_MAX_LEN);

	MEMCPY(rg_addr, RANGE_SERVER_TEST, STRLEN(RANGE_SERVER_TEST));
	
	/* Current table dir. */
	str1_to_str2(tab_dir, '/', tab_name);

	/* Save the table dir for the creating of tablet file. */
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

	keycol = par_get_colval_by_colid(command, tab_hdr.tab_key_colid, &keycolen);

	MEMSET(sstab_name, SSTABLE_NAME_MAX_LEN);

	if (tab_hdr.tab_tablet > 0)
	{
		/* 1st step: search the table tabletscheme to get the right tablet. */
		MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
		str1_to_str2(tab_meta_dir, '/', "tabletscheme");

		rp = tablet_schm_srch_row(&tab_hdr, tab_hdr.tab_id, 0, tab_meta_dir, keycol, keycolen);

		name = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
						&namelen);

		
		
		/* 2nd step: search the table tabletN to get the right sstable. */
		MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
		str1_to_str2(tab_meta_dir, '/', name);

		int tabletid;

		tabletid = *(int *)row_locate_col(rp, sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
						&namelen);

		rp = tablet_srch_row(&tab_hdr, tab_hdr.tab_id, tabletid, tab_meta_dir, keycol, keycolen);

		/* Get the file name for sstable. */
		name = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLET, &sstab_namelen);

		/* 3rd step: copy the sstable name into the common buffer. */		
		MEMCPY(sstab_name, name, STRLEN(name));

		sstab_id = *(int *)row_locate_col(rp, sizeof(ROWFMT), ROW_MINLEN_IN_TABLET, &namelen);

		char *testcol;
		testcol = row_locate_col(rp, sizeof(ROWFMT) + sizeof(int) + SSTABLE_NAME_MAX_LEN + RANGE_ADDR_MAX_LEN, 
						ROW_MINLEN_IN_TABLET, &namelen);
		
		res_sstab_id = *(int *)testcol;

		if(!SSTAB_MAP_RESERV(res_sstab_id))
		{
			assert(SSTAB_MAP_USED(res_sstab_id));

			res_sstab_id = meta_get_free_sstab();

			SSTAB_MAP_SET(res_sstab_id, SSTAB_RESERVED);

			int rlen = ROW_GET_LENGTH(rp, ROW_MINLEN_IN_TABLET);
			char *rp_tmp = (char *)MEMALLOCHEAP(rlen);

			MEMCPY(rp_tmp, rp, rlen);

			tablet_del_row(&tab_hdr, tab_hdr.tab_id, tabletid, tab_meta_dir, rp, ROW_MINLEN_IN_TABLET);

			char *colptr;
			colptr = row_locate_col(rp_tmp, sizeof(ROWFMT) + sizeof(int) + SSTABLE_NAME_MAX_LEN + RANGE_ADDR_MAX_LEN, 
						ROW_MINLEN_IN_TABLET, &namelen);

			*(int *)colptr = res_sstab_id;

			tablet_ins_row(&tab_hdr, tab_hdr.tab_id, tabletid, tab_meta_dir, rp_tmp, ROW_MINLEN_IN_TABLET);

			MEMFREEHEAP(rp_tmp);
			
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
	
	/* This's the 1st insertion. */
	if (tab_hdr.tab_tablet == 0)
	{
		char	*sstab_rp;
		int	sstab_rlen;

		/* 
		** Building a row that save the information of sstable,  this row
		** is also the index, so we have to specify a  key for this row.
		*/
		sstab_rlen = ROW_MINLEN_IN_TABLET + keycolen + 4 + 4;

		sstab_rp = MEMALLOCHEAP(sstab_rlen);

		sstab_id = meta_get_free_sstab();
		SSTAB_MAP_SET(sstab_id, SSTAB_USED);

		res_sstab_id = meta_get_free_sstab();
		SSTAB_MAP_SET(res_sstab_id, SSTAB_RESERVED);
		
		
		/* 1st step: build a sstab row and check it into the file "tablet". */
		tablet_min_rlen = tablet_bld_row(sstab_rp, sstab_rlen, tab_name, tab_name_len, 
						sstab_id, res_sstab_id, sstab_name, STRLEN(sstab_name), 
						rg_addr, keycol, keycolen, tab_hdr.tab_key_coltype);
		
		tablet_crt(&tab_hdr, tab_dir, sstab_rp, tablet_min_rlen);
		
		(tab_hdr.tab_tablet)++;
		(tab_hdr.tab_sstab)++;

		MEMFREEHEAP(sstab_rp);
	}

	LSEEK(fd1, 0, SEEK_SET);
	/* Update the table header information with new tablet #. */
	status = WRITE(fd1, &tab_hdr, sizeof(TABLEHDR));

	assert(status == sizeof(TABLEHDR));
	
	CLOSE(fd1);

	
	/* Building the response information. */

	/* Get the meta data for the column. */
	col_buf_len = sizeof(INSMETA) + sizeof(TABLEHDR) + tab_hdr.tab_col * (sizeof(COLINFO));
	col_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(col_buf, col_buf_len);


	/* Fill the INSERT_META with the information. */
	col_buf_idx = 0;
		
	MEMCPY((col_buf + col_buf_idx), RANGE_SERVER_TEST, STRLEN(RANGE_SERVER_TEST));
	col_buf_idx += RANGE_ADDR_MAX_LEN;

	*(int *)(col_buf + col_buf_idx) = RANGE_PORT_TEST;
	col_buf_idx += sizeof(int);

	/* Put the sstab id for the buffersearch. */
	*(int *)(col_buf + col_buf_idx) = sstab_id;
	col_buf_idx += sizeof(int);

	/* Put the reserved sstab id for the buffersearch. */
	*(int *)(col_buf + col_buf_idx) = res_sstab_id;
	col_buf_idx += sizeof(int);
	
	/* Get the sstable file name. -- *********** This's the key point for the response information, other is the common info. */
	MEMCPY((col_buf + col_buf_idx), sstab_name, STRLEN(sstab_name));
	col_buf_idx += SSTABLE_NAME_MAX_LEN;

	/* Skip the fill for the status in the INSERT_META, because this field is a running value. */
	col_buf_idx += sizeof(int);

	/* Get the # of column. */
        *(int *)(col_buf + col_buf_idx) = tab_hdr.tab_col;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr.tab_varcol;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr.tab_row_minlen;
	col_buf_idx += sizeof(int);

	/* Put the table header into this buffer. */
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

	/* Get the column meta. */
	READ(fd1, (col_buf + col_buf_idx), tab_hdr.tab_col * sizeof(COLINFO));

	CLOSE(fd1);

	col_buf_idx += tab_hdr.tab_col * sizeof(COLINFO);
	rtn_stat = TRUE;

exit:

	if (rtn_stat)
	{
		/* Send to client. */
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
meta_seltab(TREE *command, TABINFO *tabinfo)
{
	
	char	*tab_name;
	int	tab_name_len;
	char	tab_dir[TABLE_NAME_MAX_LEN];
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];/* For sysobject and 
						 ** syscolumn files.
						 */
	char   	*col_buf;
	int	col_buf_idx;
	int	col_buf_len;
	int	fd1;
	int	rtn_stat;
	TABLEHDR	tab_hdr;
	char	*keycol;
	int	keycolen;
	int	sstab_rlen;
	int	sstab_idx;
	char	*resp;
	char	*rp;
	char	*name;
	int	namelen;
	char	sstab_name[SSTABLE_NAME_MAX_LEN];
	int	sstab_id;
	int	res_sstab_id;


	assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	/* Current table dir. */
	str1_to_str2(tab_dir, '/', tab_name);

	/* Save the table dir for the creating of tablet file. */
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

	/* Read the table header information from the file directly. */
	READ(fd1, &tab_hdr, sizeof(TABLEHDR));	

	CLOSE(fd1);

	/* TODO: tablet check. */
	
	assert(tab_hdr.tab_tablet > 0);

	if (tab_hdr.tab_tablet == 0)
	{
		printf("Table should have one tablet at least! \n");
		ex_raise(EX_ANY);
	}

	/*
	** TODO: Create index while the metaserver is booting and now
	** we can scan this index.
	*/

	keycol = par_get_colval_by_colid(command, tab_hdr.tab_key_colid, &keycolen);

	/* 1st step: search the table tabletscheme to get the right tablet. */
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	rp = tablet_schm_srch_row(&tab_hdr, tab_hdr.tab_id, 0, tab_meta_dir, keycol, keycolen);

	name = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
					&namelen);
	
	/* 2nd step: search the table tabletN to get the right sstable. */
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', name);

	int tabletid;

	tabletid = *(int *)row_locate_col(rp, sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
					&namelen);

	rp = tablet_srch_row(&tab_hdr, tab_hdr.tab_id, tabletid, tab_meta_dir, keycol, keycolen);

	/* Get the file name for sstable. */
	name = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLET, &namelen);

	MEMSET(sstab_name, SSTABLE_NAME_MAX_LEN);
	/* 3rd step: copy the sstable name into the common buffer. */		
	MEMCPY(sstab_name, name, STRLEN(name));

	/* Get the sstab id. */
	sstab_id = *(int *)row_locate_col(rp, sizeof(ROWFMT), ROW_MINLEN_IN_TABLET, &namelen);

	res_sstab_id = *(int *)row_locate_col(rp, sizeof(ROWFMT) + sizeof(int) + SSTABLE_NAME_MAX_LEN + RANGE_ADDR_MAX_LEN, 
						ROW_MINLEN_IN_TABLET, &namelen);
	
	/* 
	** Read the file "tablet0", its row formate as follows:
	**	| row header |sstab id| sstable name | Ranger server IP | key column value | Key column offset (optional) |
	**
	*/

	/* The selecting value is not exist. */
	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		goto exit;
	}

	/* Building the response information. */

	/* Get the meta data for the column. */
	col_buf_len = sizeof(INSMETA) + sizeof(TABLEHDR) + tab_hdr.tab_col * (sizeof(COLINFO));
	col_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(col_buf, col_buf_len);


	/* Fill the INSERT_META with the information. */
	col_buf_idx = 0;
		
	MEMCPY((col_buf + col_buf_idx), RANGE_SERVER_TEST, STRLEN(RANGE_SERVER_TEST));
	col_buf_idx += RANGE_ADDR_MAX_LEN;

	*(int *)(col_buf + col_buf_idx) = RANGE_PORT_TEST;
	col_buf_idx += sizeof(int);

	/* put the sstab id for the bufsearch in the ranger server. */
	*(int *)(col_buf + col_buf_idx) = sstab_id;
	col_buf_idx += sizeof(int);

	/* Put the reserved sstab id for the buffersearch. */
	*(int *)(col_buf + col_buf_idx) = res_sstab_id;
	col_buf_idx += sizeof(int);

	/* Get the sstable file name. */
	MEMCPY((col_buf + col_buf_idx), sstab_name, SSTABLE_NAME_MAX_LEN);
	col_buf_idx += SSTABLE_NAME_MAX_LEN;

	/* Skip the fill for the status in the INSERT_META, because this field is a running value. */
	col_buf_idx += sizeof(int);

	/* Get the # of column. */
        *(int *)(col_buf + col_buf_idx) = tab_hdr.tab_col;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr.tab_varcol;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr.tab_row_minlen;
	col_buf_idx += sizeof(int);

	/* Put the table header into this buffer. */
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

	/* Get the column meta. */
	READ(fd1, (col_buf + col_buf_idx), tab_hdr.tab_col * sizeof(COLINFO));

	CLOSE(fd1);

	col_buf_idx += tab_hdr.tab_col * sizeof(COLINFO);
	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		/* Send to client, just send the sstable name. */
		resp = conn_build_resp_byte(RPC_SUCCESS, col_buf_idx, col_buf);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;
}




/* 
** The command is as follows:
**	addsstab into table_name (new_sstab_name, keycol, sstab_id)
*/
char *
meta_addsstab(TREE *command, TABINFO *tabinfo)
{
	
	char	*tab_name;
	int	tab_name_len;
	char	tab_dir[TABLE_NAME_MAX_LEN];
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];/* For sysobject and 
						 ** syscolumn files.
						 */
	int	fd1;
	int	rtn_stat;
	TABLEHDR	tab_hdr;
	char	*keycol;
	int	keycolen;
	char	*resp;
	int	tablet_min_rlen;
	int	namelen;
	char	*name;
	char	*rp;
	int	status;
	char	*rg_addr;
	char	*sstab_name;
	int	sstab_name_len;
	
	char	*sstab_rp;
	int	sstab_rlen;


	assert(command);

	rtn_stat = FALSE;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	rg_addr = RANGE_SERVER_TEST;
	
	/* Current table dir. */
	str1_to_str2(tab_dir, '/', tab_name);

	/* Save the table dir for the creating of tablet file. */
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


	keycol = par_get_colval_by_colid(command, 3, &keycolen);
	sstab_name= par_get_colval_by_colid(command, 1, &sstab_name_len);

	int colen;
	char *colptr = par_get_colval_by_colid(command, 2, &colen);

	int sstab_id;

	sstab_id = m_atoi(colptr, colen);
	
	SSTAB_MAP_SET(sstab_id, SSTAB_USED);
	
	/* 
	** Building a row that save the information of sstable,  this row
	** is also the index, so we have to specify a  key for this row.
	*/
	sstab_rlen = ROW_MINLEN_IN_TABLET + keycolen + 4 + 4;

	sstab_rp = MEMALLOCHEAP(sstab_rlen);



	int res_sstab_id;
	res_sstab_id = meta_get_free_sstab();
	SSTAB_MAP_SET(res_sstab_id, SSTAB_RESERVED);		

	/* 1st step: build a sstab row and check it into the file "tablet". */
	tablet_min_rlen = tablet_bld_row(sstab_rp, sstab_rlen, tab_name, tab_name_len, 
					sstab_id, res_sstab_id,sstab_name, sstab_name_len,
					rg_addr, keycol, keycolen, tab_hdr.tab_key_coltype);


	/* 1st step: search the table tabletscheme to get the right tablet. */
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	/* 0 is the reserved tabletscheme id. */
	rp = tablet_schm_srch_row(&tab_hdr, tab_hdr.tab_id, 0, tab_meta_dir, keycol, keycolen);

	name = row_locate_col(rp, sizeof(int) + sizeof(ROWFMT), ROW_MINLEN_IN_TABLETSCHM, 
					&namelen);
	
	/* 2nd step: search the table tabletN to get the right sstable. */
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
	
	/* Update the table header information with new tablet #. */
	status = WRITE(fd1, &tab_hdr, sizeof(TABLEHDR));

	assert(status == sizeof(TABLEHDR));
	
	CLOSE(fd1);

	
	/* Building the response information. */

	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		/* Send to client. */
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);
	}
	else
	{
		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
	}

	return resp;

}


/* return the index of sstab map. */
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
	
parse_again:
	parser_open(tmp_req_buf);

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
		/* TODO: addserver should be removed.*/
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
		resp = meta_seltab(command, tabinfo);
		printf("I got here - SELECTING TABLE\n");
	    	break;

	    case DELETE:
	    	break;
	    case ADDSSTAB:
	    	resp = meta_addsstab(command, tabinfo);
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

/*
** TODO: row formate. */
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
