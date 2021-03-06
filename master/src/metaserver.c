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
#include "master/metaserver.h"
#include "tabinfo.h"
#include "rpcfmt.h"
#include "parser.h"
#include "ranger/rangeserver.h"
#include "conf.h"
#include "netconn.h"
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
#include "sstab.h"
#include "rebalancer.h"
#include "thread.h"
#include "log.h"
#include "m_socket.h"
#include "rginfo.h"
#include "interface.h"
#include "index.h"
#include "qryoptimizer.h"
#include "heartbeat.h"
#include "masterinfo.h"


extern	TSS	*Tss;
extern	char	Kfsserver[32];
extern	int	Kfsport;

#define SSTAB_FREE	0
#define SSTAB_USED	1
#define SSTAB_RESERVED	2

#define	META_RECOVERY_INTERVAL	5

SSTAB_INFOR	*sstab_map;

TAB_SSTAB_MAP *tab_sstabmap;


#define SSTAB_MAP_SET(i, flag)	(sstab_map[i].sstab_stat = flag)

#define SSTAB_MAP_FREE(i)	(sstab_map[i].sstab_stat == SSTAB_FREE)
#define SSTAB_MAP_USED(i)	(sstab_map[i].sstab_stat == SSTAB_USED)
#define SSTAB_MAP_RESERV(i)	(sstab_map[i].sstab_stat == SSTAB_RESERVED)

#define SSTAB_MAP_GET_SPLIT_TS(i)		(sstab_map[i].split_ts)
#define SSTAB_MAP_SET_SPLIT_TS(i, split_ts)	(sstab_map[i].split_ts = (unsigned int)split_ts)




MASTER_INFOR	*Master_infor = NULL;

extern pthread_mutex_t mutex;
extern pthread_cond_t cond;

extern int msg_list_len;
extern MSG_DATA * msg_list_head;
extern MSG_DATA * msg_list_tail;


struct stat st;


static void
meta_heartbeat_setup(RANGE_PROF * rg_addr);

static int
meta_failover_rg(char * req_buf);

static char *
meta_get_splits(char * req_buf);

static char *
meta_get_reader_meta(char * req_buf);

static int
meta_get_free_sstab();

static int
meta_update();

static RANGE_PROF *
meta_get_rg();

static RANGE_PROF *
meta_get_rg_by_ip_port(char *rgip, int rgport);

static int
meta_collect_rg(char * req_buf);

static void
meta_save_rginfo();

static char *
meta_checktable(TREE *command);

static char *
meta_checkranger(TREE *command);

static int
meta_table_is_exist(char *tabname);

static int
meta_load_sysmeta();

static int
meta_rg_statistics(char *tab_dir, int tab_id, REBALANCE_STATISTICS *rbs);

static RANGE_PROF *
meta_get_next_rg(int *j);

static int
meta_crt_rgstate_file(RANGE_PROF *rg_prof, char *rgip, int rgport);

static int
meta__recovery_addsstab(char * rgip,int rgport);

static TABLEHDR *
meta_get_sysobj_by_tabid(int tabid);

static int
meta_check_validation();

static int
meta_clean4crtidx(char * req_buf);

static int
meta__recovery_crtidx();


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
	int	metaport;
	char	rang_server[256];
	int	fd;
	SVR_IDX_FILE	*filebuf;
	int i;


	MEMSET(port, 32);
	Master_infor = MEMALLOCHEAP(sizeof(MASTER_INFOR));
	MEMCPY(Master_infor->conf_path, conf_path, STRLEN(conf_path));

	
	Master_infor->meta_systab = malloc(sizeof(META_SYSTABLE));
	memset(Master_infor->meta_systab, 0, sizeof(META_SYSTABLE));

	
	Master_infor->meta_sysobj = malloc(sizeof(META_SYSOBJECT));
	memset(Master_infor->meta_sysobj, 0, sizeof(META_SYSOBJECT));

	
	Master_infor->meta_syscol = malloc(sizeof(META_SYSCOLUMN));
	memset(Master_infor->meta_syscol, 0, sizeof(META_SYSCOLUMN));

	
	Master_infor->meta_sysindex = malloc(sizeof(META_SYSINDEX));
	memset(Master_infor->meta_sysindex, 0, sizeof(META_SYSINDEX));

	conf_get_value_by_key(port, conf_path, CONF_PORT_KEY);

	metaport = m_atoi(port, STRLEN(port));
	if(metaport != INDEFINITE)
	{
		Master_infor->port = metaport;
	}
	else
	{
		Master_infor->port = META_DEFAULT_PORT;
	}

	SPINLOCK_ATTR_INIT(Master_infor->mutexattr);
	SPINLOCK_ATTR_SETTYPE(Master_infor->mutexattr, PTHREAD_MUTEX_RECURSIVE);
	SPINLOCK_INIT(Master_infor->rglist_spinlock, &(Master_infor->mutexattr));
	
#ifdef MT_KFS_BACKEND
	MEMSET(Kfsserver, 32);
	conf_get_value_by_key(Kfsserver, conf_path, CONF_KFS_IP);
	conf_get_value_by_key(port, conf_path, CONF_KFS_PORT);

	Kfsport = m_atoi(port, STRLEN(port));

#endif
	if (STAT(MT_META_TABLE, &st) != 0)
	{
		MKDIR(status, MT_META_TABLE, 0755);

		MEMSET(rang_server, 256);
		MEMCPY(rang_server, MT_META_TABLE, STRLEN(MT_META_TABLE));
		str1_to_str2(rang_server, '/', "systable");

		OPEN(fd, rang_server, (O_CREAT|O_WRONLY|O_TRUNC));
			
		WRITE(fd, Master_infor->meta_systab, sizeof(META_SYSTABLE));
	
		CLOSE(fd);		

		Master_infor->last_tabid = 0;
	}
	else
	{		
		MEMSET(rang_server, 256);
		MEMCPY(rang_server, MT_META_TABLE, STRLEN(MT_META_TABLE));
		str1_to_str2(rang_server, '/', "systable");

		OPEN(fd, rang_server, (O_RDONLY));
		
		READ(fd, Master_infor->meta_systab, sizeof(META_SYSTABLE));
	
		CLOSE(fd);

		Master_infor->last_tabid = Master_infor->meta_systab->last_tabid;
	}

	meta_load_sysmeta();

	if (STAT(MT_META_REGION, &st) != 0)
	{
		MKDIR(status, MT_META_REGION, 0755); 

		MEMSET(rang_server, 256);
		MEMCPY(rang_server, MT_META_REGION, STRLEN(MT_META_REGION));
		str1_to_str2(rang_server, '/', "rangeserverlist");
	
		OPEN(fd, rang_server, (O_CREAT|O_WRONLY|O_TRUNC));
		
		
//		WRITE(fd, rang_server, STRLEN(rang_server));

		filebuf = &(Master_infor->rg_list);
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
	
		CLOSE(fd);		
	}
	else
	{
		
		MEMSET(rang_server, 256);
		MEMCPY(rang_server, MT_META_REGION, STRLEN(MT_META_REGION));
		str1_to_str2(rang_server, '/', "rangeserverlist");
	
		OPEN(fd, rang_server, (O_RDONLY));
		
		
//		WRITE(fd, rang_server, STRLEN(rang_server));

		MEMSET(&(Master_infor->rg_list), SVR_IDX_FILE_SIZE);

		READ(fd, &(Master_infor->rg_list), SVR_IDX_FILE_SIZE);

		
		CLOSE(fd);

		
		RANGE_PROF *rg_addr = (RANGE_PROF *)(Master_infor->rg_list.data);

		for(i = 0; i < Master_infor->rg_list.nextrno; i++)
		{
			
			if (rg_addr[i].rg_stat & RANGER_IS_ONLINE)
			{
				rg_addr[i].rg_stat &= ~RANGER_IS_ONLINE;
				rg_addr[i].rg_stat |= (RANGER_IS_OFFLINE | RANGER_NEED_RECOVERY);
			}
		}
		
		
	}

	
	if (STAT(MT_META_BACKUP, &st) != 0)
	{
		MKDIR(status, MT_META_BACKUP, 0755); 
	}
	else
	{
		;
	}

	if (STAT(MT_META_INDEX, &st) != 0)
	{
		MKDIR(status, MT_META_INDEX, 0755); 

		MEMSET(rang_server, 256);
		MEMCPY(rang_server, MT_META_INDEX, STRLEN(MT_META_INDEX));
		str1_to_str2(rang_server, '/', "sysindex");
	
		OPEN(fd, rang_server, (O_CREAT|O_WRONLY|O_TRUNC));

		Master_infor->meta_sysindex->idx_magic[0] = 's';
		Master_infor->meta_sysindex->idx_magic[1] = 'y';
		Master_infor->meta_sysindex->idx_magic[2] = 's';
		Master_infor->meta_sysindex->idx_magic[3] = 'i';
		Master_infor->meta_sysindex->idx_magic[4] = 'n';
		Master_infor->meta_sysindex->idx_magic[5] = 'd';
		Master_infor->meta_sysindex->idx_magic[6] = 'e';
		Master_infor->meta_sysindex->idx_magic[7] = 'x';
		
		WRITE(fd, Master_infor->meta_sysindex, sizeof(META_SYSINDEX));
	
		CLOSE(fd);

	}
	else
	{		
		meta_load_sysindex((char *)Master_infor->meta_sysindex);
	}

	if (STAT(LOG_FILE_DIR, &st) != 0)
	{
		MKDIR(status, LOG_FILE_DIR, 0755); 
	}

	if (STAT(BACKUP_DIR, &st) != 0)
	{
		MKDIR(status, BACKUP_DIR, 0755); 
	}
		
	tab_sstabmap = NULL;
	sstab_map = NULL;
	
	ca_setup_pool();

	return;
}


static void
meta_save_rginfo()
{
	char    rang_server[256];
	int     fd;


	P_SPINLOCK(Master_infor->rglist_spinlock);
	
	MEMSET(rang_server, 256);
	MEMCPY(rang_server, MT_META_REGION, STRLEN(MT_META_REGION));
	str1_to_str2(rang_server, '/', "rangeserverlist");

	OPEN(fd, rang_server, (O_RDWR));

	WRITE(fd, &(Master_infor->rg_list), SVR_IDX_FILE_SIZE);

	CLOSE(fd);

	V_SPINLOCK(Master_infor->rglist_spinlock);
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

	filebuf = (SVR_IDX_FILE *)MEMALLOCHEAP(SVR_IDX_FILE_SIZE);
	
	OPEN(fd, rang_server, (O_CREAT|O_WRONLY|O_TRUNC));

	READ(fd,filebuf,SVR_IDX_FILE_SIZE);
	

	PUT_TO_BUFFER(filebuf->data, filebuf->freeoff, 
				command->sym.command.tabname,
				command->sym.command.tabname_len);
	PUT_TO_BUFFER(filebuf->data, filebuf->freeoff, 
				command->left->right->sym.constant.value,
				command->left->right->sym.constant.len);

	WRITE(fd, filebuf, SVR_IDX_FILE_SIZE);

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
	char		*col_buf;	
	int		col_buf_idx;
	int		minlen;
	int		varcol;		
	int		colcnt;
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	int		tab_key_coloff;
	int		tab_key_colid;
	int		tab_key_coltype;
	COLINFO		col_info;
	char 		*resp;
	SVR_IDX_FILE	*tablet_store;
	SSTAB_INFOR	*sstab_map_tmp;


	Assert(command);
	
	rtn_stat = FALSE;
	resp = NULL;
	tablet_store = NULL;
	sstab_map_tmp = NULL;
	col_buf = NULL;
	tab_hdr	= NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	
	MEMSET(tab_dir, 256);
	MEMSET(tab_dir1, 256);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	
	if (STAT(tab_dir, &st) != 0)
	{
		if (meta_table_is_exist(tab_dir) != -1)
		{
			traceprint("TAB_CRT: table information in memory doesn't match with the one on disk!\n", tab_name);
			goto exit;
		}
		
		MKDIR(status, tab_dir, 0755);        

		if (status < 0)
		{
			goto exit;
		}
	}
	else
	{
		traceprint("Table %s has been created.\n",tab_name);
		goto exit;
	}

	if (((Master_infor->meta_systab)->tabnum + 1) > TAB_MAX_NUM)
	{
		traceprint("Table # of system owning expand the limited number.\n");
		goto exit;
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

	
	col_buf = MEMALLOCHEAP(COL_MAX_NUM * sizeof(COLINFO));
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

		if (colcnt > COL_MAX_NUM)
		{
			traceprint("The # of column (%d) expands the limit.\n", colcnt);
			goto exit;
		}
		
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

	
	tab_hdr->tab_id = ++Master_infor->last_tabid;
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

	CLOSE(fd);
	
	
	sstab_map_tmp = (SSTAB_INFOR *)malloc(SSTAB_MAP_SIZE);

	
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

	
	tablet_store = (SVR_IDX_FILE *)MEMALLOCHEAP(sizeof(SVR_IDX_FILE));

	
	MEMSET(tab_dir1, TABLE_NAME_MAX_LEN);

	
	MEMCPY(tab_dir1, tab_dir, STRLEN(tab_dir));

	
	str1_to_str2(tab_dir1, '/', "tabletinranger");
	
	OPEN(fd, tab_dir1, (O_CREAT|O_WRONLY|O_TRUNC));

	if (fd < 0)
	{
		goto exit;
	}

	
	WRITE(fd, tablet_store, sizeof(SVR_IDX_FILE));

	CLOSE(fd);

	
	MEMSET(tab_dir1, TABLE_NAME_MAX_LEN);

	MEMCPY(tab_dir1, MT_META_BACKUP, STRLEN(MT_META_BACKUP));

	str1_to_str2(tab_dir1, '/', tab_name);
	
	if (STAT(tab_dir1, &st) != 0)
	{
		MKDIR(status, tab_dir1, 0755);
	}
		

	MEMSET(tab_dir1, 256);
	MEMCPY(tab_dir1, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir1, '/', "systable");

	OPEN(fd, tab_dir1, (O_RDWR));
	

	
	Master_infor->meta_systab->last_tabid = Master_infor->last_tabid;

	MEMCPY(Master_infor->meta_systab->meta_tabdir[Master_infor->meta_systab->tabnum], 
					tab_dir, STRLEN(tab_dir));
	
	(Master_infor->meta_systab)->tabnum++;

	
	WRITE(fd, Master_infor->meta_systab, sizeof(META_SYSTABLE));

	CLOSE(fd);

	MEMCPY(&(Master_infor->meta_sysobj->sysobject[Master_infor->meta_systab->tabnum - 1]), 
					tab_hdr,sizeof(TABLEHDR));

	Master_infor->meta_syscol->colnum[Master_infor->meta_systab->tabnum - 1] = tab_hdr->tab_col;
	MEMCPY(&(Master_infor->meta_syscol->colinfor[Master_infor->meta_systab->tabnum - 1]),
					col_buf, col_buf_idx);
	
	rtn_stat = TRUE;
	
exit:	
	if (tab_hdr != NULL)
	{
		MEMFREEHEAP(tab_hdr);
	}
	
	if (sstab_map_tmp != NULL)
	{
		free(sstab_map_tmp);
	}

	if (tablet_store != NULL)
	{
		MEMFREEHEAP(tablet_store);
	}

	if (col_buf != NULL)
	{
		MEMFREEHEAP(col_buf);
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

#if 0
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

	
	key = row_locate_col(row, (sizeof(ROWFMT) + sizeof(int)),
					minrowlen, &ign);
	
	TABINFO_INIT(tabinfo, systab, NULL, 0, tabinfo->t_sinfo, minrowlen,
					TAB_META_SYSTAB, 0 ,0);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, 4, 1, INT4, 
					sizeof(ROWFMT) + sizeof(int));
			
	blkins(tabinfo, row);

	tabinfo_pop();
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
}

#endif


char *
meta_instab(TREE *command, TABINFO *tabinfo)
{
	LOCALTSS(tss);
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	char		tab_tabletschm_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	char		*keycol;
	int		keycolen;
	int		sstab_idx;
	char		sstab_name[SSTABLE_NAME_MAX_LEN];
	int		sstab_namelen;
	char   		*col_buf;
	int		col_buf_idx;
	int		col_buf_len;
	char		*resp;
	int		tablet_min_rlen;
	int		namelen;
	char		*name;
	char		*rp;
	char		*tabletschm_rp;
	char		*rg_addr;
	int		rg_port;
	int		sstab_id;
	int		res_sstab_id;
	RANGE_PROF	*rg_prof;
	int		sstabmap_chg;
	int		rpc_status;
	int		rg_suspect;
	int		tabidx;
	int		tabhdr_update;
	int 		tabletid;


	Assert(command);

	rtn_stat = FALSE;
	sstabmap_chg = FALSE;
	tabhdr_update = FALSE;
	sstab_idx = 0;
	col_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	rpc_status = 0;
	rg_suspect = FALSE;	

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	
	volatile struct
	{
		char	*newrp;
		char	*tabletschm_newrp;
		char	*sstab_rp;
	} copy;

	copy.newrp = NULL;
	copy.tabletschm_newrp = NULL;
	copy.sstab_rp = NULL;

	resp = NULL;

	if(ex_handle(EX_ANY, yxue_handler))
	{
		if (copy.newrp)
		{
			MEMFREEHEAP(copy.newrp);
		}

		if (copy.tabletschm_newrp)
		{
			MEMFREEHEAP(copy.tabletschm_newrp);
		}

		if (copy.sstab_rp)
		{
			MEMFREEHEAP(copy.sstab_rp);
		}

		ex_delete();
		ex_raise(EX_ANY);
	}

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}


	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	if (!meta_check_validation())
	{
		traceprint("Meta on this table is not valid.\n");
		goto exit;
	}

	keycol = par_get_colval_by_colid(command, tab_hdr->tab_key_colid, 
					&keycolen);

	sstab_map = sstab_map_get(tab_hdr->tab_id, tab_dir, &tab_sstabmap);

	Assert(sstab_map != NULL);

	if (sstab_map == NULL)
	{
		traceprint("Table %s has no sstabmap in the metaserver!", tab_name);
		ex_raise(EX_ANY);
	}

	if (tab_hdr->tab_tablet > 0)
	{
		
		MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_tabletschm_dir, tab_dir, STRLEN(tab_dir));
		str1_to_str2(tab_tabletschm_dir, '/', "tabletscheme");

		tabletschm_rp = tablet_schm_srch_row(tab_hdr->tab_id, 
						     TABLETSCHM_ID, 
						     tab_tabletschm_dir,
						     keycol, keycolen);

		name = row_locate_col(tabletschm_rp, 
				      TABLETSCHM_TABLETNAME_COLOFF_INROW,
				      ROW_MINLEN_IN_TABLETSCHM, &namelen);
		
		
		MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
		str1_to_str2(tab_meta_dir, '/', name);

		char	tabletname[TABLE_NAME_MAX_LEN];

		MEMSET(tabletname, TABLE_NAME_MAX_LEN);
		MEMCPY(tabletname, name, STRLEN(name));
		
		tabletid = *(int *)row_locate_col(tabletschm_rp, 
						TABLETSCHM_TABLETID_COLOFF_INROW,
						ROW_MINLEN_IN_TABLETSCHM, 
						&namelen);

		
		int ign;
		rg_addr = row_locate_col(tabletschm_rp, 
					 TABLETSCHM_RGADDR_COLOFF_INROW,
					 ROW_MINLEN_IN_TABLETSCHM, &ign);

		rg_port = *(int *)row_locate_col(tabletschm_rp, 
						 TABLETSCHM_RGPORT_COLOFF_INROW,
					 	 ROW_MINLEN_IN_TABLETSCHM, &ign);

		if (DEBUG_TEST(tss))
		{
			traceprint("select ranger server %s/%d for insert\n", rg_addr, rg_port);		
		}
		tss->tcur_rgprof = rebalan_get_rg_prof_by_addr(rg_addr, rg_port);

		Assert(tss->tcur_rgprof);

		if (tss->tcur_rgprof == NULL)
		{
			traceprint("Can't get the profile of ranger server %s\n", rg_addr);

			goto exit;
		}

		if (tss->tcur_rgprof->rg_stat & RANGER_IS_OFFLINE)
		{
			traceprint("Ranger server (%s:%d) is OFF-LINE\n", rg_addr, rg_port);
			goto exit;
		}
		else if (tss->tcur_rgprof->rg_stat & RANGER_IS_SUSPECT)
		{
			traceprint("Ranger server (%s:%d) is SUSPECT\n", rg_addr, rg_port);

			rg_suspect = TRUE;
			goto exit;
		}
		else if (tss->tcur_rgprof->rg_stat & RANGER_NEED_RECOVERY)
		{
			traceprint("Ranger server (%s:%d) is being in the recovery\n", rg_addr, rg_port);

			rg_suspect = TRUE;
			goto exit;
		}
		else if (tss->tcur_rgprof->rg_stat & RANGER_RESTART)
		{
			traceprint("Ranger server (%s:%d) is booting.\n", rg_addr, rg_port);

			rg_suspect = TRUE;
			goto exit;
		}

		//rg_port = tss->tcur_rgprof->rg_port;

		
		tabinfo->t_stat &= ~TAB_TABLET_KEYROW_CHG;
		
		rp = tablet_srch_row(tabinfo, tab_hdr->tab_id, tabletid, 
				     tab_meta_dir, keycol, keycolen);

		
		name = row_locate_col(rp, TABLET_SSTABNAME_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLET, &sstab_namelen);

		sstab_namelen = STRLEN(name);

		MEMSET(sstab_name, SSTABLE_NAME_MAX_LEN);
		
			
		MEMCPY(sstab_name, name, sstab_namelen);

		sstab_id = *(int *)row_locate_col(rp, TABLET_SSTABID_COLOFF_INROW, 
						  ROW_MINLEN_IN_TABLET, &namelen);

		char *pres_sstab_id;
		pres_sstab_id = row_locate_col(rp, TABLET_RESSSTABID_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLET, &namelen);
		
		res_sstab_id = *(int *)pres_sstab_id;

		
		if(   (!SSTAB_MAP_RESERV(res_sstab_id)) 
		   || (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG))
		{
			int rlen, rlen_c5, sstab_res;

			sstab_res = FALSE;
			if (!SSTAB_MAP_RESERV(res_sstab_id))
			{
				Assert(SSTAB_MAP_USED(res_sstab_id));

				if (!SSTAB_MAP_USED(res_sstab_id))
				{
					traceprint("SSTable map hit error!\n");

					goto exit;
				}

				res_sstab_id = meta_get_free_sstab();

				SSTAB_MAP_SET(res_sstab_id, SSTAB_RESERVED);

				sstab_res= TRUE;
				sstabmap_chg = TRUE;
			}

			if (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG)
			{
				
				rlen_c5 = ROW_MINLEN_IN_TABLET + sizeof(int) 
						+ keycolen + sizeof(int);
			}

			
			rlen = (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG) ? 
					rlen_c5 : ROW_GET_LENGTH(rp, 
							ROW_MINLEN_IN_TABLET);
			
			char	*newrp = (char *)MEMALLOCHEAP(rlen);

			copy.newrp = newrp;

			if (sstab_res)
			{
				tablet_upd_col(newrp, rp, ROW_GET_LENGTH(rp, 
						ROW_MINLEN_IN_TABLET), 
						TABLET_RESSSTABID_COLID_INROW, 
						(char *)(&res_sstab_id), 
						sizeof(int));
			}

			if (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG)
			{
				tablet_upd_col(newrp, rp, ROW_GET_LENGTH(rp, 
							ROW_MINLEN_IN_TABLET), 
					       TABLET_KEY_COLID_INROW, 
					       keycol, keycolen);								
			}

			
			if (tablet_upd_row(tab_hdr,tab_hdr->tab_id, tabletid,
				tab_meta_dir, rp, newrp, ROW_MINLEN_IN_TABLET))
			{
										
				int	status = FALSE;
				char	cmd_str[TABLE_NAME_MAX_LEN];
				
				MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
				
#ifdef MT_KFS_BACKEND
				sprintf(cmd_str, "%s/%s/%s",  tss->metabackup, 
							tab_name, tabletname);
				RMFILE(status, cmd_str);
				if(!status)
#else			
				sprintf(cmd_str, "rm -rf %s/%s/%s",  tss->metabackup, 
							tab_name, tabletname);
				
				if (!system(cmd_str))
#endif
				{
					status = TRUE;
				}
		
				if (!status)
				{
					traceprint("Failed to remove the backup director for the tablet split.\n");
				}				

			}
			MEMFREEHEAP(newrp);
			copy.newrp = NULL;

			if (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG)
			{
				char	*tabletschm_newrp;
				int	rlen_c3;

				rlen_c3 = ROW_MINLEN_IN_TABLETSCHM + sizeof(int) 
							+ keycolen + sizeof(int);
				tabletschm_newrp = (char *)MEMALLOCHEAP(rlen_c3);

				copy.tabletschm_newrp = tabletschm_newrp;

				
				tablet_schm_upd_col(tabletschm_newrp, tabletschm_rp,
							TABLETSCHM_KEY_COLID_INROW, 
						    	keycol, keycolen);

				
				char	*key;
				int	keylen;
				
				key = row_locate_col(tabletschm_rp, -1, 
						ROW_MINLEN_IN_TABLETSCHM, &keylen);
				
				
				if (!tablet_schm_del_row(tab_hdr->tab_id, TABLETSCHM_ID,
							tab_tabletschm_dir,
							key, keylen))
				{
					ex_raise(EX_ANY);
				}

				if (!tablet_schm_ins_row(tab_hdr->tab_id, TABLETSCHM_ID, 
							tab_tabletschm_dir, 
							tabletschm_newrp,
							tab_hdr->tab_tablet, 0))
				{
					ex_raise(EX_ANY);
				}

				tabinfo->t_stat &= ~TAB_TABLET_KEYROW_CHG;
				
				MEMFREEHEAP(tabletschm_newrp);
				copy.tabletschm_newrp = NULL;
			}
			
		}

	}
	else if (tab_hdr->tab_tablet == 0)
	{
		sstab_bld_name(sstab_name, tab_name, tab_name_len, 
						tab_hdr->tab_sstab);
	}
	else
	{
		Assert(0);

		ex_raise(EX_ANY);
	}
	
	
	if (tab_hdr->tab_tablet == 0)
	{
		char	*sstab_rp;
		int	sstab_rlen;

		
		sstab_rlen = ROW_MINLEN_IN_TABLET + keycolen + 4 + COLOFFSETENTRYSIZE;

		
#if 0
		((!TYPE_IS_FIXED(tab_hdr->tab_key_coltype)) ? 
		COLOFFSETENTRYSIZE : 0);
#endif
		sstab_rp = MEMALLOCHEAP(sstab_rlen);
		copy.sstab_rp = sstab_rp;

		sstab_id = meta_get_free_sstab();
		SSTAB_MAP_SET(sstab_id, SSTAB_USED);

		res_sstab_id = meta_get_free_sstab();
		SSTAB_MAP_SET(res_sstab_id, SSTAB_RESERVED);
		sstabmap_chg = TRUE;

		if(Master_infor->rg_list.nextrno > 0)
		{
			
			//rg_prof = (RANGE_PROF *)(Master_infor->rg_list.data);
			rg_prof = meta_get_rg();

			if (!rg_prof)
			{
				traceprint("Ranger server is un-available for insert\n");

				goto exit;
			}

			Assert(rg_prof->rg_stat & RANGER_IS_ONLINE);
		}
		else
		{
			Assert(0);

			ex_raise(EX_ANY);
		}
		
		
		tablet_min_rlen = tablet_bld_row(sstab_rp, sstab_rlen, tab_name,
						tab_name_len, sstab_id, 
						res_sstab_id, sstab_name,
						STRLEN(sstab_name), keycol,
						keycolen, tab_hdr->tab_key_coltype);
	
		rg_addr = rg_prof->rg_addr;
		rg_port = rg_prof->rg_port;
		
		if (!tablet_crt(tab_hdr, tab_dir, rg_addr, sstab_rp, 
				tablet_min_rlen, rg_port))
		{
			traceprint("TABLET_CRT: hit err.\n");
			
			ex_raise(EX_ANY);
		}

		
		tabletid = 1; 
		(tab_hdr->tab_tablet)++;
		(tab_hdr->tab_sstab)++;

		tabhdr_update = TRUE;

		(rg_prof->rg_tablet_num)++;

		meta_save_rginfo();

		MEMFREEHEAP(sstab_rp);

		copy.sstab_rp = NULL;
	}

	if (tabhdr_update)
	{
		if (!meta_save_sysobj(tab_dir, (char *)tab_hdr))
		{
			goto exit;
		}
	}	

	

	
	col_buf_len = sizeof(INSMETA) + sizeof(TABLEHDR) 
				+ tab_hdr->tab_col * (sizeof(COLINFO));
	col_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(col_buf, col_buf_len);

	
	col_buf_idx = 0;

	
	MEMCPY((col_buf + col_buf_idx), rg_addr, STRLEN(rg_addr));
	col_buf_idx += RANGE_ADDR_MAX_LEN;

	
	*(int *)(col_buf + col_buf_idx) = rg_port;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = RANGER_IS_ONLINE;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = 0;
	col_buf_idx += sizeof(int);

	
	col_buf_idx += sizeof(int) + TABLE_NAME_MAX_LEN + sizeof(pthread_t);

	
	*(int *)(col_buf + col_buf_idx) = sstab_id;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = res_sstab_id;
	col_buf_idx += sizeof(int);

	
	*(unsigned int *)(col_buf + col_buf_idx) = SSTAB_MAP_GET_SPLIT_TS(sstab_id);
	col_buf_idx += sizeof(int);
	
	
	MEMCPY((col_buf + col_buf_idx), sstab_name, STRLEN(sstab_name));
	col_buf_idx += SSTABLE_NAME_MAX_LEN;

	
	col_buf_idx += sizeof(int);

	
        *(int *)(col_buf + col_buf_idx) = tab_hdr->tab_col;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr->tab_varcol;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr->tab_row_minlen;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = tabletid;
	col_buf_idx += sizeof(int);

	
	MEMCPY((col_buf + col_buf_idx), tab_hdr, sizeof(TABLEHDR));
	col_buf_idx += sizeof(TABLEHDR);
	
	
	Assert(Master_infor->meta_syscol->colnum[tabidx] == tab_hdr->tab_col);

	MEMCPY((col_buf + col_buf_idx), &(Master_infor->meta_syscol->colinfor[tabidx]),tab_hdr->tab_col * sizeof(COLINFO));

	col_buf_idx += tab_hdr->tab_col * sizeof(COLINFO);

	
	if (sstabmap_chg)
	{
		sstab_map_put(-1, tss->ttab_sstabmap);
	}
	rtn_stat = TRUE;

exit:

	if (rtn_stat)
	{
		
		rpc_status |= RPC_SUCCESS;		
		resp = conn_build_resp_byte(rpc_status, col_buf_idx, col_buf);
	}
	else
	{
		if (rg_suspect)
		{
			rpc_status |= RPC_RETRY;
		}
		else
		{
			rpc_status |= RPC_FAIL;
		}
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	if (col_buf != NULL)
	{
		MEMFREEHEAP(col_buf);
	}

	ex_delete();

	return resp;
}


static int
meta__droptab_meta(int tabidx)
{
	char	tab_dir[TABLE_NAME_MAX_LEN];
	int 	fd;
	int	i;


	
	MEMSET(Master_infor->meta_systab->meta_tabdir[tabidx], TABLE_NAME_MAX_LEN);

	
	for (i = tabidx; i < ((Master_infor->meta_systab)->tabnum - 1); i++)
	{
		
		MEMCPY(Master_infor->meta_systab->meta_tabdir[i], 
				Master_infor->meta_systab->meta_tabdir[i + 1], 
				TABLE_NAME_MAX_LEN);

		
		MEMCPY(&(Master_infor->meta_sysobj->sysobject[i]), 
				&(Master_infor->meta_sysobj->sysobject[i + 1]),
				sizeof(TABLEHDR));

		
		Master_infor->meta_syscol->colnum[i] =
				Master_infor->meta_syscol->colnum[i + 1];		
		MEMCPY(&(Master_infor->meta_syscol->colinfor[i]),
				&(Master_infor->meta_syscol->colinfor[i + 1]),
				COL_MAX_NUM * sizeof(COLINFO));
	}

	
	if (i == (Master_infor->meta_systab)->tabnum)
	{
		MEMSET(Master_infor->meta_systab->meta_tabdir[i - 1], TABLE_NAME_MAX_LEN);
		MEMSET(&(Master_infor->meta_sysobj->sysobject[i - 1]), sizeof(TABLEHDR));
		Master_infor->meta_syscol->colnum[i - 1] = 0;
		MEMSET(&(Master_infor->meta_syscol->colinfor[i - 1]), COL_MAX_NUM * sizeof(COLINFO));
	}
	
	(Master_infor->meta_systab)->tabnum--;
	
	
	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', "systable");

	OPEN(fd, tab_dir, (O_RDWR));

	WRITE(fd, Master_infor->meta_systab, sizeof(META_SYSTABLE));

	CLOSE(fd);

	return TRUE;

}

char *
meta_droptab(TREE *command)
{
	
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	char		*resp;
	char   		*col_buf;
	int		col_buf_idx;
	int		col_buf_len;
	RANGE_PROF	*rg_prof;
	char		*rg_addr;
	int		rg_port;
	int		rpc_status;
	int		tabidx;


	Assert(command);

	rtn_stat = FALSE;
	col_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	rpc_status = 0;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}


	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	
	tab_hdr->tab_stat |= TAB_DROPPED;

	if (!meta_save_sysobj(tab_dir,(char *)tab_hdr))
	{
		tab_hdr->tab_stat &= ~TAB_DROPPED;
		goto exit;
	}

	if(Master_infor->rg_list.nextrno > 0)
	{
		
		rg_prof = meta_get_rg();

		if (!rg_prof)
		{
			traceprint("Ranger server is un-available for insert\n");
			goto exit;
		}

		Assert(rg_prof->rg_stat & RANGER_IS_ONLINE);
			
		if (!(rg_prof->rg_stat & RANGER_IS_ONLINE))
		{
			traceprint("Ranger server %s is off-line\n", rg_prof->rg_addr);
			goto exit;
		}
	}
	else
	{
		Assert(0);

		traceprint("No ranger server is avlable\n");
		ex_raise(EX_ANY);
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
		
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, col_buf_idx, col_buf);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
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
	
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	char		*resp;
	int		rpc_status;
	int		tabidx;


	Assert(command);

	rtn_stat = FALSE;
	rpc_status = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);
	
	if (!(tab_hdr->tab_stat & TAB_DROPPED))
	{
		traceprint("This table has NOT been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}
	
#ifdef MT_KFS_BACKEND

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

	meta__droptab_meta(tabidx);

exit:	
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	return resp;
}


char *
meta_seldeltab(TREE *command, TABINFO *tabinfo)
{
	LOCALTSS(tss);
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	char   		*col_buf;
	int		col_buf_idx;
	int		col_buf_len;
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
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
	char		*rg_addr;
	int		rg_port;
	int		rpc_status;
	int		rg_suspect;
	int		tabidx;
	

	Assert(command);

	rtn_stat = FALSE;
	col_buf= NULL;
	sstab_rlen = 0;
	sstab_idx = 0;
	rpc_status = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	rg_suspect = FALSE; 

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist.\n", tab_name);
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}
	
	str1_to_str2(tab_meta_dir, '/', "sysobjects");

	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", tab_name);
		rpc_status |= RPC_TAB_HAS_NO_DATA;
		goto exit;
	}

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	if (   (command->sym.command.querytype != SELECT) 
	    && (!meta_check_validation()))
	{
		traceprint("Meta on this table is not valid.\n");
		goto exit;
	}

	sstab_map = sstab_map_get(tab_hdr->tab_id, tab_dir, &tab_sstabmap);

	Assert(sstab_map != NULL);

	if (sstab_map == NULL)
	{
		traceprint("Table %s has no sstabmap in the metaserver!", tab_name);
		ex_raise(EX_ANY);
	}
	
	keycol = par_get_colval_by_colid(command, tab_hdr->tab_key_colid, 
						&keycolen);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	rp = tablet_schm_srch_row(tab_hdr->tab_id, TABLETSCHM_ID,
				tab_meta_dir, keycol, keycolen);

	name = row_locate_col(rp, TABLETSCHM_TABLETNAME_COLOFF_INROW, 
				ROW_MINLEN_IN_TABLETSCHM, &namelen);

	
	int ign;
	rg_addr = row_locate_col(rp, TABLETSCHM_RGADDR_COLOFF_INROW,
				ROW_MINLEN_IN_TABLETSCHM, &ign);
	rg_port = *(int *)row_locate_col(rp, TABLETSCHM_RGPORT_COLOFF_INROW, 
					 ROW_MINLEN_IN_TABLETSCHM, &ign);
	
	tss->tcur_rgprof = rebalan_get_rg_prof_by_addr(rg_addr, rg_port);

	Assert(tss->tcur_rgprof);

	
	if (tss->tcur_rgprof == NULL)
	{
		traceprint("Can't get the profile of ranger server %s\n", rg_addr);

		goto exit;
	}

	if (tss->tcur_rgprof->rg_stat & RANGER_IS_OFFLINE)
	{
		traceprint("Ranger server (%s:%d) is OFF-LINE\n", rg_addr, rg_port);

		goto exit;
	}
	else if (tss->tcur_rgprof->rg_stat & RANGER_IS_SUSPECT)
	{
		traceprint("Ranger server (%s:%d) is SUSPECT\n", rg_addr, rg_port);

		rg_suspect = TRUE;	
		goto exit;
	}
	else if (tss->tcur_rgprof->rg_stat & RANGER_NEED_RECOVERY)
	{
		traceprint("Ranger server (%s:%d) is being in the recovery\n", rg_addr, rg_port);

		rg_suspect = TRUE;
		goto exit;
	}
	else if (tss->tcur_rgprof->rg_stat & RANGER_RESTART)
	{
		traceprint("Ranger server (%s:%d) is booting.\n", rg_addr, rg_port);

		rg_suspect = TRUE;
		goto exit;
	}

	//rg_port = tss->tcur_rgprof->rg_port;	
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', name);

	int tabletid;

	tabletid = *(int *)row_locate_col(rp, TABLETSCHM_TABLETID_COLOFF_INROW, 
					  ROW_MINLEN_IN_TABLETSCHM, &namelen);

	rp = tablet_srch_row(tabinfo, tab_hdr->tab_id, tabletid, 
				tab_meta_dir, keycol, keycolen);

	
	name = row_locate_col(rp, TABLET_SSTABNAME_COLOFF_INROW,
				ROW_MINLEN_IN_TABLET, &namelen);

	MEMSET(sstab_name, SSTABLE_NAME_MAX_LEN);
	
			
	MEMCPY(sstab_name, name, STRLEN(name));

	
	sstab_id = *(int *)row_locate_col(rp,TABLET_SSTABID_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLET, &namelen);

	res_sstab_id = *(int *)row_locate_col(rp, TABLET_RESSSTABID_COLOFF_INROW, 
					      ROW_MINLEN_IN_TABLET, &namelen);

	

		
	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		goto exit;
	}
	
	

	
	col_buf_len = sizeof(INSMETA) + sizeof(TABLEHDR) 
				+ tab_hdr->tab_col * (sizeof(COLINFO));
	col_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(col_buf, col_buf_len);

	
	col_buf_idx = 0;
		
	MEMCPY((col_buf + col_buf_idx), rg_addr, STRLEN(rg_addr));
	col_buf_idx += RANGE_ADDR_MAX_LEN;

	*(int *)(col_buf + col_buf_idx) = rg_port;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = RANGER_IS_ONLINE;
	col_buf_idx += sizeof(int);
	
	*(int *)(col_buf + col_buf_idx) = 0;
	col_buf_idx += sizeof(int);

	
	col_buf_idx += sizeof(int) + TABLE_NAME_MAX_LEN + sizeof(pthread_t);

	
	*(int *)(col_buf + col_buf_idx) = sstab_id;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = res_sstab_id;
	col_buf_idx += sizeof(int);

	
	*(unsigned int *)(col_buf + col_buf_idx) = SSTAB_MAP_GET_SPLIT_TS(sstab_id);
	col_buf_idx += sizeof(int);

	
	MEMCPY((col_buf + col_buf_idx), sstab_name, SSTABLE_NAME_MAX_LEN);
	col_buf_idx += SSTABLE_NAME_MAX_LEN;

	
	col_buf_idx += sizeof(int);

	
        *(int *)(col_buf + col_buf_idx) = tab_hdr->tab_col;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr->tab_varcol;
	col_buf_idx += sizeof(int);

	*(int *)(col_buf + col_buf_idx) = tab_hdr->tab_row_minlen;
	col_buf_idx += sizeof(int);

	
	*(int *)(col_buf + col_buf_idx) = tabletid;
	col_buf_idx += sizeof(int);

	
	MEMCPY((col_buf + col_buf_idx), tab_hdr, sizeof(TABLEHDR));
	col_buf_idx += sizeof(TABLEHDR);
	
	Assert(Master_infor->meta_syscol->colnum[tabidx] == tab_hdr->tab_col);
	
	MEMCPY((col_buf + col_buf_idx), &(Master_infor->meta_syscol->colinfor[tabidx]),tab_hdr->tab_col * sizeof(COLINFO));
	
	col_buf_idx += tab_hdr->tab_col * sizeof(COLINFO);
	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, col_buf_idx, col_buf);
	}
	else
	{
		if (rg_suspect)
		{
			rpc_status |= RPC_RETRY;
		}
		else
		{
			rpc_status |= RPC_FAIL;
		}
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	if (col_buf != NULL)
	{
		MEMFREEHEAP(col_buf);
	}

	return resp;
}




char *
meta_selrangetab(TREE *command, TABINFO *tabinfo)
{
	LOCALTSS(tss);
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	char   		*col_buf;
	int		col_buf_idx;
	int		col_buf_len;
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	int		sstab_rlen;
	int		sstab_idx;
	char		*resp;
	char		*rp;
	char		*name;
	int		namelen;
	char		sstab_name[SSTABLE_NAME_MAX_LEN];
	int		sstab_id;
	int		res_sstab_id;
	char		*rg_addr;
	int		rg_port;
	int		key_is_expand;
	int		rpc_status;
	int		rg_suspect;
	int		tabidx;


	Assert(command);

	rtn_stat = FALSE;
	col_buf= NULL;
	sstab_rlen = 0;
	sstab_idx = 0;
	rpc_status = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	rg_suspect = FALSE;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist.\n", tab_name);
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}
	
	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", tab_name);
		rpc_status |= RPC_TAB_HAS_NO_DATA;
		goto exit;
	}

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	sstab_map = sstab_map_get(tab_hdr->tab_id, tab_dir, &tab_sstabmap);

	Assert(sstab_map != NULL);

	if (sstab_map == NULL)
	{
		traceprint("Table %s has no sstabmap in the metaserver!", tab_name);
		ex_raise(EX_ANY);
	}

	char	*range_leftkey;
	int	leftkeylen;
	char	*range_rightkey;
	int	rightkeylen;
	
	range_leftkey = par_get_colval_by_colid(command, 1, &leftkeylen);
	range_rightkey = par_get_colval_by_colid(command, 2, &rightkeylen);
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");


	char	*keycol;
	int	keycolen;
	int	k;

	col_buf_len = sizeof(SELRANGE) + sizeof(SVR_IDX_FILE) + sizeof(TABLEHDR) + 
					tab_hdr->tab_col * (sizeof(COLINFO));
	col_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(col_buf, col_buf_len);
	col_buf_idx = 0;


	
	for (k = 0; k < 2; k++)
	{
		key_is_expand = FALSE;
		
		keycol = (k == 0) ? range_leftkey : range_rightkey;
		keycolen = (k == 0) ? leftkeylen : rightkeylen;

		if ((keycolen == 1) && (!strncasecmp("*", keycol, keycolen)))
		{
			
			key_is_expand = TRUE;
			
			if (k == 0)
			{
				rp = tablet_schm_get_row(tab_hdr->tab_id, 
							TABLETSCHM_ID,
							tab_meta_dir, 0);
			}
			else
			{
				rp = tablet_schm_get_row(tab_hdr->tab_id,
							TABLETSCHM_ID,
							tab_meta_dir, -1);
			}
		}
		else
		{
			
			rp = tablet_schm_srch_row(tab_hdr->tab_id,
						TABLETSCHM_ID, tab_meta_dir, 
						keycol, keycolen);
		}
		
		name = row_locate_col(rp, TABLETSCHM_TABLETNAME_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, &namelen);

		
		int ign;
		rg_addr = row_locate_col(rp, TABLETSCHM_RGADDR_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, &ign);

		rg_port = *(int *)row_locate_col(rp, 
					TABLETSCHM_RGPORT_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, &ign);
		
		tss->tcur_rgprof = rebalan_get_rg_prof_by_addr(rg_addr, rg_port);

		Assert(tss->tcur_rgprof);

		
		if (tss->tcur_rgprof == NULL)
		{
			traceprint("Can't get the profile of ranger server %s\n", rg_addr);

			goto exit;
		}

		if (tss->tcur_rgprof->rg_stat & RANGER_IS_OFFLINE)
		{
			traceprint("Ranger server (%s:%d) is OFF-LINE\n", rg_addr, rg_port);

			goto exit;
		}
		else if (tss->tcur_rgprof->rg_stat & RANGER_IS_SUSPECT)
		{
			traceprint("Ranger server (%s:%d) is SUSPECT\n", rg_addr, rg_port);
			rg_suspect = TRUE;
			goto exit;
		}
		else if (tss->tcur_rgprof->rg_stat & RANGER_NEED_RECOVERY)
		{
			traceprint("Ranger server (%s:%d) is being in the recovery\n", rg_addr, rg_port);

			rg_suspect = TRUE;
			goto exit;
		}
		else if (tss->tcur_rgprof->rg_stat & RANGER_RESTART)
		{
			traceprint("Ranger server (%s:%d) is booting.\n", rg_addr, rg_port);

			rg_suspect = TRUE;
			goto exit;
		}
		
		MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
		str1_to_str2(tab_meta_dir, '/', name);

		int tabletid;

		tabletid = *(int *)row_locate_col(rp, 
					TABLETSCHM_TABLETID_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, &namelen);

		if (key_is_expand)
		{
			if (k == 0)
			{
				rp = tablet_get_1st_or_last_row(tab_hdr->tab_id,
								tabletid, 
								tab_meta_dir,
								TRUE, TRUE);
			}
			else
			{
				rp = tablet_get_1st_or_last_row(tab_hdr->tab_id, 
								tabletid, 
								tab_meta_dir, 
								FALSE, TRUE);
			}
		}
		else
		{
			rp = tablet_srch_row(tabinfo, tab_hdr->tab_id, 
						tabletid, tab_meta_dir, keycol, 
						keycolen);
		}
		
		name = row_locate_col(rp, TABLET_SSTABNAME_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLET, &namelen);

		MEMSET(sstab_name, SSTABLE_NAME_MAX_LEN);
				
		MEMCPY(sstab_name, name, STRLEN(name));

		
		sstab_id = *(int *)row_locate_col(rp,TABLET_SSTABID_COLOFF_INROW,
						ROW_MINLEN_IN_TABLET, &namelen);

		res_sstab_id = *(int *)row_locate_col(rp, 
						TABLET_RESSSTABID_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLET, &namelen);

		
		if (tabinfo->t_sinfo->sistate & SI_NODATA)
		{
			goto exit;
		}

		
		MEMCPY((col_buf + col_buf_idx), rg_addr, STRLEN(rg_addr));
		col_buf_idx += RANGE_ADDR_MAX_LEN;

		*(int *)(col_buf + col_buf_idx) = rg_port;
		col_buf_idx += sizeof(int);

		*(int *)(col_buf + col_buf_idx) = RANGER_IS_ONLINE;
		col_buf_idx += sizeof(int);
		
		*(int *)(col_buf + col_buf_idx) = 0;
		col_buf_idx += sizeof(int);

		
		col_buf_idx += sizeof(int) + TABLE_NAME_MAX_LEN + sizeof(pthread_t);
		
		*(int *)(col_buf + col_buf_idx) = sstab_id;
		col_buf_idx += sizeof(int);

		
		*(int *)(col_buf + col_buf_idx) = res_sstab_id;
		col_buf_idx += sizeof(int);

		
		*(unsigned int *)(col_buf + col_buf_idx) = SSTAB_MAP_GET_SPLIT_TS(sstab_id);
		col_buf_idx += sizeof(int);

		
		MEMCPY((col_buf + col_buf_idx), sstab_name, SSTABLE_NAME_MAX_LEN);
		col_buf_idx += SSTABLE_NAME_MAX_LEN;

		
		col_buf_idx += sizeof(int);

		
	        *(int *)(col_buf + col_buf_idx) = tab_hdr->tab_col;
		col_buf_idx += sizeof(int);

		*(int *)(col_buf + col_buf_idx) = tab_hdr->tab_varcol;
		col_buf_idx += sizeof(int);

		*(int *)(col_buf + col_buf_idx) = tab_hdr->tab_row_minlen;
		col_buf_idx += sizeof(int);

		col_buf_idx += sizeof(int);

	}

	MEMCPY((col_buf + col_buf_idx), &(Master_infor->rg_list), 
					sizeof(SVR_IDX_FILE));
	col_buf_idx += sizeof(SVR_IDX_FILE);
	
	MEMCPY((col_buf + col_buf_idx), tab_hdr, sizeof(TABLEHDR));
	col_buf_idx += sizeof(TABLEHDR);
	
	Assert(Master_infor->meta_syscol->colnum[tabidx] == tab_hdr->tab_col);

	MEMCPY((col_buf + col_buf_idx), &(Master_infor->meta_syscol->colinfor[tabidx]),tab_hdr->tab_col * sizeof(COLINFO));

	col_buf_idx += tab_hdr->tab_col * sizeof(COLINFO);
	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, col_buf_idx, col_buf);
	}
	else
	{
		if (rg_suspect)
		{
			rpc_status |= RPC_RETRY;
		}
		else
		{
			rpc_status |= RPC_FAIL;
		}
		
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	if (col_buf != NULL)
	{
		MEMFREEHEAP(col_buf);
	}

	return resp;
}


char *
meta_selwheretab(TREE *command, TABINFO *tabinfo)
{
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	char   		*col_buf;
	int		col_buf_idx;
	int		col_buf_len;
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	int		sstab_rlen;
	int		sstab_idx;
	char		*resp;
	char		*rp;
	char		*name;
	int		namelen;
	int		key_is_expand;
	SELWHERE	selwhere;
	int		rpc_status;
	int		tabidx;
	int		querytype;
	char		*resp_buf;


	Assert(command);

	rtn_stat = FALSE;
	col_buf= NULL;
	sstab_rlen = 0;
	sstab_idx = 0;
	rpc_status = 0;
	resp_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	querytype = command->sym.command.querytype;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist.\n", tab_name);
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}
	
	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", tab_name);
		rpc_status |= RPC_TAB_HAS_NO_DATA;
		goto exit;
	}

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	if (!meta_check_validation())
	{
		traceprint("Meta on this table is not valid.\n");
		goto exit;
	}

	char	*range_leftkey;
	int	leftkeylen;
	char	*range_rightkey;
	int	rightkeylen;
	int	colmap; 
	int	syscolidx;
	

	Assert(Master_infor->meta_syscol->colnum[tabidx] == tab_hdr->tab_col);
	
	col_buf = (char *)(&(Master_infor->meta_syscol->colinfor[tabidx]));

	MEMSET(&selwhere, sizeof(SELWHERE));
			
	if (   (querytype == SELECTWHERE) || (querytype == SELECTCOUNT) 
	    || (querytype == SELECTSUM) || (querytype == DELETEWHERE)
	    || (querytype == UPDATE))
	{
		

		
		int i;

		for (i = 0; i < tab_hdr->tab_col; i++)
		{
			
			if (((COLINFO *)col_buf)[i].col_id == 1)
			{
				break;
			}
		}	

		CONSTANT *cons = par_get_constant_by_colname(command, 
						((COLINFO *)col_buf)[i].col_name);

	

		if ((cons == NULL) ||(cons->constat & CONSTANT_LIKE_OP))
		{
			range_leftkey = "*\0";
			range_rightkey = "*\0";
			leftkeylen = 1;
			rightkeylen= 1;

			
			colmap = qryopt_get_colmap_by_cmd(command, 
							(COLINFO *)col_buf,
							tab_hdr->tab_col);
			syscolidx = index_get_meta_by_colmap(tab_hdr->tab_id,
					colmap, Master_infor->meta_sysindex);

			if (syscolidx != -1)
			{
				MEMCPY(&(selwhere.idxmeta), 
				   &(Master_infor->meta_sysindex->idx_meta[syscolidx]),
				   sizeof(IDXMETA));
				selwhere.use_idxmeta = TRUE;
			}
		}
		else
		{
			range_leftkey = cons->value;
			range_rightkey = cons->rightval;
			leftkeylen = cons->len;
			rightkeylen= cons->rightlen;
		}
	}
	else if (querytype == SELECTRANGE)
	{
		range_leftkey = par_get_colval_by_colid(command, 1, &leftkeylen);
		range_rightkey = par_get_colval_by_colid(command, 2, &rightkeylen);	
	}
	
	char	*keycol;
	int	keycolen;
	int	k;
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");
	
	for (k = 0; k < 2; k++)
	{
		key_is_expand = FALSE;
		
		keycol = (k == 0) ? range_leftkey : range_rightkey;
		keycolen = (k == 0) ? leftkeylen : rightkeylen;

		if ((keycolen == 1) && (!strncasecmp("*", keycol, keycolen)))
		{
			key_is_expand = TRUE;
			
			if (k == 0)
			{
				
				rp = tablet_schm_get_row(tab_hdr->tab_id,
							TABLETSCHM_ID, 
							tab_meta_dir, 0);
			}
			else
			{
				
				rp = tablet_schm_get_row(tab_hdr->tab_id,
							TABLETSCHM_ID, 
							tab_meta_dir, -1);
			}
		}
		else
		{
			
			rp = tablet_schm_srch_row(tab_hdr->tab_id, 
						TABLETSCHM_ID, tab_meta_dir, 
					  	keycol, keycolen);
		}

		
		name = row_locate_col(rp, TABLETSCHM_TABLETNAME_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, &namelen);


		if (k == 0)
		{
			MEMCPY(selwhere.lefttabletname, name, STRLEN(name));
			selwhere.leftnamelen = STRLEN(name);
		}
		else
		{
			MEMCPY(selwhere.righttabletname, name, STRLEN(name));
			selwhere.rightnamelen = STRLEN(name);
		}
	}	

	col_buf_len = sizeof(SELWHERE) + sizeof(SVR_IDX_FILE) + sizeof(TABLEHDR) + 
					tab_hdr->tab_col * (sizeof(COLINFO));
	
	resp_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(resp_buf, col_buf_len);
	
	col_buf_idx = 0;		
	
	
	MEMCPY((resp_buf + col_buf_idx), &selwhere, sizeof(SELWHERE));
	col_buf_idx += sizeof(SELWHERE);

	
	MEMCPY((resp_buf + col_buf_idx), &(Master_infor->rg_list), 
					sizeof(SVR_IDX_FILE));
	col_buf_idx += sizeof(SVR_IDX_FILE);

	
	
	MEMCPY((resp_buf + col_buf_idx), tab_hdr, sizeof(TABLEHDR));

	col_buf_idx += sizeof(TABLEHDR);

	MEMCPY((resp_buf + col_buf_idx), col_buf, tab_hdr->tab_col * sizeof(COLINFO));
	
	col_buf_idx += tab_hdr->tab_col * sizeof(COLINFO);
	


	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, col_buf_idx, resp_buf);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	if (resp_buf != NULL)
	{
		MEMFREEHEAP(resp_buf);
	}

	return resp;
}


char *
meta_addsstab(TREE *command, TABINFO *tabinfo)
{
	LOCALTSS(tss);
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	char		*keycol;
	int		keycolen;
	char		*resp;
	int		tablet_min_rlen;
	int		namelen;
	char		*name;
	char		*rp;
	char		*sstab_name;
	int		sstab_name_len;
	
	char		*sstab_rp;
	int		sstab_rlen;
	char		*rg_addr;
	int		rpc_status;
	int		tabidx;
	int		resp_buf_len;
	char		*resp_buf;


	Assert(command);

	rtn_stat = FALSE;
	rpc_status = 0;
	resp_buf_len = 0;
	resp_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	volatile struct
	{
		char	*resp_buf;
		char	*sstab_rp;
	} copy;

	copy.resp_buf = NULL;
	copy.sstab_rp = NULL;
	tabinfo = NULL;

	resp = NULL;

	if(ex_handle(EX_ANY, yxue_handler))
	{
		if (copy.resp_buf)
		{
			MEMFREEHEAP(copy.resp_buf);
		}

		if (copy.sstab_rp)
		{
			MEMFREEHEAP(copy.sstab_rp);
		}

		traceprint("Add sstable hit error!\n");
		ex_delete();
		ex_raise(EX_ANY);
	}
	
	str1_to_str2(tab_dir, '/', tab_name);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}


	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);
	
	Assert(tab_hdr->tab_tablet > 0);

	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s should be has one tablet at least\n", tab_name);
		rpc_status |= RPC_TAB_HAS_NO_DATA;
		goto exit;
	}

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}
		
	
	sstab_name= par_get_colval_by_colid(command, 1, &sstab_name_len);

	int colen;
	char *colptr = par_get_colval_by_colid(command, 2, &colen);

	sstab_map = sstab_map_get(tab_hdr->tab_id, tab_dir, &tab_sstabmap);

	
	int sstab_id;

	sstab_id = m_atoi(colptr, colen);

	int split_ts;

	colptr = par_get_colval_by_colid(command, 3, &colen);
	split_ts = m_atoi(colptr, colen);

	int split_sstabid;
	
	colptr = par_get_colval_by_colid(command, 4, &colen);
	split_sstabid = m_atoi(colptr, colen);
	
	keycol = par_get_colval_by_colid(command, 5, &keycolen);

	
	SSTAB_MAP_SET_SPLIT_TS(split_sstabid, split_ts);
	

	
	SSTAB_MAP_SET(sstab_id, SSTAB_USED);

	
	sstab_rlen = ROW_MINLEN_IN_TABLET + keycolen + 4 + 4;

	sstab_rp = MEMALLOCHEAP(sstab_rlen);

	copy.sstab_rp = sstab_rp;

	int res_sstab_id;
	res_sstab_id = meta_get_free_sstab();
	SSTAB_MAP_SET(res_sstab_id, SSTAB_RESERVED);		

	
	tablet_min_rlen = tablet_bld_row(sstab_rp, sstab_rlen, tab_name, 
					 tab_name_len, sstab_id, res_sstab_id,
					 sstab_name, sstab_name_len, keycol, 
					 keycolen, tab_hdr->tab_key_coltype);


	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	
	rp = tablet_schm_srch_row(tab_hdr->tab_id, TABLETSCHM_ID, 
				  tab_meta_dir, keycol, keycolen);

	
	name = row_locate_col(rp, TABLETSCHM_TABLETNAME_COLOFF_INROW, 
			      ROW_MINLEN_IN_TABLETSCHM, &namelen);

	char	tabletname[TABLE_NAME_MAX_LEN];

	MEMSET(tabletname, TABLE_NAME_MAX_LEN);
	MEMCPY(tabletname, name, STRLEN(name));

	int ign;
	rg_addr = row_locate_col(rp, TABLETSCHM_RGADDR_COLOFF_INROW, 
				 ROW_MINLEN_IN_TABLETSCHM, &ign);
	int rg_port = *(int *)row_locate_col(rp, TABLETSCHM_RGPORT_COLOFF_INROW,
				 ROW_MINLEN_IN_TABLETSCHM, &ign);

	tss->tcur_rgprof = rebalan_get_rg_prof_by_addr(rg_addr, rg_port);

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', name);

	int tabletid;
	tabletid = *(int *)row_locate_col(rp, TABLETSCHM_TABLETID_COLOFF_INROW, 
					  ROW_MINLEN_IN_TABLETSCHM, &namelen);

	char	ri_sstab[TABLE_NAME_MAX_LEN];
	char	tmp_ri_sstab[TABLE_NAME_MAX_LEN];
		
	MEMSET(ri_sstab, TABLE_NAME_MAX_LEN);
	MEMSET(tmp_ri_sstab, TABLE_NAME_MAX_LEN);
	MEMCPY(ri_sstab, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	str1_to_str2(ri_sstab, '/', tab_name);
	
	MEMCPY(tmp_ri_sstab, sstab_name, sstab_name_len);

	str1_to_str2(ri_sstab, '/', tmp_ri_sstab);

	
	sstab_map_put(-1, tss->ttab_sstabmap);

	
	if (tablet_ins_row(tab_hdr, tab_hdr->tab_id, tabletid, tab_meta_dir, 
			   sstab_rp, ROW_MINLEN_IN_TABLET))
	{
		if (index_root_crt_empty(tab_hdr->tab_id, tab_name,
					tab_hdr->tab_tablet - 1,
					tabletid,					
					Master_infor->meta_sysindex))
		{
			int	buf_idx;
			IDX_ROOT_SPLIT	idx_root_split;
			
			
			resp_buf_len = sizeof(INSMETA) + sizeof(IDX_ROOT_SPLIT);
			
			resp_buf = MEMALLOCHEAP(resp_buf_len);

			copy.resp_buf = resp_buf;
			
			MEMSET(resp_buf, resp_buf_len);

			
			buf_idx = 0;

			
			MEMCPY((resp_buf + buf_idx), rg_addr, STRLEN(rg_addr));
			buf_idx += RANGE_ADDR_MAX_LEN;

			
			*(int *)(resp_buf + buf_idx) = rg_port;
			buf_idx += sizeof(int);

			
			*(int *)(resp_buf + buf_idx) = RANGER_IS_ONLINE;
			buf_idx += sizeof(int);

			idx_root_split.idx_srcroot_id = tabletid;
			idx_root_split.idx_destroot_id = tab_hdr->tab_tablet - 1;
			idx_root_split.idx_tabid = tab_hdr->tab_id;
			idx_root_split.idx_ts = tab_hdr->index_ts;

			MEMSET(idx_root_split.idx_tabname, TABLE_NAME_MAX_LEN);

			MEMCPY(idx_root_split.idx_tabname, tab_name, 
							STRLEN(tab_name));

			MEMCPY((resp_buf + sizeof(INSMETA)), &idx_root_split, 
						sizeof(IDX_ROOT_SPLIT));
			
		}
		
		ri_rgstat_putdata(tss->tcur_rgprof->rg_statefile, ri_sstab,
					RG_SSTABLE_DELETED, NULL);

		int	status = FALSE;
		char	cmd_str[TABLE_NAME_MAX_LEN];
		
		MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
		
#ifdef MT_KFS_BACKEND
		sprintf(cmd_str, "%s/%s/%s",  tss->metabackup, tab_name, 
					tabletname);
		RMFILE(status, cmd_str);
		if(!status)
#else			
		sprintf(cmd_str, "rm -rf %s/%s/%s",  tss->metabackup, tab_name,
					tabletname);
		
		if (!system(cmd_str))
#endif
		{
			status = TRUE;
		}

		if (!status)
		{
			traceprint("Failed to remove the backup director for the tablet split.\n");
		}

		meta_save_rginfo();		
	}

	ri_rgstat_deldata(tss->tcur_rgprof->rg_statefile, ri_sstab);
	
	MEMFREEHEAP(sstab_rp);

	copy.sstab_rp = NULL;

	
	(tab_hdr->tab_sstab)++;

	
	if (!meta_save_sysobj(tab_dir, (char *)tab_hdr))
	{
		goto exit;
	}

	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, resp_buf_len, resp_buf);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	if (resp_buf != NULL)
	{
		MEMFREEHEAP(resp_buf);
	}

	ex_delete();

	return resp;

}



static int
meta_get_free_sstab()
{
	int i = 0;

	while (i < SSTAB_MAP_ITEM)
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
		traceprint("sstab_map[%d] == %d \n", begin, sstab_map[begin].sstab_stat);

		begin++;
	}
}
static int
meta_crt_rg_logbackup_file(char *rgip, int rgport)
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

	if (STAT(rglogfile, &st) != 0)
	{
		MKDIR(status, rglogfile, 0755);
	}
	
	str1_to_str2(rglogfile, '/', "log");

	
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


static int
meta_crt_rgstate_file(RANGE_PROF *rg_prof, char *rgip, int rgport)
{
	char		rgstatefile[256];
	char		rgname[64];
	RG_STATE	*statefile;
	int		fd;


	MEMSET(rgstatefile, 256);
	MEMCPY(rgstatefile, MT_RANGE_STATE, STRLEN(MT_RANGE_STATE));

	MEMSET(rgname, 64);
	sprintf(rgname, "%s%d", rgip, rgport);

	str1_to_str2(rgstatefile, '/', rgname);

	if (STAT(rgstatefile, &st) == 0)
	{
		traceprint("Ranger state file (%s)is exist", rgstatefile);
		return FALSE;
	}
	
	
	OPEN(fd, rgstatefile, (O_CREAT|O_WRONLY|O_TRUNC));

	statefile = (RG_STATE *)malloc(sizeof(RG_STATE));
	MEMSET(statefile, sizeof(RG_STATE));

	statefile->offset = RG_STATE_HEADER;

	WRITE(fd, statefile, sizeof(RG_STATE));

	CLOSE(fd);

	free(statefile);

	
	MEMCPY(rg_prof->rg_statefile, rgstatefile, STRLEN(rgstatefile));

	return TRUE;
}

static int
meta_collect_rg(char * req_buf)
{
	char		*str;
	int		i;
	SVR_IDX_FILE	*rglist;
	int		found, start_heartbeat;
	RANGE_PROF	*rg_addr;
	int		rginfo_save;

	
	if (strncasecmp(RPC_RG2MASTER_REPORT, req_buf, 
				STRLEN(RPC_RG2MASTER_REPORT)) != 0)
	{
		return FALSE;
	}
	
	str = req_buf + RPC_MAGIC_MAX_LEN;

	rglist = &(Master_infor->rg_list);
	rginfo_save = FALSE;
	found = FALSE;
	start_heartbeat = TRUE;
	rg_addr = (RANGE_PROF *)(rglist->data);

	P_SPINLOCK(Master_infor->rglist_spinlock);
	
	for(i = 0; i < rglist->nextrno; i++)
	{
		if (   !strncasecmp(str, rg_addr[i].rg_addr, RANGE_ADDR_MAX_LEN)
		    && (rg_addr[i].rg_port == *(int *)(str + RANGE_ADDR_MAX_LEN))
		   )
		{
			found = TRUE;
			
			if(rg_addr[i].rg_stat & RANGER_IS_OFFLINE)
			{
				if(rg_addr[i].rg_stat & RANGER_NEED_RECOVERY)
				{
					rg_addr[i].rg_stat |= RANGER_RESTART;

					break;
				}
				
				rg_addr[i].rg_stat &= ~RANGER_IS_OFFLINE;
				rg_addr[i].rg_stat |= RANGER_IS_ONLINE;

				rginfo_save = TRUE;
				
				break;
			}
			else if(rg_addr[i].rg_stat & RANGER_IS_ONLINE)
			{
				if (HB_RANGER_IS_ON(&(Master_infor->heart_beat_data[i])))
				{
					
					traceprint("\n rg server with same ip and port is already on line \n");
					start_heartbeat = FALSE;
				}
				else
				{
					start_heartbeat = TRUE;
				}
				break;
			}

			
		}
	}


	if (!found)
	{
		MEMCPY(rg_addr[i].rg_addr, str, RANGE_ADDR_MAX_LEN);
		rg_addr[i].rg_port = *(int *)(str + RANGE_ADDR_MAX_LEN);
		rg_addr[i].rg_stat = RANGER_IS_ONLINE;
		rg_addr[i].rg_tablet_num = 0;
		rg_addr[i].rg_index = i;

		(rglist->nextrno)++;

		meta_crt_rg_logbackup_file(rg_addr[i].rg_addr,
						rg_addr[i].rg_port);

		meta_crt_rgstate_file(&(rg_addr[i]),rg_addr[i].rg_addr,
						rg_addr[i].rg_port);
		
		rginfo_save = TRUE;
	}

	if(start_heartbeat)
	{			
		meta_heartbeat_setup(rg_addr + i);
	}

	if (rginfo_save)
	{
		meta_save_rginfo();
	}

	V_SPINLOCK(Master_infor->rglist_spinlock);
	
	return TRUE;
}



int
meta_rebalan_svr_idx_file(char *tab_dir, REBALANCE_DATA *rbd)
{
	SVR_IDX_FILE	*tablet_store;
	char		tab_dir1[256];
	int		fd;
	int		status;
	RANGE_PROF	*rg_prof;
	int 		i, j;
	int		max_tablet;
	int		min_tablet;
	int		max_rg;
	int		min_rg;
	int		total_tablet;
	int		transfer_tablet;
	SVR_IDX_FILE	*temp_store;


	tablet_store = (SVR_IDX_FILE *)MEMALLOCHEAP(sizeof(SVR_IDX_FILE));
	total_tablet = 0;
	transfer_tablet = 0;
	
	MEMSET(tab_dir1, 256);

	MEMCPY(tab_dir1, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_dir1, '/', "tabletinranger");
	
	OPEN(fd, tab_dir1, (O_RDWR));

	if (fd < 0)
	{
		goto exit;
	}

		
	status = READ(fd, tablet_store, sizeof(SVR_IDX_FILE));

	Assert(status == sizeof(SVR_IDX_FILE));

	

	//rg_prof = (RANGE_PROF *)tablet_store->data;
	temp_store = &(Master_infor->rg_list);
	rg_prof = (RANGE_PROF *)(temp_store->data);

	for(j = 0; j < temp_store->nextrno; j++)
	{
		if(rg_prof[j].rg_stat & RANGER_IS_ONLINE)
		{
			max_tablet = min_tablet = rg_prof[j].rg_tablet_num;
			max_rg = min_rg = j;
			break;
		}
	}
	total_tablet += rg_prof[j].rg_tablet_num;
	
	for(i = j + 1; i < temp_store->nextrno; i++)
	{
		if(rg_prof[i].rg_stat & RANGER_IS_ONLINE)
		{
			if (rg_prof[i].rg_tablet_num > max_tablet)
			{
				max_tablet = rg_prof[i].rg_tablet_num;
				max_rg = i;
			}
			else if (rg_prof[i].rg_tablet_num < min_tablet)
			{
				min_tablet = rg_prof[i].rg_tablet_num;
				min_rg = i;
			}
			total_tablet += rg_prof[i].rg_tablet_num;
		}
	}

	if (max_tablet != min_tablet)
	{
		if (((max_tablet - min_tablet) / i) < 1)
		{
			transfer_tablet = 0;
		}
		else 
		{
			transfer_tablet = (total_tablet / i) - min_tablet;
		}
	}

	if (transfer_tablet > 0)
	{
		rg_prof[max_rg].rg_tablet_num = max_tablet - transfer_tablet;
		rg_prof[min_rg].rg_tablet_num = min_tablet + transfer_tablet;

		
		//WRITE(fd, tablet_store, sizeof(SVR_IDX_FILE));

		MEMCPY(rbd->rbd_max_tablet_rg, rg_prof[max_rg].rg_addr, 
			STRLEN(rg_prof[max_rg].rg_addr));
		rbd->rbd_max_tablet_rgport = rg_prof[max_rg].rg_port;
		MEMCPY(rbd->rbd_min_tablet_rg, rg_prof[min_rg].rg_addr, 
			STRLEN(rg_prof[min_rg].rg_addr));
		rbd->rbd_min_tablet_rgport = rg_prof[min_rg].rg_port;

		meta_save_rginfo();
	}

	
exit:
	CLOSE(fd);
	MEMFREEHEAP(tablet_store);

	return transfer_tablet;
}


int
meta_rebalan_process(REBALANCE_DATA *rbd)
{
	int		sockfd;
	RPCRESP		*resp;
	//RANGE_PROF	*rg_prof;
	int		rtn_stat;


	rtn_stat = TRUE;
	//rg_prof = rebalan_get_rg_prof_by_addr(rbd->rbd_max_tablet_rg);
	
	sockfd = conn_open(rbd->rbd_max_tablet_rg, rbd->rbd_max_tablet_rgport);

	//rg_prof = rebalan_get_rg_prof_by_addr(rbd->rbd_min_tablet_rg);

	//rbd->rbd_min_tablet_rgport = rg_prof->rg_port;

	tcp_put_data(sockfd, (char *)rbd, sizeof(REBALANCE_DATA));

	resp = conn_recv_resp(sockfd);

	if ((resp == NULL) || (resp->status_code != RPC_SUCCESS))
	{
		rtn_stat = FALSE;
		traceprint("\n ERROR \n");
	}
	
	conn_close(sockfd, NULL, resp);

	return rtn_stat;

}

char *
meta_rebalancer(TREE *command)
{
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[256];
	char		tab_dir1[256];	
	char 		*resp;
	char		tab_tabletschm_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	char		*tablet_schm_bp;
	int		rpc_status;
	int		tabidx;
	
	TABINFO		*tabinfo;
	int		minrowlen;
	BLK_ROWINFO	blk_rowinfo;
	BUF		*bp;


	Assert(command);
	
	rtn_stat = FALSE;
	resp = NULL;
	rpc_status = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	tabinfo = NULL;
	tablet_schm_bp = NULL;

	
	MEMSET(tab_dir, 256);
	MEMSET(tab_dir1, 256);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}	

	TABLEHDR		*tab_hdr;
	REBALANCE_STATISTICS	rbs;
	
	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (!meta_rg_statistics(tab_dir, tab_hdr->tab_id, &rbs))
	{
		rtn_stat = TRUE;
		goto exit;
	}
	
	MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_tabletschm_dir, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_tabletschm_dir, '/', "tabletscheme");

	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	
	TABINFO_INIT(tabinfo, tab_tabletschm_dir, tab_name, tab_name_len, tabinfo->t_sinfo, minrowlen, 
			TAB_SCHM_INS, tab_hdr->tab_id, TABLETSCHM_ID);
	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, TABLETSCHM_KEY_COLID_INROW, 
		       VARCHAR, -1);
			
	bp = blk_getsstable(tabinfo);

	tablet_schm_bp = (char *)(bp->bsstab->bblk);

	BLOCK *blk;

	int		i = 0, rowno;
	int		*offset;
	char 		*rp;
	char		*addr_in_blk;
	int		addrlen_in_blk;
	int 		port_in_blk;
	int 		portlen_in_blk;

	int		tablet_cnt = 0;
	int		rg_idx = 0;
	RANGE_PROF	*rg_prof = NULL;
	
	while (rbs.rbs_tablet_tot_num)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);

		
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
				rowno < blk->bnextrno; rowno++, offset--)
		{
			if (   (tablet_cnt == rbs.rbs_tablet_av_num) 
			    && (rbs.rbs_rg_num > 0))
			{
				tablet_cnt = 0;
			}

			if (tablet_cnt == 0)
			{
				rg_prof = meta_get_next_rg(&rg_idx);

				if (rg_prof == NULL)
				{
					bufunkeep(bp->bsstab);
					bufdestroy(bp->bsstab);
										
					MEMFREEHEAP(tabinfo->t_sinfo);
					MEMFREEHEAP(tabinfo);
					
					tabinfo_pop();

					goto exit;
				}

				rbs.rbs_rg_num--;

			}
			
			rp = (char *)blk + *offset;
		
			Assert(*offset < blk->bfreeoff);
		
			addr_in_blk = row_locate_col(rp, 
					TABLETSCHM_RGADDR_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, 
					&addrlen_in_blk);
			port_in_blk = *(int *)row_locate_col(rp, 
					TABLETSCHM_RGPORT_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM,
					&portlen_in_blk);
			
			MEMCPY(addr_in_blk, rg_prof->rg_addr,
					RANGE_ADDR_MAX_LEN);
			
			int *tmp_addr = (int *)row_locate_col(rp, 
					TABLETSCHM_RGPORT_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, 
					&portlen_in_blk);
			
			*tmp_addr = rg_prof->rg_port;
		
			tablet_cnt++;
			rbs.rbs_tablet_tot_num--;
		
		}

		i++;

		if (i > (BLK_CNT_IN_SSTABLE - 1))
		{
			break;
		}			
	
	}

	bufpredirty(bp->bsstab);
	bufdirty(bp->bsstab);
	
	bufunkeep(bp->bsstab);
	
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

	
	rtn_stat = TRUE;
	
exit:

	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}
	
	return resp;

}


char *
meta_sharding(TREE *command, int sharding_table)
{
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[256];
	char 		*resp;
	char		tab_tabletschm_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	int		rpc_status;
	int		tabidx;
	int		tablet_is_sharding;
	int		resp_buf_len;
	char		*resp_buf;


	Assert(command);
	
	rtn_stat = FALSE;
	tablet_is_sharding = FALSE;
	resp = NULL;
	rpc_status = 0;
	resp_buf_len = 0;
	resp_buf = NULL;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;

	MEMSET(tab_dir, 256);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}	

	TABLEHDR		*tab_hdr;
	
	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->has_index && sharding_table)
	{
		traceprint("Table %s can not be sharding because it has at least one index! You can use the 'sharding tablet' to replace it.\n", tab_name);

		goto exit;
	}
	
	MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_tabletschm_dir, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_tabletschm_dir, '/', "tabletscheme");

	char 		*rp;
	char		*rg_addr;
	int		ign;
	int 		rg_port;	
	char 		* tabletname;
	int		tabletid;
	int		rowno;

	rowno = 0;

	while(TRUE)
	{	
		rp = tablet_schm_get_row(tab_hdr->tab_id, TABLETSCHM_ID,
					tab_tabletschm_dir, rowno);	

		if (rp == NULL)
		{
			break;
		}

		tabletname = row_locate_col(rp, 
					TABLETSCHM_TABLETNAME_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &ign);

		if (!sharding_table)
		{
			Assert((command->left) && (command->left->right));
			
			
			if (strncasecmp(tabletname, 
					command->left->right->sym.constant.value,
					command->left->right->sym.constant.len) != 0)
			{
				rowno++;
				continue;
			}
		}
		
		rg_addr = row_locate_col(rp, TABLETSCHM_RGADDR_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &ign);
		
		rg_port = *(int *)row_locate_col(rp, 
					TABLETSCHM_RGPORT_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &ign);

		tabletid = *(int *)row_locate_col(rp, 
					TABLETSCHM_TABLETID_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, &ign);

		tablet_is_sharding = tablet_sharding(tab_hdr, rg_addr, rg_port,
					tab_dir, tab_hdr->tab_id, tabletname, 
					tabletid);
		if (!sharding_table)
		{
			if (tab_hdr->has_index && 
			    index_root_crt_empty(tab_hdr->tab_id, tab_name,
					tab_hdr->tab_tablet - 1,
					tabletid,					
					Master_infor->meta_sysindex))
			{
				int	buf_idx;
				IDX_ROOT_SPLIT	idx_root_split;
				
				
				resp_buf_len = sizeof(INSMETA) + sizeof(IDX_ROOT_SPLIT);
				
				resp_buf = MEMALLOCHEAP(resp_buf_len);

				MEMSET(resp_buf, resp_buf_len);

				
				buf_idx = 0;

				
				MEMCPY((resp_buf + buf_idx), rg_addr, STRLEN(rg_addr));
				buf_idx += RANGE_ADDR_MAX_LEN;

				
				*(int *)(resp_buf + buf_idx) = rg_port;
				buf_idx += sizeof(int);

				
				*(int *)(resp_buf + buf_idx) = RANGER_IS_ONLINE;
				buf_idx += sizeof(int);

				idx_root_split.idx_srcroot_id = tabletid;
				idx_root_split.idx_destroot_id = tab_hdr->tab_tablet - 1;
				idx_root_split.idx_tabid = tab_hdr->tab_id;
				idx_root_split.idx_ts = tab_hdr->index_ts;

				MEMSET(idx_root_split.idx_tabname, TABLE_NAME_MAX_LEN);

				MEMCPY(idx_root_split.idx_tabname, tab_name, 
								STRLEN(tab_name));

				MEMCPY((resp_buf + sizeof(INSMETA)), &idx_root_split, 
							sizeof(IDX_ROOT_SPLIT));
				
			}
			
			break;
		}
		
		if (tablet_is_sharding)
		{
			rowno += 2;
		}
		else
		{
			rowno++;
		}
	}
	
	rtn_stat = TRUE;
	
exit:

	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, resp_buf_len, resp_buf);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	
	if (resp_buf != NULL)
	{
		MEMFREEHEAP(resp_buf);
	}
	
	return resp;

}

static int
meta_rg_statistics(char *tab_dir, int tab_id, REBALANCE_STATISTICS *rbs)
{
	char		tab_tabletschm_dir[TABLE_NAME_MAX_LEN];
	char		*tablet_schm_bp;
	TABINFO 	*tabinfo;
	int		minrowlen;
	BLK_ROWINFO	blk_rowinfo;
	BUF		*bp;
	int		tablet_num;
	int		rg_num;
	int		rebala_need;
	RANGE_PROF	*rg_prof;
	

	MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_tabletschm_dir, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_tabletschm_dir, '/', "tabletscheme");

	

	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;


	TABINFO_INIT(tabinfo, tab_tabletschm_dir, NULL, 0, tabinfo->t_sinfo,
			minrowlen, TAB_SRCH_DATA, tab_id, TABLETSCHM_ID);
	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, TABLETSCHM_KEY_COLID_INROW, 
		       VARCHAR, -1);
			
	bp = blk_getsstable(tabinfo);

	tablet_schm_bp = (char *)(bp->bsstab->bblk);

	BLOCK *blk;

	int 	i, j, rowno;
	int	*offset;
	char	*rp;
	char	*addr_in_blk;
	int	addrlen_in_blk;
	int	port_in_blk;
	int	portlen_in_blk;
	SVR_IDX_FILE	*temp_store;
	int	rg_is_found;

	i = j = 0;

	temp_store = &(Master_infor->rg_list);
	rg_prof = (RANGE_PROF *)(temp_store->data);
	tablet_num = 0;
	rg_num = 0;
	rebala_need = FALSE;
	
	
	for(j = 0; j < temp_store->nextrno; j++)
	{
		rg_prof[j].rg_tablet_num = 0;
		
		if(rg_prof[j].rg_stat & RANGER_IS_ONLINE)
		{
			
			rg_num++;
		}
	}

	while (TRUE)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);

		
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
				rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;
		
			Assert(*offset < blk->bfreeoff);
		
			addr_in_blk = row_locate_col(rp, 
					TABLETSCHM_RGADDR_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, 
					&addrlen_in_blk);
			port_in_blk = *(int *)row_locate_col(rp, 
					TABLETSCHM_RGPORT_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM,
					&portlen_in_blk);

			rg_is_found = FALSE;
			
			for(j = 0; j < temp_store->nextrno; j++)
			{
				
				if(   !strncasecmp(rg_prof[j].rg_addr, 
					addr_in_blk, RANGE_ADDR_MAX_LEN) 
		   		   && (rg_prof[j].rg_port == port_in_blk))
				{
					rg_prof[j].rg_tablet_num++;
					tablet_num++;

					rg_is_found = TRUE;
				}
			}

			if (!rg_is_found)
			{
				traceprint("Ranegr (%s:%d) is not found.\n", addr_in_blk, port_in_blk);
				goto exit;
			}
				
		}

		i++;
		
		if (i > (BLK_CNT_IN_SSTABLE - 1)) 
		{
			break;
		}			
	}


	rbs->rbs_tablet_av_num = (tablet_num / rg_num);
	rbs->rbs_tablet_tot_num = tablet_num;
	rbs->rbs_rg_num = rg_num;

	
	for(j = 0; j < temp_store->nextrno; j++)
	{
		if(rg_prof[j].rg_stat & RANGER_IS_ONLINE)
		{
			if (rg_prof[j].rg_tablet_num < rbs->rbs_tablet_av_num)
			{
				rebala_need = TRUE;
				break;
			}
		}
	}

exit:	
	bufunkeep(bp->bsstab);

	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();
	

	return rebala_need;

}



char *
meta_crtidx(TREE *command)
{
	char		*tab_name;
	int		tab_name_len;
	char		*idx_name;
	int		idx_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	char 		*resp;
	int		tabidx;
	int		rpc_status;
	char		*resp_buf;
	int		resp_buf_len;
	int		resp_buf_idx;
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];


	Assert(command);
	
	rtn_stat = FALSE;
	resp = NULL;
	rpc_status = 0;
	resp_buf = NULL;
	resp_buf_len = 0;
	resp_buf_idx = 0;

	
	idx_name = command->sym.command.tabname;
	idx_name_len = command->sym.command.tabname_len;

	
	tab_name = (command->right)->sym.command.tabname;
	tab_name_len = (command->right)->sym.command.tabname_len;

	
	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}


	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	char	rg_tab_dir[TABLE_NAME_MAX_LEN];
	
	
	MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(rg_tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
	str1_to_str2(rg_tab_dir, '/', tab_name);
	
	
	int	status;

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, rg_tab_dir, STRLEN(rg_tab_dir));
	str1_to_str2(tab_meta_dir, '/', idx_name);
	
	
	if (STAT(tab_meta_dir, &st) == 0)
	{
		traceprint("Index %s on table %s is exist.\n", idx_name, tab_name);
		goto exit;
	}
	else
	{
		MKDIR(status, tab_meta_dir, 0755);
	}
	

	int	meta_num;
	
	meta_num = index_get_meta_by_idxname(tab_hdr->tab_id, idx_name, 
				Master_infor->meta_sysindex);

	if (meta_num != -1)
	{
		traceprint("The index (%s) has been exist on the table (%s)", idx_name, tab_name);
		goto exit;
	}

	
	++(Master_infor->last_tabid);
	
	IDXMETA	idxmeta;
	COLINFO *colinfo = (COLINFO *)(&(Master_infor->meta_syscol->colinfor[tabidx]));

	MEMSET(&idxmeta, sizeof(IDXMETA));
	MEMCPY(idxmeta.idxname, idx_name, idx_name_len);
	index_bld_meta(&idxmeta, tab_hdr, colinfo, command, Master_infor->last_tabid);

	index_ins_meta(&idxmeta, Master_infor->meta_sysindex);

	meta_save_sysindex((char *)(Master_infor->meta_sysindex));

	tab_hdr->has_index = TRUE;

	
	tab_hdr->index_ts = Master_infor->meta_sysindex->idx_ver;

	meta_save_sysobj(tab_dir, (char *)tab_hdr);
	
	rtn_stat = TRUE;

	int	fd;
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_meta_dir, '/', "systable");

	OPEN(fd, tab_meta_dir, (O_RDWR));
	
	
	Master_infor->meta_systab->last_tabid = Master_infor->last_tabid;

	
	WRITE(fd, Master_infor->meta_systab, sizeof(META_SYSTABLE));

	CLOSE(fd);

	resp_buf_len = sizeof(IDXMETA) + sizeof(SVR_IDX_FILE) + sizeof(TABLEHDR) + 
					tab_hdr->tab_col * (sizeof(COLINFO));
	
	resp_buf = MEMALLOCHEAP(resp_buf_len);
	MEMSET(resp_buf, resp_buf_len);
	
	resp_buf_idx = 0;		
	
	
	MEMCPY((resp_buf + resp_buf_idx), &idxmeta, sizeof(IDXMETA));
	resp_buf_idx += sizeof(IDXMETA);

	
	MEMCPY((resp_buf + resp_buf_idx), &(Master_infor->rg_list), 
					sizeof(SVR_IDX_FILE));
	resp_buf_idx += sizeof(SVR_IDX_FILE);
	
	
	MEMCPY((resp_buf + resp_buf_idx), tab_hdr, sizeof(TABLEHDR));

	resp_buf_idx += sizeof(TABLEHDR);

	
	MEMCPY((resp_buf + resp_buf_idx), colinfo, tab_hdr->tab_col * sizeof(COLINFO));
	
	resp_buf_idx += tab_hdr->tab_col * sizeof(COLINFO);

	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, resp_buf_idx, resp_buf);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	if (resp_buf != NULL)
	{
		MEMFREEHEAP(resp_buf);
	}

	
	return resp;
}


char *
meta_dropidx(TREE *command)
{
	char		*tab_name;
	int		tab_name_len;
	char		*idx_name;
	int		idx_name_len;
	char		tab_dir[256];
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	char 		*resp;
	int		tabidx;
	int		rpc_status;
	char		*resp_buf;
	int		resp_buf_len;
	int		resp_buf_idx;


	Assert(command);
	
	rtn_stat	= FALSE;
	resp		= NULL;
	rpc_status	= 0;
	resp_buf	= NULL;
	resp_buf_len	= 0;
	resp_buf_idx	= 0;

	
	idx_name = command->sym.command.tabname;
	idx_name_len = command->sym.command.tabname_len;

	
	tab_name = (command->right)->sym.command.tabname;
	tab_name_len = (command->right)->sym.command.tabname_len;

	
	MEMSET(tab_dir, 256);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	int	meta_num;
	
	meta_num = index_get_meta_by_idxname(tab_hdr->tab_id, idx_name, 
				Master_infor->meta_sysindex);

	if (meta_num == -1)
	{
		traceprint("The index (%s) is not exist on the table (%s)", idx_name, tab_name);
		goto exit;
	}

	IDXMETA *idx_meta = Master_infor->meta_sysindex->idx_meta;

	idx_meta[meta_num].idx_stat &= ~IDX_IN_WORKING;
	idx_meta[meta_num].idx_stat |= IDX_IN_DROP;

	meta_save_sysindex((char *)(Master_infor->meta_sysindex));

	RANGE_PROF	*rg_prof;
	
	if(Master_infor->rg_list.nextrno > 0)
	{
		
		rg_prof = meta_get_rg();

		if (!rg_prof)
		{
			traceprint("Ranger server is un-available for insert\n");
			goto exit;
		}

		Assert(rg_prof->rg_stat & RANGER_IS_ONLINE);
			
		if (!(rg_prof->rg_stat & RANGER_IS_ONLINE))
		{
			traceprint("Ranger server %s is off-line\n", rg_prof->rg_addr);
			goto exit;
		}
	}
	else
	{
		Assert(0);

		traceprint("No ranger server is avlable\n");
		ex_raise(EX_ANY);
	}

	
	resp_buf_len = RANGE_ADDR_MAX_LEN + sizeof(int);
	
	resp_buf = MEMALLOCHEAP(resp_buf_len);
	MEMSET(resp_buf, resp_buf_len);

	resp_buf_idx = 0;
		
	MEMCPY((resp_buf + resp_buf_idx), rg_prof->rg_addr, 
					STRLEN(rg_prof->rg_addr));
	resp_buf_idx += RANGE_ADDR_MAX_LEN;

	*(int *)(resp_buf + resp_buf_idx) = rg_prof->rg_port;
	resp_buf_idx += sizeof(int);

	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, resp_buf_idx, resp_buf);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	if (resp_buf != NULL)
	{
		MEMFREEHEAP(resp_buf);
	}

	
	return resp;
}


char *
meta_removeidx(TREE *command)
{
	char		*tab_name;
	int		tab_name_len;
	char		*idx_name;
	int		idx_name_len;
	char		tab_dir[256];
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	char 		*resp;
	int		tabidx;
	int		rpc_status;
	int		resp_buf_len;
	int		resp_buf_idx;


	Assert(command);
	
	rtn_stat	= FALSE;
	resp		= NULL;
	rpc_status	= 0;
	resp_buf_len	= 0;
	resp_buf_idx	= 0;

	
	idx_name = command->sym.command.tabname;
	idx_name_len = command->sym.command.tabname_len;

	
	tab_name = (command->right)->sym.command.tabname;
	tab_name_len = (command->right)->sym.command.tabname_len;

	
	MEMSET(tab_dir, 256);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_name);

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	if (!index_del_meta(tab_hdr->tab_id, idx_name, 
					Master_infor->meta_sysindex))
	{
		
		goto exit;
	}

	if (!index_tab_has_index(Master_infor->meta_sysindex, tab_hdr->tab_id))
	{
		tab_hdr->has_index = FALSE;
	}

	meta_save_sysindex((char *)(Master_infor->meta_sysindex));

	
	tab_hdr->index_ts = Master_infor->meta_sysindex->idx_ver;

	meta_save_sysobj(tab_dir, (char *)tab_hdr);
	
	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	return resp;
}

char *
meta_handler(char *req_buf, int fd)
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
	tss->tmaster_infor = Master_infor;
	tss->metabackup = MT_META_BACKUP;

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

		resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);

		ex_delete();
		
		goto close;
	}

	
	if (meta_collect_rg(req_buf))
	{		
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);

		return resp;
	}
	//update meta and rg list  process
	if (meta_failover_rg(req_buf))
	{		
		
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);

		return resp;
	}

	if (meta_clean4crtidx(req_buf))
	{
		resp = conn_build_resp_byte(RPC_SUCCESS, 0, NULL);

		return resp;
	}

	if((resp = meta_get_splits(req_buf)) != NULL)
	{
		return resp;
	}

	if((resp = meta_get_reader_meta(req_buf)) != NULL)
	{
		return resp;
	}
	
parse_again:
	if (!parser_open(tmp_req_buf + sizeof(int), *(int *)tmp_req_buf))
	{
		parser_close();
		tss->tstat |= TSS_PARSER_ERR;
		traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
		return NULL;
	}

	command = tss->tcmd_parser;
	resp_buf_idx = 0;
	resp_buf_size = 0;
	
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

		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - CREAT TABLE\n");
		}
		
		break;

	    case INSERT:
	    	resp = meta_instab(command, tabinfo);
		break;

	    case CRTINDEX:
	    	resp = meta_crtidx(command);

		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - CREAT INDEX\n");
		}
		
	    	break;

	    case SELECT:
		resp = meta_seldeltab(command, tabinfo);
		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - SELECT TABLE\n");
		}
		
	    	break;

	    case DELETE:
	    	resp = meta_seldeltab(command, tabinfo);

		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - DELETE TABLE\n");
		}
		
	    	break;

	    case SELECTRANGE:
#if 0
	    	resp = meta_selrangetab(command, tabinfo);
		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - SELECTRANGE TABLE\n");
		}
		
	    	break;
#endif		
  	    case SELECTWHERE:
	    case SELECTCOUNT:
	    case SELECTSUM:
	    case DELETEWHERE:
	    case UPDATE:
		resp = meta_selwheretab(command, tabinfo);
		
		if (DEBUG_TEST(tss))
		{
			traceprint("I got here - SELECTWHERE TABLE\n");
		}

		break;
		
	    case ADDSSTAB:
	    	resp = meta_addsstab(command, tabinfo);
	    	break;
		
	    case DROPTAB:
	    	resp = meta_droptab(command);
	    	break;
		
	    case REMOVETAB:
	    	resp = meta_removtab(command);
	    	break;

	    case DROPINDEX:
	    	resp = meta_dropidx(command);
	    	break;
		
	    case REMOVEINDEX:
	    	resp = meta_removeidx(command);
	    	break;
		
	    case MCCTABLE:
	    case MCCSSTAB:
	    	resp = meta_checktable(command);
		break;
		
	    case MCCRANGER:
	    	resp = meta_checkranger(command);
	    	break;
		
	    case REBALANCE:
	    	resp = meta_rebalancer(command);
	    	break;
		
	    case SHARDINGTABLE:
	    	resp = meta_sharding(command, TRUE);
		break;
		
	    case SHARDINGTABLET:
	    	resp = meta_sharding(command, FALSE);
		break;

	    default:
	    	break;
	}

	session_close(tabinfo);	

close:

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

	Assert(rowidx == rlen);
}

static RANGE_PROF *
meta_get_rg()
{
	RANGE_PROF	*rg_prof;
	int		i, j;
	int		min_tablet;
	int		min_rg;
	SVR_IDX_FILE 	*temp_store;
	
	temp_store = &(Master_infor->rg_list);
	rg_prof = (RANGE_PROF *)(temp_store->data);

	for(i = 0; i < temp_store->nextrno; i++)
	{
		if(rg_prof[i].rg_stat & RANGER_IS_ONLINE)
		{
			min_tablet = rg_prof[i].rg_tablet_num;
			min_rg = i;
			break;
		}
	}
	
	if(i == temp_store->nextrno)
	{
		traceprint("No available rg server for insert!\n");
		return NULL;
	}
	
	for(j = i; j < temp_store->nextrno; j++)
	{
		if(   (rg_prof[j].rg_tablet_num < min_tablet)
		   && (rg_prof[j].rg_stat & RANGER_IS_ONLINE))
		{
			min_tablet = rg_prof[j].rg_tablet_num;
			min_rg = j;
		}			
	}

	return rg_prof + min_rg;
}


static RANGE_PROF *
meta_get_next_rg(int *j)
{
	RANGE_PROF	*rg_prof;
	int		i;
	SVR_IDX_FILE 	*temp_store;
	
	temp_store = &(Master_infor->rg_list);
	rg_prof = (RANGE_PROF *)(temp_store->data);

	for(i = *j; i < temp_store->nextrno; i++)
	{
		if(rg_prof[i].rg_stat & RANGER_IS_ONLINE)
		{
			break;
		}
	}

	*j = i + 1;
	
	if((i == temp_store->nextrno) || (i > temp_store->nextrno))
	{
		traceprint("No available rg server for insert!\n");
		return NULL;
	}
	
	return rg_prof + i;
}


static RANGE_PROF *
meta_get_rg_by_ip_port(char *rgip, int rgport)

{
	int		i;
	SVR_IDX_FILE	*rglist;
	int		found, start_heartbeat;
	RANGE_PROF	*rg_addr;

	
	
	rglist = &(Master_infor->rg_list);
	found = FALSE;
	start_heartbeat = TRUE;
	rg_addr = (RANGE_PROF *)(rglist->data);

	for(i = 0; i < rglist->nextrno; i++)
	{
		if (   !strncasecmp(rgip, rg_addr[i].rg_addr, RANGE_ADDR_MAX_LEN)
		    && (rg_addr[i].rg_port == rgport)
		   )
		{
			return (rg_addr + i);
		}
	}

	return NULL;
}

void * 
meta_heartbeat(void *args)
{
	RANGE_PROF 	*rg_addr = (RANGE_PROF *)args;
	int 		hb_conn;
	char		send_buf[256];
	int 		idx;
	RPCRESP 	*resp;
	int 		rg_index = rg_addr->rg_index;
	char 		*hb_recv_buf;
	MSG_DATA	*new_msg;
	int		sleeptime;

	
	sleep(10);

	hb_recv_buf = Master_infor->heart_beat_data[rg_index].recv_data;

	
	if((hb_conn = conn_open(rg_addr->rg_addr, rg_addr->rg_port)) < 0)
	{
		perror("error in create connection to rg server when meta server start heart beat: ");
		rg_addr->rg_stat |= RANGER_IS_SUSPECT;
		goto finish;

	}

	signal (SIGPIPE,SIG_IGN);

	
	HB_SET_RANGER_ON(&(Master_infor->heart_beat_data[rg_index]));
	
	sleeptime = HEARTBEAT_INTERVAL;
	while(TRUE)
	{
		sleep(sleeptime);
		MEMSET(send_buf, 256);
		
		idx = 0;
		PUT_TO_BUFFER(send_buf, idx, RPC_REQUEST_MAGIC, 
					RPC_MAGIC_MAX_LEN);
		PUT_TO_BUFFER(send_buf, idx, RPC_MASTER2RG_HEARTBEAT,
					RPC_MAGIC_MAX_LEN);
		
		tcp_put_data(hb_conn, send_buf, idx);

		traceprint("\n###### meta sent heart beat to %s/%d. \n", rg_addr->rg_addr, rg_addr->rg_port);

		resp = conn_recv_resp_meta(hb_conn, hb_recv_buf);

		traceprint("\n###### meta recv heart beat from %s/%d. \n", rg_addr->rg_addr, rg_addr->rg_port);

		if (resp->status_code == RPC_UNAVAIL)
		{
			traceprint("\n rg server is un-available \n");

			if (rg_addr->rg_stat & RANGER_IS_SUSPECT)
			{
				goto finish;
			}

//			P_SPINLOCK(Master_infor->rglist_spinlock);
			rg_addr->rg_stat |= RANGER_IS_SUSPECT;
//			V_SPINLOCK(Master_infor->rglist_spinlock);
			sleeptime = 3;

		}
		else if (resp->status_code != RPC_SUCCESS)
		{
			traceprint("\n We got a non-success response. \n");

			if (rg_addr->rg_stat & RANGER_IS_SUSPECT)
			{
                        	goto finish;
			}

//			P_SPINLOCK(Master_infor->rglist_spinlock);
			rg_addr->rg_stat |= RANGER_IS_SUSPECT;
//			V_SPINLOCK(Master_infor->rglist_spinlock);
			sleeptime = 3;
		}

		if ((resp->status_code & RPC_SUCCESS) && (rg_addr->rg_stat & RANGER_IS_SUSPECT))
		{
//			P_SPINLOCK(Master_infor->rglist_spinlock);
			rg_addr->rg_stat &= ~RANGER_IS_SUSPECT;
//			V_SPINLOCK(Master_infor->rglist_spinlock);
			sleeptime = HEARTBEAT_INTERVAL;
		}

		
	}

finish:
	
		//update meta and rg_list here, put update task to msg list

		HB_SET_RANGER_OFF(&(Master_infor->heart_beat_data[rg_index]));
		traceprint("\n HEARTBEAT hit error. \n");
		
		new_msg = (MSG_DATA *)msg_mem_alloc();
				
		idx = 0;
		
		PUT_TO_BUFFER(new_msg->data, idx, RPC_REQUEST_MAGIC, 
						RPC_MAGIC_MAX_LEN);
		PUT_TO_BUFFER(new_msg->data, idx, RPC_FAILOVER, 
						RPC_MAGIC_MAX_LEN);
		PUT_TO_BUFFER(new_msg->data, idx, rg_addr->rg_addr, 
						RANGE_ADDR_MAX_LEN);
		PUT_TO_BUFFER(new_msg->data, idx, &(rg_addr->rg_port), 
						RANGE_PORT_MAX_LEN);
		new_msg->fd = -1;
		new_msg->n_size = idx;
		new_msg->block_buffer = NULL;
		new_msg->next = NULL;
		
		pthread_mutex_lock(&mutex);
		if (msg_list_head == NULL)
		{
			msg_list_head = new_msg;
			msg_list_tail = new_msg;
		} 
		else
		{
			msg_list_tail->next = new_msg;
			msg_list_tail = new_msg;
		}
		msg_list_len++;
		
		pthread_cond_signal(&cond);
		pthread_mutex_unlock(&mutex);

		if(hb_conn > 0)
		{
			close(hb_conn);
		}
		
		pthread_detach(pthread_self());

	return NULL;
	
}


static void
meta_heartbeat_setup(RANGE_PROF * rg_addr)
{
	
	pthread_create(&(rg_addr->tid), NULL, meta_heartbeat, (void *)rg_addr);
}

static int
meta_get_min_rg(char *target_ip, int * target_port)
{
	RANGE_PROF *rg_prof;
	int i, j;
	int min_tablet;
	int 	min_rg;
	SVR_IDX_FILE *temp_store;
	
	temp_store = &(Master_infor->rg_list);
	rg_prof = (RANGE_PROF *)(temp_store->data);

	for(i = 0; i < temp_store->nextrno; i++)
	{
		if(rg_prof[i].rg_stat & RANGER_IS_ONLINE)
		{
			min_tablet = rg_prof[i].rg_tablet_num;
			min_rg = i;
			break;
		}
	}
	
	if(i == temp_store->nextrno)
	{
		traceprint("No available rg server exist!\n");
		return -1;
	}
	
	for(j = i; j < temp_store->nextrno; j++)
	{
		if(   (rg_prof[j].rg_tablet_num < min_tablet)
		   && (rg_prof[j].rg_stat & RANGER_IS_ONLINE))
		{
			min_tablet = rg_prof[j].rg_tablet_num;
			min_rg = j;
		}			
	}

	MEMCPY(target_ip, rg_prof[min_rg].rg_addr, 
				STRLEN(rg_prof[min_rg].rg_addr));
	*target_port = rg_prof[min_rg].rg_port;

	return min_rg;
}



static int
meta_transfer_target(char * src_ip, int src_port, char *target_ip, int * target_port)
{
	RANGE_PROF	*rg_prof;
	int		i, j, found;
	SVR_IDX_FILE	*temp_store;
	
	temp_store = &(Master_infor->rg_list);
	rg_prof = (RANGE_PROF *)(temp_store->data);
	found = FALSE;
	j = -1;

	for(i = 0; i < temp_store->nextrno; i++)
	{
		
		if(rg_prof[i].rg_stat & RANGER_IS_ONLINE)
		{
			j = i;

			if (found)
			{
				break;
			}
		}

		if(   !found
		   && (!strncasecmp(rg_prof[i].rg_addr, src_ip,	RANGE_ADDR_MAX_LEN))
		   && (rg_prof[i].rg_port == src_port))
		{
			found = TRUE;

			if (j != -1)
			{
				
				break;
			}
		}
	}	

	if (j == -1)
	{
		traceprint("No ranger server is avalable.\n");
		return -1;
	}

	MEMCPY(target_ip, rg_prof[j].rg_addr, STRLEN(rg_prof[j].rg_addr));
	*target_port = rg_prof[j].rg_port;

	return j;
}


int meta_transfer_notify(char * rg_addr, int rg_port)
{
	int hb_conn;
	char	send_buf[256];
	int idx;
	RPCRESP * resp;
	int rtn_stat = TRUE;
		
	if((hb_conn = conn_open(rg_addr, rg_port)) < 0)
	{
		perror("error in create connection to rg server on meta server: ");
		rtn_stat= FALSE;
		goto finish;
	
	}
	
	MEMSET(send_buf, 256);
			
	idx = 0;
	PUT_TO_BUFFER(send_buf, idx, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_buf, idx, RPC_MASTER2RG_NOTIFY, RPC_MAGIC_MAX_LEN);
				
	tcp_put_data(hb_conn, send_buf, idx);
	
	resp = conn_recv_resp(hb_conn);
	
	if ((resp == NULL) || (resp->status_code != RPC_SUCCESS))
	{
		traceprint("\n ERROR when send rsync notify to client \n");
		rtn_stat = FALSE;
			
	}
		
finish:
	if(hb_conn > 0)
		close(hb_conn);

	return rtn_stat;
}


static int
meta_tablet_update(char * table_name, char * rg_addr, int rg_port)
{
	char		tab_dir[256];
	int 		fd;
	char		tab_tabletschm_dir[TABLE_NAME_MAX_LEN];
	char		*tablet_schm_bp;
	char		target_ip[RANGE_ADDR_MAX_LEN];
	int		target_port;
	int		target_index;
	
	tablet_schm_bp = NULL;
		
	MEMSET(tab_dir, 256);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', table_name);

	if (STAT(tab_dir, &st) != 0)
	{		
		traceprint("Table %s is not exist!\n", table_name);
		goto exit;
	}

	MEMSET(target_ip, RANGE_ADDR_MAX_LEN);	
	target_index = meta_transfer_target(rg_addr, rg_port, target_ip, &target_port);

	if(target_index < 0)
	{
		return FALSE;
	}
			
	if(rg_port > 0)
	{
		int notify_ret = meta_transfer_notify(target_ip, target_port);
		if(!notify_ret)
		{		
			traceprint("ERROR when send rsync notify to rg server!\n");
			goto exit;
		}
	}

	TABLEHDR	tab_hdr;
	
	MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_tabletschm_dir, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_tabletschm_dir, '/', "sysobjects");

	OPEN(fd, tab_tabletschm_dir, (O_RDONLY));
	if (fd < 0)
	{
		traceprint("Table is not exist! \n");
		goto exit;
	}

	READ(fd, &tab_hdr, sizeof(TABLEHDR));
	CLOSE(fd);

	if (tab_hdr.tab_tablet == 0)
	{
		traceprint("Table is Empty! \n");
		goto exit;
	}
	
	MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_tabletschm_dir, tab_dir, STRLEN(tab_dir));
	
	str1_to_str2(tab_tabletschm_dir, '/', "tabletscheme");

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

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, tab_tabletschm_dir, table_name, STRLEN(table_name),
			tabinfo->t_sinfo, minrowlen, TAB_SCHM_INS, tab_hdr.tab_id, 
			TABLETSCHM_ID);

	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0,
			TABLETSCHM_KEY_COLID_INROW, VARCHAR, -1);

	bp = blk_getsstable(tabinfo);

	tablet_schm_bp = (char *)(bp->bsstab->bblk);
		
	BLOCK *blk;

	int i, rowno;
	int *offset;
	char	*rp;
	char	*addr_in_blk;
	int addrlen_in_blk;
	int port_in_blk;
	int portlen_in_blk;
	RANGE_PROF *rg_prof = (RANGE_PROF *)(Master_infor->rg_list.data);
		
	for(i = 0; i < BLK_CNT_IN_SSTABLE; i ++)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);	
			
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;
			
			Assert(*offset < blk->bfreeoff);
			
			addr_in_blk = row_locate_col(rp, 
					TABLETSCHM_RGADDR_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM,
					&addrlen_in_blk);
			
			port_in_blk = *(int *)row_locate_col(rp,
					TABLETSCHM_RGPORT_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, 
					&portlen_in_blk);

			if(   (!strncasecmp(rg_addr, addr_in_blk,
						RANGE_ADDR_MAX_LEN)
			   && (rg_port == port_in_blk))
					||(rg_port == -1))
			{
				traceprint("transfer meta from [%s:%d] to [%s:%d] \n", addr_in_blk, port_in_blk, target_ip, target_port);
				
				MEMCPY(addr_in_blk, target_ip, 
						RANGE_ADDR_MAX_LEN);
				int *tmp_addr = (int *)row_locate_col(rp, 
						TABLETSCHM_RGPORT_COLOFF_INROW,
						ROW_MINLEN_IN_TABLETSCHM, 
						&portlen_in_blk);
				
				*tmp_addr = target_port;
				
				rg_prof[target_index].rg_tablet_num ++;				
			}
				
				
		}
		
	}

	bufpredirty(bp->bsstab);
	bufdirty(bp->bsstab);
	bufunkeep(bp->bsstab);

	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

	meta_save_rginfo();
	
		
exit:

	return TRUE;
	
}


static int
meta_update(char * rg_addr, int rg_port)
{
	char	tab_dir[256];
	int	rtn_stat;


	rtn_stat = TRUE;	
	MEMSET(tab_dir, 256);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	if (STAT(tab_dir, &st) != 0)
	{		
		traceprint("Table dir %s is not exist!\n", tab_dir);

		rtn_stat = FALSE;
		goto exit;
	}

#ifdef MT_KFS_BACKEND

	MT_ENTRIES	mt_entries;

	MEMSET(&mt_entries, sizeof(MT_ENTRIES));

	if (!READDIR(tab_dir, (char *)&mt_entries))
	{
		traceprint("Read dir %s hit error.\n", tab_dir);

		rtn_stat = FALSE;
		goto exit;
	}

	int i;

	for (i = 0; i < mt_entries.ent_num; i++)
	{
		if(   strcmp(mt_entries.tabname[i],".")==0 
		   || strcmp(mt_entries.tabname[i],"..")==0)
		{
			continue;
		}
		
		if (!meta_tablet_update(mt_entries.tabname[i], rg_addr, rg_port))
		{
			

			rtn_stat = FALSE;
			
			break;
		}
	}

#else
	char tab_name[256];
	DIR *pDir ;
	struct dirent *ent ;
	
	pDir=opendir(tab_dir);
	while((ent=readdir(pDir))!=NULL)
	{
		if(ent->d_type & DT_DIR)
		{
			if(strcmp(ent->d_name,".")==0 || strcmp(ent->d_name,"..")==0)
			{
				continue;
			}
			
			MEMSET(tab_name, 256);
			sprintf(tab_name, "%s", ent->d_name);
			if (!meta_tablet_update(tab_name, rg_addr, rg_port))
			{
				

				rtn_stat = FALSE;
				break;
			}
		}
	}
#endif

exit:
	return rtn_stat;
}



static int
meta_failover_rg(char * req_buf)
{
	char		*str;
	int		i;
	SVR_IDX_FILE	*rglist;
	int		found;
	RANGE_PROF	*rg_addr;

	
	if (strncasecmp(RPC_FAILOVER, req_buf, STRLEN(RPC_FAILOVER)) != 0)
	{
		return FALSE;
	}
	
	
	str = req_buf + RPC_MAGIC_MAX_LEN;


	rglist = &(Master_infor->rg_list);
	found = FALSE;
	rg_addr = (RANGE_PROF *)(rglist->data);

	P_SPINLOCK(Master_infor->rglist_spinlock);
	
	for(i = 0; i < rglist->nextrno; i++)
	{
		if (   !strncasecmp(str, rg_addr[i].rg_addr, RANGE_ADDR_MAX_LEN)
		    && (rg_addr[i].rg_port == *(int *)(str + RANGE_ADDR_MAX_LEN))
		   )
		{
			found = TRUE;
			
			if(rg_addr[i].rg_stat & RANGER_IS_ONLINE)
			{
				
				
				Assert(rg_addr[i].rg_stat & RANGER_IS_SUSPECT);

				
				rg_addr[i].rg_stat &= ~(RANGER_IS_ONLINE | RANGER_IS_SUSPECT);
				rg_addr[i].rg_stat = RANGER_IS_OFFLINE | RANGER_NEED_RECOVERY;
		
				break;
			}
			else
			{
				traceprint("\n error, rg server to be off-line is already off line \n");
			}
		}
	}

	if (!found)
	{
		traceprint("\n error, rg server to be off_line is not exist in rg list \n");
	}

	V_SPINLOCK(Master_infor->rglist_spinlock);
	
	return TRUE;
}

static int
meta_clean4crtidx(char * req_buf)
{
	int		meta_num;
	IDXMETA		*idxmeta;
	int		result;
	TABLEHDR	*tab_hdr;
	char		tab_dir[TABLE_NAME_MAX_LEN];

	
	if (strncasecmp(RPC_CRTIDX_DONE_MAGIC, req_buf, 
					STRLEN(RPC_FAILOVER)) != 0)
	{
		return FALSE;
	}


	result = *(int *)(req_buf + RPC_MAGIC_MAX_LEN);

	idxmeta = (IDXMETA *)(req_buf + RPC_MAGIC_MAX_LEN + sizeof(int));


	meta_num = index_get_meta_by_idxname(idxmeta->idx_tabid, 
					     idxmeta->idxname, 
					     Master_infor->meta_sysindex);

	if (meta_num == -1)
	{
		traceprint("The index (%s) is not exist.", idxmeta->idxname);
		return TRUE;
	}

	tab_hdr = meta_get_sysobj_by_tabid(idxmeta->idx_tabid);
	
	if (!result)
	{
		index_del_meta(idxmeta->idx_tabid, idxmeta->idxname,
				Master_infor->meta_sysindex);

		if (Master_infor->meta_sysindex->idx_num == 0)
		{
			tab_hdr->has_index = FALSE;
		}
	}
	else
	{
		Master_infor->meta_sysindex->idx_meta[meta_num].idx_stat
							= IDX_IN_WORKING;
	}
	
	
	tab_hdr->index_ts = Master_infor->meta_sysindex->idx_ver;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	str1_to_str2(tab_dir, '/', tab_hdr->tab_name);

	meta_save_sysindex((char *)(Master_infor->meta_sysindex));
	meta_save_sysobj(tab_dir, (char *)tab_hdr);
	
	return TRUE;
}


static int
meta_check_validation()
{	
	IDXMETA		*idx_meta;
	int		meta_num;
	META_SYSINDEX	*meta_sysidx;


	
	meta_sysidx = Master_infor->meta_sysindex;
	
	idx_meta = meta_sysidx->idx_meta;
	
	for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
	{
		if (   (idx_meta->idx_stat & IDX_IN_CREATE)
		    || (idx_meta->idx_stat & IDX_IN_DROP))
		{
			return FALSE;
		}
		
		idx_meta++;
	}

	return TRUE;
}



void *
meta_recovery()
{
	int		i;
	SVR_IDX_FILE	*rglist;
	RANGE_PROF	*rg_addr;	
	
again:
	sleep(META_RECOVERY_INTERVAL);
	rglist = &(Master_infor->rg_list);
	rg_addr = (RANGE_PROF *)(rglist->data);

	for(i = 0; i < rglist->nextrno; i++)
	{
		if(!(rg_addr[i].rg_stat & RANGER_NEED_RECOVERY))
		{
			continue;
		}
		
		P_SPINLOCK(Master_infor->rglist_spinlock);
		
		Assert(  (rg_addr[i].rg_stat & RANGER_IS_OFFLINE) 
		       | (rg_addr[i].rg_stat & RANGER_RESTART));

		
		char	send_buf[256];
		char	recv_buf[128];

		MEMSET(send_buf, 256);
		MEMSET(recv_buf, 128);

		int 		idx = 0;
		int		fd;
		RPCRESP 	*resp;

		RANGE_PROF *rg_prof;

		if (rg_addr[i].rg_stat & RANGER_RESTART)
		{
			rg_prof = rg_addr + i;

			Assert(rg_prof);
		}
		else
		{
			rg_prof = meta_get_rg();
		}
		
		if (rg_prof == NULL) 
		{
			traceprint("Can not get the ranger server for the recovery.\n");
			V_SPINLOCK(Master_infor->rglist_spinlock);
			continue;
		}
		
		if ((fd = conn_open(rg_prof->rg_addr, rg_prof->rg_port)) < 0)
		{
			traceprint("Fail to connect to server (%s:%d) for the recovery.\n", rg_prof->rg_addr, rg_prof->rg_port);
			V_SPINLOCK(Master_infor->rglist_spinlock);
			goto again;
		}
		
		PUT_TO_BUFFER(send_buf, idx, RPC_REQUEST_MAGIC, 
					RPC_MAGIC_MAX_LEN);
		PUT_TO_BUFFER(send_buf, idx, RPC_RECOVERY,
					RPC_MAGIC_MAX_LEN);
		PUT_TO_BUFFER(send_buf, idx, rg_addr[i].rg_addr, 
					RANGE_ADDR_MAX_LEN);
		PUT_TO_BUFFER(send_buf, idx, &(rg_addr[i].rg_port), 
					sizeof(int));
			
		tcp_put_data(fd, send_buf, idx);

		resp = conn_recv_resp_meta(fd, recv_buf);

		conn_close(fd, NULL, NULL);

		if (resp->status_code == RPC_UNAVAIL)
		{
			traceprint("\n rg server is un-available \n");
			V_SPINLOCK(Master_infor->rglist_spinlock);
			goto again;

		}
		else if (resp->status_code != RPC_SUCCESS)
		{
			traceprint("\n We got a non-success response. \n");
 			V_SPINLOCK(Master_infor->rglist_spinlock);
                        goto again;
		}		

		
		if (!meta__recovery_addsstab(rg_prof->rg_addr, rg_prof->rg_port))
		{
			traceprint("Recovery for the Add SStable is failed.\n");
			V_SPINLOCK(Master_infor->rglist_spinlock);
			goto again;
		}

		if (!meta__recovery_crtidx())
		{
			traceprint("Recovery for the CREATE INDEX is failed.\n");
			V_SPINLOCK(Master_infor->rglist_spinlock);
			goto again;
		}

		int res_stat = rg_addr[i].rg_stat;
		
		rg_addr[i].rg_stat &= ~RANGER_NEED_RECOVERY;

		if (rg_prof->rg_stat & RANGER_RESTART)
		{
			Assert((rg_addr + i) == rg_prof);

			rg_addr[i].rg_stat &= ~(RANGER_IS_OFFLINE | RANGER_RESTART);

			rg_addr[i].rg_stat |= RANGER_IS_ONLINE;
		}						
		
		//update tablet
		if (!meta_update(rg_addr[i].rg_addr, rg_addr[i].rg_port))
		{
			rg_addr[i].rg_stat= res_stat;

			V_SPINLOCK(Master_infor->rglist_spinlock);

			goto again;
		}
		
		rg_addr[i].rg_tablet_num = 0;

		meta_save_rginfo();

		V_SPINLOCK(Master_infor->rglist_spinlock);
		
	}

	goto again;
	
	return NULL;
}


static int
meta__bld_addsstab_clause(SSTAB_SPLIT_INFO *split_info, char *send_buf)
{
	char		tab_name[TABLE_NAME_MAX_LEN];
		

	MEMSET(send_buf, LINE_BUF_SIZE);
	
	MEMSET(tab_name, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_name,split_info->tab_name,split_info->tab_name_len);

	int	sstab_len = STRLEN(split_info->newsstabname);
	int	i = strmnstr(split_info->newsstabname, "/", sstab_len);
	
		
	sprintf(send_buf, "addsstab into %s (%s, %d, %d, %d, %s)",
		tab_name, split_info->newsstabname + i, split_info->split_sstabid,
		split_info->split_ts, split_info->sstab_id, split_info->sstab_key);		

	return TRUE;
}



static int
meta__recovery_addsstab(char * rgip,int rgport)
{
	int			fd;
	RG_STATE		*rgstate;	
	int			statelen;
	int			rtn_state;
	SSTAB_SPLIT_INFO	split_info;	
	SSTAB_SPLIT_INFO	*prgstat;
	char			*rgstat_off;	
	RPCRESP 		*resp;
	char	statefile[TABLE_NAME_MAX_LEN];
	char	add_sstab_cmd_buf[LINE_BUF_SIZE];
	
	
	rtn_state = TRUE;
	statelen = sizeof(RG_STATE);
	rgstate = (RG_STATE *)malloc(statelen);
	
	ri_get_rgstate(statefile, rgip, rgport);
	
	OPEN(fd, statefile, (O_RDWR));
	
	if (fd < 0)
	{		
		goto exit;
	}

	MEMSET(rgstate, statelen);
	READ(fd, rgstate, statelen);
	
	int	i;

	rgstat_off = (char *)(rgstate->sstab_state);
			
	for (i = 0; i < rgstate->sstab_split_num; i++)
	{	
		prgstat = (SSTAB_SPLIT_INFO *)rgstat_off;

		
		Assert(rgstat_off < ((char *)rgstate + 
			rgstate->offset - SSTAB_SPLIT_INFO_HEADER));

		MEMCPY(&split_info, prgstat, SSTAB_SPLIT_INFO_HEADER);

		split_info.sstab_key = rgstat_off + SSTAB_SPLIT_INFO_HEADER;

		if (split_info.sstab_state & RG_SSTABLE_RECOVERED)
		{
			rgstat_off += (SSTAB_SPLIT_INFO_HEADER + 
						split_info.sstab_keylen);

			continue;
		}

		if (meta__bld_addsstab_clause(&split_info, add_sstab_cmd_buf))
		{
			
			resp = (RPCRESP *)meta_handler(add_sstab_cmd_buf, -1);

			if (!resp || !(resp->status_code & RPC_SUCCESS))
			{
				rtn_state = FALSE;
				break;
			}

			split_info.sstab_state |= RG_SSTABLE_RECOVERED;
		}
		else
		{
			
			rtn_state = FALSE;
			break;
		}	
		
		rgstat_off += (SSTAB_SPLIT_INFO_HEADER + split_info.sstab_keylen);
	}

	if (i == rgstate->sstab_split_num)
	{
		
		MEMSET(rgstate, sizeof(RG_STATE));
		rgstate->offset = RG_STATE_HEADER;
	}
	
#ifdef MT_KFS_BACKEND
	CLOSE(fd);
	
	OPEN(fd, statefile, (O_RDWR));
	
	if (fd < 0)
	{	
		rtn_state = FALSE;
		goto exit;
	}
#else
	
	LSEEK(fd, 0, SEEK_SET);
#endif
	WRITE(fd, rgstate, sizeof(RG_STATE));
	
exit:
	free (rgstate);

	CLOSE(fd);

	return rtn_state;
}


static int
meta__recovery_crtidx()
{
	int		meta_num;
	IDXMETA		*idx_meta;
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	TABLEHDR	*tab_hdr;
	
	
	idx_meta =  Master_infor->meta_sysindex->idx_meta;
		
	for(meta_num = 0; meta_num <  Master_infor->meta_sysindex->idx_num; 
								meta_num++)
	{
		if (idx_meta->idx_stat & IDX_IN_CREATE)
		{
			tab_hdr = meta_get_sysobj_by_tabid(idx_meta->idx_tabid);
			
			MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(tab_meta_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
			str1_to_str2(tab_meta_dir, '/', tab_hdr->tab_name);	
			str1_to_str2(tab_meta_dir, '/', idx_meta->idxname);
			
#ifdef MT_KFS_BACKEND

			RMDIR(status, tab_meta_dir);
			if(!status)
#else

			char	cmd_str[TABLE_NAME_MAX_LEN];
			MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
			
			sprintf(cmd_str, "rm -rf %s", tab_meta_dir);
			
			if (system(cmd_str) != 0)
#endif
			{
				traceprint("remove directory (%s) is failed.\n", cmd_str);
			}


	
			
			index_del_meta(idx_meta->idx_tabid, idx_meta->idxname,
					Master_infor->meta_sysindex);

			if (Master_infor->meta_sysindex->idx_num == 0)
			{
				tab_hdr->has_index = FALSE;
			}
		
		
			
			tab_hdr->index_ts = Master_infor->meta_sysindex->idx_ver;

			MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
			MEMCPY(tab_meta_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
			str1_to_str2(tab_meta_dir, '/', tab_hdr->tab_name);

			meta_save_sysindex((char *)(Master_infor->meta_sysindex));
			meta_save_sysobj(tab_meta_dir, (char *)tab_hdr);
	
		
		}

		idx_meta++;
	}

	return TRUE;
}


int
meta_ranger_is_online(char *rg_ip, int rg_port)
{
	int		i;
	SVR_IDX_FILE 	*rglist = &(Master_infor->rg_list);			
	RANGE_PROF 	*rg_addr = (RANGE_PROF *)(rglist->data);


	for(i = 0; i < rglist->nextrno; i++)
	{
		if (   !strncasecmp(rg_ip, rg_addr[i].rg_addr,
					RANGE_ADDR_MAX_LEN)
		    && (rg_addr[i].rg_port == rg_port)
		   )
		{
			if(rg_addr[i].rg_stat & RANGER_IS_ONLINE)
			{
				return TRUE;
			}
		}
	}

	return FALSE;
}

void
meta_check_tablet(char *tabdir, int tabid, char *tabletname, int tabletid)
{
	char		tab_tabletschm_dir[TABLE_NAME_MAX_LEN];
	char		*tablet_schm_bp;
	TABINFO		*tabinfo;
	int		minrowlen;
	BLK_ROWINFO	blk_rowinfo;
	BUF		*bp;


	MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_tabletschm_dir, tabdir, STRLEN(tabdir));
	
	str1_to_str2(tab_tabletschm_dir, '/', tabletname);
	

	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLET;

	TABINFO_INIT(tabinfo, tab_tabletschm_dir, NULL, 0, tabinfo->t_sinfo,
			minrowlen, TAB_SCHM_SRCH, tabid, tabletid);
	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, TABLET_KEY_COLID_INROW, 
		       VARCHAR, -1);

	bp = blk_getsstable(tabinfo);

	tablet_schm_bp = (char *)(bp->bsstab->bblk);
		
	BLOCK 		*blk;

	int		i, rowno;
	int 		*offset;
	char		*rp;
	char 		*key_in_blk;
	int		keylen_in_blk;
	char		*lastkey_in_blk;
	int		lastkeylen_in_blk;
	int 		result;
	int		rowcnt = 0;
	
	for(i = 0; i < BLK_CNT_IN_SSTABLE; i ++)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);	
			
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;

			
			if(*offset > blk->bfreeoff)
			{
				traceprint("%s(%d): %dth block contains invalid %dth offset\n",tabletname, tabletid, blk->bblkno, rowno);
			}

			key_in_blk = row_locate_col(rp,
						TABLE_KEY_FAKE_COLOFF_INROW,
						minrowlen, &keylen_in_blk);

			if (rowno > 0)
			{
				result = row_col_compare(VARCHAR, key_in_blk,
						keylen_in_blk, lastkey_in_blk, 
						lastkeylen_in_blk);
				
				if (result != GR)
				{
					traceprint("%s(%d): the %dth block hit index issue\n",tabletname, tabletid, blk->bblkno);
				}
			}
			
			lastkey_in_blk = key_in_blk;
			lastkeylen_in_blk = keylen_in_blk; 
		}
		
		rowcnt += blk->bnextrno;
	}

	traceprint("%s(%d) has %d rows.\n",tabletname, tabletid, rowcnt);

	bufunkeep(bp->bsstab);
	
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();
}


void
meta_check_tabletschme(char *tabdir, int tabid)
{
	char		tab_tabletschm_dir[TABLE_NAME_MAX_LEN];
	char		*tablet_schm_bp;
	TABINFO		*tabinfo;
	int		minrowlen;
	BLK_ROWINFO	blk_rowinfo;
	BUF		*bp;


	MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_tabletschm_dir, tabdir, STRLEN(tabdir));
	
	str1_to_str2(tab_tabletschm_dir, '/', "tabletscheme");
	

	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, tab_tabletschm_dir, NULL, 0, tabinfo->t_sinfo,
			minrowlen, (TAB_RESERV_BUF | TAB_SCHM_SRCH), tabid,
			TABLETSCHM_ID);

	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, TABLETSCHM_KEY_COLID_INROW, 
					VARCHAR, -1);

	
	bp = blk_getsstable(tabinfo);

	tablet_schm_bp = (char *)(bp->bsstab->bblk);
		
	BLOCK 		*blk;

	int		i, rowno;
	int 		*offset;
	char		*rp;
	char		*addr_in_blk;
	int 		port_in_blk;
	char 		*key_in_blk;
	int		keylen_in_blk;
	char		*lastkey_in_blk;
	int		lastkeylen_in_blk;
	int		ign;
	int 		result;
	char		*tabletname;
	int		tabletid;

		
	for(i = 0; i < BLK_CNT_IN_SSTABLE; i ++)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);	
			
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;

			
			if(*offset > blk->bfreeoff)
			{
				traceprint("tabletscheme: %dth block contains invalid %dth offset\n", blk->bblkno, rowno);
			}

			key_in_blk = row_locate_col(rp, 
						TABLETSCHM_KEY_FAKE_COLOFF_INROW,
						minrowlen, &keylen_in_blk);

			if (rowno > 0)
			{
				result = row_col_compare(VARCHAR, key_in_blk, 
						keylen_in_blk, lastkey_in_blk, 
						lastkeylen_in_blk);
				
				if (result != GR)
				{
					traceprint("tabletscheme: the %dth block hit index issue\n", blk->bblkno);
				}
			}
			
			lastkey_in_blk = key_in_blk;
			lastkeylen_in_blk = keylen_in_blk; 
			
			addr_in_blk = row_locate_col(rp, 
						TABLETSCHM_RGADDR_COLOFF_INROW,
						ROW_MINLEN_IN_TABLETSCHM, &ign);
			
			port_in_blk = *(int *)row_locate_col(rp, 
						TABLETSCHM_RGPORT_COLOFF_INROW, 
						ROW_MINLEN_IN_TABLETSCHM, &ign);

			if (!meta_ranger_is_online(addr_in_blk, port_in_blk))
			{
				traceprint("tabletscheme: ranger server %s:%d is NOT online\n", addr_in_blk, port_in_blk);
			}


			tabletname = row_locate_col(rp, 
					TABLETSCHM_TABLETNAME_COLOFF_INROW, 
					minrowlen, &ign);
			tabletid = *(int *)row_locate_col(rp, 
					TABLETSCHM_TABLETID_COLOFF_INROW, 
					minrowlen, &ign);

			traceprint("tabletscheme: Row(%d): tablet (%s, %d) locates at the ranger server (%s:%d)\n",rowno, tabletname, tabletid, addr_in_blk, port_in_blk);

			meta_check_tablet(tabdir, tabid, tabletname, tabletid);
				
		}
		
	}

	bufunkeep(bp->bsstab);
	
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

}


static char *
meta_checktable(TREE *command)
{
	char		*tab_name;
	int		tab_name_len;
	char		tab_dir[TABLE_NAME_MAX_LEN];
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	TABLEHDR	*tab_hdr;
	int		sstab_rlen;
	int		sstab_idx;
	char		*resp;
	int		tabidx;
	int		rpc_status;
	int		querytype;
	char		*resp_buf;
	int		resp_buflen;


	Assert(command);

	rtn_stat = FALSE;
	sstab_rlen = 0;
	sstab_idx = 0;
	rpc_status = 0;
	resp_buf = NULL;
	resp_buflen = 0;
	tab_name = command->sym.command.tabname;
	tab_name_len = command->sym.command.tabname_len;
	querytype = command->sym.command.querytype;

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));
	
	str1_to_str2(tab_dir, '/', tab_name);
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	if ((tabidx = meta_table_is_exist(tab_dir)) == -1)
	{
		traceprint("Table %s is not exist!\n", tab_name);

		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}


	tab_hdr = &(Master_infor->meta_sysobj->sysobject[tabidx]);

	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", tab_name);
		goto exit;
	}

	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}

	if (querytype == MCCTABLE)
	{
		index_tab_check_index(Master_infor->meta_sysindex, tab_hdr->tab_id);

		meta_check_tabletschme(tab_dir, tab_hdr->tab_id);
	}
	else if (querytype == MCCSSTAB)
	{
		
		
		resp_buflen = sizeof(SELWHERE) + sizeof(SVR_IDX_FILE) + sizeof(TABLEHDR);
	
		resp_buf = MEMALLOCHEAP(resp_buflen);
		MEMSET(resp_buf, resp_buflen);

		int	bufidx = sizeof(SELWHERE);
		
		
		MEMCPY((resp_buf + bufidx), &(Master_infor->rg_list), 
						sizeof(SVR_IDX_FILE));
		bufidx += sizeof(SVR_IDX_FILE);
		
		
		MEMCPY((resp_buf + bufidx), tab_hdr, sizeof(TABLEHDR));

		bufidx += sizeof(TABLEHDR);		
	}
	
	rtn_stat = TRUE;

exit:
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;

		resp = conn_build_resp_byte(rpc_status, resp_buflen, resp_buf);		
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}

	if (resp_buf != NULL)
	{
		MEMFREEHEAP(resp_buf);
	}

	return resp;
}




static char *
meta_checkranger(TREE *command)
{
	int		rtn_stat;
	char		*resp;
	RANGE_PROF	*rg_prof;
	int		i;
	SVR_IDX_FILE 	*temp_store;


	Assert(command);

	rtn_stat = FALSE;	
	
	temp_store = &(Master_infor->rg_list);
	rg_prof = (RANGE_PROF *)(temp_store->data);

	for(i = 0; i < temp_store->nextrno; i++)
	{
		if (rg_prof[i].rg_stat & RANGER_IS_ONLINE)
		{
			traceprint("Ranger %d (%s:%d) is ON-LINE\n", i, rg_prof[i].rg_addr, rg_prof[i].rg_port);
			rtn_stat = TRUE;
		}

		if (rg_prof[i].rg_stat & RANGER_IS_OFFLINE)
		{
			traceprint("Ranger %d (%s:%d) is OFF-LINE\n", i, rg_prof[i].rg_addr, rg_prof[i].rg_port);
			rtn_stat = TRUE;
		}

		if (rg_prof[i].rg_stat & RANGER_IS_SUSPECT)
		{
			traceprint("Ranger %d (%s:%d) is SUSPECT\n", i, rg_prof[i].rg_addr, rg_prof[i].rg_port);
			rtn_stat = TRUE;
		}

		
		if (rg_prof[i].rg_stat & RANGER_NEED_RECOVERY)
		{
			traceprint("Ranger %d (%s:%d) need to recovry\n", i, rg_prof[i].rg_addr, rg_prof[i].rg_port);
			rtn_stat = TRUE;
		}

		
		if (rg_prof[i].rg_stat & RANGER_RESTART)
		{
			traceprint("Ranger %d (%s:%d) has been restart.\n", i, rg_prof[i].rg_addr, rg_prof[i].rg_port);
			rtn_stat = TRUE;
		}
		
		if (!rtn_stat)
		{
			traceprint("Ranger %d (%s:%d) is invalid-state\n", i, rg_prof[i].rg_addr, rg_prof[i].rg_port);
		}

		if (HB_RANGER_IS_ON(&(Master_infor->heart_beat_data[i])))
		{
			traceprint("Ranger %d (%s:%d) has created the heartbeat with metaserver.\n", i, rg_prof[i].rg_addr, rg_prof[i].rg_port);
		}
		else
		{
			traceprint("Ranger %d (%s:%d) has not created the heartbeat with metaserver.\n", i, rg_prof[i].rg_addr, rg_prof[i].rg_port);
		}
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

static int
meta_table_is_exist(char *tabname)
{
	int	i;
	int	tabidx;


	tabidx = -1;


	for (i = 0; i < Master_infor->meta_systab->tabnum; i++)
	{
		if(strcmp(tabname, Master_infor->meta_systab->meta_tabdir[i])==0)
		{
			return i;
		}
	}

	return tabidx;
}

static int
meta_load_sysmeta()
{
	int		i;
	int		fd;
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	int		status;

	
	for (i = 0; i < Master_infor->meta_systab->tabnum; i++)
	{
		MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_meta_dir, Master_infor->meta_systab->meta_tabdir[i],
			STRLEN(Master_infor->meta_systab->meta_tabdir[i]));

		
		str1_to_str2(tab_meta_dir, '/', "sysobjects");


		OPEN(fd, tab_meta_dir, (O_RDWR));
		
		if (fd < 0)
		{
			return FALSE;
		}

		status = READ(fd, &(Master_infor->meta_sysobj->sysobject[i]),
				sizeof(TABLEHDR));

		if(status != sizeof(TABLEHDR))
		{
			CLOSE(fd);

			return FALSE;
		}

		CLOSE(fd);


		MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
		
		MEMCPY(tab_meta_dir, Master_infor->meta_systab->meta_tabdir[i],
			STRLEN(Master_infor->meta_systab->meta_tabdir[i]));

		
		str1_to_str2(tab_meta_dir, '/', "syscolumns");

		OPEN(fd, tab_meta_dir, (O_RDONLY));

		if (fd < 0)
		{
			return FALSE;
		}

		Master_infor->meta_syscol->colnum[i] = Master_infor->meta_sysobj->sysobject[i].tab_col;

		Assert(Master_infor->meta_sysobj->sysobject[i].tab_col < COL_MAX_NUM);
		
		status = READ(fd, &(Master_infor->meta_syscol->colinfor[i]),
			Master_infor->meta_sysobj->sysobject[i].tab_col * sizeof(COLINFO));

		if(status != Master_infor->meta_sysobj->sysobject[i].tab_col * sizeof(COLINFO))
		{
			CLOSE(fd);

			return FALSE;
		}

		CLOSE(fd);


		

	}

	return TRUE;
}


static TABLEHDR *
meta_get_sysobj_by_tabid(int tabid)
{
	int		i;
	TABLEHDR	*tab_hdr;


	for (i = 0; i < Master_infor->meta_systab->tabnum; i++)
	{
		tab_hdr = &(Master_infor->meta_sysobj->sysobject[i]);

		if (tab_hdr->tab_id == tabid)
		{
			return tab_hdr;
		}	
	}
	
	return NULL;
}

char *
meta_get_splits(char * req_buf)
{
	char * table_name;
	char table_dir[TABLE_NAME_MAX_LEN];
	char table_meta_dir[TABLE_NAME_MAX_LEN];

	int table_idx;
	int rpc_status = 0;
	char *resp = NULL;
	int rtn_stat = FALSE;

	TABLEHDR *tab_hdr;
	char *col_buf;
		
	if (strncasecmp(RPC_MAPRED_GET_SPLITS, req_buf, STRLEN(RPC_MAPRED_GET_SPLITS)) != 0)
	{
		return NULL;
	}
		
	table_name = req_buf + RPC_MAGIC_MAX_LEN;

	MEMSET(table_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(table_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	str1_to_str2(table_dir, '/', table_name);
	
	if ((table_idx = meta_table_is_exist(table_dir)) == -1)
	{
		traceprint("Table %s is not exist.\n", table_name);
		rpc_status |= RPC_TABLE_NOT_EXIST;
		goto exit;
	}
		
	tab_hdr = &(Master_infor->meta_sysobj->sysobject[table_idx]);
	col_buf = (char *)(&(Master_infor->meta_syscol->colinfor[table_idx]));
	
	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", table_name);
		goto exit;
	}
	
	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		goto exit;
	}

	MEMSET(table_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(table_meta_dir, table_dir, TABLE_NAME_MAX_LEN);
	str1_to_str2(table_meta_dir, '/', "tabletscheme");
	
	TABINFO 	*tabinfo;
	int 	minrowlen;
	BLK_ROWINFO blk_rowinfo;
	BUF 	*bp;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));
	
	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));
	
	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;
	
	tabinfo_push(tabinfo);
	
	minrowlen = ROW_MINLEN_IN_TABLETSCHM;
	
	TABINFO_INIT(tabinfo, table_meta_dir, NULL, 0, tabinfo->t_sinfo,
			minrowlen, TAB_SCHM_INS, tab_hdr->tab_id, 
			TABLETSCHM_ID);
	
	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0,
			TABLETSCHM_KEY_COLID_INROW, VARCHAR, -1);
	
	bp = blk_getsstable(tabinfo);
	
	char *tablet_schm_bp = (char *)(bp->bsstab->bblk);
			
	BLOCK *blk;
	
	int i, rowno;
	int *offset;
	char	*rp;
	char	*addr_in_blk;
	int addrlen_in_blk;
	int port_in_blk;
	int portlen_in_blk;
	char * tablet_name_in_blk;
	int tablet_name_len_in_blk;

	MT_SPLIT *splits = NULL;
	int split_count = 0;
	int split_number = 0;

	for(i = 0; i < BLK_CNT_IN_SSTABLE; i ++)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);
		split_count += blk->bnextrno;
	}

	splits = (MT_SPLIT *)MEMALLOCHEAP(sizeof(MT_SPLIT) * split_count);
	MEMSET(splits, sizeof(MT_SPLIT) * split_count);
			
	for(i = 0; i < BLK_CNT_IN_SSTABLE; i ++)
	{
		blk = (BLOCK *)(tablet_schm_bp + i * BLOCKSIZE);	
				
		for(rowno = 0, offset = ROW_OFFSET_PTR(blk); 
			rowno < blk->bnextrno; rowno++, offset--)
		{
			rp = (char *)blk + *offset;
				
			Assert(*offset < blk->bfreeoff);
				
			addr_in_blk = row_locate_col(rp, 
					TABLETSCHM_RGADDR_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM,
					&addrlen_in_blk);
				
			port_in_blk = *(int *)row_locate_col(rp,
					TABLETSCHM_RGPORT_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, 
					&portlen_in_blk);

			tablet_name_in_blk = row_locate_col(rp,
					TABLETSCHM_TABLETNAME_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLETSCHM, 
					&tablet_name_len_in_blk);

			MT_SPLIT *current = splits + split_number;
			MEMCPY(current->range_ip, addr_in_blk, RANGE_ADDR_MAX_LEN);
			current->range_port = port_in_blk;
			MEMCPY(current->tablet_name, tablet_name_in_blk, strlen(tablet_name_in_blk));
			MEMCPY(current->table_name, table_name, strlen(table_name));

			split_number++;
		}
			
	}
	
	bufunkeep(bp->bsstab);
	
	session_close(tabinfo);
	
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
	
	tabinfo_pop();
		
	rtn_stat = TRUE;
	
exit:
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, split_count * sizeof(MT_SPLIT), (char *)splits);
		traceprint("get %d splits.\n", split_count);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}
	
	if (splits)
	{
		MEMFREEHEAP(splits);
	}
	
	return resp;
}

char *
meta_get_reader_meta(char * req_buf)
{
	char * table_name;
	char table_dir[TABLE_NAME_MAX_LEN];

	int table_idx;
	int rpc_status = 0;

	TABLEHDR *tab_hdr;
	char *col_buf;

	int rtn_stat = TRUE;

	int col_buf_len;
	char *resp_buf;
	int col_buf_idx;
	char * resp;

		
	if (strncasecmp(RPC_MAPRED_GET_META, req_buf, STRLEN(RPC_MAPRED_GET_META)) != 0)
	{
		return NULL;
	}
		
	table_name = req_buf + RPC_MAGIC_MAX_LEN;

	MEMSET(table_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(table_dir, MT_META_TABLE, STRLEN(MT_META_TABLE));

	str1_to_str2(table_dir, '/', table_name);
	
	if ((table_idx = meta_table_is_exist(table_dir)) == -1)
	{
		traceprint("Table %s is not exist.\n", table_name);
		rpc_status |= RPC_TABLE_NOT_EXIST;
		rtn_stat = FALSE;
		goto exit;
	}
		
	tab_hdr = &(Master_infor->meta_sysobj->sysobject[table_idx]);
	col_buf = (char *)(&(Master_infor->meta_syscol->colinfor[table_idx]));
	
	if (tab_hdr->tab_tablet == 0)
	{
		traceprint("Table %s has no data.\n", table_name);
		rtn_stat = FALSE;
		goto exit;
	}
	
	if (tab_hdr->tab_stat & TAB_DROPPED)
	{
		traceprint("This table has been dropped.\n");
		rtn_stat = FALSE;
		goto exit;
	}
	
exit:
	col_buf_len = sizeof(TABLEHDR) + tab_hdr->tab_col * (sizeof(COLINFO));
	
	resp_buf = MEMALLOCHEAP(col_buf_len);
	MEMSET(resp_buf, col_buf_len);

	col_buf_idx = 0;		

	MEMCPY((resp_buf + col_buf_idx), tab_hdr, sizeof(TABLEHDR));
	col_buf_idx += sizeof(TABLEHDR);

	MEMCPY((resp_buf + col_buf_idx), col_buf, tab_hdr->tab_col * sizeof(COLINFO));
	col_buf_idx += tab_hdr->tab_col * sizeof(COLINFO);
	
	if (rtn_stat)
	{
		rpc_status |= RPC_SUCCESS;
		resp = conn_build_resp_byte(rpc_status, col_buf_len, resp_buf);
	}
	else
	{
		rpc_status |= RPC_FAIL;
		resp = conn_build_resp_byte(rpc_status, 0, NULL);
	}
	
	if(resp_buf)
	{
		MEMFREEHEAP(resp_buf);
	}
	
	return resp;
}


int main(int argc, char *argv[])
{
	char *conf_path;
	pthread_t pthread_id;


	mem_init_alloc_regions();

	Trace = 0;
	conf_path = META_DEFAULT_CONF_PATH;
	conf_get_path(argc, argv, &conf_path);

	tss_setup(TSS_OP_METASERVER);
	
	meta_server_setup(conf_path);


	pthread_create(&pthread_id, NULL, meta_recovery, NULL);;
	
	startup(Master_infor->port, TSS_OP_METASERVER, meta_handler);

	return TRUE;
}
