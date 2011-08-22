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

#include "global.h"
#include "conf.h"
#include "netconn.h"

/*
** 256MB * 1024 * 1024 = 256TB
** 256MB * (4*4 * 4*4*4) * (4*4*4 *4*4)
*/
/* 
MetaServer Behavior
------------------
1. metaserver setup(first setup):

                 maxtable/tableid -- allocation of table id 
                 maxtable/table   -- save the metadata of user table 
       file:     maxtable/rg_server -- save the addr of range server

 2. create table:

                 maxtable/table/table_name -- save the tablet of user table
        file:   maxtable/table/table_name/table_name -- save the tablet information
                               | header (fixed size) | tablet row1 |tablet  row2  |  tablet row...|
                 maxtable/table/table_name/table_col -- save the information of column, 
                                                        such as the name /offset of column 

 3. insert data(first data of table):

                  maxtable/table/table_name/tablet0 -- as the root storage unit of table
                                | header (fixed size)| sstable row1 | sstable row2  |  sstable row...|
                  

 RangeServer Behavior
 -------------------
 1. metaserver setup:(first setup)        No
 2. create table:		              No
 3. insert data:(first data of table)	-> just call the API from GFS	
  
                  


		< 1st Version of Design>
                            =================
metaserver:
                          row header,  fixed col,    var-col,            var-col2
	table ( <row header, tablet0,  key of 1st row, key of last row>, <r_hd,tablet1..>, <r_hd, tablet2...>)

	tablet( <r_hd, sstable0,  key of 1st row, key of last row, range IP, range port>, <>)

range server

	sstable(block0, block1, block2....)
	             |
	           save the block index (<block1, key of 1st row, key of last row >)  




		<2nd Version of Design>
                             ==================
metaserver:
                          
	table (tab_hdr,  <r_hd, tablet0, key of first row,  <r_hd,tablet1,  key of first row>, <tablet2...>)

	tablet( tablet hdr, <r_hd, sstable0,  range addr, key of 1st row>, <>)

range server

	sstable(block0, block1, block2....)
	             |
	           save the block index (<block1, key of 1st row>)  


		<3rd Version of Design> -- Current Implementation
                             ==================
metaserver:
	1. table scheme  files, as follows:
		a. table header file
		b. sysobject file
		c. syscolumns file

	table header file manage some table scheme information, such as the tablet #,...
		
	2. tablet files, as follows:
		tablet scheme file which is a table in fact, its each row stores the corresponding tablet information,
			such as tablet name, tablet's 1st key, sstable# of this tablet.
		tablet files. There are many tablet files, its name like tablet0, tablet1,tablet2,....tabletn.
	
                          
	tablet_scheme (<r_hd, tablet0, sstable #, key of first row>, <r_hd,tablet1, sstable #, key of first row>, <tablet2...>)

	tablet( <r_hd, sstable0,  range addr, key of 1st row>, <>)

range server

	sstable(block0, block1, block2....)
	              
*/

struct srch_info;
struct buf;

#define RANGE_ADDR_MAX_LEN      64
#define RANGE_SERVER_TEST       "127.0.0.1\0"
#define RANGE_PORT_TEST         1949
#define SSTABLE_NAME_MAX_LEN    128
#define TABLE_NAME_MAX_LEN      256
#define TABLET_NAME_MAX_LEN	32

struct stat st;

typedef struct key_col
{
	int	col_offset;	/* the offset of key column */
} KEYCOL;

/* table ( tab_hdr, <tablet0, key of first row,  <tablet1,  key of first row>, <tablet2...>) */
typedef struct table_hdr
{
	int     tab_id;
	char    tab_name[128];
	int	tab_tablet;	/* # of tablet. It stands for the # of tablet row */
	int	tab_sstab;	/* # of sstable of the whole TABLE, not the TABLET. */
	int	tab_row_minlen;
	int	tab_key_colid;	/* column id of key column */
	int	tab_key_coloff;	/* the offset of key column that data store according to it */
	int	tab_key_coltype;/* Column type of key column */
	int	tab_col;	/* # of column */
	int	tab_varcol;	/* # of var-column */
	int 	offset_c1;	/* the column of tablet name */
	int 	offset_c2;	/* the column of key */
} TABLEHDR;

/* Each tablet has 256M data. */
typedef struct tablet_hdr
{
	char	firstkey[256];
	char    tblet_name[128];
	int	tblet_sstab;	/* # of sstable*/
	int	offset_c1;	/* sstable name */
	int	offset_c2;	/* range addr */
	int	offset_c3;	/* key*/
	int	tabletid;	
}TABLETHDR;

/*
** sycolumn scheme:
**             1.  each table has one syscolumn
**             2. Clo_id (int) | Col_name (char 64) | Col_length (int)| Col_offset (int) |Col_type (int) 
*/
typedef struct col_info
{
        int     col_id;
        char    col_name[64];
        int     col_len;
        int     col_offset;
        int     col_type;
} COLINFO;

/* Each sstable has 64M data. */
typedef struct sstable
{
	char    sstab_name[128];
}SSTABLE;


typedef struct rg_prof
{
	char	rg_addr[RANGE_ADDR_MAX_LEN];
	int	rg_port;
} RANGE_PROF;

typedef union infor_hdr
{
	struct rg_prof	rg_info;
	char		magic[RPC_MAGIC_MAX_LEN];
} INFOR_HDR;

/* This structure should be corresponding to the formate of metaserver returns. */
typedef struct insert_meta
{
	union infor_hdr	i_hdr;
	int		sstab_id;
	int		res_sstab_id;
	char    	sstab_name[SSTABLE_NAME_MAX_LEN];
	int		status;
	int		col_num;	/* # of column in the table */
	int		varcol_num;	/* # of var-length column */
	int		row_minlen;
} INSMETA;

/* following definition is for the status field of insert_meta. */
#define	INS_META_1ST	0x0001	/* Falg if this insert is first row in this sstable file. */

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
	struct buf	*t_dnew;	 /* ptr to next dirty buf */
	struct buf	*t_dold;	 /* ptr to last dirty buf */
//	TABLEHDR	*t_tabhdr;
	int		t_tabid;
	int		t_key_colid;
	int		t_key_coltype;
	int		t_key_coloff;
	struct tab_info	*t_nexttab;	/* Tabinfo link, insert header. */
	TABLETHDR	*t_tablethdr;
	COLINFO		*t_colinfo;
	INSMETA		*t_insmeta;
	int		t_row_minlen;
	int		t_stat;
	int		t_sstab_id;	/* current sstable id */
	char		*t_sstab_name;	/* current sstable name */
	int		t_split_tabletid;
	
	struct srch_info	
			*t_sinfo;
	struct buf	*t_keptbuf;
	INSRG		*t_insrg;
} TABINFO;

/* Following is for the t_stat field of tab_info*/

#define TAB_META_SYSTAB		0x0001
#define TAB_SCHM_SRCH		0x0002	/* We need just to get the right and exist row in the tabletscheme table. */
#define TAB_CRT_NEW_FILE	0x0004
#define	TAB_SRCH_DATA		0x0008	/* Search data in the ranger server. */
#define TAB_SSTAB_SPLIT		0x0010  /* We need to submit the sstab name to Master if it's true. */
#define TAB_SSTAB_1ST_ROW_CHG	0x0020
#define TAB_KEPT_BUF_VALID	0x0040	/* If it's true, the t_keptbuf is valid. */
#define TAB_INS_DATA		0x0080
#define TAB_SCHM_INS		0x0100	/* Insert data into tablet or tabletscheme. */
#define TAB_GET_RES_SSTAB	0x0200	/* True if we want to get the reserved sstable while sstable hit split case. */
#define TAB_TABLET_SPLIT	0x0400
#define TAB_TABLET_CRT_NEW	0x0800	/* Raise the # of tablet in the tablet header to count. */


#define TAB_IS_SYSTAB(tabinfo)	(tabinfo->t_stat & TAB_META_SYSTAB)


/* Following is for the ranger server saving. */
#define SVR_IDX_FILE_HDR	16
#define SVR_IDX_FILE_BLK	(64 * 1024)
typedef	struct svr_idx_file
{
	int		nextrno;	/* if data, next free row number */
	int		freeoff; 	/* first unused byte */
	short		stat;
	char		pad2[6];
	char		data[SVR_IDX_FILE_BLK - 16]; /* actual useful data */
}SVR_IDX_FILE;


/* 
** minlen is needed in the insert case and this value will be saved into disk. 
** In the search case, we can skip this value or use the invalid value.
*/
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

#endif /* MASTER_H_ */
