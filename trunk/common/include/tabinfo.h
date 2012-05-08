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

#ifndef	TABINFO_H_
#define TABINFO_H_


struct srch_info;
struct buf;
struct block_row_info;
struct col_info;
struct insert_meta;
struct insert_ranger;
struct select_range;






/* Each tablet has 256M data. */
typedef struct tablet_hdr
{
	char	firstkey[256];
	char    tblet_name[128];
	int	tblet_sstab;		/* # of sstable*/
	int	offset_c1;		/* sstable name */
	int	offset_c2;		/* range addr */
	int	offset_c3;		/* key */
	int	tabletid;	
}TABLETHDR;


/* Following definition is for the tab_stat. */
#define	TAB_DROPPED	0x0001		/* This table has been dropped. */



typedef struct tab_info
{
	struct buf	*t_dnew;	/* ptr to next dirty buf */	 
	struct buf	*t_dold;	/* ptr to last dirty buf */ 
//	TABLEHDR	*t_tabhdr;
	int		t_tabid;
	char		*t_tab_name;	/* Table name */
	int		t_tab_namelen;
	int		t_key_colid;
	int		t_key_coltype;
	int		t_key_coloff;
	struct tab_info	*t_nexttab;	/* Tabinfo link, insert header. */
	TABLETHDR	*t_tablethdr;
	struct col_info	*t_colinfo;
	struct insert_meta
			*t_insmeta;
	struct select_range
			*t_selrg;
	int		t_row_minlen;
	int		t_stat;
	int		t_tablet_id;	/* Current tablet id. */
	int		t_sstab_id;	/* current sstable id */
	char		*t_sstab_name;	/* current sstable name */
	int		t_split_tabletid;
	struct block_row_info 
			*t_rowinfo;
	struct srch_info	
			*t_sinfo;
	struct buf	*t_keptbuf;
	struct buf	*t_resbuf;
	struct insert_ranger
			*t_insrg;
	unsigned int	t_insdel_old_ts_lo;		
	unsigned int    t_insdel_new_ts_lo;
	char		*t_cur_rowp;	/* Ptr to the working row. */
	int		t_cur_rowlen;
	RID		t_currid;	/* Current working RID information. */
	
	/* Support the index. */
	int		t_index_ts;	/* Transferred from TABLEHDR. */
	int		t_has_index;	/* Transferred from TABLEHDR. */
} TABINFO;

/* Following is for the t_stat field of tab_info*/
#define TAB_META_SYSTAB		0x00000001
#define TAB_SCHM_SRCH		0x00000002	/* We need just to get the right
						** and exist row in the 
						** tabletscheme table.
						*/
#define TAB_CRT_NEW_FILE	0x00000004
#define	TAB_SRCH_DATA		0x00000008	/* Search data in the ranger
						** server.
						*/
#define TAB_SSTAB_SPLIT		0x00000010	/* We need to submit the sstab
						** name to Master if it's true.
						*/  
#define TAB_SSTAB_1ST_ROW_CHG	0x00000020
#define TAB_KEPT_BUF_VALID	0x00000040	/* If it's true, the t_keptbuf 
						** is valid.
						*/
#define TAB_INS_DATA		0x00000080	/* Reserved. */
#define TAB_SCHM_INS		0x00000100	/* Insert data into tablet or 
						** tabletscheme. 
						*/
#define TAB_GET_RES_SSTAB	0x00000200	/* True if we want to get the 
						** reserved sstable while
						** sstable hit split case.
						*/
#define TAB_TABLET_SPLIT	0x00000400
#define TAB_TABLET_CRT_NEW	0x00000800	/* Raise the # of tablet in the
						** tablet header to count.
						*/
#define TAB_TABLET_KEYROW_CHG	0x00001000	/* True and  the 1st row of 1st
						** tablet must be changed. 
						*/
#define TAB_DEL_DATA		0x00002000	/* True if we is processing a
						** delete case.
						*/
#define	TAB_RETRY_LOOKUP	0x00004000	/* Retry to lookup the metadata. */
#define	TAB_DO_SPLIT		0x00008000
#define TAB_RESERV_BUF		0x00010000
#define TAB_INS_SPLITING_SSTAB	0x00020000
#define	TAB_LOG_SKIP_LOG	0x00040000	/* Skip it for recovery. */
#define	TAB_SCHM_UPDATE		0x00080000	/* Update for the tablet or
						** tabletschme.
						*/
#define	TAB_DO_INDEX		0x00100000	/* Table is processing in the 
						** index related work, such
						** as the building index, delete
						** index, insert index.
						*/
#define	TAB_INS_INDEX		0x00200000	/* Insert index row. */
#define	TAB_SRCH_RANGE		0x00400000	/* Range query*/
#define	TAB_NOLOG_MODEL		0x00800000	/* No log model. */
#define	TAB_DEL_INDEX		0x01000000	/* Delete index row. */
#define	TAB_RID_UPDATE		0x02000000	/* RID update case. */
#define	TAB_SKIP_SSTAB_REGIEST	0x04000000	/* Skip this sstable to register
						** to its parent node.
						*/	


#define TAB_IS_SYSTAB(tabinfo)	(tabinfo->t_stat & TAB_META_SYSTAB)



void
tabinfo_push(TABINFO *tabinfo);

void
tabinfo_pop();

#endif

