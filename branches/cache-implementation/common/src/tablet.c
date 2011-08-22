/*
** tablet.c 2011-08-08 xueyingfei
**
** Copyright Transoft Corp.
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
#include "master/metaserver.h"
#include "strings.h"
#include "buffer.h"
#include "block.h"
#include "cache.h"
#include "memcom.h"
#include "strings.h"
#include "utils.h"
#include "row.h"
#include "tablet.h"
#include "tss.h"
#include "type.h"
#include "session.h"
#include "tabinfo.h"


extern	TSS	*Tss;


#define	INVALID_TABLETID	1

// char *rp - the row in the tablet (sstabid|sstable row | ranger |key col)
// int minlen - min length of the row in the tablet
void
tablet_crt(TABLEHDR *tablehdr, char *tabledir, char *rp, int minlen)
{
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];
	char	tablet_name[32];
	int	keycolen;
	char	*keycol;
	TABINFO tabinfo;
	SINFO	sinfo;


	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMSET(tablet_name, 32);
	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tabinfo_push(&tabinfo);
		

	MEMCPY(tab_meta_dir, tabledir, STRLEN(tabledir));

	assert(tablehdr->tab_tablet == 0);

	tablehdr->tab_tablet = 1;
	/* 1st step: create the tablet name. */
	build_file_name("tablet", tablet_name, tablehdr->tab_tablet);
	str1_to_str2(tab_meta_dir, '/', tablet_name);
	
	keycol = row_locate_col(rp, -1, minlen, &keycolen);
	
	/* 
	** We are inserting one row into the file "tablet0", so the min row length is not 
	** the one from header. 
	*/
	TABINFO_INIT(&tabinfo, tab_meta_dir, &sinfo, minlen, TAB_CRT_NEW_FILE,
			tablehdr->tab_id, tablehdr->tab_tablet);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEYCOLID, VARCHAR, -1);
	
	blkins(&tabinfo, rp);
	
	/* 2nd step: insert the tablet scheme into the table tabletscheme. */
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tabledir, STRLEN(tabledir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");
	
	/*
	** Temp solution: tablet name | 1st key
	*/
	int rlen = ROW_MINLEN_IN_TABLETSCHM + keycolen + sizeof(int) + sizeof(int);
	char *temprp = (char *)MEMALLOCHEAP(rlen);
	
	
	tablet_schm_bld_row(temprp, rlen, tablehdr->tab_tablet, tablet_name, keycol, keycolen);
	
	tablet_schm_ins_row(tablehdr->tab_id, 0, tab_meta_dir, temprp, tablehdr->tab_tablet);

	/* Maybe we should call table close and add a new function neamed with session_open. */
	session_close( &tabinfo);

	MEMFREEHEAP(temprp);

	/* Restore the previous table information. */
	tabinfo_pop();

}

void
tablet_ins_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen)
{
	int	keycolen;
	char	*keycol;
	TABINFO tabinfo;
	SINFO	sinfo;


	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tabinfo_push(&tabinfo);
		

	keycol = row_locate_col(rp, -1, minlen, &keycolen);
	
	/* 
	** We are inserting one row into the file "tablet0", so the min row length is not 
	** the one from header. 
	*/
	TABINFO_INIT(&tabinfo, tablet_name, &sinfo, minlen, TAB_SCHM_INS, tabid, sstabid);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEYCOLID, VARCHAR, -1);

	tabinfo.t_split_tabletid = tablehdr->tab_tablet;
	
	blkins(&tabinfo, rp);

	session_close(&tabinfo);

	tabinfo_pop();
}


void
tablet_del_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen)
{
	int	keycolen;
	char	*keycol;
	TABINFO tabinfo;
	SINFO	sinfo;


	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tabinfo_push(&tabinfo);
		

	keycol = row_locate_col(rp, -1, minlen, &keycolen);
	
	/* 
	** We are inserting one row into the file "tablet0", so the min row length is not 
	** the one from header. 
	*/
	TABINFO_INIT(&tabinfo, tablet_name, &sinfo, minlen, TAB_SRCH_DATA, tabid, sstabid);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEYCOLID, VARCHAR, -1);
	
	blkdel(&tabinfo, rp);

	session_close(&tabinfo);

	tabinfo_pop();
}



/* 
** Following is the formate of SSTABLE row:
**	| row header |sstab id | sstable name | ranger server addr | reserverd sstab id |key col value |
*/
int
tablet_bld_row(char *sstab_rp, int sstab_rlen, char *tab_name, int tab_name_len,
		int sstab_id, int res_sstab_id, char *sstab_name, int sstab_name_len, 
		char *rang_addr, char *keycol, int keycolen, int keycol_type)
{
	int	sstab_idx;
	int min_rlen;


	sstab_idx = 0;
	min_rlen = 0;
	
	/* 
	** Building a row that save the information of sstable, 
	** this row is also the index, so we have to specify a 
	** key for this row.
	**
	** 64 : range addr
	** 128 : sstable name length 
	** 4 : offet of var-column
	** 4 : row length
	*/
	row_build_hdr((sstab_rp + sstab_idx), 0, 0, 1);

	/* 
	** The format of this row is difference with data row. 
	** The row length is saved following the row header.
	*/
	sstab_idx += sizeof(ROWFMT);

	/* Put c1 to the row */
	PUT_TO_BUFFER(sstab_rp, sstab_idx, &sstab_id, sizeof(int));
		
	/* Put c2 to the row */		
	PUT_TO_BUFFER(sstab_rp, sstab_idx, sstab_name, sstab_name_len);

	sstab_idx += (SSTABLE_NAME_MAX_LEN - sstab_name_len);

	/* Put c3 to the row */
	PUT_TO_BUFFER(sstab_rp, sstab_idx, rang_addr, RANGE_ADDR_MAX_LEN);

	/* Put c4 to the row */
	PUT_TO_BUFFER(sstab_rp, sstab_idx, &res_sstab_id, sizeof(int));

	/* Put row length into the row after the done of fixed columns. */
	PUT_TO_BUFFER(sstab_rp, sstab_idx, &sstab_rlen, sizeof(int));

//	*(int *)((char *)sstab_rp + sstab_idx) = sstab_rlen;
//	sstab_idx += sizeof(int);
	

	/* The key of this row. */
	PUT_TO_BUFFER(sstab_rp, sstab_idx, keycol, keycolen);

	min_rlen = sstab_idx - sizeof(int);

	if (!TYPE_IS_FIXED(keycol_type))
	{
		/* Put the coloffset into the tail of row. */
		*(int *)(sstab_rp + sstab_idx) = sstab_idx - keycolen;

		sstab_idx += COLOFFSETENTRYSIZE;

		min_rlen -= keycolen;
	}
	
	/* Disable the offset */
	if (0)
	{
		TABLETHDR	tablet_hdr;
		PUT_TO_BUFFER(sstab_rp, sstab_idx, &(tablet_hdr.offset_c3), 
				sizeof(int));
	}

	assert(sstab_idx == sstab_rlen);

	return min_rlen;
}



char *
tablet_srch_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *systab, char *key, int keylen)
{
	TABINFO	*tabinfo;
	int	minrowlen;
	BUF	*bp;
	int	offset;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo; 
	
	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLET;

	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, TAB_SCHM_SRCH, tabid, sstabid);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_KEYCOLID, VARCHAR, -1);			
	
	bp = blkget(tabinfo);
	offset = blksrch(tabinfo, bp);

	bufunkeep(bp->bsstab);
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

	return ((char *)(bp->bblk) + offset);
}



/* 
** Following is the row formate of the table tabletscheme:
**	| row header | tabletid| tablet name | 1st key |
** Maybe we should consider the tablet space.
*/
void 
tablet_schm_bld_row(char *rp, int rlen, int tabletid, char *tabletname, char *keycol, int keycolen)
{
	int		rowidx;


	rowidx = 0;
	
	row_build_hdr((rp + rowidx), 0, 0, 1);

	/* 
	** The format of this row is difference with data row. 
	** The row length is saved following the row header.
	*/
	rowidx += sizeof(ROWFMT);

	
	/* Put c1 to the row */ 	
	PUT_TO_BUFFER(rp, rowidx, &tabletid, sizeof(int));
	
	/* Put c2 to the row */ 	
	PUT_TO_BUFFER(rp, rowidx, tabletname, TABLET_NAME_MAX_LEN);

	/* Put row length into the row after the done of fixed columns. */
	PUT_TO_BUFFER(rp, rowidx, &rlen, sizeof(int));

	/* Put c3 to the row */
	PUT_TO_BUFFER(rp, rowidx, keycol, keycolen);

	
	/* Put the coloffset into the tail of row. */
	*(int *)(rp + rowidx) = rowidx - keycolen;

	rowidx += COLOFFSETENTRYSIZE;
	
	assert(rowidx == rlen);
}


void
tablet_schm_ins_row(int tabid, int sstabid, char *systab, char *row, int tabletnum)
{
	TABINFO	*tabinfo;
	int	minrowlen;
	char	*key;
	int	keylen;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	/* 
	** We should pass the virtual value for coloffset of varchar column into row_locate_col,
	** even if the true vaule is (minrowlen + sizeof(int)).
	*/
	key = row_locate_col(row, -1, minrowlen, &keylen);
	
	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, tabletnum ? TAB_SCHM_INS : TAB_CRT_NEW_FILE,
			tabid, sstabid);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_SCHM_KEYCOLID, VARCHAR, -1);
			
	blkins(tabinfo, row);

	
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();
}




char *
tablet_schm_srch_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *systab, char *key, int keylen)
{
	TABINFO	*tabinfo;
	int	minrowlen;
	BUF	*bp;
	int	offset;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, TAB_SCHM_SRCH, tabid, sstabid);

	/* VIRTUAL OFFSET. */
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_SCHM_KEYCOLID, VARCHAR, -1);
			
	
	bp = blkget(tabinfo);
	offset = blksrch(tabinfo, bp);

	bufunkeep(bp->bsstab);
	session_close(tabinfo);
	
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
	
	tabinfo_pop();

	return ((char *)(bp->bblk) + offset);
}


/* Random nameing for the new sstable by the creating time. */
void
tablet_namebyname(char *old_sstab, char *new_sstab)
{
	char	nameidx[64];
	int	idxpos;
	char	tmpsstab[SSTABLE_NAME_MAX_LEN];
	int	old_sstab_len;
	time_t	timer;

	MEMSET(nameidx, 64);
	old_sstab_len = STRLEN(old_sstab);
	idxpos = str1nstr(old_sstab, "tablet", old_sstab_len);

	MEMSET(tmpsstab, SSTABLE_NAME_MAX_LEN);
	MEMCPY(tmpsstab, old_sstab, idxpos);

	time(&timer);
	sprintf(nameidx, "%ld", timer );

	sprintf(new_sstab, "%s%s", tmpsstab,nameidx);
	
	return;
}


void
tablet_split(TABINFO *srctabinfo, BUF *srcbp, char *rp)
{
	BUF	*destbuf;
	TABINFO * tabinfo;
	BLOCK	*nextblk;
	BLOCK	*blk;
	char	*key;
	int	keylen;
	int	ins_nxtsstab;
	char	*tablet_key;
	int	tablet_keylen;
	int	table_nameidx;
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];


	nextblk = srcbp->bsstab->bblk;

	ins_nxtsstab = (nextblk->bblkno > ((BLK_CNT_IN_SSTABLE / 2) - 1)) ? TRUE : FALSE;
	
	while (nextblk->bnextblkno < (BLK_CNT_IN_SSTABLE / 2))
	{		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
	}


	destbuf = bufgrab(srctabinfo);
	
	bufhash(destbuf);

	blk = destbuf->bblk;
		
	blk_init(blk);

	while(nextblk->bblkno != -1)
	{
		assert(nextblk->bfreeoff > BLKHEADERSIZE);

		BLOCK_MOVE(blk,nextblk);
		
		if (nextblk->bnextblkno == -1)
		{
			break;
		}
		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
		blk = (BLOCK *) ((char *)blk + BLOCKSIZE);
	}


	MEMSET(destbuf->bsstab_name, 256);

	tablet_namebyname(srctabinfo->t_sstab_name, destbuf->bsstab_name);

	table_nameidx = str01str(srctabinfo->t_sstab_name, "tablet", STRLEN(srctabinfo->t_sstab_name));
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo_push(tabinfo);

	/* sstable id is the tablet id and split_tabletid is the new splited tablet id. */
	TABINFO_INIT(tabinfo, destbuf->bsstab_name, tabinfo->t_sinfo, ROW_MINLEN_IN_TABLET, TAB_KEPT_BUF_VALID,
			srctabinfo->t_tabid, srctabinfo->t_split_tabletid);

	if (ins_nxtsstab)
	{
		tabinfo->t_keptbuf = destbuf;	

		key = row_locate_col(rp, -1, ROW_MINLEN_IN_TABLET, &keylen);
		
		SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_KEYCOLID, VARCHAR, -1);
		
		blkins(tabinfo, rp);
	}
	else
	{
		bufpredirty(destbuf);
		bufdirty(destbuf);
	}


	tablet_key = row_locate_col(destbuf->bblk->bdata, -1, destbuf->bblk->bminlen, &tablet_keylen);
	
	/*
	** Temp solution: tablet name | 1st key
	*/
	int rlen = ROW_MINLEN_IN_TABLETSCHM + tablet_keylen + sizeof(int) + sizeof(int);
	char *temprp = (char *)MEMALLOCHEAP(rlen);

	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, srctabinfo->t_sstab_name, table_nameidx);
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	table_nameidx = str01str(destbuf->bsstab_name, "tablet", STRLEN(destbuf->bsstab_name));
	tablet_schm_bld_row(temprp, rlen, INVALID_TABLETID, destbuf->bsstab_name + table_nameidx + 1, tablet_key, tablet_keylen);

	/* 0 is reserved for tablet_schem. */
	tablet_schm_ins_row(srctabinfo->t_tabid, 0, tab_meta_dir, temprp, INVALID_TABLETID);

	session_close(tabinfo);

	
	MEMFREEHEAP(temprp);

	if (tabinfo)
	{
		if (tabinfo->t_sinfo)
		{
			MEMFREEHEAP(tabinfo->t_sinfo);
		}

		MEMFREEHEAP(tabinfo);
	}

	tabinfo_pop();
	
}



