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
#include "rebalancer.h"


extern	TSS	*Tss;


// char *rp - the row in the tablet (sstabid|sstable row | ranger |key col)
// int minlen - min length of the row in the tablet
void
tablet_crt(TABLEHDR *tablehdr, char *tabledir, char *rg_addr, char *rp, int minlen, int port)
{
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	char		tablet_name[32];
	int		keycolen;
	char		*keycol;
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;


	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMSET(tablet_name, 32);
	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tabinfo_push(&tabinfo);
		

	MEMCPY(tab_meta_dir, tabledir, STRLEN(tabledir));

	Assert(tablehdr->tab_tablet == 0);

	tablehdr->tab_tablet = 1;
	
	build_file_name("tablet", tablet_name, tablehdr->tab_tablet);
	str1_to_str2(tab_meta_dir, '/', tablet_name);
	
	keycol = row_locate_col(rp, -1, minlen, &keycolen);
	
	
	TABINFO_INIT(&tabinfo, tab_meta_dir, &sinfo, minlen, TAB_CRT_NEW_FILE,
			tablehdr->tab_id, tablehdr->tab_tablet);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEY_COLID_INROW, VARCHAR, -1);
	
	blkins(&tabinfo, rp);
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tabledir, STRLEN(tabledir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");
	
	
	int rlen = ROW_MINLEN_IN_TABLETSCHM + keycolen + sizeof(int) + sizeof(int);
	char *temprp = (char *)MEMALLOCHEAP(rlen);
	
	
	tablet_schm_bld_row(temprp, rlen, tablehdr->tab_tablet, tablet_name, rg_addr, keycol, keycolen, port);
	
	tablet_schm_ins_row(tablehdr->tab_id, TABLETSCHM_ID, tab_meta_dir, temprp, 0);

	
	session_close( &tabinfo);

	MEMFREEHEAP(temprp);

	
	tabinfo_pop();

}

void
tablet_ins_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen)
{
	LOCALTSS(tss);
	int		keycolen;
	char		*keycol;
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;


	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tabinfo_push(&tabinfo);
		

	keycol = row_locate_col(rp, -1, minlen, &keycolen);
	
	
	TABINFO_INIT(&tabinfo, tablet_name, &sinfo, minlen, TAB_SCHM_INS, tabid, sstabid);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEY_COLID_INROW, VARCHAR, -1);

	tabinfo.t_split_tabletid = tablehdr->tab_tablet;
	
	blkins(&tabinfo, rp);

	session_close(&tabinfo);

	if (tabinfo.t_stat & TAB_TABLET_CRT_NEW)
	{
		RANGE_PROF *rg_prof;
		rg_prof = rebalan_get_rg_prof_by_addr(tss->tcur_rgprof->rg_addr, tss->tcur_rgprof->rg_port);
		(rg_prof->rg_tablet_num)++;
		
		(tablehdr->tab_tablet)++;
	}

	tabinfo_pop();
}


void
tablet_del_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name,
			char *rp, int minlen)
{
	int		keycolen;
	char		*keycol;
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;


	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tabinfo_push(&tabinfo);
		

	keycol = row_locate_col(rp, -1, minlen, &keycolen);
	
	
	TABINFO_INIT(&tabinfo, tablet_name, &sinfo, minlen, TAB_SRCH_DATA,
		     tabid, sstabid);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEY_COLID_INROW, 
		       VARCHAR, -1);
	
	blkdel(&tabinfo);

	session_close(&tabinfo);

	tabinfo_pop();
}

int
tablet_upd_col(char *newrp, char *oldrp, int rlen, int colid, char *newcolval, int newvalen)
{
	char	*colptr;
	int	colen;
	int	coloffset;
	int	rtn_stat = TRUE;

	switch (colid)
	{
	    case TABLET_SSTABID_COLID_INROW:	
	    case TABLET_SSTABNAME_COLID_INROW:
	    	break;
	    case TABLET_RESSSTABID_COLID_INROW:

		MEMCPY(newrp, oldrp, rlen);
		colptr = row_locate_col(newrp, TABLET_RESSSTABID_COLOFF_INROW, 
					ROW_MINLEN_IN_TABLET, &colen);

		*(int *)colptr = *(int *)newcolval;

		Assert (newvalen == sizeof(int));
	    	
	    	break;
		
	    case TABLET_KEY_COLID_INROW:

		MEMCPY(newrp, oldrp, ROW_MINLEN_IN_TABLET);
		
	    	coloffset = TABLET_KEY_COLOFF_INROW;

		
		
		PUT_TO_BUFFER(newrp, coloffset, newcolval, newvalen);

		
		*(int *)(newrp + coloffset) = coloffset - newvalen;

		coloffset += sizeof(int);

		colptr = newrp + ROW_MINLEN_IN_TABLET;

		int ign = 0;
		
		PUT_TO_BUFFER(colptr, ign, &coloffset, sizeof(int));

		break;
		
	    default:
	    	rtn_stat = FALSE;
	    	break;
		
	}

	return rtn_stat;
}


int
tablet_bld_row(char *sstab_rp, int sstab_rlen, char *tab_name, int tab_name_len,
		int sstab_id, int res_sstab_id, char *sstab_name, int sstab_name_len, 
		char *keycol, int keycolen, int keycol_type)
{
	int	sstab_idx;
	int min_rlen;


	sstab_idx = 0;
	min_rlen = 0;
	
	
	row_build_hdr((sstab_rp + sstab_idx), 0, 0, 1);

	
	sstab_idx += sizeof(ROWFMT);

	
	PUT_TO_BUFFER(sstab_rp, sstab_idx, &sstab_id, sizeof(int));
		
			
	PUT_TO_BUFFER(sstab_rp, sstab_idx, sstab_name, sstab_name_len);

	sstab_idx += (SSTABLE_NAME_MAX_LEN - sstab_name_len);

	
//	PUT_TO_BUFFER(sstab_rp, sstab_idx, rang_addr, RANGE_ADDR_MAX_LEN);

	
	PUT_TO_BUFFER(sstab_rp, sstab_idx, &res_sstab_id, sizeof(int));

	
	PUT_TO_BUFFER(sstab_rp, sstab_idx, &sstab_rlen, sizeof(int));

//	*(int *)((char *)sstab_rp + sstab_idx) = sstab_rlen;
//	sstab_idx += sizeof(int);
	

	
	PUT_TO_BUFFER(sstab_rp, sstab_idx, keycol, keycolen);

	min_rlen = sstab_idx - sizeof(int);

	if (!TYPE_IS_FIXED(keycol_type))
	{
		
		*(int *)(sstab_rp + sstab_idx) = sstab_idx - keycolen;

		sstab_idx += COLOFFSETENTRYSIZE;

		min_rlen -= keycolen;
	}
	
	
	if (0)
	{
		TABLETHDR	tablet_hdr;
		PUT_TO_BUFFER(sstab_rp, sstab_idx, &(tablet_hdr.offset_c3), 
				sizeof(int));
	}

	Assert(sstab_idx == sstab_rlen);

	return min_rlen;
}



char *
tablet_srch_row(TABINFO *usertabinfo, TABLEHDR *tablehdr, int tabid, int sstabid, 
			char *systab, char *key, int keylen)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	BUF		*bp;
	int		offset;
	BLK_ROWINFO	blk_rowinfo;

	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo; 
	
	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLET;

	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, TAB_SCHM_SRCH, 
		     tabid, sstabid);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_KEY_COLID_INROW, 
		       VARCHAR, -1);			
	
	bp = blkget(tabinfo);
//	offset = blksrch(tabinfo, bp);

	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);
	offset = tabinfo->t_rowinfo->roffset;

	bufunkeep(bp->bsstab);
	session_close(tabinfo);

	if (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG)
	{
		usertabinfo->t_stat |= TAB_TABLET_KEYROW_CHG;
	}
	
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

	return ((char *)(bp->bblk) + offset);
}


void 
tablet_schm_bld_row(char *rp, int rlen, int tabletid, char *tabletname, 
			char *rang_addr, char *keycol, int keycolen, int port)
{
	int		rowidx;


	rowidx = 0;
	
	row_build_hdr((rp + rowidx), 0, 0, 1);

	
	rowidx += sizeof(ROWFMT);

	
	 	
	PUT_TO_BUFFER(rp, rowidx, &tabletid, sizeof(int));

	PUT_TO_BUFFER(rp, rowidx, &port, sizeof(int));
	
	 	
	PUT_TO_BUFFER(rp, rowidx, tabletname, TABLET_NAME_MAX_LEN);

	
	PUT_TO_BUFFER(rp, rowidx, rang_addr, RANGE_ADDR_MAX_LEN);

	
	PUT_TO_BUFFER(rp, rowidx, &rlen, sizeof(int));

	
	PUT_TO_BUFFER(rp, rowidx, keycol, keycolen);

	
	
	*(int *)(rp + rowidx) = rowidx - keycolen;

	rowidx += COLOFFSETENTRYSIZE;
	
	Assert(rowidx == rlen);
}


void
tablet_schm_upd_col(char *newrp, char *oldrp, int colid, char *newcolval, int newvalen)
{
	int	coloffset;
	char	*colptr;

	
	switch (colid)
	{
	    case TABLETSCHM_TABLETID_COLID_INROW:
	    case TABLETSCHM_TABLETNAME_COLID_INROW:
	    case TABLETSCHM_RGADDR_COLID_INROW:
	    	break;
		
	    case TABLETSCHM_KEY_COLID_INROW:
	    	MEMCPY(newrp, oldrp, ROW_MINLEN_IN_TABLETSCHM);

		coloffset = TABLETSCHM_KEY_COLOFF_INROW;

		PUT_TO_BUFFER(newrp, coloffset, newcolval, newvalen);

		
		*(int *)(newrp + coloffset) = coloffset - newvalen;

		coloffset += sizeof(int);

		colptr = newrp + ROW_MINLEN_IN_TABLETSCHM;

		int ign = 0;
		
		PUT_TO_BUFFER(colptr, ign, &coloffset, sizeof(int));
	    	break;
		
	    default:
	    	break;
	}
}


void
tablet_schm_ins_row(int tabid, int sstabid, char *systab, char *row, int tabletnum)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	char		*key;
	int		keylen;
	BLK_ROWINFO	blk_rowinfo;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	
	key = row_locate_col(row, -1, minrowlen, &keylen);
	
	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, tabletnum ? 
		     TAB_SCHM_INS : TAB_CRT_NEW_FILE, tabid, sstabid);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLETSCHM_KEY_COLID_INROW, 
		       VARCHAR, -1);
			
	blkins(tabinfo, row);

	
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();
}




void
tablet_schm_del_row(int tabid, int sstabid, char *systab, char *row)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	char		*key;
	int		keylen;
	BLK_ROWINFO	blk_rowinfo;
	
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));
	
	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	
	key = row_locate_col(row, -1, minrowlen, &keylen);
	
	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, TAB_SRCH_DATA,
		     tabid, sstabid);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLETSCHM_KEY_COLID_INROW,
		       VARCHAR, -1);
			
	blkdel(tabinfo);
	
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();
}


char *
tablet_schm_srch_row(TABLEHDR *tablehdr, int tabid, int sstabid, 
			  char *systab, char *key, int keylen)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	BUF		*bp;
	int		offset;
	BLK_ROWINFO	blk_rowinfo;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, 
		     TAB_SCHM_SRCH, tabid, sstabid);

	
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, 
		       TABLETSCHM_KEY_COLID_INROW, VARCHAR, -1);
			
	
	bp = blkget(tabinfo);
//	offset = blksrch(tabinfo, bp);
	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);
	offset = tabinfo->t_rowinfo->roffset;
	
	bufunkeep(bp->bsstab);
	session_close(tabinfo);
	
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
	
	tabinfo_pop();

	return ((char *)(bp->bblk) + offset);
}



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
tablet_namebyid(TABINFO *tabinfo, char *new_sstab)
{
	LOCALTSS(tss);
	char	nameidx[64];
	int	idxpos;
	char	tmpsstab[SSTABLE_NAME_MAX_LEN];
	int	old_sstab_len;
	char	*old_sstab;



	old_sstab = tabinfo->t_sstab_name;

	old_sstab_len = STRLEN(old_sstab);
	idxpos = str1nstr(old_sstab, "tablet", old_sstab_len);

	MEMSET(nameidx, 64);

	Assert(tabinfo->t_stat & TAB_TABLET_SPLIT);
	sprintf(nameidx, "%d", tabinfo->t_split_tabletid);
	
	MEMSET(tmpsstab, SSTABLE_NAME_MAX_LEN);
	MEMCPY(tmpsstab, old_sstab, idxpos);

//	printf("tabinfo->t_insmeta->res_sstab_id = %d \n", tabinfo->t_insmeta->res_sstab_id);

	sprintf(new_sstab, "%s%s", tmpsstab,nameidx);

	if (DEBUG_TEST(tss))
	{
		traceprint("new_sstab = %s--------%d---\n", new_sstab,tabinfo->t_split_tabletid);
	}
	
	return;
}


void
tablet_split(TABINFO *srctabinfo, BUF *srcbp, char *rp)
{
	LOCALTSS(tss);
	BUF		*destbuf;
	TABINFO 	* tabinfo;
	BLOCK		*nextblk;
	BLOCK		*blk;
	char		*key;
	int		keylen;
	int		ins_nxtsstab;
	char		*tablet_key;
	int		tablet_keylen;
	int		table_nameidx;
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	BLK_ROWINFO	blk_rowinfo;


	ins_nxtsstab = (srcbp->bblk->bblkno > ((BLK_CNT_IN_SSTABLE / 2) - 1)) ? TRUE : FALSE;

	nextblk = srcbp->bsstab->bblk;
	
	while (nextblk->bnextblkno < (BLK_CNT_IN_SSTABLE / 2 + 1))
	{		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
	}

	
	srctabinfo->t_stat |= TAB_TABLET_SPLIT;

	destbuf = bufgrab(srctabinfo);

	
	
	bufhash(destbuf);

	blk = destbuf->bblk;
		
	blk_init(blk);

	while(nextblk->bblkno != -1)
	{
		Assert(nextblk->bfreeoff > BLKHEADERSIZE);

		BLOCK_MOVE(blk,nextblk);
		
		if (nextblk->bnextblkno == -1)
		{
			break;
		}
		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
		blk = (BLOCK *) ((char *)blk + BLOCKSIZE);
	}


	MEMSET(destbuf->bsstab_name, 256);

	tablet_namebyid(srctabinfo, destbuf->bsstab_name);

	srctabinfo->t_stat &= ~TAB_TABLET_SPLIT;

	table_nameidx = str01str(srctabinfo->t_sstab_name, "tablet", 
				 STRLEN(srctabinfo->t_sstab_name));
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo_push(tabinfo);

	
	TABINFO_INIT(tabinfo, destbuf->bsstab_name, tabinfo->t_sinfo, 
		     ROW_MINLEN_IN_TABLET, TAB_KEPT_BUF_VALID,
		     srctabinfo->t_tabid, srctabinfo->t_split_tabletid);

	if (ins_nxtsstab)
	{
		tabinfo->t_keptbuf = destbuf;	

		key = row_locate_col(rp, -1, ROW_MINLEN_IN_TABLET, &keylen);
		
		SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_KEY_COLID_INROW, 
			       VARCHAR, -1);
		
		blkins(tabinfo, rp);
	}
	else
	{
		bufpredirty(destbuf);
		bufdirty(destbuf);
	}

	tablet_key = row_locate_col(destbuf->bblk->bdata, -1, destbuf->bblk->bminlen,
				    &tablet_keylen);
	
	
	int rlen = ROW_MINLEN_IN_TABLETSCHM + tablet_keylen + sizeof(int) + sizeof(int);
	char *temprp = (char *)MEMALLOCHEAP(rlen);

	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, srctabinfo->t_sstab_name, table_nameidx);
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	table_nameidx = str01str(destbuf->bsstab_name, "tablet", STRLEN(destbuf->bsstab_name));
	tablet_schm_bld_row(temprp, rlen, srctabinfo->t_split_tabletid,
			    destbuf->bsstab_name + table_nameidx + 1, 
			    tss->tcur_rgprof->rg_addr, tablet_key, tablet_keylen, 
			    tss->tcur_rgprof->rg_port);

	
	tablet_schm_ins_row(srctabinfo->t_tabid, TABLETSCHM_ID, tab_meta_dir, temprp, 
			    INVALID_TABLETID);

	session_close(tabinfo);

	srctabinfo->t_stat |= TAB_TABLET_CRT_NEW;
	
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



