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

#include "master/metaserver.h"
#include "tabinfo.h"
#include "strings.h"
#include "buffer.h"
#include "rpcfmt.h"
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
#include "rebalancer.h"
#include "metadata.h"
#include "sstab.h"


extern	TSS	*Tss;
extern	KERNEL	*Kernel;


// char *rp - the row in the tablet (sstabid|sstable row | ranger |key col)
// int minlen - min length of the row in the tablet
int
tablet_crt(TABLEHDR *tablehdr, char *tabledir, char *rg_addr, char *rp, int minlen, int port)
{
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	char		tablet_name[32];
	int		keycolen;
	char		*keycol;
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		rtn_stat;


	rtn_stat = TRUE;
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
	
	
	TABINFO_INIT(&tabinfo, tab_meta_dir, NULL, 0, &sinfo, minlen,
			TAB_CRT_NEW_FILE, tablehdr->tab_id, 
			tablehdr->tab_tablet);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEY_COLID_INROW, VARCHAR, -1);
	
	if (!blkins(&tabinfo, rp))
	{
		tablehdr->tab_tablet = 0;
		tabinfo_pop();
		return FALSE;
	}
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tabledir, STRLEN(tabledir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");
	
	
	int rlen = ROW_MINLEN_IN_TABLETSCHM + keycolen + sizeof(int) + sizeof(int);
	char *temprp = (char *)MEMALLOCHEAP(rlen);
	
	
	tablet_schm_bld_row(temprp, rlen, tablehdr->tab_tablet, tablet_name, 
				rg_addr, keycol, keycolen, port);
	
	rtn_stat = tablet_schm_ins_row(tablehdr->tab_id, TABLETSCHM_ID, 
					tab_meta_dir, temprp, 0, 0);

	
	session_close( &tabinfo);

	MEMFREEHEAP(temprp);

	
	tabinfo_pop();

	return rtn_stat;

}



int
tablet_ins_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen)
{
	LOCALTSS(tss);
	int		keycolen;
	char		*keycol;
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		tablet_crt_new;


	tablet_crt_new = FALSE;
	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tabinfo_push(&tabinfo);
		

	keycol = row_locate_col(rp, -1, minlen, &keycolen);
	
	
	TABINFO_INIT(&tabinfo, tablet_name, NULL, 0, &sinfo, minlen, 
				TAB_SCHM_INS, tabid, sstabid);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEY_COLID_INROW, VARCHAR, -1);

	tabinfo.t_split_tabletid = tablehdr->tab_tablet;
	
	if (!blkins(&tabinfo, rp))
	{
		traceprint("TABLET_INSROW: hit err.\n");
		ex_raise(EX_ANY);
	}

	session_close(&tabinfo);

	if (tabinfo.t_stat & TAB_TABLET_CRT_NEW)
	{
		RANGE_PROF *rg_prof;
		rg_prof = rebalan_get_rg_prof_by_addr(tss->tcur_rgprof->rg_addr,
						tss->tcur_rgprof->rg_port);
		(rg_prof->rg_tablet_num)++;
		tablet_crt_new = TRUE;
		
		(tablehdr->tab_tablet)++;
	}

	tabinfo_pop();

	return tablet_crt_new;
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
	
	
	TABINFO_INIT(&tabinfo, tablet_name, NULL, 0, &sinfo, minlen, 
			TAB_SRCH_DATA, tabid, sstabid);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEY_COLID_INROW, 
		       VARCHAR, -1);
	
	blkdel(&tabinfo);

	session_close(&tabinfo);

	tabinfo_pop();
}


int
tablet_upd_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name,
			char *oldrp, char *newrp, int minlen)
{
	LOCALTSS(tss);
	int		keycolen;
	char		*keycol;
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		tablet_crt_new;


	tablet_crt_new = FALSE;
	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tabinfo_push(&tabinfo);
		

	keycol = row_locate_col(oldrp, -1, minlen, &keycolen);
	
	
	TABINFO_INIT(&tabinfo, tablet_name, NULL, 0, &sinfo, minlen,
			TAB_SCHM_UPDATE, tabid, sstabid);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEY_COLID_INROW, 
		       VARCHAR, -1);
	
	if (!blkupdate(&tabinfo, newrp))
	{
		ex_raise(EX_ANY);
	}

	if (tabinfo.t_stat & TAB_TABLET_CRT_NEW)
	{
		RANGE_PROF *rg_prof;
		rg_prof = rebalan_get_rg_prof_by_addr(tss->tcur_rgprof->rg_addr,
						tss->tcur_rgprof->rg_port);
		(rg_prof->rg_tablet_num)++;
		tablet_crt_new = TRUE;
		
		(tablehdr->tab_tablet)++;
	}

	session_close(&tabinfo);

	tabinfo_pop();

	return tablet_crt_new;
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

		Assert(min_rlen == ROW_MINLEN_IN_TABLET);
	}
	
	Assert(sstab_idx == sstab_rlen);

	return min_rlen;
}



char *
tablet_srch_row(TABINFO *usertabinfo, int tabid, int sstabid, 
			char *systab, char *key, int keylen)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	BUF		*bp;
	int		rnum;
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

	TABINFO_INIT(tabinfo, systab, NULL, 0, tabinfo->t_sinfo, minrowlen, TAB_SCHM_SRCH, 
		     tabid, sstabid);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_KEY_COLID_INROW, 
		       VARCHAR, -1);			
	
	bp = blkget(tabinfo);
//	offset = blksrch(tabinfo, bp);

	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);
	rnum = tabinfo->t_rowinfo->rnum;

	Assert(rnum != -1);

	bufunkeep(bp->bsstab);
	session_close(tabinfo);

	if ((usertabinfo)&& (tabinfo->t_stat & TAB_TABLET_KEYROW_CHG))
	{
		usertabinfo->t_stat |= TAB_TABLET_KEYROW_CHG;
	}
	
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

	return (ROW_GETPTR_FROM_OFFTAB(bp->bblk, rnum));
}



char *
tablet_get_1st_or_last_row(int tabid, int sstabid, char *systab, int firstrow, int is_tablet)
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

	minrowlen = is_tablet ? ROW_MINLEN_IN_TABLET : ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, systab, NULL, 0, tabinfo->t_sinfo, minrowlen, 
			TAB_SCHM_SRCH, tabid, sstabid);
	
	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, 
		is_tablet ? TABLET_KEY_COLID_INROW : TABLETSCHM_KEY_COLID_INROW, 
		VARCHAR, -1);			
	
	bp = blk_getsstable(tabinfo);
	
	if (firstrow)
	{
		offset = BLKHEADERSIZE;
	}
	else
	{
		BUF	*lastbp;
		while(bp->bblk->bnextblkno != -1)
		{	
			lastbp = bp;
			bp++;

			if (bp->bblk->bfreeoff == BLKHEADERSIZE)
			{
				bp = lastbp;
				break;
			}
		}

		int *offtab = ROW_OFFSET_PTR(bp->bblk);
		
		offset = offtab[-(bp->bblk->bnextrno - 1)];		
	}

	bufunkeep(bp->bsstab);
	session_close(tabinfo);

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


int
tablet_schm_ins_row(int tabid, int sstabid, char *systab, char *row, 
			int tabletnum, int flag)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	char		*key;
	int		keylen;
	BLK_ROWINFO	blk_rowinfo;
	int		rtn_stat;


	rtn_stat = TRUE;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	
	key = row_locate_col(row, TABLETSCHM_KEY_FAKE_COLOFF_INROW, minrowlen,
				&keylen);

	flag |= tabletnum ? TAB_SCHM_INS : TAB_CRT_NEW_FILE;
	
	TABINFO_INIT(tabinfo, systab, NULL, 0, tabinfo->t_sinfo, minrowlen, flag,
			tabid, sstabid);
	
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLETSCHM_KEY_COLID_INROW, 
		       VARCHAR, -1);
			
	rtn_stat = blkins(tabinfo, row);

	
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

	return rtn_stat;
}




int
tablet_schm_del_row(int tabid, int sstabid, char *systab, char *key, int keylen)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	BLK_ROWINFO	blk_rowinfo;
	int		rtn_stat;
	
	
	rtn_stat = TRUE;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));
	
	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, systab, NULL, 0, tabinfo->t_sinfo, minrowlen, TAB_SRCH_DATA,
		     tabid, sstabid);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLETSCHM_KEY_COLID_INROW,
		       VARCHAR, -1);
			
	rtn_stat = blkdel(tabinfo);
	
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

	return rtn_stat;
}


char *
tablet_schm_srch_row(int tabid, int sstabid, char *systab, char *key, int keylen)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	BUF		*bp;
	int		rnum;
	BLK_ROWINFO	blk_rowinfo;
	char		*rp;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, systab,NULL, 0, tabinfo->t_sinfo, minrowlen,
		     TAB_SCHM_SRCH, tabid, sstabid);

	
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, 
		       TABLETSCHM_KEY_COLID_INROW, VARCHAR, -1);
			
	
	bp = blkget(tabinfo);
//	offset = blksrch(tabinfo, bp);
	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);
	rnum = tabinfo->t_rowinfo->rnum;

	
	if (   (bp->bsstab->bblk->bstat & BLK_INDEX_ROOT) 
	    && (bp->bsstab->bblk->bstat & BLK_CRT_EMPTY))
	{
		rp = NULL;
		
		Assert(rnum == -1);
	}
	else
	{
		Assert(rnum != -1);

		rp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, rnum);
	}
	
	bufunkeep(bp->bsstab);
	session_close(tabinfo);
	
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
	
	tabinfo_pop();

	return rp;
}

char *
tablet_schm_get_row(int tabid, int sstabid, char *systab, int rowno)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	BUF		*bp;
	int		offset;
	BLK_ROWINFO	blk_rowinfo;
	int		found;
	int 		*offtab;
	
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, systab, NULL, 0, tabinfo->t_sinfo, minrowlen, 
		     TAB_SCHM_SRCH, tabid, sstabid);

	
	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, 
		       TABLETSCHM_KEY_COLID_INROW, VARCHAR, -1);
			
	
	bp = blk_getsstable(tabinfo);
	
	found = FALSE;

	if (bp->bblk->bnextrno == 0)
	{
		goto finish;
	}

	if (rowno == -1)
	{
		found = TRUE;
		
		
		BUF	*lastbp;
		while(bp->bblk->bnextblkno != -1)
		{	
			lastbp = bp;
			bp++;

			if (bp->bblk->bfreeoff == BLKHEADERSIZE)
			{
				bp = lastbp;
				break;
			}
		}		

		offtab = ROW_OFFSET_PTR(bp->bblk);
		
		offset = offtab[-(bp->bblk->bnextrno - 1)];		
	}
	else if (rowno == 0)
	{
		offset = BLKHEADERSIZE;
		found = TRUE;
		
	}
	else
	{
		offset = BLKHEADERSIZE;

		
		while((rowno > 0) || (rowno == 0))
		{	
			if (bp->bblk->bfreeoff == BLKHEADERSIZE)
			{
				found = FALSE;
				break;
			}	
			
			if (rowno < bp->bblk->bnextrno)
			{
				offtab = ROW_OFFSET_PTR(bp->bblk);
				offset = offtab[-(rowno)];
				found = TRUE;
				break;
			}

			rowno -= bp->bblk->bnextrno;

			if (bp->bblk->bnextblkno != -1)
			{
				bp++;				
			}
		}	

		
	}

finish:	
	bufunkeep(bp->bsstab);
	session_close(tabinfo);
	
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
	
	tabinfo_pop();

	return found ? ((char *)(bp->bblk) + offset) : NULL;
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


int
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
	int		tablet_nameidx;
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	BLK_ROWINFO	blk_rowinfo;


	volatile struct
	{
		BUF	*destbp;
		TABINFO	*tabinfo;
		char	*temprp;
	} copy;

	copy.destbp = NULL;
	copy.tabinfo = NULL;
	copy.temprp = NULL;
	
	if(ex_handle(EX_TABLETERR, yxue_handler))
	{
		if (copy.destbp)
		{
			bufdestroy(copy.destbp->bsstab);
		}

		if (copy.tabinfo)
		{
			if (copy.tabinfo->t_sinfo)
			{
				MEMFREEHEAP(copy.tabinfo->t_sinfo);
			}

			MEMFREEHEAP(copy.tabinfo);
		}

		if (copy.temprp)
		{
			MEMFREEHEAP(copy.temprp);
		}

		ex_delete();

		return FALSE;
	}
	
	destbuf = NULL;
	ins_nxtsstab = (srcbp->bblk->bblkno > ((BLK_CNT_IN_SSTABLE / 2) - 1)) 
				? TRUE : FALSE;

	nextblk = srcbp->bsstab->bblk;
	
	while (nextblk->bnextblkno < (BLK_CNT_IN_SSTABLE / 2 + 1))
	{		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
	}

	
	srctabinfo->t_stat |= TAB_TABLET_SPLIT;

	if ((destbuf = bufsearch(srctabinfo)) == NULL)
	{
		destbuf = bufgrab(srctabinfo);
		bufhash(destbuf);
	}

	copy.destbp = destbuf;
	
	blk = destbuf->bblk;
		
	blk_init(blk);

	bufpredirty(srcbp->bsstab);
	bufpredirty(destbuf->bsstab);
	
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

	bufdirty(srcbp->bsstab);
	bufdirty(destbuf->bsstab);

	srctabinfo->t_stat &= ~TAB_TABLET_SPLIT;

	tablet_nameidx = str01str(srctabinfo->t_sstab_name, "tablet", 
				 STRLEN(srctabinfo->t_sstab_name));
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));

	copy.tabinfo = tabinfo;
	
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo_push(tabinfo);	

	
	TABINFO_INIT(tabinfo, destbuf->bsstab_name, NULL, 0, tabinfo->t_sinfo,
		     ROW_MINLEN_IN_TABLET, TAB_KEPT_BUF_VALID,
		     srctabinfo->t_tabid, srctabinfo->t_split_tabletid);

	if (ins_nxtsstab)
	{
		tabinfo->t_keptbuf = destbuf;	

		key = row_locate_col(rp, -1, ROW_MINLEN_IN_TABLET, &keylen);
		
		SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, 
				TABLET_KEY_COLID_INROW, VARCHAR, -1);
		
		if (!blkins(tabinfo, rp))
		{
			ex_raise(EX_TABLETERR);
		}
	}
	else
	{
		bufpredirty(destbuf->bsstab);
		bufdirty(destbuf->bsstab);
	}

	tablet_key = row_locate_col(destbuf->bblk->bdata, -1, 
				destbuf->bblk->bminlen, &tablet_keylen);
	
	
	int rlen = ROW_MINLEN_IN_TABLETSCHM + tablet_keylen + sizeof(int) 
			+ sizeof(int);
	
	char *temprp = (char *)MEMALLOCHEAP(rlen);

	copy.temprp = temprp;

	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, srctabinfo->t_sstab_name, tablet_nameidx);
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	tablet_nameidx = str01str(destbuf->bsstab_name, "tablet", 
					STRLEN(destbuf->bsstab_name));
	tablet_schm_bld_row(temprp, rlen, srctabinfo->t_split_tabletid,
			    destbuf->bsstab_name + tablet_nameidx + 1, 
			    tss->tcur_rgprof->rg_addr, tablet_key, 
			    tablet_keylen, tss->tcur_rgprof->rg_port);

	
	if (!tablet_schm_ins_row(srctabinfo->t_tabid, TABLETSCHM_ID, tab_meta_dir, 
				temprp, INVALID_TABLETID, 0))
	{
		ex_raise(EX_TABLETERR);
	}


	
	char	cmd_str[TABLE_NAME_MAX_LEN];

	int	status = TRUE;
	MEMSET(cmd_str, TABLE_NAME_MAX_LEN);

	
	int i = strmnstr(srctabinfo->t_sstab_name, "/", STRLEN(srctabinfo->t_sstab_name));

	
	int j = strmnstr(srctabinfo->t_sstab_name, "/", i - 1);
				
#ifdef MT_KFS_BACKEND
	
	sprintf(cmd_str, "%s/%s", tss->metabackup, srctabinfo->t_sstab_name + j);

	if (COPYFILE(srctabinfo->t_sstab_name, cmd_str) != 0)
#else			
	sprintf(cmd_str, "cp %s %s/%s", srctabinfo->t_sstab_name, tss->metabackup,
				srctabinfo->t_sstab_name + j);
	
	if (system(cmd_str))
#endif
	{
		traceprint("TABLET_SPLIT: File copying hit error!\n");
		status = FALSE;		
	}

	if (!status)
	{
		traceprint("Failed to copy file for the tablet split.\n");
	}

	if (destbuf)
	{
		bufunkeep(destbuf->bsstab);
	}
	
	
	session_close(tabinfo);

	srctabinfo->t_stat |= TAB_TABLET_CRT_NEW;
	
	MEMFREEHEAP(temprp);

	copy.temprp = NULL;

	if (tabinfo)
	{
		if (tabinfo->t_sinfo)
		{
			MEMFREEHEAP(tabinfo->t_sinfo);
		}

		MEMFREEHEAP(tabinfo);
	}

	tabinfo_pop();

	ex_delete();
	
	return TRUE;
	
}


int
tablet_sharding(TABLEHDR *tablehdr, char *rg_addr, int rg_port,
			char *tabdir, int tabid, char *tabletname, int tabletid)
{
	BUF		*destbuf;
	TABINFO		*srctabinfo;
	BLOCK		*nextblk;
	BLOCK		*blk;
	char		*tablet_key;
	int		tablet_keylen;
	int		table_nameidx;
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	BLK_ROWINFO	blk_rowinfo;
	char		tab_tabletschm_dir[TABLE_NAME_MAX_LEN];
	int		minrowlen;
	BUF		*bp;
	int		i;
	int		rtn_stat;


	volatile struct
	{
		BUF	*bp;
		BUF	*destbp;
		char	*temprp;
		TABINFO	*tabinfo;
	} copy;

	copy.bp = NULL;
	copy.destbp = NULL;
	copy.temprp = NULL;
	copy.tabinfo = NULL;
	
	if(ex_handle(EX_ANY, yxue_handler))
	{
		if (copy.bp)
		{
			bufdestroy(copy.bp);
		}

		if (copy.destbp)
		{
			bufdestroy(copy.destbp);
		}

		if (copy.temprp)
		{
			MEMFREEHEAP(copy.temprp);
		}

		if (copy.tabinfo)
		{			
			session_close(copy.tabinfo);
		
			MEMFREEHEAP(copy.tabinfo->t_sinfo);
			MEMFREEHEAP(copy.tabinfo);
		
			tabinfo_pop();			
		}

		ex_delete();
		ex_raise(EX_ANY);
	}
	
	bp = NULL;
	destbuf = NULL;
	rtn_stat = FALSE;
	MEMSET(tab_tabletschm_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_tabletschm_dir, tabdir, STRLEN(tabdir));
	
	str1_to_str2(tab_tabletschm_dir, '/', tabletname);
	

	srctabinfo = MEMALLOCHEAP(sizeof(TABINFO));

	copy.tabinfo = srctabinfo;
	
	MEMSET(srctabinfo, sizeof(TABINFO));

	srctabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(srctabinfo->t_sinfo, sizeof(SINFO));

	srctabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(srctabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	srctabinfo->t_dold = srctabinfo->t_dnew = (BUF *) srctabinfo;

	srctabinfo->t_split_tabletid = tablehdr->tab_tablet;

	tabinfo_push(srctabinfo);

	minrowlen = ROW_MINLEN_IN_TABLET;

	TABINFO_INIT(srctabinfo, tab_tabletschm_dir, NULL, 0, 
			srctabinfo->t_sinfo, minrowlen, 
			TAB_SCHM_SRCH, tabid, tabletid);
	SRCH_INFO_INIT(srctabinfo->t_sinfo, NULL, 0, TABLET_KEY_COLID_INROW, 
		       VARCHAR, -1);

	bp = blk_getsstable(srctabinfo);

	copy.bp = bp;

	blk = bp->bsstab->bblk;

	sstab_shuffle(blk);
		
	
	for(i = 0; i < BLK_CNT_IN_SSTABLE; i++)
	{
		if (blk->bnextrno == 0)
		{
			break;
		}

		blk++;
	}

	blk = bp->bsstab->bblk;
	
	if (i > 1)
	{
		nextblk = (blk + (i / 2));
		rtn_stat = TRUE;
	}
	else if (i == 1)
	{
		
		int		*thisofftab;
		int		*nextofftab;
		int		offset;
		int		mvsize;
		BLOCK		*tmpblock;
		int		row_cnt;
	
		tmpblock = NULL;
		
		
		BUF_GET_RESERVED(tmpblock);
		
		MEMCPY(tmpblock, blk, sizeof(BLOCK));
		if (!blk_shuffle_data(tmpblock, blk))
		{
			MEMCPY(blk, tmpblock, sizeof(BLOCK));

			BUF_RELEASE_RESERVED(tmpblock);
			
			Assert(0);
			goto finish;
		}

		nextblk = (BLOCK *) ((char *)blk + BLOCKSIZE);
		row_cnt = blk->bnextrno;
		
		Assert((blk->bnextblkno!= -1) && (nextblk->bnextrno == 0)
			&& (row_cnt > 0));

		if (nextblk->bfreeoff > BLKHEADERSIZE)
		{
			blk_compact(nextblk);
		}
		

		i = blk->bnextrno / 2;

		thisofftab = ROW_OFFSET_PTR(blk);

		offset = thisofftab[-i];
		mvsize = blk->bfreeoff - offset;

		MEMCPY(nextblk->bdata, (char *)blk + offset, mvsize);

		blk->bnextrno = i;
		blk->bfreeoff = offset;

		MEMSET((char *)blk + offset, mvsize);
		
		nextofftab = ROW_OFFSET_PTR(nextblk);

		int j;

		for (j = 0; i < row_cnt; i++,j++)
		{
			nextofftab[-j] = thisofftab[-i] - offset + BLKHEADERSIZE;	
		}

		nextblk->bnextrno = j;
		nextblk->bfreeoff = mvsize + BLKHEADERSIZE;

		nextblk->bminlen = blk->bminlen;

		BUF_RELEASE_RESERVED(tmpblock);

		rtn_stat = TRUE;
	}
	else
	{
		goto finish;
	}
	

	
	srctabinfo->t_stat |= TAB_TABLET_SPLIT;

	if ((destbuf = bufsearch(srctabinfo)) == NULL)
	{
		destbuf = bufgrab(srctabinfo);
		bufhash(destbuf);
	}

	copy.destbp = destbuf;
	
	blk = destbuf->bblk;
		
	blk_init(blk);

	while((nextblk->bblkno != -1) && (nextblk->bnextrno > 0))
	{
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

	
	bufpredirty(destbuf->bsstab);
	bufdirty(destbuf->bsstab);

	tablet_key = row_locate_col(destbuf->bblk->bdata, -1, destbuf->bblk->bminlen,
				    &tablet_keylen);
	
	int rlen = ROW_MINLEN_IN_TABLETSCHM + tablet_keylen + sizeof(int) + sizeof(int);
	char *temprp = (char *)MEMALLOCHEAP(rlen);

	copy.temprp = temprp;

	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, srctabinfo->t_sstab_name, table_nameidx);
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	table_nameidx = str01str(destbuf->bsstab_name, "tablet", 
					STRLEN(destbuf->bsstab_name));
	tablet_schm_bld_row(temprp, rlen, srctabinfo->t_split_tabletid,
			    destbuf->bsstab_name + table_nameidx + 1, 
			    rg_addr, tablet_key, tablet_keylen, 
			    rg_port);

	
	if (!tablet_schm_ins_row(srctabinfo->t_tabid, TABLETSCHM_ID, tab_meta_dir, temprp, 
			    INVALID_TABLETID, 0))
	{
		ex_raise(EX_ANY);
	}

	
	(tablehdr->tab_tablet)++;

	
	meta_save_sysobj(tabdir,(char *)tablehdr);
	
	MEMFREEHEAP(temprp);

	
	bufpredirty(bp->bsstab);
	bufdirty(bp->bsstab);


finish:	
	if (bp)
	{
		bufunkeep(bp->bsstab);
	}

	if (destbuf)
	{
		bufunkeep(destbuf->bsstab);
	}
	
	session_close(srctabinfo);

	MEMFREEHEAP(srctabinfo->t_sinfo);
	MEMFREEHEAP(srctabinfo);

	tabinfo_pop();

	ex_delete();

	return rtn_stat;
	
}


int
tablet_schm_get_totrow(int tabid, int sstabid, char *systab, char *key, int keylen)
{
	TABINFO		*tabinfo;
	int		minrowlen;
	BUF		*bp;
	BLK_ROWINFO	blk_rowinfo;
	int		totrow;


	totrow = 0;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, systab,NULL, 0, tabinfo->t_sinfo, minrowlen,
		     TAB_SCHM_SRCH, tabid, sstabid);

	
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, 
		       TABLETSCHM_KEY_COLID_INROW, VARCHAR, -1);
			
	
	bp = blk_getsstable(tabinfo);

	totrow = blk_get_totrow_sstab(bp->bsstab);
	
	bufunkeep(bp->bsstab);
	session_close(tabinfo);
	
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
	
	tabinfo_pop();

	return totrow;
}


