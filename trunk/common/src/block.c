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
#include "memcom.h"
#include "rpcfmt.h"
#include "parser.h"
#include "ranger/rangeserver.h"
#include "buffer.h"
#include "block.h"
#include "cache.h"
#include "row.h"
#include "strings.h"
#include "utils.h"
#include "tss.h"
#include "sstab.h"
#include "metadata.h"
#include "tablet.h"
#include "b_search.h"
#include "timestamp.h"
#include "log.h"
#include "hkgc.h"
#include "type.h"
#include "rginfo.h"
#include "index.h"


extern TSS	*Tss;
extern RANGEINFO *Range_infor;
extern KERNEL	*Kernel;


static int
blk_get_inspos(BLOCK *blk, int rlen, int inspos);


BUF *
blkget(TABINFO *tabinfo)
{
	LOCALTSS(tss);
	BUF		*lastbp;
	BUF		*bp;
	int     	blkidx;
	int		last_blkidx;
	char		last_sstab[SSTABLE_NAME_MAX_LEN];
	int		last_sstabid;
	int		nextsstabnum;
	int		stat_chg;
	BLK_ROWINFO	blk_rinfo;


	volatile struct
	{
		BUF	*bp;
	} copy;

	copy.bp = NULL;
	
	if(ex_handle(EX_BUFERR, yxue_handler))
	{
		bp = copy.bp;

		if (bp)
		{
			bufunkeep(bp->bsstab);
		}

		ex_delete();

		return NULL;
	}
	
	stat_chg = FALSE;

nextsstab:

	bp = blk_getsstable(tabinfo);

	Assert(bp);
	
	if (bp == NULL)
	{
		traceprint("Buffer is NULL!\n");

		ex_raise(EX_BUFERR);
	}

	copy.bp = bp;

	

	
	tabinfo->t_sinfo->sistate |= SI_INDEX_BLK;
	blkidx = blksrch(tabinfo, bp);

	Assert(blkidx < BLK_CNT_IN_SSTABLE);

	if (blkidx >= BLK_CNT_IN_SSTABLE)
	{
		traceprint("Block index is error!\n");

		ex_raise(EX_BUFERR);
	}
	
	tabinfo->t_sinfo->sistate &= ~SI_INDEX_BLK;

	
	if (   !(tss->topid & TSS_OP_INDEX_CASE)
	    && (tss->topid & TSS_OP_RANGESERVER) 
	    && (   (tss->topid & TSS_OP_INSTAB) 
	        || (tss->topid & TSS_OP_SELDELTAB)))
	{
		BUF	*tmpbp;
		int	tmpblkidx;

	
		tmpblkidx = blkidx;		
		tmpbp = bp->bsstab + tmpblkidx;
		

		
		if (   (tmpbp->bsstab->bblk->bstat & BLK_SSTAB_SPLIT)
		    && !stat_chg)
		{
			
			if (tmpbp->bsstab->bblk->bsstab_split_ts_lo 
						== tabinfo->t_insmeta->ts_low)
			{
				tmpbp->bsstab->bblk->bstat &= ~BLK_SSTAB_SPLIT;
				goto finish;
			}

			
			Assert(   (tmpbp->bsstab->bblk->bsstab_split_ts_lo 
						> tabinfo->t_insmeta->ts_low)
			       || (tmpbp->bsstab->bblk->bsstab_split_ts_lo 
						== tabinfo->t_insmeta->ts_low));
			
			if (!(   (tmpbp->bsstab->bblk->bsstab_split_ts_lo 
					> tabinfo->t_insmeta->ts_low)
			      || (tmpbp->bsstab->bblk->bsstab_split_ts_lo 
					== tabinfo->t_insmeta->ts_low)))
			{
				traceprint("splitting timestamp on sstab hit error!\n");

				ex_raise(EX_BUFERR);
			}
			
			if (tss->topid & TSS_OP_INSTAB) 
			{
				tabinfo->t_stat |= TAB_INS_SPLITING_SSTAB;
			}
		}

		
		if (   !(tmpbp->bsstab->bblk->bstat & BLK_SSTAB_SPLIT)
		    && !stat_chg)
		{
			if(   !(tabinfo->t_stat & TAB_DO_SPLIT) 
			   && (tmpbp->bsstab->bblk->bsstab_split_ts_lo 
			   			< tabinfo->t_insmeta->ts_low))
			{
				
								
				tabinfo->t_stat |= TAB_RETRY_LOOKUP;
			}

			
			goto finish;
		}

		
		if (   (   (tss->topid & TSS_OP_SELDELTAB) 
			&& (tabinfo->t_sinfo->sistate & SI_NODATA))
		    || (   (tss->topid & TSS_OP_INSTAB)
		    	&& (tabinfo->t_rowinfo->rnum == tmpbp->bblk->bnextrno)))
		{
			
			if (tmpblkidx < (BLK_CNT_IN_SSTABLE - 1))
			{
				tmpblkidx++;
				tmpbp = bp->bsstab + tmpblkidx;

				if (tmpbp->bblk->bnextrno > 0)
				{
					goto finish;
				}
				else
				{
					Assert(tmpbp->bblk->bnextrno == 0);

					traceprint("Row no in the block hit error!\n");

					ex_raise(EX_BUFERR);
				}
			}

			nextsstabnum = tmpbp->bsstab->bblk->bnextsstabnum;

			if (nextsstabnum == -1)
			{
				goto finish;;
			}

			
			MEMSET(last_sstab, SSTABLE_NAME_MAX_LEN);
			MEMCPY(last_sstab, tabinfo->t_sstab_name, 
						STRLEN(tabinfo->t_sstab_name));
			last_sstabid = tabinfo->t_sstab_id;
			last_blkidx = blkidx;
			lastbp = bp;
			MEMCPY(&blk_rinfo, tabinfo->t_rowinfo,
						sizeof(BLK_ROWINFO));
			
			
//			blk_getnextsstab(tabinfo, tmpbp->bsstab->bblk);

			MEMSET(tabinfo->t_sstab_name, SSTABLE_NAME_MAX_LEN);			
			
			sstab_namebyid(last_sstab, tabinfo->t_sstab_name, 
								nextsstabnum);

			tabinfo->t_sstab_id = nextsstabnum;			

			stat_chg = TRUE;

			goto nextsstab;			
		}

		
		if (stat_chg && (tabinfo->t_rowinfo->rnum == 0))
		{
			tabinfo->t_sstab_id = last_sstabid;

			MEMSET(tabinfo->t_sstab_name, SSTABLE_NAME_MAX_LEN);
			MEMCPY(tabinfo->t_sstab_name, last_sstab, 
							STRLEN(last_sstab));
			bp = lastbp;
			blkidx = last_blkidx;

			MEMCPY(tabinfo->t_rowinfo, &blk_rinfo, 
							sizeof(BLK_ROWINFO));
		}

		
	}

finish:

	ex_delete();
	
	return (bp->bsstab + blkidx);
}



int
blksrch(TABINFO *tabinfo, BUF *bp)
{
	LOCALTSS(tss);
	BLOCK		*blk;
	int		rowno;
	int		minrowlen;
	int     	blkidx;
	int     	result;
	B_SRCHINFO	*srchinfo;
	B_SRCHINFO	m_srchinfo;
	int		low;
	int		high;
	int		total;
	int		blkidx_get;


	blkidx = -1;
	rowno = BLOCK_EMPTY_ROWID;
	blkidx_get = (tabinfo->t_sinfo->sistate & SI_INDEX_BLK) ? TRUE : FALSE;
	
	
	if (   (!blkidx_get || (tss->topid & TSS_OP_IDXROOT_SPLIT))
	    && (BLK_GET_NEXT_ROWNO(bp->bblk) == 0))
	{
		blkidx = bp->bblk->bblkno;
		tabinfo->t_sinfo->sistate |= SI_NODATA;
		goto finish;
	}

	if (blkidx_get)
	{
		bp = bp->bsstab;
		blkidx = blk_get_location_sstab(tabinfo, bp);

		if (blkidx == -1)
		{
			
			blkidx = bp->bblk->bblkno;
			tabinfo->t_sinfo->sistate |= SI_NODATA;
			goto finish;
		}
		
		bp += blkidx;
	}

	blk		= bp->bblk;
	minrowlen 	= blk->bminlen;
	blkidx		= blk->bblkno;

	Assert(blkidx != -1);

	if (blkidx == -1)
	{
		traceprint("The block num hit error.\n");
		
		ex_raise(EX_BUFERR);
	}

	low = 0;
	high = BLK_GET_NEXT_ROWNO(bp->bblk) - 1;
	total = BLK_GET_NEXT_ROWNO(bp->bblk);
	result = LE;	

	MEMSET(&m_srchinfo, sizeof(B_SRCHINFO));
	srchinfo = &m_srchinfo;

	SRCHINFO_INIT(srchinfo, 0, high, total, result);

	Assert (!(blk->bstat & BLK_RENT_DATA));

	if (blk->bstat & BLK_RENT_DATA)
	{
		traceprint("The block rented should not be searched.\n");
		
		ex_raise(EX_BUFERR);
	}

	
	b_srch_block(tabinfo, bp, srchinfo);

	result = srchinfo->bcomp;
	rowno = srchinfo->brownum;

	
	if (!(tabinfo->t_stat & TAB_SCHM_SRCH))
	{	
		if ((result == EQ))
		{
			tabinfo->t_sinfo->sistate &= ~SI_NODATA;
		}
		else
		{
			tabinfo->t_sinfo->sistate |= SI_NODATA;
		}
	}

	
#if 0
	if (rowno == BLK_GET_NEXT_ROWNO(bp->bblk))
	{
		if (!(tabinfo->t_stat & TAB_SCHM_SRCH))
		{
			rowno++;
		}

	}

#endif

finish:

	
	tabinfo->t_rowinfo->rblknum = blkidx;
	tabinfo->t_rowinfo->rnum = rowno;
	tabinfo->t_rowinfo->rsstabid = bp->bsstab->bsstabid;
	
 	return (blkidx_get) ? blkidx : rowno;
}

void
blk_init(BLOCK *blk)
{
	while(blk->bnextblkno != -1)
	{
		blk->bfreeoff = BLKHEADERSIZE;
		blk->bnextrno = 0;
		blk->bstat = 0;

		
		MEMSET(blk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);
		
		blk = (BLOCK *) ((char *)blk + BLOCKSIZE);
		
	}

	blk->bfreeoff = BLKHEADERSIZE;
	blk->bnextrno = 0;
	blk->bstat = 0;
}



BUF *
blk_getsstable(TABINFO *tabinfo)
{
	LOCALTSS(tss);
	char	*sstab_name;
	BUF	*bp;

	if (DEBUG_TEST(tss))
	{
		traceprint("tabinfo->t_sstab_name = %s \n", tabinfo->t_sstab_name);
	}
	
	if (tabinfo->t_stat & TAB_KEPT_BUF_VALID)
	{
		
		bufkeep(tabinfo->t_keptbuf);
		
		Assert(tabinfo->t_keptbuf);

		
		return tabinfo->t_keptbuf;
	}

	if ((bp = bufsearch(tabinfo)) == NULL)
	{
		bp = bufgrab(tabinfo);

		if (!(tabinfo->t_stat & TAB_RESERV_BUF))
		{
			bufhash(bp);
		}

		
		blk_init(bp->bblk);
	
		sstab_name = tabinfo->t_sstab_name;
		
		MEMSET(bp->bsstab_name, 256);
		MEMCPY(bp->bsstab_name, sstab_name, STRLEN(sstab_name));	
	
		bp->bstat &= ~BUF_READ_EMPTY;

		
		if (   (   (tss->topid & TSS_OP_RANGESERVER) 
			&& (   (tabinfo->t_insmeta) 
			    && (tabinfo->t_insmeta->status & INS_META_1ST)))
		    || (   (   (tss->topid & TSS_OP_METASERVER) 
		    	    || (tss->topid & TSS_OP_CRTINDEX)
		    	    || (tss->topid & TSS_OP_IDXROOT_SPLIT))
		        && (tabinfo->t_stat & TAB_CRT_NEW_FILE)))
		{
			bp->bstat |= BUF_READ_EMPTY;
		}
		
		bufread(bp);

	}

	if (tabinfo->t_stat & TAB_RESERV_BUF)
	{
		bufunhash(bp);

		bp->bstat |= BUF_RESERVED;
		tabinfo->t_resbuf = bp;
	}
	
	
	if (   (tss->topid & TSS_OP_RANGESERVER) 
	    && (   (tabinfo->t_insmeta)
	        && (tabinfo->t_insmeta->status & INS_META_1ST)))
	{
		bp->bsstab->bblk->bsstabnum = tabinfo->t_insmeta->sstab_id;

		
		bp->bsstab->bblk->bnextsstabnum = -1;
		bp->bsstab->bblk->bprevsstabnum = -1;
	}
	
	if (SSTABLE_STATE(bp) & BUF_IOERR)
	{
		//return (bp + BLK_CNT_IN_SSTABLE - 1);
		return NULL;
	}

	return bp;
}


int
blkins(TABINFO *tabinfo, char *rp)
{
	LOCALTSS(tss);
	BUF	*bp;
	int	rnum;
	int	minlen;
	int	ign;
	int	rlen;
	int	i;
	int	*offtab;
	int	blk_stat;
	int	upd_in_place;
	RID	*newridp;
	int	hit_ridlist;
	int	hit_ridlist_scope;


	volatile struct
	{
		BUF	*bp;
	} copy;

	copy.bp = NULL;
	
	if(ex_handle(EX_BUFERR, yxue_handler))
	{
		bp = copy.bp;

		if (bp)
		{
			bufdestroy(bp->bsstab);
		}

		ex_delete();

		return FALSE;
	}

	minlen = tabinfo->t_row_minlen;
	blk_stat = 0;
	
	tabinfo->t_sinfo->sistate |= SI_INS_DATA;
	
	bp = blkget(tabinfo);

	copy.bp = bp;

	
	if (   (tabinfo->t_stat & TAB_RETRY_LOOKUP) 
	    || (   !(tabinfo->t_stat & TAB_INS_INDEX) 
	    	&& !(tabinfo->t_sinfo->sistate & SI_NODATA)))
	{
		bufunkeep(bp->bsstab);
		ex_delete();
		return FALSE;
	}

//	offset = blksrch(tabinfo, bp);

	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);

	if (   (tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
	    || (tabinfo->t_rowinfo->rsstabid != bp->bsstab->bsstabid))
	{
		traceprint("BLOCKINS: The searching result in block is not match with the one in memory.");

		ex_raise(EX_BUFERR);
	}
	
	rnum = tabinfo->t_rowinfo->rnum;

	ign = 0;
	rlen = ROW_GET_LENGTH(rp, minlen);

	tabinfo->t_cur_rowp = rp;
	tabinfo->t_cur_rowlen = rlen;

	hit_ridlist = (   (tabinfo->t_stat & TAB_INS_INDEX) 
			&& (!(tabinfo->t_sinfo->sistate & SI_NODATA))) 
		      ? TRUE : FALSE; 

	if (rnum != -1)
	{
		
		blk_stat = blk_check_sstab_space(tabinfo, bp, rp, rlen,
						rnum, TRUE);
	}

	if (blk_stat & BLK_INS_SPLITTING_SSTAB)
	{
		bufdestroy(bp);
		ex_delete();
		return FALSE;
	}
	
	if (blk_stat & BLK_ROW_NEXT_SSTAB)
	{
		
		goto finish;
	}

	bufpredirty(bp->bsstab);
	
	
	if (blk_stat & BLK_ROW_NEXT_BLK)
	{
		bp++;
	}

	
	if ((blk_stat & BLK_BUF_NEED_CHANGE))
	{
		rnum = blksrch(tabinfo, bp);
	}

	
	hit_ridlist_scope = (hit_ridlist && (rnum == -1)) ? TRUE : FALSE;

	offtab = ROW_OFFSET_PTR(bp->bblk);

	
	upd_in_place = (   (tabinfo->t_stat & TAB_INS_INDEX) 
			&& (!(tabinfo->t_sinfo->sistate & SI_NODATA))) 
		      ? TRUE : FALSE; 

	
	if ((upd_in_place) && !(bp->bsstab->bblk->bstat & BLK_INDEX_ROOT))
	{
		
		newridp = (RID *)row_locate_col(rp, 
				IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
				ROW_MINLEN_IN_INDEXBLK, &ign);
		
		index_addrid(bp->bblk, rnum, newridp,
			IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
			INDEXBLK_RIDNUM_COLOFF_INROW,
			ROW_MINLEN_IN_INDEXBLK);
		
		
		i = rnum;

		tabinfo->t_stat |= TAB_RID_UPDATE;
	}
	else
	{
		i = 0;
		
		if (rnum != -1)
		{
			
			for (i = bp->bblk->bnextrno; i > rnum; i--)
			{
				offtab[-i] = offtab[-(i-1)];
			}
		}

		if (hit_ridlist_scope)
		{
			ROW_SET_STATUS(rp, ROW_OVERFLOW);

			Assert(bp->bblk->bblkno > 0);
			
			if (bp->bblk->bblkno <= 0)
			{
				traceprint("Block num hit error!\n");
				ex_raise(EX_BUFERR);
			}


			BUF	*tmpbp;

			tmpbp = bp;

			do
			{			
				tmpbp--;
				
				if (tmpbp->bblk->bnextrno > 0)
				{
					
					ROW_SET_STATUS(
					   (ROW_GETPTR_FROM_OFFTAB(tmpbp->bblk,
						tmpbp->bblk->bnextrno - 1)),
						ROW_HAS_OVERFLOW);
					break;
				}

			}while(tmpbp->bblk->bblkno > 0);

		
			
		}
		
		//ROW_SET_ROWNO(rp, rnum);
		
		
		PUT_TO_BUFFER((char *)bp->bblk + bp->bblk->bfreeoff, ign, rp,
				rlen);
			
		
		offtab[-i] = bp->bblk->bfreeoff;
			
		bp->bblk->bfreeoff += rlen;
		bp->bblk->bminlen = minlen;

		BLK_GET_NEXT_ROWNO(bp->bblk)++;
	}	

	if (tss->topid & TSS_OP_RANGESERVER)
	{
		tabinfo->t_insdel_old_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
		
		bp->bsstab->bblk->bsstab_insdel_ts_lo = mtts_increment(
					bp->bsstab->bblk->bsstab_insdel_ts_lo);

		tabinfo->t_insdel_new_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
	}

		
	tabinfo->t_currid.block_id = bp->bblk->bblkno;
	tabinfo->t_currid.sstable_id = bp->bsstab->bsstabid;
	tabinfo->t_currid.roffset = offtab[-i];

	bufdirty(bp->bsstab);

	if (   (tss->topid & TSS_OP_RANGESERVER) 
	    && (!(tss->tstat & TSS_OP_RECOVERY))
	    && (!(tabinfo->t_stat & TAB_NOLOG_MODEL)))
	{
		LOGREC		logrec;
		int		logid;

		
//		logid =   (tabinfo->t_stat & TAB_LOG_SKIP_LOG)
//			? LOG_SKIP : LOG_DATA_INSERT;
		logid = (tabinfo->t_stat & TAB_INS_INDEX)
			? LOG_INDEX_INSERT: LOG_DATA_INSERT;

		log_build(&logrec, logid, tabinfo->t_insdel_old_ts_lo,
					tabinfo->t_insdel_new_ts_lo,
					tabinfo->t_sstab_name, NULL, 
					tabinfo->t_row_minlen,
					tabinfo->t_tabid,
					tabinfo->t_sstab_id,
					bp->bblk->bblkno, i, NULL, NULL);

		if (upd_in_place)
		{
			(&logrec)->loginsdel.status |= LOGINSDEL_RID_UPD;
			log_put(&logrec, (char *)newridp, sizeof(RID));
		}
		else
		{
			log_put(&logrec, rp, rlen);
		}
		
		if (0 && (tabinfo->t_stat & TAB_SSTAB_SPLIT))
		{
			hkgc_wash_sstab(TRUE);
		}
	}
		
finish:

	if (   (SSTABLE_STATE(bp) & BUF_READ_EMPTY) 
	    && (SSTABLE_STATE(bp) & BUF_DIRTY))
	{		
		DIRTYUNLINK(bp);
		bufwrite(bp);
		SSTABLE_STATE(bp) &= ~BUF_READ_EMPTY;
	}

	bufunkeep(bp->bsstab);
		
	tabinfo->t_sinfo->sistate &= ~SI_INS_DATA;

	ex_delete();

	return TRUE;
}



int
blkdel(TABINFO *tabinfo)
{
	LOCALTSS(tss);
	BUF	*bp;
	int	rnum;
	int	minlen;
	int	rlen;
	char	*rp;
	int	del_stat;


	volatile struct
	{
		BUF	*bp;
	} copy;

	copy.bp = NULL;
	
	if(ex_handle(EX_BUFERR, yxue_handler))
	{
		bp = copy.bp;

		if (bp)
		{
			bufdestroy(bp->bsstab);
		}

		ex_delete();

		return FALSE;
	}
	
	minlen = tabinfo->t_row_minlen;
	del_stat = TRUE;
	
	tabinfo->t_sinfo->sistate |= SI_DEL_DATA;
	
	bp = blkget(tabinfo);

	if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
	{
		bufunkeep(bp->bsstab);
		ex_delete();
		return FALSE;
	}

	

//	offset = blksrch(tabinfo, bp);

	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);

	if (   (tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
	    || (tabinfo->t_rowinfo->rsstabid != bp->bsstab->bsstabid))
	{
		traceprint("BLOCKDEL: The searching result in block is not match with the one in memory.");

		ex_raise(EX_BUFERR);
	}
	
	rnum = tabinfo->t_rowinfo->rnum;

	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		traceprint("We can not find the row to be deleted.\n");	
		bufunkeep(bp->bsstab);
		ex_delete();
		return FALSE;
	}

	bufpredirty(bp->bsstab);

	rp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, rnum);
	rlen = ROW_GET_LENGTH(rp, minlen);

	if (   (tss->topid & TSS_OP_RANGESERVER) 
	    && (!(tss->tstat & TSS_OP_RECOVERY))
	    && (!(tabinfo->t_stat & TAB_NOLOG_MODEL)))
	{
		tabinfo->t_cur_rowp = rp;

		tabinfo->t_cur_rowlen = rlen;

		
		tabinfo->t_insdel_old_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
		
		bp->bsstab->bblk->bsstab_insdel_ts_lo = mtts_increment(
					bp->bsstab->bblk->bsstab_insdel_ts_lo);

		tabinfo->t_insdel_new_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
		
		LOGREC		logrec;

		log_build(&logrec, LOG_DATA_DELETE, 
				tabinfo->t_insdel_old_ts_lo,
				tabinfo->t_insdel_new_ts_lo,
				tabinfo->t_sstab_name, NULL, 
				tabinfo->t_row_minlen, 
				tabinfo->t_tabid, 
				tabinfo->t_sstab_id,
				bp->bblk->bblkno, rnum, NULL, NULL);
		
		log_put(&logrec, tabinfo->t_cur_rowp,
					tabinfo->t_cur_rowlen);
	}	

	blk_delrow(tabinfo, bp->bblk, bp->bsstab->bsstabid, rp, rnum);
	
	bufdirty(bp->bsstab);
	bufunkeep(bp->bsstab);
		
	tabinfo->t_sinfo->sistate &= ~SI_DEL_DATA;

	ex_delete();

	return TRUE;
}



int
blk_delrow(TABINFO *tabinfo, BLOCK *blk, int sstabid, char *rp, int rnum)
{
	int	*offtab;
	int	i;
	
	
	ROW_SET_STATUS(rp, ROW_DELETED);
#if 0		
		if ((bp->bblk->bblkno == 0) && (offset == BLKHEADERSIZE))
		{
			
			ROW_SET_STATUS(rp, ROW_DELETED);

			del_stat = FALSE;
			
			
			
			
			goto finish;
		}
#endif


	offtab = ROW_OFFSET_PTR(blk);

		
	tabinfo->t_currid.block_id = blk->bblkno;
	tabinfo->t_currid.sstable_id = sstabid;
	tabinfo->t_currid.roffset = offtab[-rnum];

#if 0		
	if (bp->bblk->bfreeoff - offset)
	{
		MEMCPY(rp, rp + rlen, (bp->bblk->bfreeoff - offset - rlen));
	}

	offtab = ROW_OFFSET_PTR(bp->bblk);

	
	for (i = bp->bblk->bnextrno; i > 0; i--)
	{
					
		if (offtab[-(i-1)] < offset)
		{
			break;
		}
	}
#endif

	
	for(i = rnum; i < (blk->bnextrno - 1); i++)
	{
		offtab[-i] = offtab[-(i + 1)];	
	}



#if 0

	

	if (del_stat)
	{
		bp->bblk->bfreeoff -= rlen;
		BLK_GET_NEXT_ROWNO(bp->bblk)--;
	}
#endif

	BLK_GET_NEXT_ROWNO(blk)--;

	
	if (   (blk->bnextrno == 0) 
	    && !(   (tabinfo->t_stat & TAB_DEL_DATA)
		 && (tabinfo->t_has_index)))
	{
		blk_compact(blk);
	}

	return TRUE;
}


int
blkupdate(TABINFO *tabinfo, char *newrp)
{
	LOCALTSS(tss);
	BUF	*bp;
	int	rnum;
	int	minlen;
	int	oldrlen;
	int	newrlen;
	char	*oldrp;
	int	rtn_stat;
	

	rtn_stat = TRUE;
	minlen = tabinfo->t_row_minlen;
	bp = NULL;
	
	tabinfo->t_sinfo->sistate |= SI_UPD_DATA;
	
	bp = blkget(tabinfo);

	if (!bp || (tabinfo->t_stat & TAB_RETRY_LOOKUP))
	{
		rtn_stat = FALSE;
		goto cleanup;
	}

//	offset = blksrch(tabinfo, bp);

	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);

	if (   (tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
	    || (tabinfo->t_rowinfo->rsstabid != bp->bsstab->bsstabid))
	{
		traceprint("BLOCKUP: The searching result in block is not match with the one in memory.");

		rtn_stat = FALSE;
		goto cleanup;
	}
	
	rnum = tabinfo->t_rowinfo->rnum;

	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		traceprint("We can not find the row to be deleted.\n");	
		rtn_stat = FALSE;
		goto cleanup;
	}

	
	oldrp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, rnum);
	oldrlen = ROW_GET_LENGTH(oldrp, minlen);

	newrlen = ROW_GET_LENGTH(newrp, minlen);

	
	if (newrlen == oldrlen)
	{
		bufpredirty(bp->bsstab);
		
		
		MEMCPY(oldrp, newrp, newrlen);

		if (tss->topid & TSS_OP_RANGESERVER)
		{
			tabinfo->t_insdel_old_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
			
			bp->bsstab->bblk->bsstab_insdel_ts_lo = mtts_increment(
					bp->bsstab->bblk->bsstab_insdel_ts_lo);

			tabinfo->t_insdel_new_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
		}		

		bufdirty(bp->bsstab);
	}
	
	else
	{
		
		if (tabinfo->t_stat & TAB_SCHM_UPDATE)
		{
			tabinfo->t_stat |= TAB_SRCH_DATA;
		}
		
		if (!blkdel(tabinfo))
		{
			rtn_stat = FALSE;
			goto cleanup;
		}

		if (tabinfo->t_stat & TAB_SCHM_UPDATE)
		{
			tabinfo->t_stat &= ~TAB_SRCH_DATA;

			tabinfo->t_stat |= TAB_SCHM_INS;
		}

		char	*keycol;
		int	keycolen;

		keycol = row_locate_col(newrp, -1, minlen, &keycolen);

		SRCH_INFO_INIT(tabinfo->t_sinfo, keycol, keycolen,
				TABLET_KEY_COLID_INROW, VARCHAR, -1);

		
		if (!blkins(tabinfo, newrp))
		{
			rtn_stat = FALSE;
			goto cleanup;
		}

		if (tabinfo->t_stat & TAB_SCHM_UPDATE)
		{
			tabinfo->t_stat &= ~TAB_SCHM_INS;
		}
	}

cleanup:
	if(bp != NULL)
	{
		if (rtn_stat)
		{
			bufunkeep(bp->bsstab);
		}
		else
		{			
			bufdestroy(bp->bsstab);
		}
	}
	
	tabinfo->t_sinfo->sistate &= ~SI_UPD_DATA;

	return rtn_stat;
}


#if 0

void
blk_file_back_move(BLOCK *blk)
{
	int	i;
	char	*rp;
	int	offset;
	int	*offtab;
	BLOCK	*tmpblk;
	int	rlen;
	int	rminlen;


	offtab = ROW_OFFSET_PTR(blk);
	rminlen = blk->bminlen;

	
	i = blk->bnextrno;
	offset = offtab[-(i-1)];
	rp = (char *)blk + offset;
	rlen = ROW_GET_LENGTH(rp, rminlen);

	
	tmpblk = (BLOCK *) ((char *)blk + BLOCKSIZE);

	
	if (   (blk->bfreeoff - offset) 
	    && blk_check_sstab_space(NULL, NULL, rp, rlen, 0, TRUE))
	{
		BACKMOVE((char *)blk + offset, (char *)blk + offset + rlen, 
				blk->bfreeoff - offset);

		
		offtab = ROW_OFFSET_PTR(blk);
		for (i = blk->bnextrno; i > 0; i--)
		{
			if (offtab[-(i-1)] < offset)
			{
				break;
			}

			offtab[-i] = offtab[-(i-1)] + rlen;
		
		}
		offtab[-i] = offset;
	}	
}
#endif


int
blk_check_sstab_space(TABINFO *tabinfo, BUF *bp, char *rp, int rlen, 
			int ins_rnum, int data_insert_needed)
{
	LOCALTSS(tss);
	int		rtn_stat;
	int		row_cnt;
	BLOCK		*blk;
	BLOCK		*nextblk;
	LOGREC		logrec;
	int		check_ridlist;
	int		split_pos;
	
	
	blk		= bp->bblk;
	rtn_stat	= BLK_BUF_NEED_CHANGE;
	check_ridlist	= FALSE;
	split_pos 	= -1;
	

	if (ins_rnum == -1)
	{
		traceprint("The row looks expand the limit of max value.");
		Assert(0);

		ex_raise(EX_BUFERR);
	}

	if ((blk->bfreeoff + rlen) 
				< (BLOCKSIZE - BLK_TAILSIZE -
				   (ROW_OFFSET_ENTRYSIZE * (blk->bnextrno + 1))))
	{
		rtn_stat &= ~BLK_BUF_NEED_CHANGE;

		return rtn_stat;
	}

	
	if (   (tabinfo->t_stat & TAB_INS_INDEX) 
	    && (!(tabinfo->t_sinfo->sistate & SI_NODATA)))
	{
		if ((blk->bfreeoff + (sizeof(RID))) 
				< (BLOCKSIZE - BLK_TAILSIZE -
				   (ROW_OFFSET_ENTRYSIZE * (blk->bnextrno))))
		{
			rtn_stat &= ~BLK_BUF_NEED_CHANGE;

			return rtn_stat;
		}

		
		if (ins_rnum == ((blk->bnextrno) / 2))
		{
			char	*tmprp;
			
			tmprp = ROW_GETPTR_FROM_OFFTAB(blk, ins_rnum);
			
			if (ROW_WITH_OVERFLOW(tmprp))
			{			
				Assert (blk->bnextrno == (ins_rnum + 1));

				if (blk->bnextrno != (ins_rnum + 1))
				{
					traceprint("The num of row to be inserted should be equal to the nextrownum of block.\n");
					
					ex_raise(EX_BUFERR);
				}

				nextblk = (BLOCK *)((char *)blk + BLOCKSIZE);

				tmprp = ROW_GETPTR_FROM_OFFTAB(nextblk, 0);

				Assert(ROW_IS_OVERFLOW(tmprp));

				if (!(ROW_IS_OVERFLOW(tmprp)))
				{
					traceprint("The row should be an overflow row.\n");

					ex_raise(EX_BUFERR);
				}

				if ((nextblk->bfreeoff + (sizeof(RID))) 
					< (BLOCKSIZE - BLK_TAILSIZE -
					   (ROW_OFFSET_ENTRYSIZE * (nextblk->bnextrno))))
				{
					rtn_stat &= ~BLK_BUF_NEED_CHANGE;
					rtn_stat |= BLK_ROW_NEXT_BLK;
					return rtn_stat;
				}
				
			}
			
			
			ins_rnum++;
		}
	}
		
	while(1)
	{
		row_cnt = blk->bnextrno;
		
		if ((blk->bfreeoff + rlen) 
				< (BLOCKSIZE - BLK_TAILSIZE -
				   (ROW_OFFSET_ENTRYSIZE * (row_cnt + 1))))
		{
			break;
		}
		
		else
		{
			
			Assert(blk->bnextrno > 0);

			Assert (!(rtn_stat & BLK_ROW_NEXT_BLK));

			if ((blk->bnextrno <= 0) || (rtn_stat & BLK_ROW_NEXT_BLK))
			{
				traceprint("The block should not be empty.\n");
				
				ex_raise(EX_BUFERR);
			}
		}
	
						
		
		if (blk->bnextblkno == -1)
		{
			
			if (blk->bblkno > ((BLK_CNT_IN_SSTABLE / 2) - 1))
			{
				rtn_stat |= BLK_ROW_NEXT_SSTAB;
			}

			if (tss->topid & TSS_OP_RANGESERVER)
			{
				if(tabinfo->t_stat & TAB_INS_SPLITING_SSTAB)
				{
					Assert(!(tabinfo->t_stat & TAB_DO_INDEX));

					if (tabinfo->t_stat & TAB_DO_INDEX)
					{
						traceprint("It should not hit the split issue in the inde case.\n");
						ex_raise(EX_BUFERR);
					}
					
					traceprint("Hit the new split on the splitting sstable %s.\n", tabinfo->t_sstab_name);

					
					tabinfo->t_stat |= TAB_RETRY_LOOKUP;

					rtn_stat = BLK_INS_SPLITTING_SSTAB;

					break;
				}

				if (   (!(tss->tstat & TSS_OP_RECOVERY))
				    && (!(tabinfo->t_stat & TAB_NOLOG_MODEL)))
				{
					hkgc_wash_sstab(TRUE);
				}

				if (!sstab_split(tabinfo, bp, rp, 
						data_insert_needed))
				{
					traceprint("SSTABLE_SPLIT: hit fatal err.\n");
					
					ex_raise(EX_BUFERR);
				}
			}
			else if (tss->topid & TSS_OP_METASERVER)
			{
				if (!tablet_split(tabinfo, bp, rp))
				{
					traceprint("TABLET_SPLIT: hit fatal err.\n");
						
					ex_raise(EX_BUFERR);
				}
			}

			continue;
		}

		nextblk = (BLOCK *) ((char *)blk + BLOCKSIZE);
		
		
		if (nextblk->bnextrno == 0)
		{
			if (nextblk->bfreeoff > BLKHEADERSIZE)
			{
				blk_compact(nextblk);
			}
			
			if (ins_rnum > ((blk->bnextrno) / 2))
			{
				if (blk->bnextrno == 1)
				{
					rtn_stat |= BLK_ROW_NEXT_BLK;
					
					return rtn_stat;
				}
			}

			
			if (   data_insert_needed
			    && (tss->topid & TSS_OP_RANGESERVER)
			    && (!(tss->tstat & TSS_OP_RECOVERY))
			    && (!(tabinfo->t_stat & TAB_NOLOG_MODEL))
			    && (!(tabinfo->t_stat & TAB_INS_INDEX)))
			{
				Assert(   (tss->topid & TSS_OP_INSTAB)
				       || (tss->topid & TSS_OP_UPDATE));

				if (!(   (tss->topid & TSS_OP_INSTAB)
				      || (tss->topid & TSS_OP_UPDATE)))
				{
					traceprint("BLOCK_CHK_SPACE: The logging case can only be in the insert/update case.\n");
					
					ex_raise(EX_BUFERR);
				}
				
				log_build(&logrec, LOG_BEGIN, 0, 0, 
						tabinfo->t_sstab_name, 
						NULL, 0, 0, 0, 0, 0, 
						NULL, NULL);

				log_put(&logrec, NULL, 0);
						
			}

			bufpredirty(bp->bsstab);
			
			split_pos = blk_split(blk, tabinfo, rlen, ins_rnum);

			bufdirty(bp->bsstab);

			if ((ins_rnum > split_pos) || (ins_rnum == split_pos))
			{
				rtn_stat |= BLK_ROW_NEXT_BLK;

				blk = nextblk;
			}

			if (   data_insert_needed
			    && (tss->topid & TSS_OP_RANGESERVER)
			    && (!(tss->tstat & TSS_OP_RECOVERY))
			    && (!(tabinfo->t_stat & TAB_NOLOG_MODEL)))
			{
				Assert(   (tss->topid & TSS_OP_INSTAB)
				       || (tss->topid & TSS_OP_UPDATE));

				if (!(   (tss->topid & TSS_OP_INSTAB)
				      || (tss->topid & TSS_OP_UPDATE)))
				{
					traceprint("BLOCK_CHK_SPACE: The logging case can only be in the insert/update case.\n");
					
					ex_raise(EX_BUFERR);
				}
				
				tabinfo->t_insdel_old_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
	
				bp->bsstab->bblk->bsstab_insdel_ts_lo = 
						mtts_increment(bp->bsstab->bblk->bsstab_insdel_ts_lo);

				tabinfo->t_insdel_new_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
	
				log_build(&logrec, LOG_BLK_SPLIT, 
						tabinfo->t_insdel_old_ts_lo, 
						tabinfo->t_insdel_new_ts_lo,
						tabinfo->t_sstab_name, 
						tabinfo->t_sstab_name,
						0, 0, 0, 0, 0, NULL, NULL);
				
				log_put(&logrec, NULL, 0);				
				

				
				if(!(tabinfo->t_stat & TAB_INS_INDEX))
				{				
					log_build(&logrec, LOG_END, 0, 0,
						tabinfo->t_sstab_name,
						NULL, 0, 0, 0, 0, 0, NULL, NULL);

					log_put(&logrec, NULL, 0);
				}
				
				DIRTYUNLINK(bp->bsstab);
				bufwrite(bp->bsstab);
//				hkgc_wash_sstab(TRUE);
				
			}
		}
		else
		{
			
			if (blk_backmov(nextblk, tabinfo))
			{
				
				if (ins_rnum > ((blk->bnextrno) / 2))
				{
					
					if (blk->bnextrno == 1)
					{
						rtn_stat |= BLK_ROW_NEXT_BLK;
						
						return rtn_stat;
					}
				}

				bufpredirty(bp->bsstab);
				
				split_pos = blk_split(blk, tabinfo, rlen, ins_rnum);

				bufdirty(bp->bsstab);

				
				if (   (ins_rnum > 0) 
				    && (   (ins_rnum > split_pos)
				        || (ins_rnum == split_pos)))
				{
					rtn_stat |= BLK_ROW_NEXT_BLK;

					blk = nextblk;
				}

				if (   data_insert_needed
				    && (tss->topid & TSS_OP_RANGESERVER)
				    && (!(tss->tstat & TSS_OP_RECOVERY))
				    && (!(tabinfo->t_stat & TAB_NOLOG_MODEL)))
				{
					Assert(   (tss->topid & TSS_OP_INSTAB)
					       || (tss->topid & TSS_OP_UPDATE));

					if (!(   (tss->topid & TSS_OP_INSTAB)
					      || (tss->topid & TSS_OP_UPDATE)))
					{
						traceprint("BLOCK_CHK_SPACE: The logging case can only be in the insert/update case.\n");
						
						ex_raise(EX_BUFERR);
					}
					
					tabinfo->t_insdel_old_ts_lo = 
						bp->bsstab->bblk->bsstab_insdel_ts_lo;
		
					bp->bsstab->bblk->bsstab_insdel_ts_lo = 
						mtts_increment(bp->bsstab->bblk->bsstab_insdel_ts_lo);

					tabinfo->t_insdel_new_ts_lo = 
						bp->bsstab->bblk->bsstab_insdel_ts_lo;
		
					log_build(&logrec, LOG_BLK_SPLIT, 
							tabinfo->t_insdel_old_ts_lo, 
							tabinfo->t_insdel_new_ts_lo,
							tabinfo->t_sstab_name, 
							tabinfo->t_sstab_name,
							0, 0, 0, 0, 0, NULL, NULL);
					
					log_put(&logrec, NULL, 0);

					if(!(tabinfo->t_stat & TAB_INS_INDEX))
					{
						log_build(&logrec, LOG_END, 0, 0,
							tabinfo->t_sstab_name,
							NULL, 0, 0, 0, 0, 0, NULL, NULL);

						log_put(&logrec, NULL, 0);
					}
					
					 
					DIRTYUNLINK(bp->bsstab);
					bufwrite(bp->bsstab);
//					hkgc_wash_sstab(TRUE);
				}
			}
			else
			{
				
				if (blk->bblkno > ((BLK_CNT_IN_SSTABLE / 2) - 1))
				{
					rtn_stat |= BLK_ROW_NEXT_SSTAB;
				}

				if (tss->topid & TSS_OP_RANGESERVER)
				{
					if(tabinfo->t_stat & TAB_INS_SPLITING_SSTAB)
					{
						Assert(!(tabinfo->t_stat & TAB_DO_INDEX));

						if (tabinfo->t_stat & TAB_DO_INDEX)
						{
							traceprint("It should not hit the split issue in the inde case.\n");
							ex_raise(EX_BUFERR);
						}
						
						traceprint("Hit the new split on the splitting sstable %s.\n", tabinfo->t_sstab_name);

						
						tabinfo->t_stat |= TAB_RETRY_LOOKUP;

						rtn_stat = BLK_INS_SPLITTING_SSTAB;

						break;
					}

					if (   (!(tss->tstat & TSS_OP_RECOVERY))
					    && (!(tabinfo->t_stat & TAB_NOLOG_MODEL)))
					{
						hkgc_wash_sstab(TRUE);
					}
					
					if (!sstab_split(tabinfo, bp, rp,
								data_insert_needed))
					{
						traceprint("SSTABLE_SPLIT: hit fatal err.\n");
						
						ex_raise(EX_BUFERR);
					}
				}
				else if (tss->topid & TSS_OP_METASERVER)
				{
					if (!tablet_split(tabinfo, bp, rp))
					{
						traceprint("TABLET_SPLIT: hit fatal err.\n");
						
						ex_raise(EX_BUFERR);
					}
				}

			}
		}

	}	

	return rtn_stat;
}


int
blk_update_check_sstab_space(TABINFO *tabinfo, BUF *bp, char *oldrp, int oldrlen, 
				int newrlen, int rnum)
{
	int		rtn_stat;
	BLOCK		*blk;
	int		blk_stat;
	
	
	blk		= bp->bblk;
	blk_stat	= 0;
	rtn_stat	= -1;
	
	if ((newrlen < oldrlen) || (oldrlen == newrlen))
	{
		rtn_stat = UPDATE_IN_PLACE;
	}
	else if ((blk->bfreeoff + newrlen) < (BLOCKSIZE - BLK_TAILSIZE -
				(ROW_OFFSET_ENTRYSIZE * (bp->bblk->bnextrno))))
	{
		rtn_stat = UPDATE_IN_CUR_BLK;
	}
	else
	{
		rtn_stat = UPDATE_SKIP_THIS_ROW;

#if 0
		blk_stat = blk_check_sstab_space(tabinfo, bp, oldrp, newrlen, rnum, FALSE);

		if (blk_stat & BLK_INS_SPLITTING_SSTAB)
		{
			rtn_stat = UPDATE_HIT_ERROR;
		}
		else if (blk_stat & BLK_ROW_NEXT_SSTAB)
		{
			rtn_stat = UPDATE_IN_NEXT_SSTAB;
		}
		else if (blk_stat & BLK_ROW_NEXT_BLK)
		{
			rtn_stat = UPDATE_IN_NEXT_BLK;
		}
		else
		{
			rtn_stat = UPDATE_BLK_CHANGED;
		}
#endif
	}

	return rtn_stat;
}


int
blk_split(BLOCK *blk, TABINFO *tabinfo, int rlen, int inspos)
{
	LOCALTSS(tss);
	BLOCK		*nextblk;
	int		row_cnt;
	int		i,j;
	int		*thisofftab;
	int		*nextofftab;
	int		offset;
	int		mvsize;
	BLOCK		*tmpblock;
	IDXBLD		idxbld;
	IDXUPD		idxupd;
	char		rg_tab_dir[TABLE_NAME_MAX_LEN];
	int		split_pos;


	tmpblock = NULL;
	split_pos = -1;
	
	
	BUF_GET_RESERVED(tmpblock);
	
	MEMCPY(tmpblock, blk, sizeof(BLOCK));
	if (!blk_shuffle_data(tmpblock, blk))
	{
		MEMCPY(blk, tmpblock, sizeof(BLOCK));

		Assert(0);
		goto exit;
	}

	nextblk = (BLOCK *) ((char *)blk + BLOCKSIZE);
	row_cnt = blk->bnextrno;
	
	Assert((blk->bnextblkno!= -1) && (nextblk->bnextrno == 0)
		&& (row_cnt > 0));

	if (nextblk->bfreeoff > BLKHEADERSIZE)
	{
		blk_compact(nextblk);
	}
	
	
	i = split_pos = blk_get_inspos(blk, rlen, inspos);

	thisofftab = ROW_OFFSET_PTR(blk);

	offset = (i == blk->bnextrno) ? blk->bfreeoff : thisofftab[-i];
	mvsize = blk->bfreeoff - offset;

	MEMCPY(nextblk->bdata, (char *)blk + offset, mvsize);

	blk->bnextrno = i;
	blk->bfreeoff = offset;

	MEMSET((char *)blk + offset, mvsize);
	
	nextofftab = ROW_OFFSET_PTR(nextblk);
	
	for (j = 0; i < row_cnt; i++,j++)
	{
		nextofftab[-j] = thisofftab[-i] - offset + BLKHEADERSIZE;	
	}

	nextblk->bnextrno = j;
	nextblk->bfreeoff = mvsize + BLKHEADERSIZE;

	nextblk->bminlen = blk->bminlen;

	Assert((blk->bnextrno + nextblk->bnextrno) == row_cnt);
	
	if (   (tss->topid & TSS_OP_INSTAB) 
	    && (tabinfo->t_has_index))
	{			
		MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(rg_tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
		str1_to_str2(rg_tab_dir, '/', tabinfo->t_tab_name);
	
		
		idxbld.idx_tab_name = rg_tab_dir;

		idxbld.idx_stat = 0;

		idxbld.idx_root_sstab = tabinfo->t_tablet_id;

		
		idxupd.newblk = blk;

		idxupd.oldblk = tmpblock;

		idxupd.new_sstabid = tabinfo->t_sstab_id;

		idxupd.old_sstabid = tabinfo->t_sstab_id;

		idxupd.start_row = 0;			
	
		
		Assert(blk->bfreeoff > BLKHEADERSIZE);

		if (tabinfo->t_index_ts 
			!= Range_infor->rg_meta_sysindex->idx_ver)
		{
			meta_load_sysindex(
				(char *)Range_infor->rg_meta_sysindex);
		}
		
		index_update(&idxbld, &idxupd, tabinfo, 
				Range_infor->rg_meta_sysindex);


		idxbld.idx_stat = 0;

		
		idxupd.newblk = nextblk;
		
		idxupd.start_row = blk->bnextrno;			
	
		
		Assert(nextblk->bfreeoff > BLKHEADERSIZE);
		
		index_update(&idxbld, &idxupd, tabinfo, 
				Range_infor->rg_meta_sysindex);	
	
	}

exit:
	BUF_RELEASE_RESERVED(tmpblock);

	return split_pos;
}



int
blk_backmov(BLOCK *blk, TABINFO *tabinfo)
{
	LOCALTSS(tss);
	BLOCK		*nextblk;
	BLOCK		*destblk;
	IDXBLD		idxbld;
	IDXUPD		idxupd;
	char		rg_tab_dir[TABLE_NAME_MAX_LEN];
	

	if (blk->bfreeoff == BLKHEADERSIZE)
	{
		return TRUE;
	}

	nextblk = blk;

	
	while (   (nextblk->bnextblkno!= -1) 
	       && (nextblk->bfreeoff != BLKHEADERSIZE))
	{		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
	}

	
	if (nextblk->bfreeoff != BLKHEADERSIZE)
	{
		return FALSE;
	}

	if (   (tss->topid & TSS_OP_RANGESERVER)
	    && (!(tss->tstat & TSS_OP_RECOVERY))
	    && (!(tabinfo->t_stat & TAB_NOLOG_MODEL))
	    && (!(tabinfo->t_stat & TAB_INS_INDEX)))
	{		
		Assert(tss->topid & TSS_OP_INSTAB);
		
		LOGREC		logrec;
		
		log_build(&logrec, LOG_BEGIN, 0, 0, tabinfo->t_sstab_name, 
					NULL, 0, 0, 0, 0, 0, NULL, NULL);

		log_put(&logrec, NULL, 0);					
	}
				

	nextblk = (BLOCK *) ((char *)nextblk - BLOCKSIZE);

	Assert(nextblk->bfreeoff > BLKHEADERSIZE);
	
	if ((tss->topid & TSS_OP_INSTAB) && (tabinfo->t_has_index))
	{		
		MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(rg_tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
		str1_to_str2(rg_tab_dir, '/', tabinfo->t_tab_name);
	}

	while ((nextblk > blk) || (nextblk == blk))
	{
		
		destblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);

		
		Assert(   (destblk->bblkno != -1)
		       && (destblk->bfreeoff == BLKHEADERSIZE));

		
		MEMCPY(destblk->bdata, nextblk->bdata, 
					BLOCKSIZE - BLKHEADERSIZE - 4);
		
		destblk->bnextrno = nextblk->bnextrno;				
		destblk->bfreeoff = nextblk->bfreeoff;
		destblk->bminlen = nextblk->bminlen;

		if (   (tss->topid & TSS_OP_INSTAB)
		    && (tabinfo->t_has_index))
		{			
			
			idxbld.idx_tab_name = rg_tab_dir;

			idxbld.idx_stat = 0;

			idxbld.idx_root_sstab = tabinfo->t_tablet_id;

			idxupd.newblk = destblk;

			idxupd.oldblk = nextblk;

			idxupd.new_sstabid = tabinfo->t_sstab_id;

			idxupd.old_sstabid = tabinfo->t_sstab_id;
			
			idxupd.start_row = 0;

			
			Assert(destblk->bfreeoff > BLKHEADERSIZE);

			if (tabinfo->t_index_ts 
				!= Range_infor->rg_meta_sysindex->idx_ver)
			{
				meta_load_sysindex(
					(char *)Range_infor->rg_meta_sysindex);
			}
			
			index_update(&idxbld, &idxupd, tabinfo,
					Range_infor->rg_meta_sysindex);
		}

		MEMSET(nextblk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);
		nextblk->bnextrno = 0;
		nextblk->bfreeoff = BLKHEADERSIZE;
		nextblk->bminlen = 0;

		nextblk = (BLOCK *) ((char *)nextblk - BLOCKSIZE);
	}

	return TRUE;
}

int
blk_move(TABINFO *tabinfo, BLOCK *srcblk, BLOCK *destblk)
{
	return 1;
}



int
blk_putrow(TABINFO *tabinfo, BLOCK *blk, int sstabid, char *rp, int rlen,
			int rnum, int roffset)
{
	int		i;
	int		*offtab;
	int		ign = 0;
	
	
	i = 0;
	offtab = ROW_OFFSET_PTR(blk);
	
	
	if (rnum != -1)
	{
		
		for (i = blk->bnextrno; i > rnum; i--)
		{
			offtab[-i] = offtab[-(i-1)];
		}
	}

	//ROW_SET_ROWNO(rp, rnum);
	
	
	PUT_TO_BUFFER((char *)blk + roffset, ign, rp, rlen);
		
	
	offtab[-i] = roffset;

	if (roffset == blk->bfreeoff)
	{
		blk->bfreeoff += rlen;
	}
		
	BLK_GET_NEXT_ROWNO(blk)++;

		
	tabinfo->t_currid.block_id = blk->bblkno;
	tabinfo->t_currid.sstable_id = sstabid;
	tabinfo->t_currid.roffset = offtab[-i];

	return TRUE;
}



int
blk_get_location_sstab(TABINFO *tabinfo, BUF *bp)
{
	LOCALTSS(tss);
	char	*rp;
	int	coloffset;
	char	*key;
	int	coltype;
	int	keylen;
	char	*key_in_blk;
	int	keylen_in_blk;
	int	result;
	int	blkidx;

	
	blkidx		= -1;	
	coloffset	= tabinfo->t_sinfo->sicoloff;
	key 		= tabinfo->t_sinfo->sicolval;
	coltype 	= tabinfo->t_sinfo->sicoltype;
	keylen 		= tabinfo->t_sinfo->sicollen;		

	while(bp->bblk->bblkno != -1)
	{
		if (bp->bblk->bstat & BLK_RENT_DATA)
		{
			
			break;
		}
		
		if (BLK_GET_NEXT_ROWNO(bp->bblk) == 0)
		{
#if 0
			if (   FALSE && (bp->bblk->bblkno == 0)
			    && (bp->bblk->bprevsstabnum == -1)
			    && (bp->bblk->bnextsstabnum == -1))
			{
				blkidx = 0;
			}
#endif

			if (tss->topid & TSS_OP_IDXROOT_SPLIT)
			{
				break;
			}
			
			
			goto nextblk;
		}
		
		rp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, 0);

		key_in_blk = row_locate_col(rp, coloffset, bp->bblk->bminlen, 
					    &keylen_in_blk);

		result = row_col_compare(coltype, key, keylen, key_in_blk, 
							keylen_in_blk);

		
#if 0
		if (   (result == EQ) && (tabinfo->t_sinfo) 
		    && (tabinfo->t_sinfo->sistate & (SI_DEL_DATA | SI_UPD_DATA)))
		{
			blkidx = bp->bblk->bblkno;

			break;
		}
#endif
		
		if (result == LE)
		{
			
			if (blkidx == -1)
			{
				
				blkidx = bp->bblk->bblkno;
			}
			
			break;
		}
		

		
		if (result == EQ)
		{
			blkidx = bp->bblk->bblkno;
			
			break;
		}

		
		blkidx = bp->bblk->bblkno;

nextblk:
		if (bp->bblk->bnextblkno != -1)
		{
			bp++;
		}
		else
		{
			break;
		}
	}

	return blkidx;
}

int
blk_appendrow(BLOCK *blk, char *rp, int rlen)
{
	int	ign = 0;


	if ((blk->bfreeoff + rlen) > (BLOCKSIZE - BLK_TAILSIZE -
		(ROW_OFFSET_ENTRYSIZE * (BLK_GET_NEXT_ROWNO(blk) + 1))))
	{
		return FALSE;
	}
	
	PUT_TO_BUFFER((char *)blk + blk->bfreeoff, ign, rp, rlen);
	
	
	ROW_SET_OFFSET(blk, BLK_GET_NEXT_ROWNO(blk), blk->bfreeoff);
	

	blk->bfreeoff += rlen;


	BLK_GET_NEXT_ROWNO(blk)++;

	return TRUE;
}


int
blk_get_totrow_sstab(BUF *bp)
{
	int	totrow;
	

	totrow = 0;

	while(bp->bblk->bblkno != -1)
	{
		if (bp->bblk->bstat & BLK_RENT_DATA)
		{
			break;
		}
		
		totrow += BLK_GET_NEXT_ROWNO(bp->bblk);
		
		
		
		if (bp->bblk->bnextblkno != -1)
		{
			bp++;
		}
		else
		{
			break;
		}
	}

	return totrow;
}



int
blk_shuffle_data(BLOCK *srcblk, BLOCK *destblk)
{
	char	*rp;
	int	rlen;
	int	i;

	
	destblk->bfreeoff = BLKHEADERSIZE;
	destblk->bnextrno = 0;
	MEMSET(destblk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);

	for (i = 0; i < srcblk->bnextrno; i++)
	{
		rp = ROW_GETPTR_FROM_OFFTAB(srcblk, i);
		rlen = ROW_GET_LENGTH(rp, srcblk->bminlen);

		if (!blk_appendrow(destblk, rp, rlen))
		{
			traceprint("Block resort hit error for the not enough space.\n");
			return FALSE;
		}
	}

	return TRUE;
}


int
blk_compact(BLOCK *blk)
{
	Assert(blk->bnextrno == 0);

	blk->bfreeoff = BLKHEADERSIZE;
	blk->bnextrno = 0;
	MEMSET(blk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);

	return TRUE;
}



int
blk_get_inspos(BLOCK *blk, int rlen, int inspos)
{
	int	row_cnt;
	int	i;
	int	*offtab;
	int	datasize;
	int	midpos;


	datasize = BLOCKSIZE - BLKHEADERSIZE - BLK_TAILSIZE;
	
	offtab = ROW_OFFSET_PTR(blk);
	
	row_cnt = blk->bnextrno;

	midpos = row_cnt / 2;
	
	if (inspos < midpos)
	{
		
		for (i = midpos; i > inspos; i--)
		{
			if (  (blk->bfreeoff - offtab[-(i-1)]) 
			    > (rlen + ROW_OFFSET_ENTRYSIZE))
			{
				return i;
			}
		}

	}
	else
	{
		
		for (i = midpos; i < inspos; i++)
		{
			if (  (datasize - (  ROW_OFFSET_ENTRYSIZE 
					   * (row_cnt - i + 1)))
			    - (blk->bfreeoff - offtab[-i]) > rlen)
			{
				return i;
			}
		}
	}

	return i;
}
