/*
** block.c 2010-10-08 xueyingfei
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
#include "master/metaserver.h"
#include "memcom.h"
#include "buffer.h"
#include "block.h"
#include "cache.h"
#include "row.h"
#include "strings.h"
#include "utils.h"
#include "tss.h"
#include "sstab.h"
#include "tablet.h"
#include "b_search.h"
#include "timestamp.h"
#include "log.h"
#include "hkgc.h"


extern TSS	*Tss;


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


	stat_chg = FALSE;
	
nextsstab:

	bp = blk_getsstable(tabinfo);

	Assert(bp);


	if (0 && BLOCK_IS_EMPTY(bp))
	{
		tabinfo->t_rowinfo->rblknum = bp->bblk->bblkno;
		tabinfo->t_rowinfo->roffset = BLKHEADERSIZE;
		tabinfo->t_rowinfo->rsstabid = bp->bblk->bsstabid;

		
		return bp->bsstab;
	}

	
	

	
	tabinfo->t_sinfo->sistate |= SI_INDEX_BLK;
	blkidx = blksrch(tabinfo, bp);

	Assert(blkidx < BLK_CNT_IN_SSTABLE);

	tabinfo->t_sinfo->sistate &= ~SI_INDEX_BLK;

	
	if (   (tss->topid & TSS_OP_RANGESERVER) 
	    && ((tss->topid & TSS_OP_INSTAB) || (tss->topid & TSS_OP_SELDELTAB)))
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
		    	&& (tabinfo->t_rowinfo->roffset == tmpbp->bblk->bfreeoff)))
		{
			
			if (tmpblkidx < (BLK_CNT_IN_SSTABLE - 1))
			{
				tmpblkidx++;
				tmpbp = bp->bsstab + tmpblkidx;

				if (tmpbp->bblk->bfreeoff > BLKHEADERSIZE)
				{
					goto finish;
				}
				else
				{
					Assert(tmpbp->bblk->bfreeoff == BLKHEADERSIZE);				
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

		
		if (stat_chg && (tabinfo->t_rowinfo->roffset == BLKHEADERSIZE))
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
	
	return (bp->bsstab + blkidx);
}



int
blksrch(TABINFO *tabinfo, BUF *bp)
{
	BLOCK		*blk;
	char		*rp;
	int		last_offset;
	int		rowno;
	int		minrowlen;
	int     	blkidx;
	int     	result;
	B_SRCHINFO	*srchinfo;
	B_SRCHINFO	m_srchinfo;
	int		low;
	int		high;
	int		total;


	blkidx = -1;
	
	
	if (BLK_GET_NEXT_ROWNO(bp->bblk) == 0)
	{
		blkidx = bp->bblk->bblkno;
		last_offset = bp->bblk->bfreeoff;
		tabinfo->t_sinfo->sistate |= SI_NODATA;
		goto finish;
	}

	MEMSET(&m_srchinfo, sizeof(B_SRCHINFO));
	srchinfo = &m_srchinfo;

	bp = bp->bsstab;

	blkidx = blk_get_location_sstab(tabinfo, bp);

	Assert(blkidx != -1);

	bp += blkidx;

	blk		= bp->bblk;
	minrowlen 	= blk->bminlen;


	low = 0;
	high = BLK_GET_NEXT_ROWNO(bp->bblk) - 1;
	total = BLK_GET_NEXT_ROWNO(bp->bblk);
	result = LE;	

	SRCHINFO_INIT(srchinfo, 0, high, total, result);

	b_srch_block(tabinfo, bp, srchinfo);

	result = srchinfo->bcomp;
	rowno = srchinfo->brownum;
	rp = srchinfo->brow;
	last_offset = srchinfo->boffset;

	
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

	
	if (rowno == BLK_GET_NEXT_ROWNO(bp->bblk))
	{
		if (!(tabinfo->t_stat & TAB_SCHM_SRCH))
		{
			
			last_offset += ROW_GET_LENGTH(rp, minrowlen);
		}

	}

	
finish:

	
	tabinfo->t_rowinfo->rblknum = blkidx;
	tabinfo->t_rowinfo->roffset = last_offset;
	tabinfo->t_rowinfo->rsstabid = bp->bblk->bsstabid;
	
	if (tabinfo->t_sinfo->sistate & SI_INDEX_BLK)
	{
				
		return blkidx;
	}
		
        return last_offset;
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
		    || (   (tss->topid & TSS_OP_METASERVER) 
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
	int	offset;
	int	minlen;
	int	ign;
	int	rlen;
	int	i;
	int	*offtab;
	int	blk_stat;


	minlen = tabinfo->t_row_minlen;
	
	tabinfo->t_sinfo->sistate |= SI_INS_DATA;
	
	bp = blkget(tabinfo);

	

	if ((tabinfo->t_stat & TAB_RETRY_LOOKUP) 
	    || !(tabinfo->t_sinfo->sistate & SI_NODATA))
	{
		bufunkeep(bp->bsstab);
		return FALSE;
	}

	

//	offset = blksrch(tabinfo, bp);

	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);
	offset = tabinfo->t_rowinfo->roffset;

	ign = 0;
	rlen = ROW_GET_LENGTH(rp, minlen);

	blk_stat = blk_check_sstab_space(tabinfo, bp, rp, rlen, offset);

	
	bufpredirty(bp);
	
	if (blk_stat & BLK_INS_SPLITTING_SSTAB)
	{
		bufdestroy(bp);

		return FALSE;
	}
	
	if (blk_stat & BLK_ROW_NEXT_SSTAB)
	{
		goto finish;
	}

	
	if (blk_stat & BLK_ROW_NEXT_BLK)
	{
		bp++;
	}

	
	if ((blk_stat & BLK_BUF_NEED_CHANGE))
	{
		offset = blksrch(tabinfo, bp);
	}

	if (bp->bblk->bfreeoff - offset)
	{
		
		offtab = ROW_OFFSET_PTR(bp->bblk);
		
		BACKMOVE((char *)bp->bblk + offset, 
				(char *)bp->bblk + offset + rlen, 
				bp->bblk->bfreeoff - offset);

		
		for (i = bp->bblk->bnextrno; i > 0; i--)
		{
			if (offtab[-(i-1)] < offset)
			{
				break;
			}

			offtab[-i] = offtab[-(i-1)] + rlen;
		
		}
		offtab[-i] = offset;
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

	
	PUT_TO_BUFFER((char *)bp->bblk + offset, ign, rp, rlen);

	if (bp->bblk->bfreeoff == offset)
	{
		
		ROW_SET_OFFSET(bp->bblk, BLK_GET_NEXT_ROWNO(bp->bblk), offset);
	}
	


	bp->bblk->bfreeoff += rlen;

	bp->bblk->bminlen = minlen;
	
	BLK_GET_NEXT_ROWNO(bp->bblk)++;

finish:

	if (tabinfo->t_stat & TAB_SSTAB_SPLIT)
	{
		bp->bsstab->bblk->bstat |= BLK_SSTAB_SPLIT;
		tabinfo->t_stat |= TAB_LOG_SKIP_LOG;		
	}
	
	bufdirty(bp);

	/* Must to flush the first insertion into the disk. */
	if ((SSTABLE_STATE(bp) & BUF_READ_EMPTY) && (SSTABLE_STATE(bp) & BUF_DIRTY))
	{		
		DIRTYUNLINK(bp);
		bufwrite(bp);
		SSTABLE_STATE(bp) &= ~BUF_READ_EMPTY;
	}

	if ((tss->topid & TSS_OP_RANGESERVER) && (!(tss->tstat & TSS_OP_RECOVERY)))
	{
		LOGREC	logrec;
		int	logid;

		/*
		**	While one row insertion hit the split issue, the overview of log as follows:
		**	1. | ChkPoint_begin|Chkpoint_end| LOG_SKIP |LOG_SKIP|  ChkPoint_begin|Chkpoint_end|
		**	2. | ChkPoint_begin|Chkpoint_end| LOG_SKIP | ChkPoint_begin|Chkpoint_end|
		*/
		logid = (tabinfo->t_stat & TAB_LOG_SKIP_LOG)? LOG_SKIP : LOG_INSERT;

		log_build(&logrec, logid, tabinfo->t_insdel_old_ts_lo,
					tabinfo->t_insdel_new_ts_lo,
					tabinfo->t_sstab_name, NULL, 
					tabinfo->t_row_minlen, tabinfo->t_tabid,
					tabinfo->t_sstab_id);
		
		log_insert_insdel(&logrec, rp, rlen);

		if (tabinfo->t_stat & TAB_SSTAB_SPLIT)
		{
			hkgc_wash_sstab(TRUE);
		}
	}
	
	bufunkeep(bp->bsstab);
		
	tabinfo->t_sinfo->sistate &= ~SI_INS_DATA;

	return TRUE;
}



int
blkdel(TABINFO *tabinfo)
{
	LOCALTSS(tss);
	BUF	*bp;
	int	offset;
	int	minlen;
	int	rlen;
	int	i;
	int	*offtab;
	char	*rp;


	minlen = tabinfo->t_row_minlen;
	
	tabinfo->t_sinfo->sistate |= SI_DEL_DATA;
	
	bp = blkget(tabinfo);

	if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
	{
		bufunkeep(bp->bsstab);
		return FALSE;
	}

	bufpredirty(bp);

//	offset = blksrch(tabinfo, bp);

	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);
	offset = tabinfo->t_rowinfo->roffset;

	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		traceprint("We can not find the row to be deleted.\n");	
		return FALSE;
	}

	rp = (char *)(bp->bblk) + offset;
	rlen = ROW_GET_LENGTH(rp, minlen);

	if (tss->topid & TSS_OP_RANGESERVER)
	{
		tabinfo->t_cur_rowp = (char *)MEMALLOCHEAP(rlen);

		MEMCPY(tabinfo->t_cur_rowp, rp, rlen);

		tabinfo->t_cur_rowlen = rlen;

		if ((bp->bblk->bblkno == 0) && (offset == BLKHEADERSIZE))
		{
			
			ROW_SET_STATUS(rp, ROW_DELETED);

			
			
			goto finish;
		}		
	}
		
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

	int j;
	for(j = i; j < (bp->bblk->bnextrno - 1); j++)
	{
		offtab[-j] = offtab[-(j + 1)] - rlen;
	
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
	
	bp->bblk->bfreeoff -= rlen;
	
	BLK_GET_NEXT_ROWNO(bp->bblk)--;

finish:
	bufdirty(bp);
	bufunkeep(bp->bsstab);
		
	tabinfo->t_sinfo->sistate &= ~SI_DEL_DATA;

	return TRUE;
}


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
	    && blk_check_sstab_space(NULL, NULL, rp, rlen, 0))
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



int
blk_check_sstab_space(TABINFO *tabinfo, BUF *bp, char *rp, int rlen, 
			int ins_offset)
{
	LOCALTSS(tss);
	int	blkno;
	int	rtn_stat;
	int	row_cnt;
	BLOCK	*blk;
	BLOCK	*nextblk;
	int	*thisofftab;

	
	blk = bp->bblk;
	rtn_stat = BLK_BUF_NEED_CHANGE;
	blkno = blk->bblkno;
	row_cnt = blk->bnextrno;

	if ((blk->bfreeoff + rlen) < (BLOCKSIZE - BLK_TAILSIZE -
					(ROW_OFFSET_ENTRYSIZE * (row_cnt + 1))))
	{
		rtn_stat &= ~BLK_BUF_NEED_CHANGE;

		return rtn_stat;
	}
	
	while(1)
	{
		row_cnt = blk->bnextrno;
		
		
		if ((blk->bfreeoff + rlen) < (BLOCKSIZE - BLK_TAILSIZE -
					(ROW_OFFSET_ENTRYSIZE * (row_cnt + 1))))
		{
			break;
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
					

					traceprint("Hit the new split on the splitting sstable %s.\n", tabinfo->t_sstab_name);

					
					tabinfo->t_stat |= TAB_RETRY_LOOKUP;

					rtn_stat = BLK_INS_SPLITTING_SSTAB;

					break;
				}

				if (!(tss->tstat & TSS_OP_RECOVERY))
				{
					hkgc_wash_sstab(TRUE);
				}
				
				sstab_split(tabinfo, bp, rp);
			}
			else if (tss->topid & TSS_OP_METASERVER)
			{
				tablet_split(tabinfo, bp, rp);
			}

			continue;
		}

		nextblk = (BLOCK *) ((char *)blk + BLOCKSIZE);

		
		if (nextblk->bfreeoff == BLKHEADERSIZE)
		{
			thisofftab = ROW_OFFSET_PTR(blk);

			if (ins_offset > thisofftab[-((blk->bnextrno) / 2)])
			{
				rtn_stat |= BLK_ROW_NEXT_BLK;

				
				if (blk->bnextrno == 1)
				{
					return rtn_stat;
				}
			}
			
			blk_split(blk);
		}
		else
		{
			if (blk_backmov(nextblk))
			{
				thisofftab = ROW_OFFSET_PTR(blk);

				if (ins_offset > thisofftab[-((blk->bnextrno) / 2)])
				{
					rtn_stat |= BLK_ROW_NEXT_BLK;

					if (blk->bnextrno == 1)
					{
						return rtn_stat;
					}
				}
				
				blk_split(blk);
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
						traceprint("Hit the new split on the splitting sstable %s.\n", tabinfo->t_sstab_name);

						
						tabinfo->t_stat |= TAB_RETRY_LOOKUP;

						rtn_stat = BLK_INS_SPLITTING_SSTAB;

						break;
					}

					if (!(tss->tstat & TSS_OP_RECOVERY))
					{
						hkgc_wash_sstab(TRUE);
					}
					
					sstab_split(tabinfo, bp, rp);
				}
				else if (tss->topid & TSS_OP_METASERVER)
				{
					tablet_split(tabinfo, bp, rp);
				}
			}
		}
			
	}	

	return rtn_stat;

	
}



void
blk_split(BLOCK *blk)
{
	BLOCK	*nextblk;
	int	rowcnt;
	int	i,j;
	int	*thisofftab;
	int	*nextofftab;
	int	offset;
	int	mvsize;

	nextblk = (BLOCK *) ((char *)blk + BLOCKSIZE);
	rowcnt = blk->bnextrno;

	Assert((blk->bnextblkno!= -1) && (nextblk->bfreeoff == BLKHEADERSIZE)
		&& (rowcnt > 0));

	
	i = rowcnt / 2;
	
	thisofftab = ROW_OFFSET_PTR(blk);

	offset = thisofftab[-i];
	mvsize = blk->bfreeoff - offset;

	MEMCPY(nextblk->bdata, (char *)blk + offset, mvsize);

	blk->bnextrno = i;
	blk->bfreeoff = offset;

	MEMSET((char *)blk + offset, mvsize);
	
	nextofftab = ROW_OFFSET_PTR(nextblk);
	
	for (j = 0; i < rowcnt; i++,j++)
	{
		nextofftab[-j] = thisofftab[-i] - offset + BLKHEADERSIZE;		
	}

	nextblk->bnextrno = j;
	nextblk->bfreeoff = mvsize + BLKHEADERSIZE;

	nextblk->bminlen = blk->bminlen;

	Assert((blk->bnextrno + nextblk->bnextrno) == rowcnt);

	return;
}


int
blk_backmov(BLOCK *blk)
{
	BLOCK	*nextblk;
	BLOCK	*tmpblk;

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

	nextblk = (BLOCK *) ((char *)nextblk - BLOCKSIZE);

	while ((nextblk > blk) || (nextblk == blk))
	{
		tmpblk = (BLOCK *) ((char *)nextblk+ BLOCKSIZE);

		
		Assert(   (tmpblk->bblkno != -1) 
		       && (tmpblk->bfreeoff == BLKHEADERSIZE));


		BLOCK_MOVE(tmpblk,nextblk);
		
		nextblk = (BLOCK *) ((char *)nextblk - BLOCKSIZE);
	}

	return TRUE;
}

int
blk_putrow()
{
	return 1;
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
	int	offset;
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
		if (BLK_GET_NEXT_ROWNO(bp->bblk) == 0)
		{
			
			if (   FALSE && (bp->bblk->bblkno == 0)
			    && (bp->bblk->bprevsstabnum == -1)
			    && (bp->bblk->bnextsstabnum == -1))
			{
				blkidx = 0;
			}
			break;
		}
		
		rp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, 0);
		offset = ROW_OFFSET_PTR(bp->bblk)[-(0)];

		key_in_blk = row_locate_col(rp, coloffset, bp->bblk->bminlen, 
					    &keylen_in_blk);

		result = row_col_compare(coltype, key, keylen, key_in_blk, 
							keylen_in_blk);


		
		if (   (result == EQ) && (tabinfo->t_sinfo) 
		    && (tabinfo->t_sinfo->sistate & SI_DEL_DATA))
		{
			blkidx = bp->bblk->bblkno;

			break;
		}

		
		if ((result == LE) || (   (result == EQ) 
				       && (tss->topid & TSS_OP_INSTAB)))
		{
			
			if (bp->bblk->bblkno == 0)
			{
				blkidx = 0;
			}
			
			break;
		}

		blkidx = bp->bblk->bblkno;

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

