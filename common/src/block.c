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
#include "buffer.h"
#include "block.h"
#include "cache.h"
#include "row.h"
#include "strings.h"
#include "utils.h"
#include "tss.h"
#include "sstab.h"
#include "tablet.h"


extern TSS	*Tss;


#define	BLK_BUF_NEED_CHANGE	0x0001
#define BLK_ROW_NEXT_SSTAB	0x0002	
#define BLK_ROW_NEXT_BLK	0x0004



BUF *
blkget(TABINFO *tabinfo)
{
	BUF	*lastbp;
	int     blkidx;


	lastbp = blk_getsstable(tabinfo);

	assert(lastbp);

	if (0 && BLOCK_IS_EMPTY(lastbp))
	{
		tabinfo->t_rowinfo->rblknum = lastbp->bblk->bblkno;
		tabinfo->t_rowinfo->roffset = BLKHEADERSIZE;
		tabinfo->t_rowinfo->rsstabid = lastbp->bblk->bsstabid;
		
		
		return lastbp->bsstab;
	}

	

	
	tabinfo->t_sinfo->sistate |= SI_INDEX_BLK;
	blkidx = blksrch(tabinfo, lastbp);

	assert(blkidx < BLK_CNT_IN_SSTABLE);

	tabinfo->t_sinfo->sistate &= ~SI_INDEX_BLK;

	return (lastbp->bsstab + blkidx);

	

}


int
blksrch(TABINFO *tabinfo, BUF *bp)
{
	LOCALTSS(tss);
	int	*offset;
	BLOCK	*blk;
	char	*rp;
	int	last_offset;
	int	rowno;
	int	keylen_in_blk;
	int	coloffset;
	int	minrowlen;
	char    *key_in_blk;
	char    *key;
	int     keylen;
	int     coltype;
	int     blkidx;
	int     result;
	int	match;
	int	end_row_offset;


	coloffset = tabinfo->t_sinfo->sicoloff;
	key = tabinfo->t_sinfo->sicolval;
	coltype = tabinfo->t_sinfo->sicoltype;
	keylen = tabinfo->t_sinfo->sicollen;	
	blkidx = -1;
	
	
	if (BLK_GET_NEXT_ROWNO(bp) == 0)
	{
		blkidx = bp->bblk->bblkno;
		last_offset = bp->bblk->bfreeoff;
		goto finish;
	}


srch_again:	
	blk		= bp->bblk;
	minrowlen 	= blk->bminlen;
	blkidx 		= -1;
	match 		= LE;	

	for(rowno = 0, offset = ROW_OFFSET_PTR(bp->bblk); 
			rowno < BLK_GET_NEXT_ROWNO(bp);	rowno++, offset--)
	{
		rp = (char *)(bp->bblk) + *offset;

		assert(*offset < bp->bblk->bfreeoff);

		key_in_blk = row_locate_col(rp, coloffset, minrowlen, 
						&keylen_in_blk);

        	result = row_col_compare(coltype, key, keylen, key_in_blk, 
				keylen_in_blk);

		
		if (tabinfo->t_stat & TAB_SCHM_SRCH)
		{
			if ((result == GR) || (result == EQ) || (rowno == 0))
			{
				last_offset = *offset;
			}

			if (result == LE)
			{
				if (   (tabinfo->t_sstab_id == 1) && (rowno == 0) 
				    && (bp->bblk->bblkno == 0))
				{
					assert(last_offset == *offset);
					
					tabinfo->t_stat |= TAB_TABLET_KEYROW_CHG;
				}
				
				break;
			}				
		}
		else
		{
			last_offset = *offset;
		}
		
	        if ((result == EQ) || ((tabinfo->t_sinfo->sistate & SI_INS_DATA) && (result == LE)))
	        {
	        	if (   (result == EQ) && (tabinfo->t_stat & TAB_SRCH_DATA) 
			    && (tss->topid & TSS_OP_RANGESERVER))
	        	{
	        		
	        		if (ROW_IS_DELETED(rp))
	        		{
	        			assert((bp->bblk->bblkno == 0) && (*offset == BLKHEADERSIZE));

					
					result = LE;
	        			continue;
	        		}
	        		
	        	}
			
	                break;
	        }

	//	blkidx = ROW_GET_ROWNO(rp); 
	}

	
	if ((result != EQ) && !(tabinfo->t_stat & TAB_SCHM_SRCH))
	{
		tabinfo->t_sinfo->sistate |= SI_NODATA;
	}

	blkidx = bp->bblk->bblkno;
	
	

	if (rowno == BLK_GET_NEXT_ROWNO(bp))
	{
		if (!(tabinfo->t_stat & TAB_SCHM_SRCH))
		{
			
			last_offset += ROW_GET_LENGTH(rp, minrowlen);
		}
		
		if ((bp->bblk->bnextblkno != -1) && ((tabinfo->t_stat & TAB_SRCH_DATA)
			|| (tabinfo->t_sinfo->sistate & SI_INDEX_BLK)
			|| (tabinfo->t_stat & TAB_INS_DATA)
			|| (tabinfo->t_stat & TAB_SCHM_INS)
			|| (tabinfo->t_stat & TAB_SCHM_SRCH)))
		{
			bp++;

			assert(bp->bblk->bblkno != -1);

			
			if (bp->bblk->bfreeoff != BLKHEADERSIZE)
			{
				tabinfo->t_sinfo->sistate &= ~SI_NODATA;
				end_row_offset = last_offset;
				goto srch_again;
			}
		}
	}
	else if ((rowno == 0) && (result == LE))
	{
		assert(last_offset == BLKHEADERSIZE);

		
		if ((bp->bblk->bblkno != 0) && (bp->bblk->bfreeoff > BLKHEADERSIZE))
		{
			bp--;
			
			
			last_offset = end_row_offset;

			blkidx = bp->bblk->bblkno;
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

	printf("tabinfo->t_sstab_name = %s \n", tabinfo->t_sstab_name);

	if (tabinfo->t_stat & TAB_KEPT_BUF_VALID)
	{
		assert(tabinfo->t_keptbuf);

		
		return tabinfo->t_keptbuf;
	}

	if ((bp = bufsearch(tabinfo)) == NULL)
	{
		bp = bufgrab(tabinfo);

		
		bufhash(bp);

		
		blk_init(bp->bblk);
	
		sstab_name = tabinfo->t_sstab_name;
		
		MEMSET(bp->bsstab_name, 256);
		MEMCPY(bp->bsstab_name, sstab_name, STRLEN(sstab_name));	
	
		bp->bstat &= ~BUF_READ_EMPTY;
		
		if (   ((tss->topid & TSS_OP_RANGESERVER) && (tabinfo->t_insmeta->status & INS_META_1ST))
		    || ((tss->topid & TSS_OP_METASERVER) && (tabinfo->t_stat & TAB_CRT_NEW_FILE)))
		{
			bp->bstat |= BUF_READ_EMPTY;
		}
		
		bufread(bp);

	}

	if (SSTABLE_STATE(bp) != BUF_IOERR)
	{
		//return (bp + BLK_CNT_IN_SSTABLE - 1);
		return bp;
	}

	return NULL;
}


int
blkins(TABINFO *tabinfo, char *rp)
{
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

	change_status(bp, NONKEPT, KEPT);
	//bufpredirty(bp);

//	offset = blksrch(tabinfo, bp);

	assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);
	offset = tabinfo->t_rowinfo->roffset;

	ign = 0;
	rlen = ROW_GET_LENGTH(rp, minlen);

	blk_stat = blk_check_sstab_space(tabinfo, bp, rp, rlen, offset);

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
		
		BACKMOVE((char *)bp->bblk + offset, (char *)bp->bblk + offset + rlen, 
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
	
		

	PUT_TO_BUFFER((char *)bp->bblk + offset, ign, rp, rlen);

	if (bp->bblk->bfreeoff == offset)
	{
		
		ROW_SET_OFFSET(bp->bblk, BLK_GET_NEXT_ROWNO(bp), offset);
	}
	


	bp->bblk->bfreeoff += rlen;

	bp->bblk->bminlen = minlen;
	
	BLK_GET_NEXT_ROWNO(bp)++;

finish:

	bufdirty(bp);
		
	tabinfo->t_sinfo->sistate &= ~SI_INS_DATA;

	change_status(bp, KEPT, NONKEPT);

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

	change_status(bp, NONKEPT, KEPT);
	//bufpredirty(bp);

//	offset = blksrch(tabinfo, bp);

	assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);
	offset = tabinfo->t_rowinfo->roffset;

	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		printf("We can not find the row to be deleted.\n");	
		return FALSE;
	}

	rp = (char *)(bp->bblk) + offset;
	rlen = ROW_GET_LENGTH(rp, minlen);

	if (   (tss->topid & TSS_OP_RANGESERVER) && (bp->bblk->bblkno == 0) 
	    && (offset == BLKHEADERSIZE))
	{
		
		ROW_SET_STATUS(rp, ROW_DELETED);

		
		

		goto finish;
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


	bp->bblk->bfreeoff -= rlen;
	
	BLK_GET_NEXT_ROWNO(bp)--;

finish:
	bufdirty(bp);
		
	tabinfo->t_sinfo->sistate &= ~SI_DEL_DATA;

	change_status(bp, KEPT, NONKEPT);

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

	
	if ((blk->bfreeoff - offset) && blk_check_sstab_space(NULL, NULL, rp, rlen, 0))
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
blk_check_sstab_space(TABINFO *tabinfo, BUF *bp, char *rp, int rlen, int ins_offset)
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

	if ((blk->bfreeoff + rlen) < (BLOCKSIZE - BLK_TAILSIZE - (ROW_OFFSET_ENTRYSIZE * (row_cnt + 1))))
	{
		rtn_stat &= ~BLK_BUF_NEED_CHANGE;

		return rtn_stat;
	}
	
	while(1)
	{
		
		if ((blk->bfreeoff + rlen) < (BLOCKSIZE - BLK_TAILSIZE - (ROW_OFFSET_ENTRYSIZE * (row_cnt + 1))))
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

	assert((blk->bnextblkno!= -1) && (nextblk->bfreeoff == BLKHEADERSIZE)
		&& (rowcnt > 1));

	
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

	assert((blk->bnextrno + nextblk->bnextrno) == rowcnt);

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

	while ((nextblk->bnextblkno!= -1) && (nextblk->bfreeoff != BLKHEADERSIZE))
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

		assert((tmpblk->bblkno != -1) && (tmpblk->bfreeoff == BLKHEADERSIZE));


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

