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
#include "memcom.h"
#include "master/metaserver.h"
#include "tabinfo.h"
#include "utils.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "cache.h"
#include "dskio.h"
#include "spinlock.h"
#include "tss.h"
#include "hkgc.h"


extern KERNEL	*Kernel;
extern TSS	*Tss;

int	bufwrite_cnt = 0;






void
bufread(BUF *bp)
{
	BLKIO	*blkioptr;

	
	blkioptr = MEMALLOCHEAP(sizeof(BLKIO));

	
	blkioptr->bioflags = DBREAD;
	blkioptr->biomaddr = (char *)(bp->bsstab->bblk);
	blkioptr->biosize = SSTABLE_SIZE;
	blkioptr->biobp = bp;

	
	if (!dstartio(blkioptr))
	{
		SSTABLE_STATE(bp) |= BUF_IOERR;
	}

	MEMFREEHEAP(blkioptr);

	
	if (bp->bstat & BUF_READ_EMPTY)
	{
		bp->bblk->bfreeoff = BLKHEADERSIZE;
	}

	return;
}



void
bufwait(BUF *bp)
{
	
	while (SSTABLE_STATE(bp) & BUF_WRITING)
	{
		sleep(2);
	}

	if (SSTABLE_STATE(bp) & BUF_IOERR)
	{
		traceprint("IO error\n");
	}

}


BUF *
bufgrab(TABINFO *tabinfo)
{
	LOCALTSS(tss);
	BUF	*bp;
	int	bp_res_cnt;
	BUF	*tmpbp;


	bp_res_cnt = 0;
	tmpbp = Kernel->ke_buflru->bsstabold;
retry:
	bp = Kernel->ke_buflru->bsstabold;

	if (SSTABLE_STATE(bp) & BUF_KEPT)
	{
		if (bp->bsstabold == tmpbp)
		{
			traceprint("All the caches have been kept!\n");
			Assert(0);
		}
		LRUUNLINK(bp);

		MRULINK(bp, Kernel->ke_buflru);
		goto retry;
	}
	
	if (SSTABLE_STATE(bp) & BUF_RESERVED)
	{
		LRUUNLINK(bp);

		MRULINK(bp, Kernel->ke_buflru);

		bp_res_cnt++;

		
		if (bp_res_cnt > 6)
		{
			traceprint("buffer pool must not exceed 6 reserved buffer\n ");
		}
		
		goto retry;
	}

	bufwait(bp);

	
	if (SSTABLE_STATE(bp) & BUF_DIRTY)
	{
		if (tss->topid & TSS_OP_RANGESERVER)
		{
			hkgc_wash_sstab(TRUE);
		}
		else
		{
			DIRTYUNLINK(bp);
			bufwrite(bp);
		}
	}
	
	bufkeep(bp);

	bufunhash(bp);

	bp->btabid = tabinfo->t_tabid;

	if (tabinfo->t_stat & TAB_GET_RES_SSTAB)
	{
		bp->bsstabid = tabinfo->t_insmeta->res_sstab_id;
	}
	else if (tabinfo->t_stat & TAB_TABLET_SPLIT)
	{
		bp->bsstabid = tabinfo->t_split_tabletid;
	}
	else
	{
		bp->bsstabid = tabinfo->t_sstab_id;
	}

	return (bp);
}

void
bufwrite(BUF *bp)
{
	
	if (!(SSTABLE_STATE(bp) & BUF_WRITING))
	{
		bufawrite(bp);
	}
}


void
bufawrite(BUF *bp)
{
	LOCALTSS(tss);
	BLKIO		*blkioptr;
	

	if (DEBUG_TEST(tss))
	{
		traceprint(" Enter into the buffer writting.\n");
	}
	
//	P_SPINLOCK(BUF_SPIN);

	if (!(SSTABLE_STATE(bp) & BUF_DIRTY))
	{
//		V_SPINLOCK(BUF_SPIN);
		return;
	}
	
	SSTABLE_STATE(bp) |= BUF_WRITING;

//	V_SPINLOCK(BUF_SPIN);

	
	blkioptr = MEMALLOCHEAP(sizeof(BLKIO));

	
	blkioptr->bioflags = DBWRITE;
	blkioptr->biomaddr = (char *)(bp->bsstab->bblk);
	
	
	blkioptr->biosize = SSTABLE_SIZE;
	blkioptr->biobp = bp;

	
	if (!dstartio(blkioptr))
	{
		SSTABLE_STATE(bp) |= BUF_IOERR;
	}

	SSTABLE_STATE(bp) &= ~(BUF_DIRTY|BUF_WRITING);

	MEMFREEHEAP(blkioptr);

	bufwrite_cnt++;

	return;
}



BUF *
bufsearch(TABINFO *tabinfo)
{
	BUF	*bufptr;			
	BUF	**hashptr;				
	int	sstabno;
	int	tabid;


	tabid= tabinfo->t_tabid;

	if (tabinfo->t_stat & TAB_GET_RES_SSTAB)
	{
		Assert(   tabinfo->t_insmeta->res_sstab_id 
		       && (   tabinfo->t_insmeta->res_sstab_id 
		           != tabinfo->t_sstab_id));
		       
		sstabno = tabinfo->t_insmeta->res_sstab_id;
	}
	else if (tabinfo->t_stat & TAB_TABLET_SPLIT)
	{
		sstabno = tabinfo->t_split_tabletid;
	}
	else
	{
		sstabno = tabinfo->t_sstab_id;
	}

	
	hashptr = BUFHASH(sstabno, tabid);

	
//	P_SPINLOCK(BUF_SPIN);

	
	for (bufptr = *hashptr; bufptr; bufptr = bufptr->bhash)
	{
		if (   (bufptr->bsstabid == sstabno) 
		    && (bufptr->btabid = tabid))
		{			
//			V_SPINLOCK(BUF_SPIN);
			bufkeep(bufptr);

			bufwait(bufptr);

			return (bufptr);
		}
	}

	
//	V_SPINLOCK(BUF_SPIN);
	return (NULL);
}

int
bufhashsize()
{
	return (BUFHASHSIZE * sizeof (BUF *));
}


BUF *
bufhash(BUF *bp)
{
	BUF	**hashptr;	
	BUF	*bufptr;


	
	Assert(!(SSTABLE_STATE(bp) & BUF_HASHED));	

//	P_SPINLOCK(BUF_SPIN);

	
	hashptr = BUFHASH(bp->bsstabid, bp->btabid);

	
	if (*hashptr)
	{
		for (bufptr = *hashptr; bufptr; bufptr = bufptr->bhash)
		{
			if (   (bufptr->bblkno == bp->bblkno)
			    && (bufptr->bsstabid == bp->bsstabid)
			    && (bufptr->btabid == bp->btabid))
			{				
//				V_SPINLOCK(BUF_SPIN);

				
				Assert(0);
				return (bufptr);
			}
		}
		
		bp->bhash = *hashptr;
	}

	*hashptr = bp;

	
	SSTABLE_STATE(bp) &= ~BUF_NOTHASHED;
	SSTABLE_STATE(bp) |= BUF_HASHED;

//	V_SPINLOCK(BUF_SPIN);

	
	return ((BUF *) NULL);
}


void
bufunhash(BUF *bp)
{
	BUF		**hashptr;	
	BUF		*bufptr;	
	BUF		**lastptr;	


//	P_SPINLOCK(BUF_SPIN);

 	
 	
 	if ((SSTABLE_STATE(bp) & BUF_HASHED) && (bp->bblkno != bp->bblk->bblkno))
	{
		goto fail;
	}

	
 	hashptr = BUFHASH(bp->bsstabid, bp->btabid);

	
	bufptr = *hashptr;
	lastptr = hashptr;

	
	while (bufptr)
	{
		
		if (bufptr == bp)
		{
			*lastptr = bufptr->bhash;
			bufptr->bhash = NULL;
			bufptr->bstat &= ~BUF_HASHED;
//			V_SPINLOCK(BUF_SPIN);
			return;
		}		
		
		else
		{
			lastptr = &bufptr->bhash;
			bufptr = bufptr->bhash;
		}
	}
	
	
	if (SSTABLE_STATE(bp) & BUF_HASHED)
	{
		goto fail;				
	}
	else
	{
		return;
	}

fail:
//	V_SPINLOCK(BUF_SPIN);

	traceprint(" Block header error \n");

	return;
}


void
bufdestroy(BUF *bp)
{
	while(SSTABLE_STATE(bp) & BUF_WRITING)
	{
		sleep(5);
	}
	
	
	SSTABLE_STATE(bp) |= BUF_DESTROY;

	
	if (SSTABLE_STATE(bp) & BUF_HASHED)
	{
		bufunhash(bp);
	}
	
	
	if (SSTABLE_STATE(bp) & BUF_DIRTY)
	{
		SSTABLE_STATE(bp) &= ~BUF_DIRTY;
	}

//	P_SPINLOCK(BUF_SPIN);

	buffree(bp);

//	V_SPINLOCK(BUF_SPIN);
}


void
bufpredirty(BUF *bp)
{
//	P_SPINLOCK(BUF_SPIN);

	if (!(SSTABLE_STATE(bp) & BUF_DIRTY))
	{
		SSTABLE_STATE(bp) |= BUF_DIRTY;
	}

//	V_SPINLOCK(BUF_SPIN);

	return;
}


void
bufdirty(BUF *bp)
{	
	Assert(   (SSTABLE_STATE(bp) & BUF_DIRTY) 
	       && (SSTABLE_STATE(bp) & BUF_KEPT));

//	P_SPINLOCK(BUF_SPIN);

	if (   (SSTABLE_STATE(bp) & BUF_DIRTY) 
	    && !(SSTABLE_STATE(bp) & BUF_IN_WASH))
	{
		bufdlink(BUF_GET_SSTAB(bp));
	}

//	V_SPINLOCK(BUF_SPIN);

	return;
}


void
bufdlink(BUF *bp)
{
#ifdef	MT_ASYNC_IO

	
	if ((bp->bdnew == bp) && (bp->bdold == bp))
	{
		bp->bdold = Kernel->ke_bufwash->bdold;
		bp->bdnew = Kernel->ke_bufwash;
		Kernel->ke_bufwash->bdold->bdnew = bp;
		Kernel->ke_bufwash->bdold = bp;
		bp->bstat |= BUF_IN_WASH;
	}
#else
	LOCALTSS(tss);
	TABINFO	*tabinfo;

	tabinfo = tss->ttabinfo;
	
	
	if ((bp->bdnew == bp) && (bp->bdold == bp))
	{
		bp->bdold = tabinfo->t_dold;
		bp->bdnew = (BUF *) tabinfo;
		tabinfo->t_dold->bdnew = bp;
		tabinfo->t_dold = bp;
	}
#endif
	return;
}

void
bufdunlink(BUF *bp)
{
	bp->bdnew->bdold = bp->bdold;
	bp->bdold->bdnew = bp->bdnew;
	bp->bdold = bp;
	bp->bdnew = bp;

	bp->bstat &= ~BUF_IN_WASH;

	return;
}

void
buffree(BUF *bp)
{
	
	LRUUNLINK(bp->bsstab);
	
	
	LRULINK(bp->bsstab, Kernel->ke_buflru);

	bp->bsstabid = -1;

	
	SSTABLE_STATE(bp) &= ~(BUF_NOTHASHED | BUF_DESTROY | BUF_DIRTY 
				| BUF_IOERR | BUF_KEPT);

	bp->bkeep = 0;
	
	return;
}


void
bufkeep(BUF *bp)
{
	
	LRUUNLINK(bp);
	
	
	MRULINK(bp, Kernel->ke_buflru);

	bp->bkeep++;
	
	bp->bstat |= BUF_KEPT;
}

void
bufunkeep(BUF *bp)
{
	LOCALTSS(tss);
	
	if (bp->bstat & (BUF_NOTHASHED | BUF_DESTROY))
	{
		buffree(bp);
	}
	else
	{
		
		LRUUNLINK(bp);

		
		MRULINK(bp, Kernel->ke_buflru);
	}

	
	if (tss->topid & TSS_OP_RANGESERVER)
	{
		Assert(bp->bkeep > 0);
	}
	
	if (bp->bkeep > 0)
	{
		bp->bkeep--;
	}
	
	if (bp->bkeep == 0)
	{
		bp->bstat &= ~BUF_KEPT;
	}
}



