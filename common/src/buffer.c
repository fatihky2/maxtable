/*
** buffer.c 2010-10-19 xueyingfei
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
#include "global.h"
#include "memcom.h"
#include "master/metaserver.h"
#include "buffer.h"
#include "block.h"
#include "cache.h"
#include "dskio.h"
#include "spinlock.h"
#include "tss.h"
#include "hkgc.h"


extern KERNEL	*Kernel;
extern TSS	*Tss;




void
bufread(BUF *bp)
{
	BLKIO	*blkioptr;


	
	bufwait(bp);

	
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
		sleep(5);
	}

	if (SSTABLE_STATE(bp) & BUF_DIRTY)
	{
		bufawrite(bp);
	}

	if (SSTABLE_STATE(bp) & BUF_IOERR)
	{
		printf("IO error\n");
	}
}


BUF *
bufgrab(TABINFO *tabinfo)
{
	BUF	*bp;		
	

	bp = Kernel->ke_buflru->bsstabold;

	bufwait(bp);

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

	bufwait(bp);
}


void
bufawrite(BUF *bp)
{
	BLKIO		*blkioptr;


	printf(" Enter into the buffer writting.\n");
	//P_SPINLOCK(BUF_SPIN);

	if (!(SSTABLE_STATE(bp) & BUF_DIRTY))
	{
		//V_SPINLOCK(BUF_SPIN);
		printf("error!!!!\n");
		return;
	}
	
	SSTABLE_STATE(bp) |= BUF_WRITING;

	
	blkioptr = malloc(sizeof(BLKIO));

	
	blkioptr->bioflags = DBWRITE;
	blkioptr->biomaddr = (char *)(bp->bsstab->bblk);
	
	
	blkioptr->biosize = SSTABLE_SIZE;
	blkioptr->biobp = bp;

	
	if (!dstartio(blkioptr))
	{
		SSTABLE_STATE(bp) |= BUF_IOERR;
	}

	SSTABLE_STATE(bp) &= ~(BUF_DIRTY|BUF_WRITING);

	//V_SPINLOCK(BUF_SPIN);

	

	free(blkioptr);

	return;
}




BUF *
bufsearch(TABINFO *tabinfo)
{
	BUF	*bufptr;		
	BUF	**hashptr;		
	int	sstabno;
	int	tabid;


	sstabno = tabinfo->t_sstab_id;
	tabid= tabinfo->t_tabid;

	
	hashptr = BUFHASH(sstabno, tabid);

	

	P_SPINLOCK(BUF_SPIN);

	for (bufptr = *hashptr; bufptr; bufptr = bufptr->bhash)
	{
		if (   (bufptr->bsstabid == sstabno) 
		    && (bufptr->btabid = tabid))
		{
			V_SPINLOCK(BUF_SPIN);
			return (bufptr);
		}
	}

	
	V_SPINLOCK(BUF_SPIN);
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


	
	assert(!(SSTABLE_STATE(bp) & BUF_HASHED));	

	P_SPINLOCK(BUF_SPIN);

	
	hashptr = BUFHASH(bp->bsstabid, bp->btabid);

	
	if (*hashptr)
	{
		for (bufptr = *hashptr; bufptr; bufptr = bufptr->bhash)
		{
			if (   (bufptr->bblkno == bp->bblkno)
			    && (bufptr->bsstabid == bp->bsstabid)
			    && (bufptr->btabid == bp->btabid))
			{				
				V_SPINLOCK(BUF_SPIN);

				
				assert(0);
				return (bufptr);
			}
		}
		
		bp->bhash = *hashptr;
	}

	*hashptr = bp;

	
	SSTABLE_STATE(bp) &= ~BUF_NOTHASHED;
	SSTABLE_STATE(bp) |= BUF_HASHED;

	V_SPINLOCK(BUF_SPIN);

	
	return ((BUF *) NULL);
}



void
bufunhash(BUF *bp)
{
	BUF		**hashptr;	
	BUF		*bufptr;	
	BUF		**lastptr;	


	P_SPINLOCK(BUF_SPIN);

 	
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
			V_SPINLOCK(BUF_SPIN);
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

fail:
	V_SPINLOCK(BUF_SPIN);

	printf(" Block header error \n");

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
		;
		SSTABLE_STATE(bp) &= ~BUF_DIRTY;
	}

	P_SPINLOCK(BUF_SPIN);

	buffree(bp);

	V_SPINLOCK(BUF_SPIN);
}


void
bufpredirty(BUF *bp)
{
/*retry:	
	bufwait(bp);
	
	P_SPINLOCK(BUF_SPIN);
*/
	if (!(SSTABLE_STATE(bp) & BUF_DIRTY))
	{
		SSTABLE_STATE(bp) |= BUF_DIRTY;
	}
/*	else
	{
		V_SPINLOCK(BUF_SPIN);
		goto retry;
	}

	V_SPINLOCK(BUF_SPIN);
*/
	return;
}
void bufdirty(BUF *bp)
{	
	//assert(SSTABLE_STATE(bp) & BUF_DIRTY);
	if (!(SSTABLE_STATE(bp) & BUF_DIRTY))
	{
		SSTABLE_STATE(bp) |= BUF_DIRTY;
		put_io_list(BUF_GET_SSTAB(bp));
	}

	
	
	/*P_SPINLOCK(BUF_SPIN);

	if (SSTABLE_STATE(bp) & BUF_DIRTY)
	{
		bufdlink(BUF_GET_SSTAB(bp));
	}

	V_SPINLOCK(BUF_SPIN);*/

	return;
}


void
bufdlink(BUF *bp)
{
	LOCALTSS(tss);
	TABINFO	*tabinfo;

	tabinfo = tss->ttabinfo;
	
	
	if ((bp->bdnew == bp) && (bp->bdold == bp))
	{
		bp->bdold = tabinfo->t_dold;
		bp->bdnew = (BUF *) tabinfo;
		tabinfo->t_dnew->bdnew = bp;
		tabinfo->t_dold = bp;
	}

	return;
}

void
bufdunlink(BUF *bp)
{
	bp->bdnew->bdold = bp->bdold;
	bp->bdold->bdnew = bp->bdnew;
	bp->bdold = bp;
	bp->bdnew = bp;

	return;
}

void
buffree(BUF *bp)
{
	
	LRUUNLINK(bp->bsstab);

	
	LRULINK(bp->bsstab, Kernel->ke_buflru);

	bp->bsstabid = -1;

	
	SSTABLE_STATE(bp) &= ~(BUF_NOTHASHED | BUF_DESTROY | BUF_DIRTY | BUF_IOERR);

	return;
}


void
bufkeep(BUF *bp)
{
	P_SPINLOCK(bufkeep_mutex);
	
	LRUUNLINK(bp);
	
	
	MRULINK(bp, Kernel->ke_buflru);
	
	bp->bstat |= BUF_KEPT;

	V_SPINLOCK(bufkeep_mutex);
}

void
bufunkeep(BUF *bp)
{
	P_SPINLOCK(bufkeep_mutex);

	if (bp->bstat & (BUF_NOTHASHED | BUF_DESTROY))
	{
		buffree(bp);
	}
	else
	{
		
		LRUUNLINK(bp);

		
		MRULINK(bp, Kernel->ke_buflru);
	}

	bp->bstat &= ~BUF_KEPT;

	V_SPINLOCK(bufkeep_mutex);
}



