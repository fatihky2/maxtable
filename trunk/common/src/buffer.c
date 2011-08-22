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


extern KERNEL	*Kernel;
extern TSS	*Tss;
/*
**	1. one read, mult-write(HKGC and Server)
**
**	2. (dirty and writting)/(read and writting) may simultanious run
**
**	3. dirty and read only one at the same time
**
**	Example:
**			      CASE				EXIST
**
**		I	Reading + Writting				Yes
**		II	Reading + Dirty (DIRTY state has been set )	Yes
**		III	Reading + Reading				No
**		IV	Writting  + Writting
**		V	Writting + Dirty
*/


/* 
** Buffer reading doesn't need to get the lock, only check if it has a writting or
** dirty behavior. 
*/
void
bufread(BUF *bp)
{
	BLKIO	*blkioptr;


	/* Waiting for the completement of writing */
	bufwait(bp);

	/* get blkio structure to allow kernel to do the i/o */
	blkioptr = MEMALLOCHEAP(sizeof(BLKIO));

	/* fill in the blkio structure */
	blkioptr->bioflags = DBREAD;
	blkioptr->biomaddr = (char *)(bp->bsstab->bblk);
	blkioptr->biosize = SSTABLE_SIZE;
	blkioptr->biobp = bp;

	/* start the i/o */
	if (!dstartio(blkioptr))
	{
		SSTABLE_STATE(bp) |= BUF_IOERR;
	}

	MEMFREEHEAP(blkioptr);

	/* We need to initialize the block header if this block is the allocated firstly. */
	if (bp->bstat & BUF_READ_EMPTY)
	{
		bp->bblk->bfreeoff = BLKHEADERSIZE;
	}

	return;
}


/* buf_wait */
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
	BUF	*bp;		/* current buffer */
	

	bp = Kernel->ke_buflru->bsstabold;

	bufwait(bp);

	bufkeep(bp);

	bufunhash(bp);

	bp->btabid = tabinfo->t_tabid;

	if (tabinfo->t_stat & TAB_GET_RES_SSTAB)
	{
		bp->bsstabid = tabinfo->t_insmeta->res_sstab_id;
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
	/* Check if buffer is already being written */
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
	P_SPINLOCK(BUF_SPIN);

	if (!(SSTABLE_STATE(bp) & BUF_DIRTY))
	{
		V_SPINLOCK(BUF_SPIN);
		return;
	}
	
	SSTABLE_STATE(bp) |= BUF_WRITING;

	/* get blkio structure to allow kernel to do the i/o */
	blkioptr = MEMALLOCHEAP(sizeof(BLKIO));

	/* fill in the blkio structure */
	blkioptr->bioflags = DBWRITE;
	blkioptr->biomaddr = (char *)(bp->bsstab->bblk);
	
	/* Writing the whole sstable, and the last block save the block index. */
	blkioptr->biosize = SSTABLE_SIZE;
	blkioptr->biobp = bp;

	/* start the i/o */
	if (!dstartio(blkioptr))
	{
		SSTABLE_STATE(bp) |= BUF_IOERR;
	}

	SSTABLE_STATE(bp) &= ~(BUF_DIRTY|BUF_WRITING);

	V_SPINLOCK(BUF_SPIN);

	/* Exist the buffer writting. */

	MEMFREEHEAP(blkioptr);

	return;
}



/* We need to fix for the SSTABLE writting/reading */
BUF *
bufsearch(TABINFO *tabinfo)
{
	BUF	*bufptr;		/* ptr to buffer header */
	BUF	**hashptr;		/* array of buf struct ptrs */
	int	sstabno;
	int	tabid;


	sstabno = tabinfo->t_sstab_id;
	tabid= tabinfo->t_tabid;

	/* move to slot in hash table */
	hashptr = BUFHASH(sstabno, tabid);

	/* 
	** for each entry in overflow chain, compare dbid and
	** page looking for exact match
	*/

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

	/* page not found */
	V_SPINLOCK(BUF_SPIN);
	return (NULL);
}

int
bufhashsize()
{
	return (BUFHASHSIZE * sizeof (BUF *));
}


/* 
** Only if we need to read a buufer, we can hash this buffer, so we don't need to
** get the lock ?    -- NO, because if the wrtting has a I/O error, we must destroy
** this buffer or its neighbor buffer, we MUST get the lock for this hash.
*/
BUF *
bufhash(BUF *bp)
{
	LOCALTSS(tss);
	BUF	**hashptr;	/* array of hashed buf ptrs */
	BUF	*bufptr;


	if (tss->topid & TSS_OP_METASERVER)
	{
		return NULL;
	}
	
	/* make sure buffer not already hashed */
	assert(!(SSTABLE_STATE(bp) & BUF_HASHED));	

	P_SPINLOCK(BUF_SPIN);

	/* determine hash value and index to that entry in tbl */
	hashptr = BUFHASH(bp->bsstabid, bp->btabid);

	/* if entry there, insert current bufhdr in overflow chain */
	if (*hashptr)
	{
		for (bufptr = *hashptr; bufptr; bufptr = bufptr->bhash)
		{
			if (   (bufptr->bblkno == bp->bblkno)
			    && (bufptr->bsstabid == bp->bsstabid)
			    && (bufptr->btabid == bp->btabid))
			{				
				V_SPINLOCK(BUF_SPIN);

				/* bug issue: the buffer has been linked to the hash link. */
				assert(0);
				return (bufptr);
			}
		}
		
		bp->bhash = *hashptr;
	}

	*hashptr = bp;

	/* make sure bit saying buffer not hashed is off */
	SSTABLE_STATE(bp) &= ~BUF_NOTHASHED;
	SSTABLE_STATE(bp) |= BUF_HASHED;

	V_SPINLOCK(BUF_SPIN);

	/* return null to indicate success */
	return ((BUF *) NULL);
}


/* 
** Can we set its lock in the logic of its caller ? It doesn't make sense if we want to have a match 
** with bufhash().
*/
void
bufunhash(BUF *bp)
{
	BUF		**hashptr;	/* hashed array of buf ptrs */
	BUF		*bufptr;	/* ptr to buffer header */
	BUF		**lastptr;	/* hashed array of buf ptrs */


	P_SPINLOCK(BUF_SPIN);

 	/* 
 	** keep track in errorlog of cases where we may be silently doing
 	**  away with a 'trashed' page in the cache
	*/
 	if ((SSTABLE_STATE(bp) & BUF_HASHED) && (bp->bblkno != bp->bblk->bblkno))
	{
		goto fail;
	}


	/* determine hash value and index to that entry in hash tbl */
 	hashptr = BUFHASH(bp->bsstabid, bp->btabid);

	/* get first buffer ptr */
	bufptr = *hashptr;
	lastptr = hashptr;

	/* 
	**  if there are buf entries, search for one that matches
	**   exactly 
	*/
	while (bufptr)
	{
		/* if buf found unlink it and return */
		if (bufptr == bp)
		{
			*lastptr = bufptr->bhash;
			bufptr->bhash = NULL;
			bufptr->bstat &= ~BUF_HASHED;
			V_SPINLOCK(BUF_SPIN);
			return;
		}
		
		/* if not found, move to next buf and update parent ptr */
		else
		{
			lastptr = &bufptr->bhash;
			bufptr = bufptr->bhash;
		}
	}
	
	/* if not found but BUF_HASHED set we have a conflict */
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
	
	/* put the buffer in the destroy state */
	SSTABLE_STATE(bp) |= BUF_DESTROY;

	/* if the buffer is hashed, unhash it */
	if (SSTABLE_STATE(bp) & BUF_HASHED)
	{
		bufunhash(bp);
	}
	
	/* if the buffer is on a descriptor dirty chain, remove it and
	** take it out of the BUF_DIRTY or BW_OFFSET state.
	*/
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
retry:	
	bufwait(bp);
	
	P_SPINLOCK(BUF_SPIN);

	if (!(SSTABLE_STATE(bp) & BUF_DIRTY))
	{
		SSTABLE_STATE(bp) |= BUF_DIRTY;
	}
	else
	{
		V_SPINLOCK(BUF_SPIN);
		goto retry;
	}

	V_SPINLOCK(BUF_SPIN);

	return;
}


void
bufdirty(BUF *bp)
{	
	assert(SSTABLE_STATE(bp) & BUF_DIRTY);
	
	P_SPINLOCK(BUF_SPIN);

	if (SSTABLE_STATE(bp) & BUF_DIRTY)
	{
		bufdlink(BUF_GET_SSTAB(bp));
	}

	V_SPINLOCK(BUF_SPIN);

	return;
}


void
bufdlink(BUF *bp)
{
	LOCALTSS(tss);
	TABINFO	*tabinfo;

	tabinfo = tss->ttabinfo;
	
	/* if buffer is already linked, do nothing */
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
	/* unlink the buffer from wherever it currently is */
	LRUUNLINK(bp->bsstab);

	/* now move it to LRU side of chain in Resource */
	LRULINK(bp->bsstab, Kernel->ke_buflru);

	bp->bsstabid = -1;

	/* turn off the bit that got us here */
	SSTABLE_STATE(bp) &= ~(BUF_NOTHASHED | BUF_DESTROY | BUF_DIRTY | BUF_IOERR);
	return;
}


void
bufkeep(BUF *bp)
{
	
	/* take buffer out of place on LRU chain */
	LRUUNLINK(bp);
	
	/* ... and put buffer in the wash section */
	MRULINK(bp, Kernel->ke_buflru);
	
	bp->bstat |= BUF_KEPT;
}

void
bufunkeep(BUF *bp)
{
	if (bp->bstat & (BUF_NOTHASHED | BUF_DESTROY))
	{
		buffree(bp);
	}
	else
	{
		/* take buffer off kept chain */
		LRUUNLINK(bp);

		/* return buffer to lru chain */
		MRULINK(bp, Kernel->ke_buflru);
	}

	bp->bstat &= ~BUF_KEPT;
}



