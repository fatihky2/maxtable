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
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "cache.h"
#include "memcom.h"
#include "strings.h"
#include "utils.h"


extern	KERNEL	*Kernel;

void
ca_init_buf(BUF *bp, BLOCK *blk, BUF *sstab)
{
	bp->bblk = blk;
	bp->bsstab = sstab;
	bp->bsstab_size = BLK_CNT_IN_SSTABLE;

	
	bp->bhash = NULL;

	
	bp->bdnew = bp->bdold = bp;

//	bp->bsstabnew = bp->bsstabold = NULL;
}

void
ca_init_blk(BLOCK *blk, int blkno, int sstabid, int bp_cnt)
{
	MEMSET(blk, BLOCKSIZE);
	blk->bblkno = blkno;	

	blk->bnextblkno = (blkno < (bp_cnt-1)) ? (blkno + 1) : -1;
	
	blk->bfreeoff = BLKHEADERSIZE;
	blk->bnextrno = 0;
	blk->bstat = 0;
	blk->bsstabid = sstabid;
	blk->pad2[0] = 'y';
	blk->pad2[1] = 'x';
	blk->pad2[2] = 'u';
	blk->pad2[3] = 'e';	
}

void
ca_init_sstab(BUF *sstab, BLOCK *blk, int sstabid, int bp_cnt)
{
	int	numbuf;
	BUF	*bp;

	bp = sstab;
	for (numbuf = 0; numbuf < bp_cnt; numbuf++)
	{
		ca_init_buf(bp, blk, sstab);

		ca_init_blk(blk, numbuf, sstabid, bp_cnt);
		blk = (BLOCK *) ((char *)blk + BLOCKSIZE);
		bp++;
	}	
}

long
ca_get_struct_size()
{
	long		struct_size;	
	struct_size = 0;
	
	struct_size += bufhashsize();

	struct_size += sizeof (BUF);

	struct_size += (sizeof(BUF) * BLOCK_MAX_COUNT);

	return struct_size;
}


int
ca_setup_pool()
{
	char		*ca_memptr;
	BUF		*bp, *bp2, *sbufptr; 
	BLOCK		*blkptr;
	int		i,j, count;
	long		struct_size;	
	long		temp;


	struct_size = 0;
	
	
	ca_memptr = mem_os_malloc(BLOCK_CACHE_SIZE);
	MEMSET(ca_memptr, BLOCK_CACHE_SIZE);

	Kernel->ke_bufhash = (BUF **)ca_memptr;
	ca_memptr += bufhashsize();
	struct_size += bufhashsize();
	
	
	sbufptr = (BUF *) ca_memptr;
	ca_memptr += sizeof (BUF);
	struct_size += sizeof (BUF);

	
		
	bp = (BUF *) ca_memptr;
	ca_memptr += (sizeof(BUF) * BLOCK_MAX_COUNT);
	struct_size += (sizeof(BUF) * BLOCK_MAX_COUNT);

	temp = (BLOCKSIZE - (((long)ca_memptr) & ~BLOCKMASK));
	ca_memptr += temp;
	struct_size += temp;

	
	blkptr = (BLOCK *) ca_memptr;

	
	temp = struct_size/(sizeof (BLOCK)) + ((struct_size % (sizeof (BLOCK))) ? 1:0);

	//ca_memptr += (sizeof (BLOCK) * BLOCK_MAX_COUNT);

	
	Kernel->ke_buflru = sbufptr;

	
	Kernel->ke_bufwash = sbufptr;
	Kernel->ke_bufwash->bdnew = Kernel->ke_bufwash->bdold = Kernel->ke_bufwash;

	
	bp->bsstabold = sbufptr;
	sbufptr->bsstabnew = bp;

	
	bp2 = bp + BLK_CNT_IN_SSTABLE;

	count = BLOCK_MAX_COUNT - temp - 1;

	traceprint("BLOCK_MAX_COUNT : %d  --  Block # : %d  -- temp # : %ld\n", BLOCK_MAX_COUNT, count, temp);
	i = 0;
	j = 0;
	
	while(i < count)
	{
		i += BLK_CNT_IN_SSTABLE;
		if (i < count)
		{
			
			bp->bsstabnew = bp2;

			
			bp2->bsstabold = bp;	

			ca_init_sstab(bp, blkptr, j, BLK_CNT_IN_SSTABLE);

			j++;
			traceprint("-----------\n");
			traceprint("SSTABLE # :  %d\n", j);
			traceprint("Header of SSTABLE : 0x%x\n", bp);
			traceprint("bp->bsstabold : 0x%x\n", bp->bsstabold);
			traceprint("bp->bsstabnew : 0x%x\n", bp->bsstabnew);
			traceprint("bp->bblk : 0x%x\n", bp->bblk);
			bp += BLK_CNT_IN_SSTABLE;
			bp2 += BLK_CNT_IN_SSTABLE;
			blkptr += BLK_CNT_IN_SSTABLE;		
		}
		else
		{
			//Assert(temp == (i - count - 1));
			
			
			bp->bsstabnew = sbufptr;
			sbufptr->bsstabold = bp;
			ca_init_sstab(bp, blkptr, j, (BLK_CNT_IN_SSTABLE - (i - count)));

			j++;
			traceprint("-----------\n");
			traceprint("SSTABLE # :  %d\n", j);
			traceprint("Header of SSTABLE : 0x%x\n", bp);
			traceprint("bp->bsstabold : 0x%x\n", bp->bsstabold);
			traceprint("bp->bsstabnew : 0x%x\n", bp->bsstabnew);
			traceprint("bp->bblk : 0x%x\n", bp->bblk);
		}
	}
	
	

	traceprint("-----------\n");

	traceprint("Header of SSTABLE : 0x%x\n", sbufptr);
	traceprint("sbufptr->bsstabold : 0x%x\n", sbufptr->bsstabold);
	traceprint("sbufptr->bsstabnew : 0x%x\n", sbufptr->bsstabnew);
	traceprint("sbufptr->bblk : 0x%x\n", sbufptr->bblk);

	return TRUE;
}

static void
ca_prt_sstab(BUF *bp)
{
	BLOCK	*blk;


	blk = bp->bblk;
	traceprint("Printing every block\n");
	while(blk->bblkno != -1)
	{
		traceprint("BLOCK no = %d\n",blk->bblkno);
		traceprint("BLOCK NextBlock no = %d\n",blk->bnextblkno);
		traceprint("BLOCK ADDRESS = 0x%x\n", blk);

		if (blk->bnextblkno != -1)
		{
			blk = (BLOCK *) ((char *)blk + BLOCKSIZE);
		}
		else
		{
			break;
		}
	}	
}


void
ca_prt_bp()
{
	BUF	*tempbp;
	BUF	*bp;
	int	i,j;

	
	tempbp = Kernel->ke_buflru;

	traceprint("Header of Kernel->ke_buflru (dummy buffer) 0x%x\n", tempbp);

	i = j = 0;
	bp = tempbp->bsstabnew;
	
	while((i < BLOCK_MAX_COUNT) && bp && (bp != tempbp))
	{	
		j++;
		traceprint("SSTABLE # :  %d\n", j);
		traceprint("Header of SSTABLE : 0x%x\n", bp);
		traceprint("bp->bsstabnew : 0x%x\n", bp->bsstabnew);
		traceprint("bp->bsstabold : 0x%x\n", bp->bsstabold);
		traceprint("bp->bblk : 0x%x\n", bp->bblk);

		ca_prt_sstab(bp);
		
		i += BLK_CNT_IN_SSTABLE;
		bp = bp->bsstabnew;
	}

	return;
}

void
ca_prt_cache()
{
	BUF	*tempbp;
	BUF	*bp;
	int	i,j;

	
	tempbp = Kernel->ke_buflru;

	traceprint("Header of Kernel->ke_buflru (dummy buffer) 0x%x\n", tempbp);

	i = j = 0;
	bp = tempbp->bsstabnew;
	
	while((i < BLOCK_MAX_COUNT) && bp && (bp != tempbp))
	{	
		j++;
		traceprint("SSTABLE # :  %d\n", j);
		traceprint("Header of SSTABLE : 0x%x\n", bp);
		traceprint("bp->bkeep : 0x%x\n", bp->bkeep);
		traceprint("bp->bstat : 0x%x\n", bp->bstat);
		traceprint("bp->bsstabnew : 0x%x\n", bp->bsstabnew);
		traceprint("bp->bsstabold : 0x%x\n", bp->bsstabold);
		traceprint("bp->bblk : 0x%x\n", bp->bblk);
		traceprint("bp->bsstab : %d\n", bp->bsstab);
		traceprint("bp->bblk->bsstabnum : 0x%x\n", bp->bblk->bsstabnum);
		traceprint("bp->bblk->bprevsstabnum : 0x%x\n", bp->bblk->bprevsstabnum);
		traceprint("bp->bblk->bnextsstabnum : 0x%x\n", bp->bblk->bnextsstabnum);
		
		
		i += BLK_CNT_IN_SSTABLE;
		bp = bp->bsstabnew;
	}

	return;
}

void
ca_prt_dirty()
{
	BUF	*tempbp;
	BUF	*bp;
	int	i,j;
	
	tempbp = Kernel->ke_bufwash;

	traceprint("Header of Kernel->ke_bufwash (dummy buffer) 0x%x\n", tempbp);

	i = j = 0;
	bp = tempbp->bdnew;
	
	while((i < BLOCK_MAX_COUNT) && bp && (bp != tempbp))
	{	
		j++;
		traceprint("SSTABLE # :  %d\n", j);
		traceprint("Header of SSTABLE : 0x%x\n", bp);
		traceprint("bp->bkeep : 0x%x\n", bp->bkeep);
		traceprint("bp->bstat : 0x%x\n", bp->bstat);
		traceprint("bp->bdnew : 0x%x\n", bp->bdnew);
		traceprint("bp->bdold : 0x%x\n", bp->bdold);
		traceprint("bp->bblk : 0x%x\n", bp->bblk);
		
		
		i += BLK_CNT_IN_SSTABLE;
		bp = bp->bdnew;
	}

	return;
}

void
ca_prt_hash()
{
	BUF	**hashptr;	
	BUF	*bp;
	int	i;
	
	hashptr = Kernel->ke_bufhash;

	traceprint("Header of Kernel->ke_bufhash 0x%x\n", hashptr);

	i = 0;
		
	while(i < (BUFHASHSIZE - 1))
	{	
		for (bp = *(hashptr + i); bp; bp = bp->bhash)
		{
			traceprint("Hash Index # :  %d\n", i);
			traceprint("Header of SSTABLE : 0x%x\n", bp);
			traceprint("bp->bkeep : 0x%x\n", bp->bkeep);
			traceprint("bp->bstat : 0x%x\n", bp->bstat);
			traceprint("bp->bhash : 0x%x\n", bp->bhash);
			traceprint("bp->bblk : 0x%x\n", bp->bblk);
			
		}

		i++;
	}

	return;
}



