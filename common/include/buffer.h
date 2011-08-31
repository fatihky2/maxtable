/*
** buffer.h 2010-10-19 xueyingfei
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
#ifndef BUFFER_H_
#define BUFFER_H_


typedef struct buf
{
	struct buf	*bdnew;
	struct buf	*bdold;
	long		bblkno;		
	short		bkeep;		
	short		bstat;		
	char		bsstab_name[256];
	int		bsstabid;
	int		btabid;
	struct block	*bblk;		
	struct buf	*bhash;		
	struct buf	*bsstabnew;		
	struct buf	*bsstabold;		
	struct buf	*bsstab;	
	int		bsstab_size;		
} BUF;



#define	BUF_WRITING	0x1	
#define	BUF_KEPT	0x2	
#define	BUF_IOERR	0x4	
#define	BUF_NOTHASHED	0x8	
#define	BUF_HASHED	0x10	
#define	BUF_DIRTY	0x20
#define	BUF_DESTROY	0x40
#define BUF_READ_EMPTY	0x80	


#define SSTABLE_STATE(bp)	(bp->bsstab->bstat)
#define	BUF_GET_SSTAB(bp)	(bp->bsstab)
#define BUFHASHSIZE		4096
#define BUFHASHMASK		(BUFHASHSIZE - 1)

#define BUFHASHINDEX(sstab,tabid)	(((tabid ^ (tabid << 8))^ (sstab ^ (sstab<<4))) & BUFHASHMASK)



#define BUFHASH(sstab,tabid)	(Kernel->ke_bufhash + BUFHASHINDEX(sstab,tabid))





#define	LRUUNLINK(bp)	        bp->bsstabold->bsstabnew = bp->bsstabnew, \
			        bp->bsstabnew->bsstabold = bp->bsstabold, \
			        bp->bsstabold = bp, bp->bsstabnew = bp

#define LRULINK(bp, hdr)	bp->bsstabold = hdr->bsstabold, \
				bp->bsstabnew = hdr, \
				hdr->bsstabold->bsstabnew = bp, \
				hdr->bsstabold = bp

#define MRULINK(bp, hdr)	bp->bsstabnew = hdr->bsstabnew, \
				bp->bsstabold = hdr, \
				hdr->bsstabnew->bsstabold = bp, \
				hdr->bsstabnew = bp


#define DIRTYLINK(bp, hdr)	bufdlink(bp, hdr)
#define DIRTYUNLINK(bp)		bufdunlink(bp)

struct tab_info;


void
bufread(BUF *bp);

void
bufwait(BUF *bp);

BUF *
bufgrab();

void
bufwrite(BUF *bp);

void
bufawrite(BUF *bp);

BUF *
bufsearch(struct tab_info *tabinfo);

int
bufhashsize();

BUF *
bufhash(BUF *bp);

void
bufunhash(BUF *bp);

void
bufdestroy(BUF *bp);

void
bufpredirty(BUF *bp);
 
void
bufdirty(BUF *bp);

void
buffree(BUF *bp);

void
bufdlink(BUF *bp);

void
bufdunlink(BUF *bp);

void
bufkeep(BUF *bp);

void
bufunkeep(BUF *bp);



#endif
