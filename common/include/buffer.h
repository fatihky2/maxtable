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


#define	BUF_WRITING	0x0001			
#define	BUF_KEPT	0x0002		
#define	BUF_IOERR	0x0004		
#define	BUF_NOTHASHED	0x0008		
#define	BUF_HASHED	0x0010		
#define	BUF_DIRTY	0x0020
#define	BUF_DESTROY	0x0040
#define BUF_READ_EMPTY	0x0080		
#define BUF_RESERVED	0x0100
#define BUF_IN_HKWASH	0x0200		
#define	BUF_IN_WASH	0x0400		


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


#define	BUF_GET_RESERVED(bp)					\
	do {							\
		if (Kernel->ke_bufresv.bufidx < BUF_RESV_MAX)	\
		{						\
			(bp) = (BLOCK*)(Kernel->ke_bufresv.bufresv[Kernel->ke_bufresv.bufidx]);	\
			(Kernel->ke_bufresv.bufidx)++;		\
		}						\
	}while (0)
	
#define	BUF_RELEASE_RESERVED(bp)				\
	do{							\
		Assert((void *)(bp) == Kernel->ke_bufresv.bufresv[Kernel->ke_bufresv.bufidx - 1]);	\
		(Kernel->ke_bufresv.bufidx)--;			\
		(bp) = NULL;					\
	}while (0)

#define	BUF_GET_LOGBUF(bp)					\
	do{							\
		(bp) = Kernel->ke_logbuf;			\
	}while(0)

#define	BUF_RELEASE_LOGBUF(bp)					\
	do{							\
		(bp) = NULL;					\
	}while(0)


#define	SSTAB_GET_RESERVED(sstab)						\
	do {									\
		if (Kernel->ke_sstabresv.sstabidx < SSTAB_RESV_MAX)		\
		{								\
			(sstab) = (char *)(Kernel->ke_sstabresv.sstabresv[Kernel->ke_sstabresv.sstabidx]);	\
			(Kernel->ke_sstabresv.sstabidx)++;			\
		}								\
	}while (0)
	
#define	SSTAB_RELEASE_RESERVED(sstab)						\
	do{									\
		Assert((void *)(sstab) == Kernel->ke_sstabresv.sstabresv[Kernel->ke_sstabresv.sstabidx - 1]);	\
		(Kernel->ke_sstabresv.sstabidx)--;				\
		(sstab) = NULL;							\
	}while (0)
		

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
