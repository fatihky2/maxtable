/*
** block.h 2010-10-08 xueyingfei
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
#ifndef BLOCK_H_
#define BLOCK_H_


/* Size of 'BLOCK' structure. */
#define	BLKHEADERSIZE	40

/* The size of one block. */
//#define	BLOCKSIZE		(64 * 1024)			/* 64K */
#define	BLOCKSIZE		(512)


/* # of block in one sstable. */
#define	BLK_CNT_IN_SSTABLE	16

/* The size of one sstable */
#define	SSTABLE_SIZE		(BLK_CNT_IN_SSTABLE * BLOCKSIZE)/* 1M  */

/* Max # of sstable in the current system */
#define	SSTABLE_MAX_COUNT	16

/* Max # of block in the current system */
//#define	BLOCK_MAX_COUNT		(16 * SSTABLE_MAX_COUNT + 4)	/* 256 block */
#define	BLOCK_MAX_COUNT		(16 * SSTABLE_MAX_COUNT + 40 + 256 + 40)	/* 256 block */	

/* Max size of cache */
#define	BLOCK_CACHE_SIZE	((BLOCK_MAX_COUNT) * BLOCKSIZE)	/* 256 * 64K = 16M */

#define	BLOCKMASK		(~(BLOCKSIZE - 1))


typedef	struct block
{
	char		pad2[4];
	
	int		bblkno;		/* logical blk # of this block */
	int		bnextblkno;	/* next block no, -1 is the temenal flag. */
	int		bsstabid;	/* SSTAB id */
	int		btabid;		/* TABLE id */
	unsigned int	bts_lo;		/* low value of block timestamp */
	int		bnextrno;	/* if data, next free row number */
	unsigned int    bts_hi;	 	/* high order of block timestamp value */
	int		bfreeoff; 	/* first unused byte */
	short		bstat; 		/* type of block, etc. */
	short		bminlen; 	/* row length */
	char		bdata[BLOCKSIZE - BLKHEADERSIZE - 4]; /* actual useful data */
	int		boffsets[1];	/* negative growing offset table */
}BLOCK;


/* Following definitions are  for bstat */
#define	BLK_TABLET_SCHM		0x0001	/* In the current implementation tablet_schme only has one file with size 1 M. */


#define	ROW_OFFSET_ENTRYSIZE	sizeof(int)
#define	BLK_TAILSIZE		sizeof(time)	/* save timestamp */
#define	BLK_GET_NEXT_ROWNO(bp)	(bp->bblk->bnextrno)


#define	ROW_OFFSET_PTR(blkptr)	((int *) (((char *)(blkptr)) +		\
                  (BLOCKSIZE - BLK_TAILSIZE - ROW_OFFSET_ENTRYSIZE)))

#define ROW_SET_OFFSET(blkptr, rnum, offset)	\
	(ROW_OFFSET_PTR(blkptr))[-((int)(rnum))] = (offset)

		   
#define BLOCK_IS_EMPTY(bp)	(bp->bblk->bfreeoff == BLKHEADERSIZE)


typedef struct srch_info
{
	int	sicoltype;
	char	*sicolval;
	int	sicollen;
	int	sicolid;
	int	sicoloff;
	int	sistate;
}SINFO;

/* Define for sistate*/
#define	SI_INDEX_BLK	0x00000001	/* Flag if this is an index block in the ranger server
									** curren implementation will not use the index stratagy 
									** in the ranger server.
									*/
#define	SI_DATA_BLK		0x00000002
#define	SI_INS_DATA		0x00000004	/* Flag it's an insertion behavior.*/
#define	SI_NODATA		0x00000008	/* We can not get the needed data. */
#define SI_DEL_DATA		0x00000010

#define SRCH_INFO_INIT(srch_info, key, keylen, colid, coltype, coloff)	\
do {																	\
		(srch_info)->sicolval = key;									\
		(srch_info)->sicollen = keylen;									\
		(srch_info)->sicolid = colid;									\
		(srch_info)->sicoltype = coltype;								\
		(srch_info)->sicoloff = coloff;									\
}while(0)

#define BLOCK_MOVE(blk, nextblk)							\
do {											\
		MEMCPY((blk)->bdata, (nextblk)->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);	\
											\
		(blk)->bnextrno = (nextblk)->bnextrno;					\
		(blk)->bfreeoff = (nextblk)->bfreeoff;					\
		(blk)->bminlen	= (nextblk)->bminlen;					\
											\
		MEMSET((nextblk)->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);		\
											\
		(nextblk)->bnextrno = 0;						\
		(nextblk)->bfreeoff = BLKHEADERSIZE;					\
		(nextblk)->bminlen = 0;							\
}while(0)

BUF *
blkget(struct tab_info *tabinfo);

int
blksrch(struct tab_info *tabinfo, BUF *bp);

BUF *
blk_getsstable(struct tab_info *tabinfo);

int
blkins(struct tab_info *tabinfo, char *rp);

int
blkdel(TABINFO *tabinfo, char *rp);


int
blk_check_sstab_space(TABINFO *tabinfo, BUF *bp, char *rp, int rlen, int ins_offset);

void
blk_split(BLOCK *blk);

int
blk_backmov(BLOCK *blk);

void
blk_init(BLOCK *blk);

#endif
