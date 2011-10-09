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



#define	BLKHEADERSIZE	52


#define	BLOCKSIZE		(64 * 1024)		
//#define BLOCKSIZE		(512)
//#define BLOCKSIZE		(128 * 1024)


//#define	BLK_CNT_IN_SSTABLE	16

#define	BLK_CNT_IN_SSTABLE	64


#define	SSTABLE_SIZE		(BLK_CNT_IN_SSTABLE * BLOCKSIZE)


/* Adjust the cache size. */
#define	SSTABLE_MAX_COUNT	64


#define	BLOCK_MAX_COUNT		(BLK_CNT_IN_SSTABLE * SSTABLE_MAX_COUNT + 24)	
//#define	BLOCK_MAX_COUNT		(16 * SSTABLE_MAX_COUNT + 40 + 256 + 51)
//#define	BLOCK_MAX_COUNT		(16 * SSTABLE_MAX_COUNT)	


#define	BLOCK_CACHE_SIZE	((BLOCK_MAX_COUNT) * BLOCKSIZE)	

#define	BLOCKMASK		(~(BLOCKSIZE - 1))


typedef	struct block
{
	char		pad2[4];
	
	int		bblkno;		
	int		bnextblkno;	
	int		bsstabnum;	
	int		bsstabid;	
	int		btabid;		
	int		bnextsstabnum;	
	int		bprevsstabnum;	
	unsigned int	bts_lo;		
	int		bnextrno;	
	unsigned int    bts_hi;	 	
	int		bfreeoff; 	
	short		bstat; 		
	short		bminlen; 	
	char		bdata[BLOCKSIZE - BLKHEADERSIZE - 4]; 
	int		boffsets[1];	
}BLOCK;



#define	BLK_TABLET_SCHM		0x0001	
#define BLK_SSTAB_SPLIT		0x0002	


#define	ROW_OFFSET_ENTRYSIZE	sizeof(int)
#define	BLK_TAILSIZE		sizeof(time)	
#define	BLK_GET_NEXT_ROWNO(bp)	(bp->bblk->bnextrno)


#define	ROW_OFFSET_PTR(blkptr)	((int *) (((char *)(blkptr)) +		\
                  (BLOCKSIZE - BLK_TAILSIZE - ROW_OFFSET_ENTRYSIZE)))

#define ROW_SET_OFFSET(blkptr, rnum, offset)	\
	(ROW_OFFSET_PTR(blkptr))[-((int)(rnum))] = (offset)

		   
#define BLOCK_IS_EMPTY(bp)	(bp->bblk->bfreeoff == BLKHEADERSIZE)



typedef struct block_row_info
{
	int	rlen;
	int	roffset;
	int	rblknum;
	int	rsstabid;
}BLK_ROWINFO;

typedef struct srch_info
{
	int	sicoltype;
	char	*sicolval;
	int	sicollen;
	int	sicolid;
	int	sicoloff;
	int	sistate;
}SINFO;


#define	SI_INDEX_BLK	0x00000001	
#define	SI_DATA_BLK	0x00000002
#define	SI_INS_DATA	0x00000004	
#define	SI_NODATA	0x00000008	
#define SI_DEL_DATA	0x00000010


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
blkdel(TABINFO *tabinfo);


int
blk_check_sstab_space(TABINFO *tabinfo, BUF *bp, char *rp, int rlen, int ins_offset);

void
blk_split(BLOCK *blk);

int
blk_backmov(BLOCK *blk);

void
blk_init(BLOCK *blk);

int
blk_get_location_sstab(struct tab_info *tabinfo, BUF *bp);


#endif
