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

#ifndef BLOCK_H_
#define BLOCK_H_



#define	BLKHEADERSIZE	68


//#define	BLOCKSIZE		(64 * 1024)		
//#define BLOCKSIZE		(512)
//#define BLOCKSIZE		(128 * 1024)



//#define	BLK_CNT_IN_SSTABLE	16
#define	BLK_CNT_IN_SSTABLE	64


#define	SSTABLE_SIZE		(BLK_CNT_IN_SSTABLE * BLOCKSIZE)


#define	SSTABLE_MAX_COUNT	128ll
//#define	SSTABLE_MAX_COUNT	64ll



//#define	BLOCK_MAX_COUNT		(BLK_CNT_IN_SSTABLE * SSTABLE_MAX_COUNT + 24)	
//#define	BLOCK_MAX_COUNT		(16 * SSTABLE_MAX_COUNT + 40 + 256 + 53)
#define       BLOCK_MAX_COUNT         (BLK_CNT_IN_SSTABLE * SSTABLE_MAX_COUNT + 5)
//#define	BLOCK_MAX_COUNT		(16 * SSTABLE_MAX_COUNT - 3)	


#define	BLOCK_CACHE_SIZE	((BLOCK_MAX_COUNT) * BLOCKSIZE)	

#define	BLOCKMASK		(~(BLOCKSIZE - 1))


#define	BLOCK_EMPTY_ROWID	-1

typedef	struct block
{
	char		pad2[4];
	
	int		bblkno;			
	int		bnextblkno;	
	int		bindex_other;	
	int		pad1;
	int		bsstabnum;	
	int		bsstabid;	
	int		btabid;		
	int		bnextsstabnum;	
	int		bprevsstabnum;	

	
	unsigned int	bsstab_split_ts_lo;
	
	int		bnextrno;		
	unsigned int    bsstab_split_ts_hi;
	unsigned int	bsstab_insdel_ts_lo;	
	unsigned int    bsstab_insdel_ts_hi;
	
	int		bfreeoff; 		
	short		bstat; 		
	short		bminlen; 	

	
	char		bdata[BLOCKSIZE - BLKHEADERSIZE - 4]; 
	
	int		boffsets[1];	
}BLOCK;



#define	BLK_TABLET_SCHM		0x0001	
#define BLK_SSTAB_SPLIT		0x0002	
#define	BLK_INDEX_ROOT		0x0004	
#define	BLK_CRT_EMPTY		0x0008	
#define	BLK_OVERFLOW_SSTAB	0x0010	
#define	BLK_RENT_DATA		0x0020	


#define	ROW_OFFSET_ENTRYSIZE	sizeof(int)
#define	BLK_TAILSIZE		sizeof(int)		
#define	BLK_GET_NEXT_ROWNO(blk)	(blk->bnextrno)


#define	ROW_OFFSET_PTR(blkptr)	((int *) (((char *)(blkptr)) +		\
                  (BLOCKSIZE - BLK_TAILSIZE - ROW_OFFSET_ENTRYSIZE)))

#define ROW_SET_OFFSET(blkptr, rnum, offset)	\
	(ROW_OFFSET_PTR(blkptr))[-((int)(rnum))] = (offset)

		   
#define BLOCK_IS_EMPTY(bp)	(bp->bblk->bfreeoff == BLKHEADERSIZE)


typedef struct block_row_info
{
	int	rlen;
	int	rnum;
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
#define	SI_UPD_DATA	0x00000020	


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
		(nextblk)->bminlen = (blk)->bminlen;							\
}while(0)


#define	BLK_BUF_NEED_CHANGE	0x0001
#define BLK_ROW_NEXT_SSTAB	0x0002	
#define BLK_ROW_NEXT_BLK	0x0004
#define BLK_INS_SPLITTING_SSTAB	0x0008


#define	UPDATE_HIT_ERROR		0
#define	UPDATE_IN_PLACE			1
#define	UPDATE_IN_CUR_BLK		2
#define	UPDATE_IN_NEXT_BLK		3
#define	UPDATE_IN_NEXT_SSTAB		4
#define	UPDATE_BLK_CHANGED		5
#define	UPDATE_SKIP_THIS_ROW		6

BUF *
blkget(struct tab_info *tabinfo);

int
blksrch(struct tab_info *tabinfo, BUF *bp);

BUF *
blk_getsstable(struct tab_info *tabinfo);

int
blkins(struct tab_info *tabinfo, char *rp);

int
blkdel(struct tab_info *tabinfo);

int
blk_check_sstab_space(struct tab_info *tabinfo, BUF *bp, char *rp, int rlen,
					int ins_rnum, int data_insert_needed);

int
blk_split(BLOCK *blk, struct tab_info *tabinfo, int rlen, int inspos);

int
blk_backmov(BLOCK *blk, struct tab_info *tabinfo);

void
blk_init(BLOCK *blk);

int
blk_get_location_sstab(struct tab_info *tabinfo, BUF *bp);

int
blk_appendrow(BLOCK *blk, char *rp, int rlen);

int
blkupdate(struct tab_info *tabinfo, char *newrp);

int
blk_get_totrow_sstab(BUF *bp);

int
blk_shuffle_data(BLOCK *srcblk, BLOCK *destblk);

int
blk_compact(BLOCK *blk);

int
blk_delrow(struct tab_info *tabinfo, BLOCK *blk, int sstabid,char *rp, int rnum);

int
blk_update_check_sstab_space(struct tab_info *tabinfo, BUF *bp, char *oldrp, int oldrlen, 
				int newrlen, int rnum);

int
blk_putrow(struct tab_info *tabinfo, BLOCK *blk, int sstabid, char *rp, int rlen,
			int rnum, int roffset);



#endif
