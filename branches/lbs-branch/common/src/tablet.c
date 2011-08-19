/*
** tablet.c 2011-08-08 xueyingfei
**
** Copyright Transoft Corp.
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
#include "strings.h"
#include "buffer.h"
#include "block.h"
#include "cache.h"
#include "memcom.h"
#include "strings.h"
#include "utils.h"
#include "row.h"
#include "tablet.h"
#include "tss.h"
#include "type.h"
#include "session.h"


extern	TSS	*Tss;


#define	INVALID_TABLETID	1

// char *rp - the row in the tablet (sstabid|sstable row | ranger |key col)
// int minlen - min length of the row in the tablet
void
tablet_crt(TABLEHDR *tablehdr, char *tabledir, char *rp, int minlen)
{
	LOCALTSS(tss);
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];
	char	tablet_name[32];
	int	keycolen;
	char	*keycol;
	TABINFO tabinfo;
	SINFO	sinfo;


	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMSET(tablet_name, 32);
	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tss->toldtabinfo = tss->ttabinfo;
	tss->ttabinfo = &tabinfo;
		

	MEMCPY(tab_meta_dir, tabledir, STRLEN(tabledir));
	
	
	build_file_name("tablet", tablet_name, tablehdr->tab_tablet);
	str1_to_str2(tab_meta_dir, '/', tablet_name);
	
	keycol = row_locate_col(rp, -1, minlen, &keycolen);
	
	
	TABINFO_INIT(&tabinfo, tab_meta_dir, &sinfo, minlen, TAB_CRT_NEW_FILE);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEYCOLID, VARCHAR, -1);
	
	blkins(&tabinfo, rp);
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tabledir, STRLEN(tabledir));
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");
	
	
	int rlen = ROW_MINLEN_IN_TABLETSCHM + keycolen + sizeof(int) + sizeof(int);
	char *temprp = (char *)MEMALLOCHEAP(rlen);
	
	
	tablet_schm_bld_row(temprp, rlen, tablehdr->tab_tablet, tablet_name, keycol, keycolen);
	
	tablet_schm_ins_row(tab_meta_dir, temprp, tablehdr->tab_tablet);

	
	session_close( &tabinfo);

	MEMFREEHEAP(temprp);

	
	tss->ttabinfo = tss->toldtabinfo;

}

void
tablet_ins_row(char *tablet_name, char *rp, int minlen)
{
	LOCALTSS(tss);
	int	keycolen;
	char	*keycol;
	TABINFO tabinfo;
	SINFO	sinfo;


	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);
	
	tss->toldtabinfo = tss->ttabinfo;
	tss->ttabinfo = &tabinfo;
		

	keycol = row_locate_col(rp, -1, minlen, &keycolen);
	
	
	TABINFO_INIT(&tabinfo, tablet_name, &sinfo, minlen, TAB_SCHM_INS);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, TABLET_KEYCOLID, VARCHAR, -1);
	
	blkins(&tabinfo, rp);

	session_close(&tabinfo);

	tss->ttabinfo = tss->toldtabinfo;
}



int
tablet_bld_row(char *sstab_rp, int sstab_rlen, char *tab_name, int tab_name_len,
		int sstab_id, char *sstab_name, int sstab_name_len, char *rang_addr,
		char *keycol, int keycolen, int keycol_type)
{
	int	sstab_idx;
	int min_rlen;


	sstab_idx = 0;
	min_rlen = 0;
	
	
	row_build_hdr((sstab_rp + sstab_idx), 0, 0, 1);

	
	sstab_idx += sizeof(ROWFMT);

	
	PUT_TO_BUFFER(sstab_rp, sstab_idx, &sstab_id, sizeof(int));
	
			
	PUT_TO_BUFFER(sstab_rp, sstab_idx, sstab_name, SSTABLE_NAME_MAX_LEN);

	
	PUT_TO_BUFFER(sstab_rp, sstab_idx, rang_addr, RANGE_ADDR_MAX_LEN);

	
	PUT_TO_BUFFER(sstab_rp, sstab_idx, &sstab_rlen, sizeof(int));

//	*(int *)((char *)sstab_rp + sstab_idx) = sstab_rlen;
//	sstab_idx += sizeof(int);
	

	
	PUT_TO_BUFFER(sstab_rp, sstab_idx, keycol, keycolen);

	min_rlen = sstab_idx - sizeof(int);

	if (!TYPE_IS_FIXED(keycol_type))
	{
		
		*(int *)(sstab_rp + sstab_idx) = sstab_idx - keycolen;

		sstab_idx += COLOFFSETENTRYSIZE;

		min_rlen -= keycolen;
	}
	
	
	if (0)
	{
		TABLETHDR	tablet_hdr;
		PUT_TO_BUFFER(sstab_rp, sstab_idx, &(tablet_hdr.offset_c3), 
				sizeof(int));
	}

	assert(sstab_idx == sstab_rlen);

	return min_rlen;
}



char *
tablet_srch_row(char *systab, char *key, int keylen)
{
	LOCALTSS(tss);
	TABINFO	*tabinfo;
	int	minrowlen;
	BUF	*bp;
	int	offset;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo; 
	
	tss->toldtabinfo= tss->ttabinfo;
	tss->ttabinfo = tabinfo;

	minrowlen = ROW_MINLEN_IN_TABLET;

	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, TAB_SCHM_SRCH);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_KEYCOLID, VARCHAR, -1);			
	
	bp = blkget(tabinfo);
	offset = blksrch(tabinfo, bp);

	bufunkeep(bp->bsstab);
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tss->ttabinfo = tss->toldtabinfo;

	return ((char *)(bp->bblk) + offset);
}




void 
tablet_schm_bld_row(char *rp, int rlen, int tabletid, char *tabletname, char *keycol, int keycolen)
{
	int		rowidx;


	rowidx = 0;
	
	row_build_hdr((rp + rowidx), 0, 0, 1);

	
	rowidx += sizeof(ROWFMT);

	
	 	
	PUT_TO_BUFFER(rp, rowidx, &tabletid, sizeof(int));
	
	 	
	PUT_TO_BUFFER(rp, rowidx, tabletname, TABLET_NAME_MAX_LEN);

	
	PUT_TO_BUFFER(rp, rowidx, &rlen, sizeof(int));

	
	PUT_TO_BUFFER(rp, rowidx, keycol, keycolen);

	
	
	*(int *)(rp + rowidx) = rowidx - keycolen;

	rowidx += COLOFFSETENTRYSIZE;
	
	assert(rowidx == rlen);
}


void
tablet_schm_ins_row(char *systab, char *row, int tabletnum)
{
	LOCALTSS(tss);
	TABINFO	*tabinfo;
	int	minrowlen;
	char	*key;
	int	keylen;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;
	tss->toldtabinfo = tss->ttabinfo;
	tss->ttabinfo = tabinfo;	

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	
	key = row_locate_col(row, -1, minrowlen, &keylen);
	
	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, tabletnum ? TAB_SCHM_INS : TAB_CRT_NEW_FILE);
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_SCHM_KEYCOLID, VARCHAR, -1);
			
	blkins(tabinfo, row);

	
	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tss->ttabinfo = tss->toldtabinfo;
}




char *
tablet_schm_srch_row(char *systab, char *key, int keylen)
{
	LOCALTSS(tss);
	TABINFO	*tabinfo;
	int	minrowlen;
	BUF	*bp;
	int	offset;
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tss->toldtabinfo = tss->ttabinfo;
	tss->ttabinfo = tabinfo;

	minrowlen = ROW_MINLEN_IN_TABLETSCHM;

	TABINFO_INIT(tabinfo, systab, tabinfo->t_sinfo, minrowlen, TAB_SCHM_SRCH);

	
	SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_SCHM_KEYCOLID, VARCHAR, -1);
			
	
	bp = blkget(tabinfo);
	offset = blksrch(tabinfo, bp);

	bufunkeep(bp->bsstab);
	session_close(tabinfo);
	
	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);
	
	tss->ttabinfo = tss->toldtabinfo;

	return ((char *)(bp->bblk) + offset);
}



void
tablet_namebyname(char *old_sstab, char *new_sstab)
{
	char	nameidx[64];
	int	idxpos;
	char	tmpsstab[SSTABLE_NAME_MAX_LEN];
	int	old_sstab_len;
	time_t	timer;

	MEMSET(nameidx, 64);
	old_sstab_len = STRLEN(old_sstab);
	idxpos = str1nstr(old_sstab, "tablet", old_sstab_len);

	MEMSET(tmpsstab, SSTABLE_NAME_MAX_LEN);
	MEMCPY(tmpsstab, old_sstab, idxpos);

	time(&timer);
	sprintf(nameidx, "%ld", timer );

	sprintf(new_sstab, "%s%s", tmpsstab,nameidx);
	
	return;
}


void
tablet_split(TABINFO *srctabinfo, BUF *srcbp, char *rp)
{
	LOCALTSS(tss);
	BUF	*destbuf;
	TABINFO * tabinfo;
	BLOCK	*nextblk;
	BLOCK	*blk;
	char	*key;
	int	keylen;
	int	ins_nxtsstab;
	char	*tablet_key;
	int	tablet_keylen;
	int	table_nameidx;
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];


	nextblk = srcbp->bsstab->bblk;

	ins_nxtsstab = (nextblk->bblkno > ((BLK_CNT_IN_SSTABLE / 2) - 1)) ? TRUE : FALSE;
	
	while (nextblk->bnextblkno < (BLK_CNT_IN_SSTABLE / 2))
	{		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
	}


	destbuf = bufgrab();
	
	bufhash(destbuf);

	blk = destbuf->bblk;
		
	blk_init(blk);

	while(nextblk->bblkno != -1)
	{
		assert(nextblk->bfreeoff > BLKHEADERSIZE);

		BLOCK_MOVE(blk,nextblk);
		
		if (nextblk->bnextblkno == -1)
		{
			break;
		}
		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
		blk = (BLOCK *) ((char *)blk + BLOCKSIZE);
	}


	MEMSET(destbuf->bsstab_name, 256);

	tablet_namebyname(srctabinfo->t_sstab_name, destbuf->bsstab_name);

	table_nameidx = str01str(srctabinfo->t_sstab_name, "tablet", STRLEN(srctabinfo->t_sstab_name));
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tss->toldtabinfo = tss->ttabinfo;
	tss->ttabinfo = tabinfo;

	TABINFO_INIT(tabinfo, destbuf->bsstab_name, tabinfo->t_sinfo, ROW_MINLEN_IN_TABLET, TAB_KEPT_BUF_VALID);

	if (ins_nxtsstab)
	{
		tabinfo->t_keptbuf = destbuf;	

		key = row_locate_col(rp, -1, ROW_MINLEN_IN_TABLET, &keylen);
		
		SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, TABLET_KEYCOLID, VARCHAR, -1);
		
		blkins(tabinfo, rp);
	}
	else
	{
		bufpredirty(destbuf);
		bufdirty(destbuf);
	}


	tablet_key = row_locate_col(destbuf->bblk->bdata, -1, destbuf->bblk->bminlen, &tablet_keylen);
	
	
	int rlen = ROW_MINLEN_IN_TABLETSCHM + tablet_keylen + sizeof(int) + sizeof(int);
	char *temprp = (char *)MEMALLOCHEAP(rlen);

	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, srctabinfo->t_sstab_name, table_nameidx);
	str1_to_str2(tab_meta_dir, '/', "tabletscheme");

	table_nameidx = str01str(destbuf->bsstab_name, "tablet", STRLEN(destbuf->bsstab_name));
	tablet_schm_bld_row(temprp, rlen, INVALID_TABLETID, destbuf->bsstab_name + table_nameidx + 1, tablet_key, tablet_keylen);
	
	tablet_schm_ins_row(tab_meta_dir, temprp, INVALID_TABLETID);

	session_close(tabinfo);

	
	MEMFREEHEAP(temprp);

	if (tabinfo)
	{
		if (tabinfo->t_sinfo)
		{
			MEMFREEHEAP(tabinfo->t_sinfo);
		}

		MEMFREEHEAP(tabinfo);
	}

	tss->ttabinfo = tss->toldtabinfo;
	
	
}



