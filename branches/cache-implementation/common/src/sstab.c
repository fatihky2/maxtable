/*
** sstab.c 2011-07-25 xueyingfei
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
#include "tss.h"
#include "session.h"
#include "row.h"
#include "utils.h"
#include <time.h>
#include "tabinfo.h"


extern	TSS	*Tss;

#define	SSTAB_NAMEIDX_MASK	(2^32 - 1)

#define SSTAB_NAMEIDX(sstab, tabid)	(((tabid ^ (tabid << 8))^ (sstab ^ (sstab<<4))) & SSTAB_NAMEIDX_MASK)

#define NAMEIDX(sstab)	((sstab) ^ ((sstab)<<4))



void
sstab_namebyname(char *old_sstab, char *new_sstab)
{
	char	nameidx[64];
	int	idxpos;
	char	tmpsstab[SSTABLE_NAME_MAX_LEN];
	int	old_sstab_len;
	time_t timer;

	MEMSET(nameidx, 64);
	old_sstab_len = STRLEN(old_sstab);
	idxpos = str1nstr(old_sstab, "sstable", old_sstab_len);

	MEMSET(tmpsstab, SSTABLE_NAME_MAX_LEN);
	MEMCPY(tmpsstab, old_sstab, idxpos);

	time(&timer);
	sprintf(nameidx, "%ld", timer );

	sprintf(new_sstab, "%s%s", tmpsstab,nameidx);
	
	
	//nameidx = NAMEIDX(nameidx + 1);

	//build_file_name(tmpsstab, new_sstab, nameidx);	

	return;
}


void
sstab_namebyid(TABINFO *tabinfo, char *new_sstab)
{
	char	nameidx[64];
	int	idxpos;
	char	tmpsstab[SSTABLE_NAME_MAX_LEN];
	int	old_sstab_len;
	char	*old_sstab;



	old_sstab = tabinfo->t_sstab_name;

	old_sstab_len = STRLEN(old_sstab);
	idxpos = str1nstr(old_sstab, "sstable", old_sstab_len);

	MEMSET(nameidx, 64);
	sprintf(nameidx, "%d", tabinfo->t_insmeta->res_sstab_id);
	
	MEMSET(tmpsstab, SSTABLE_NAME_MAX_LEN);
	MEMCPY(tmpsstab, old_sstab, idxpos);

//	printf("tabinfo->t_insmeta->res_sstab_id = %d \n", tabinfo->t_insmeta->res_sstab_id);

	sprintf(new_sstab, "%s%s", tmpsstab,nameidx);

	printf("new_sstab = %s--------%d---\n", new_sstab,tabinfo->t_insmeta->res_sstab_id);

	return;
}


void
sstab_split(TABINFO *srctabinfo, BUF *srcbp, char *rp)
{
	BUF	*destbuf;
	TABINFO * tabinfo;
	BLOCK	*nextblk;
	BLOCK	*blk;
	char	*key;
	int	keylen;
	int	ins_nxtsstab;
	char	*sstab_key;
	int	sstab_keylen;
	int	i;


	nextblk = srcbp->bsstab->bblk;

	ins_nxtsstab = (nextblk->bblkno > ((BLK_CNT_IN_SSTABLE / 2) - 1)) ? TRUE : FALSE;
	
	while (nextblk->bnextblkno < (BLK_CNT_IN_SSTABLE / 2))
	{		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
	}

	srctabinfo->t_stat |= TAB_GET_RES_SSTAB;
	destbuf = bufgrab(srctabinfo);

	srctabinfo->t_stat &= ~TAB_GET_RES_SSTAB;
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

//	sstab_namebyname(srctabinfo->t_sstab_name, destbuf->bsstab_name);
	sstab_namebyid(srctabinfo, destbuf->bsstab_name);

	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

//	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	TABINFO_INIT(tabinfo, destbuf->bsstab_name, tabinfo->t_sinfo, srcbp->bblk->bminlen, 
			TAB_KEPT_BUF_VALID, tabinfo->t_tabid, tabinfo->t_sstab_id);


	if (ins_nxtsstab)
	{
		tabinfo->t_keptbuf = destbuf;
		
		key = row_locate_col(rp, srctabinfo->t_key_coloff, srcbp->bblk->bminlen, &keylen);
		
		
		SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, srctabinfo->t_key_colid, 
				srctabinfo->t_key_coltype, srctabinfo->t_key_coloff);

		blkins(tabinfo, rp);
	}
	else
	{
		bufpredirty(destbuf);
		bufdirty(destbuf);
	}

	
	srctabinfo->t_stat |= TAB_SSTAB_SPLIT;

	srctabinfo->t_insrg = (INSRG *)MEMALLOCHEAP(sizeof(INSRG));
	MEMSET(srctabinfo->t_insrg, sizeof(INSRG));

	sstab_key = row_locate_col(destbuf->bblk->bdata, -1, destbuf->bblk->bminlen, &sstab_keylen);

	srctabinfo->t_insrg->new_keylen = sstab_keylen;
	srctabinfo->t_insrg->new_sstab_key = (char *)MEMALLOCHEAP(sstab_keylen);
	MEMSET(srctabinfo->t_insrg->new_sstab_key, sstab_keylen);


	i = strmnstr(destbuf->bsstab_name, "/", STRLEN(destbuf->bsstab_name));
	
	MEMCPY(srctabinfo->t_insrg->new_sstab_name, destbuf->bsstab_name + i, STRLEN(destbuf->bsstab_name + i));
	MEMCPY(srctabinfo->t_insrg->new_sstab_key, sstab_key, sstab_keylen);
	

	session_close(tabinfo);

	if (tabinfo)
	{
		if (tabinfo->t_sinfo)
		{
			MEMFREEHEAP(tabinfo->t_sinfo);
		}

		MEMFREEHEAP(tabinfo);
	}

	tabinfo_pop();
	
	
}





