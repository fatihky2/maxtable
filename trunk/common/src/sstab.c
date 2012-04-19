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
#include "tabinfo.h"
#include "strings.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "parser.h"
#include "ranger/rangeserver.h"
#include "block.h"
#include "cache.h"
#include "memcom.h"
#include "tss.h"
#include "session.h"
#include "row.h"
#include "utils.h"
#include <time.h>
#include "metadata.h"
#include "tabinfo.h"
#include "file_op.h"
#include "timestamp.h"
#include "log.h"
#include "hkgc.h"
#include "rginfo.h"
#include "index.h"


extern	TSS	*Tss;
extern	RANGEINFO *Range_infor;


int	sstab_split_cnt;
int	sstabsplit_idx_upd_cnt;


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
sstab_namebyid(char *old_sstab, char *new_sstab, int new_sstab_id)
{
	LOCALTSS(tss);
	char	nameidx[64];
	int	idxpos;
	char	tmpsstab[SSTABLE_NAME_MAX_LEN];
	int	old_sstab_len;
	

	old_sstab_len = STRLEN(old_sstab);
	idxpos = str1nstr(old_sstab, "sstable", old_sstab_len);

	MEMSET(nameidx, 64);
	sprintf(nameidx, "%d", new_sstab_id);
	
	MEMSET(tmpsstab, SSTABLE_NAME_MAX_LEN);
	MEMCPY(tmpsstab, old_sstab, idxpos);

//	printf("tabinfo->t_insmeta->res_sstab_id = %d \n", tabinfo->t_insmeta->res_sstab_id);

	sprintf(new_sstab, "%s%s", tmpsstab,nameidx);

	if (DEBUG_TEST(tss))
	{
		traceprint("new_sstab = %s--------%d---\n", new_sstab, new_sstab_id);
	}
	
	return;
}



void
sstab_split(TABINFO *srctabinfo, BUF *srcbp, char *rp)
{
	LOCALTSS(tss);
	BUF		*destbuf;
	TABINFO 	*tabinfo;
	BLOCK		*nextblk;
	BLOCK		*destblk;
	char		*key;
	int		keylen;
	int		ins_nxtsstab;
	char		*sstab_key;
	int		sstab_keylen;
	int		i;
	BLK_ROWINFO	blk_rowinfo;
	IDXBLD		idxbld;
	IDXUPD		idxupd;	
	char		rg_tab_dir[TABLE_NAME_MAX_LEN];
	LOGREC		*logrec;


	
	ins_nxtsstab = (srcbp->bblk->bblkno > ((BLK_CNT_IN_SSTABLE / 2) - 1))
			? TRUE : FALSE;

	destbuf = NULL;
	nextblk = srcbp->bsstab->bblk;
	logrec = NULL;

	
	while (nextblk->bnextblkno < ((BLK_CNT_IN_SSTABLE / 2) + 1))
	{		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
	}

	

	if (!(srctabinfo->t_stat & TAB_NOLOG_MODEL))
	{
	 	logrec = MEMALLOCHEAP(sizeof(LOGREC));
		
		log_build(logrec, LOG_BEGIN, 0, 0, srctabinfo->t_sstab_name, 
				NULL, 0, 0, 0, 0, 0, NULL, NULL);

		
		logrec->logbeg.begincase = (srctabinfo->t_stat & TAB_INS_INDEX)
					   ? LOGBEG_INDEX_CASE : 0;
		log_put(logrec, NULL, 0);
	}

	
	srctabinfo->t_stat |= TAB_GET_RES_SSTAB;

	if ((destbuf = bufsearch(srctabinfo)) == NULL)
	{
		destbuf = bufgrab(srctabinfo);
		
		bufhash(destbuf);
	}
	
	srctabinfo->t_stat &= ~TAB_GET_RES_SSTAB;
	
	destblk = destbuf->bblk;
		
	blk_init(destblk);

	if ((tss->topid & TSS_OP_INSTAB) && (srctabinfo->t_has_index))
	{		
		MEMSET(rg_tab_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(rg_tab_dir, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));
		str1_to_str2(rg_tab_dir, '/', srctabinfo->t_tab_name);
	}

	bufpredirty(srcbp->bsstab);
	bufpredirty(destbuf->bsstab);
	
		
	
	while(nextblk->bblkno != -1)
	{
		Assert(nextblk->bfreeoff > BLKHEADERSIZE);
		
		
		MEMCPY(destblk->bdata, nextblk->bdata, 
					BLOCKSIZE - BLKHEADERSIZE - 4);
		if (srcbp->bsstab->bsstabid == 7)
		{
			sstab_split_cnt++;
		}
		destblk->bnextrno = nextblk->bnextrno;				
		destblk->bfreeoff = nextblk->bfreeoff;
		destblk->bminlen = nextblk->bminlen;

		if (   (tss->topid & TSS_OP_INSTAB)
		    && (srctabinfo->t_has_index))
		{				
			
			idxbld.idx_tab_name = rg_tab_dir;

			idxbld.idx_stat = 0;

			idxbld.idx_root_sstab = srctabinfo->t_tablet_id;

			idxupd.newblk = destblk;

			idxupd.oldblk = nextblk;

			idxupd.new_sstabid = destbuf->bsstab->bsstabid;

			idxupd.old_sstabid = srcbp->bsstab->bsstabid;

			idxupd.start_row = 0;

			Assert(!(srctabinfo->t_stat & TAB_INS_INDEX));

			Assert(idxupd.new_sstabid != idxupd.old_sstabid);

			
			Assert(destblk->bfreeoff > BLKHEADERSIZE);

			if (srctabinfo->t_index_ts 
				!= Range_infor->rg_meta_sysindex->idx_ver)
			{
				meta_load_sysindex(
					(char *)Range_infor->rg_meta_sysindex);
			}
			if (srcbp->bsstab->bsstabid == 7)
			{
				sstabsplit_idx_upd_cnt++;
			}
//			Assert(sstab_split_cnt == sstabsplit_idx_upd_cnt);
			
			index_update(&idxbld, &idxupd, srctabinfo, 
					Range_infor->rg_meta_sysindex);	
		}

		MEMSET(nextblk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);
		nextblk->bnextrno = 0;
		nextblk->bfreeoff = BLKHEADERSIZE;
		nextblk->bminlen = 0;				
		
		if (nextblk->bnextblkno == -1)
		{
			break;
		}
		
		nextblk = (BLOCK *) ((char *)nextblk + BLOCKSIZE);
		destblk = (BLOCK *) ((char *)destblk + BLOCKSIZE);
	}

				
	MEMSET(destbuf->bsstab_name, 256);

//	sstab_namebyname(srctabinfo->t_sstab_name, destbuf->bsstab_name);
	sstab_namebyid(srctabinfo->t_sstab_name, destbuf->bsstab_name, 
				srctabinfo->t_insmeta->res_sstab_id);

	destbuf->bsstab->bblk->bnextsstabnum = srcbp->bsstab->bblk->bnextsstabnum;
	destbuf->bsstab->bblk->bprevsstabnum = srcbp->bsstab->bblk->bsstabnum;
	srcbp->bsstab->bblk->bnextsstabnum = srctabinfo->t_insmeta->res_sstab_id;
	destbuf->bsstab->bblk->bsstabnum = srctabinfo->t_insmeta->res_sstab_id;

	
	srctabinfo->t_insmeta->ts_low = mtts_increment(srctabinfo->t_insmeta->ts_low);
	srcbp->bsstab->bblk->bsstab_split_ts_lo = srctabinfo->t_insmeta->ts_low;
	
	bufdirty(srcbp->bsstab);
	bufdirty(destbuf->bsstab);
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

//	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	TABINFO_INIT(tabinfo, destbuf->bsstab_name, srctabinfo->t_tab_name,
				srctabinfo->t_tab_namelen, tabinfo->t_sinfo, 
				srcbp->bsstab->bblk->bminlen, 
				TAB_KEPT_BUF_VALID | TAB_DO_SPLIT,
				srctabinfo->t_tabid, 
				srctabinfo->t_insmeta->res_sstab_id);

	
	srctabinfo->t_stat |= TAB_SSTAB_SPLIT;

	srctabinfo->t_insrg = (INSRG *)MEMALLOCHEAP(sizeof(INSRG));
	MEMSET(srctabinfo->t_insrg, sizeof(INSRG));

	
	sstab_key = row_locate_col(destbuf->bblk->bdata, -1, 
					destbuf->bblk->bminlen, &sstab_keylen);

	srctabinfo->t_insrg->new_keylen = sstab_keylen;
	srctabinfo->t_insrg->new_sstab_key = (char *)MEMALLOCHEAP(sstab_keylen);
	MEMSET(srctabinfo->t_insrg->new_sstab_key, sstab_keylen);


	int	sstab_len = STRLEN(destbuf->bsstab_name);
	i = strmnstr(destbuf->bsstab_name, "/", sstab_len);
	
	MEMCPY(srctabinfo->t_insrg->new_sstab_name, destbuf->bsstab_name + i, 
		STRLEN(destbuf->bsstab_name + i));
	MEMCPY(srctabinfo->t_insrg->new_sstab_key, sstab_key, sstab_keylen);

	if (!(srctabinfo->t_stat & TAB_NOLOG_MODEL))
	{
		Assert(logrec);
		
		log_build(logrec , (srctabinfo->t_stat & TAB_INS_INDEX) 
				  ? LOG_INDEX_SSTAB_SPLIT : LOG_DATA_SSTAB_SPLIT, 
				srcbp->bsstab->bblk->bsstab_split_ts_lo, 0,
				srctabinfo->t_sstab_name, destbuf->bsstab_name,
				0, 0, 0, 0, 0, NULL, NULL);
		
		log_put(logrec, NULL, 0);
	}

	if (!(srctabinfo->t_stat & TAB_INS_INDEX))
	{
		SSTAB_SPLIT_INFO	split_info;

		SSTAB_SPLIT_INFO_INIT(&split_info, 0, srctabinfo->t_tab_name,
					srctabinfo->t_tab_namelen, 
					destbuf->bsstab_name,
					srctabinfo->t_sstab_id,
					srcbp->bsstab->bblk->bsstab_split_ts_lo,
					srctabinfo->t_insmeta->res_sstab_id,
					sstab_keylen, sstab_key);

		
		ri_rgstat_putdata(tss->rgstatefile, destbuf->bsstab_name, 0, &split_info);
	}
	
	if (!(srctabinfo->t_stat & TAB_NOLOG_MODEL))
	{
		Assert(logrec);
		
		log_build(logrec, LOG_END, 0, 0, srctabinfo->t_sstab_name,
					NULL, 0, 0, 0, 0, 0, NULL, NULL);

		log_put(logrec, NULL, 0);

		if (!(srctabinfo->t_stat & TAB_INS_INDEX))
		{
			hkgc_wash_sstab(TRUE);
		}
	}

       	
	
	if (ins_nxtsstab)
	{
		
		tabinfo->t_keptbuf = destbuf;
		
		key = row_locate_col(rp, srctabinfo->t_key_coloff, 
				     srcbp->bsstab->bblk->bminlen, &keylen);
		
		
		SRCH_INFO_INIT(tabinfo->t_sinfo, key, keylen, srctabinfo->t_key_colid, 
				srctabinfo->t_key_coltype, srctabinfo->t_key_coloff);

		
		tabinfo->t_stat |=  (srctabinfo->t_stat & TAB_NOLOG_MODEL) 
				  ? TAB_NOLOG_MODEL : 0;
		tabinfo->t_has_index = srctabinfo->t_has_index;
		tabinfo->t_colinfo = srctabinfo->t_colinfo;
		tabinfo->t_tablet_id = srctabinfo->t_tablet_id;
		tabinfo->t_index_ts = srctabinfo->t_index_ts;
		blkins(tabinfo, rp);

		MEMCPY(&(srctabinfo->t_currid), &(tabinfo->t_currid), sizeof(RID));		
	}
	else
	{
		
		bufpredirty(destbuf->bsstab);
		bufdirty(destbuf->bsstab);
	}
		
	if (destbuf)
	{
		bufunkeep(destbuf->bsstab);
	}

	session_close(tabinfo);
	
	if (logrec)
	{
		MEMFREEHEAP(logrec);
	}
	
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


SSTAB_INFOR *
sstab_map_get(int tabid, char *tab_dir, TAB_SSTAB_MAP **tab_sstab_map)
{
	LOCALTSS(tss);
	char		tab_dir1[TABLE_NAME_MAX_LEN];
	int		fd;
	SSTAB_INFOR	*sstab_map = NULL;
	TAB_SSTAB_MAP 	*sstab_map_tmp;


	sstab_map_tmp = *tab_sstab_map;

	while(sstab_map_tmp != NULL)
	{
		if (sstab_map_tmp->tabid == tabid)
		{
			sstab_map = sstab_map_tmp->sstab_map;

			
			goto exit;
		}

		sstab_map_tmp = sstab_map_tmp->nexttabmap;
	}
	
	
	
	MEMSET(tab_dir1, TABLE_NAME_MAX_LEN);

	
	MEMCPY(tab_dir1, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_dir1, '/', "sstabmap");
	
	OPEN(fd, tab_dir1, (O_RDONLY));

	if (fd < 0)
	{
		goto exit;
	}

	
	sstab_map_tmp = (TAB_SSTAB_MAP *)malloc(sizeof(TAB_SSTAB_MAP));

	MEMSET(sstab_map_tmp, sizeof(TAB_SSTAB_MAP));
	
	
	READ(fd, sstab_map_tmp->sstab_map, SSTAB_MAP_SIZE);

	sstab_map_tmp->tabid = tabid;
	MEMCPY(sstab_map_tmp->sstabmap_path, tab_dir1, STRLEN(tab_dir1));
	
	
	if (*tab_sstab_map != NULL)
	{
		sstab_map_tmp->nexttabmap = (*tab_sstab_map)->nexttabmap;
		(*tab_sstab_map)->nexttabmap = sstab_map_tmp;
	}
	else
	{
		*tab_sstab_map = sstab_map_tmp;
	}

	sstab_map = sstab_map_tmp->sstab_map;

	tss->ttab_sstabmap = sstab_map_tmp;
	CLOSE(fd);

exit:

	return sstab_map;
}



void
sstab_map_release(int tabid, int flag, TAB_SSTAB_MAP *tab_sstab_map)
{
	
	TAB_SSTAB_MAP *sstab_map_cur;
	TAB_SSTAB_MAP *sstab_map_last;


	sstab_map_last = sstab_map_cur = tab_sstab_map;

	while(sstab_map_cur != NULL)
	{
		if (sstab_map_cur->tabid == tabid)
		{
			if (sstab_map_last == sstab_map_cur)
			{
				free(tab_sstab_map);
				tab_sstab_map = NULL;
			}
			else
			{
				sstab_map_last->nexttabmap = 
						sstab_map_cur->nexttabmap;

				free(sstab_map_cur);
			}
			
			break;
		}
		sstab_map_last = sstab_map_cur;
		sstab_map_cur = sstab_map_cur->nexttabmap;
	}

	return;
}


int
sstab_map_put(int tabid, TAB_SSTAB_MAP *tab_sstab_map)
{
	TAB_SSTAB_MAP	*sstab_map_tmp;
	int		fd;


	sstab_map_tmp = tab_sstab_map;

	if (tabid != -1)
	{
		while(sstab_map_tmp != NULL)
		{
			if (sstab_map_tmp->tabid == tabid)
			{
				break;
			}

			sstab_map_tmp = sstab_map_tmp->nexttabmap;
		}
	}
	
	if (sstab_map_tmp == NULL)
	{
		return TRUE;
	}
	
	OPEN(fd, sstab_map_tmp->sstabmap_path, (O_RDWR));

	if (fd < 0)
	{
		return FALSE;
	}
	
	WRITE(fd, sstab_map_tmp->sstab_map, SSTAB_MAP_SIZE);

	CLOSE(fd);

	return TRUE;
}

int
sstab_bld_name(char *sstab_name, char *tab_name, int tab_name_len, int sstabid)
{
	MEMSET(sstab_name, SSTABLE_NAME_MAX_LEN);
	
	MEMCPY(sstab_name, tab_name, tab_name_len);

//	build_file_name("tablet", tablet_name, tabletid);

//	MEMCPY((sstab_name + tab_name_len), tablet_name, STRLEN(tablet_name));
	
	build_file_name("sstable", sstab_name + tab_name_len, sstabid);

	return TRUE;
}
