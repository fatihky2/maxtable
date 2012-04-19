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

#include "global.h"
#include "utils.h"
#include "list.h"
#include "master/metaserver.h"
#include "tabinfo.h"
#include "parser.h"
#include "memcom.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "cache.h"
#include "strings.h"
#include "tabinfo.h"
#include "row.h"
#include "tablet.h"
#include "type.h"
#include "session.h"
#include "b_search.h"
#include "file_op.h"
#include "tss.h"
#include "ranger/rangeserver.h"
#include "index.h"
#include "m_socket.h"
#include "sstab.h"
#include "log.h"
#include "timestamp.h"
#include "hkgc.h"
#include "token.h"


extern	TSS	*Tss;
extern	KERNEL	*Kernel;

static int
index_bld_sstabnum(int tablet_num, int sstab_index);

static int
index__del_row(TABINFO *tabinfo, char *delrow);

static int
index__range_sstab_scan(struct tablet_scancontext *tablet_scanctx, 
		IDXMETA *idxmeta, IDX_RANGE_CTX *idx_range_ctx, char *tabdir);

static int
index__get_count_or_sum(SSTAB_SCANCTX *scanctx, char *tabname, 
				int tab_name_len, int tabid);



int
index_ins_row(IDXBLD *idxbld)
{
	LOCALTSS(tss);
	char		tab_idxroot_dir[TABLE_NAME_MAX_LEN];
	char		tab_idxleaf_dir[TABLE_NAME_MAX_LEN];
	char		idx_sstab_name[64];
	char		*pidx_sstab_name;
	int		keycolen;
	char		*keycol;
	int		first_row_hit;	
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		index_sstab_num;
	char 		*index_root_rp;
	INSMETA		insmeta;
	int		idx_sstab_split;
	int		flag;


	
	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;
	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);

	
	tabinfo.t_key_coloff = IDXBLK_KEYCOL_FAKE_COLOFF_INROW;
	tabinfo.t_key_coltype = VARCHAR;
	
	tabinfo_push(&tabinfo);

	
	first_row_hit = (idxbld->idx_stat & IDXBLD_FIRST_DATAROW_IN_TABLET)
			? TRUE : FALSE;

	
	keycol = row_locate_col(idxbld->idx_rp, -1, ROW_MINLEN_IN_INDEXBLK,
					&keycolen);
	tss->topid |= TSS_OP_INDEX_CASE;
	
	if (first_row_hit)
	{
		
		int	status;

		
		index_bld_root_dir(tab_idxroot_dir, idxbld->idx_tab_name,
					idxbld->idx_meta->idxname,
					idxbld->idx_root_sstab);

		MKDIR(status, tab_idxroot_dir, 0755);

		
		index_bld_root_name(tab_idxroot_dir, idxbld->idx_tab_name, 
					idxbld->idx_meta->idxname, 
					idxbld->idx_root_sstab, FALSE);
		
		
		
		idxbld->idx_index_sstab_cnt = 1;

		
		index_bld_leaf_name(tab_idxleaf_dir, idx_sstab_name,
						idxbld->idx_tab_name,
						idxbld->idx_meta->idxname,
						idxbld->idx_root_sstab,
						idxbld->idx_index_sstab_cnt);

		
		index_sstab_num = index_bld_sstabnum(idxbld->idx_root_sstab,
						idxbld->idx_index_sstab_cnt);
	
	}
	else
	{
		
		
		
		index_bld_root_name(tab_idxroot_dir, idxbld->idx_tab_name, 
					idxbld->idx_meta->idxname, 
					idxbld->idx_root_sstab, FALSE);

		
		index_root_rp = tablet_schm_srch_row(idxbld->idx_meta->idx_id,
					idxbld->idx_root_sstab,
					tab_idxroot_dir, keycol, keycolen);

		
	
		
		idxbld->idx_index_sstab_cnt = tablet_schm_get_totrow(
					idxbld->idx_meta->idx_id, 
					idxbld->idx_root_sstab,
					tab_idxroot_dir, keycol, keycolen);

		int namelen;

		
		char *name = row_locate_col(index_root_rp, 
					TABLETSCHM_TABLETNAME_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);
				
		index_bld_root_dir(tab_idxleaf_dir, idxbld->idx_tab_name,
				idxbld->idx_meta->idxname,
				idxbld->idx_root_sstab);

		
		str1_to_str2(tab_idxleaf_dir, '/', name);
		
		
		index_sstab_num = *(int *)row_locate_col(index_root_rp, 
					TABLETSCHM_TABLETID_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);

	}

	flag = (idxbld->idx_stat & IDXBLD_NOLOG) ? TAB_NOLOG_MODEL : 0;
	
	
	TABINFO_INIT(&tabinfo, tab_idxleaf_dir, NULL, 0, &sinfo,
			ROW_MINLEN_IN_INDEXBLK,
			flag | (  first_row_hit
				? TAB_CRT_NEW_FILE : TAB_INS_INDEX),
			idxbld->idx_meta->idx_id, 
			index_sstab_num);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, INDEXBLK_KEYCOL_COLID_INROW,
			tabinfo.t_key_coltype, tabinfo.t_key_coloff);

	MEMSET(&insmeta, sizeof(INSMETA));

	insmeta.col_num = INDEXBLK_RIDARRAY_COLID_INROW;
	insmeta.row_minlen = ROW_MINLEN_IN_INDEXBLK;
	insmeta.varcol_num = 2;


	
	insmeta.res_sstab_id = index_bld_sstabnum(idxbld->idx_root_sstab,
				idxbld->idx_index_sstab_cnt + 1);

	
	Assert(STRLEN(tab_idxleaf_dir) < SSTABLE_NAME_MAX_LEN);
	
	MEMCPY(insmeta.sstab_name, tab_idxleaf_dir, STRLEN(tab_idxleaf_dir));
	
	tabinfo.t_insmeta = &insmeta;
	
	
	blkins(&tabinfo, idxbld->idx_rp);

	
	idx_sstab_split = (tabinfo.t_stat & TAB_SSTAB_SPLIT)? TRUE : FALSE;

	idxbld->idx_stat |= IDXBLD_SSTAB_SPLIT;

	if (idx_sstab_split)
	{
		Assert(tabinfo.t_insrg);

		keycol = tabinfo.t_insrg->new_sstab_key;
		keycolen = tabinfo.t_insrg->new_keylen;
		pidx_sstab_name = tabinfo.t_insrg->new_sstab_name;
		index_sstab_num = insmeta.res_sstab_id;			
	}
	else
	{
		pidx_sstab_name = idx_sstab_name;
	}	

	if (first_row_hit || idx_sstab_split)
	{
		
		int rlen = ROW_MINLEN_IN_TABLETSCHM + keycolen + sizeof(int) +
								sizeof(int);
		char *temprp = (char *)MEMALLOCHEAP(rlen);
		
		char	rg_addr[RANGE_ADDR_MAX_LEN];
		tablet_schm_bld_row(temprp, rlen, index_sstab_num,
					pidx_sstab_name, rg_addr, keycol, 
					keycolen, 1981);

		int	has_idxsstab;

		has_idxsstab =  (   first_row_hit 
				 && !(idxbld->idx_stat & IDXBLD_IDXROOT_SPLIT))
			       ? 0 : 1;

		
		tablet_schm_ins_row(idxbld->idx_meta->idx_id, 
					idxbld->idx_root_sstab, tab_idxroot_dir,
					temprp, has_idxsstab, flag);

				

		MEMFREEHEAP(temprp);
	}
	

	if (idx_sstab_split)
	{
		if (tabinfo.t_insrg->new_sstab_key)
		{
			MEMFREEHEAP(tabinfo.t_insrg->new_sstab_key);
		}

		if (tabinfo.t_insrg->old_sstab_key)
		{
			MEMFREEHEAP(tabinfo.t_insrg->old_sstab_key);
		}

		MEMFREEHEAP(tabinfo.t_insrg);
	}
	
	session_close( &tabinfo);
	tabinfo_pop();

	tss->topid &= ~TSS_OP_INDEX_CASE;

	
	idxbld->idx_stat &= ~IDXBLD_FIRST_DATAROW_IN_TABLET;

	return TRUE;
}



int
index_del_row(IDXBLD *idxbld)
{
	LOCALTSS(tss);
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		index_sstab_num;
	char 		*index_root_rp;
	char		*keycol;
	int		keycolen;
	char		*tabname;
	int		idx_rootid;
	IDXMETA		*idx_meta;
	

	
	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;
	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);

	
	tabinfo.t_key_coloff = IDXBLK_KEYCOL_FAKE_COLOFF_INROW;
	tabinfo.t_key_coltype = VARCHAR;
	
	tabinfo_push(&tabinfo);

	tss->topid |= TSS_OP_INDEX_CASE;

	
	keycol = row_locate_col(idxbld->idx_rp, -1, ROW_MINLEN_IN_INDEXBLK,
					&keycolen);
	
	tabname		= idxbld->idx_tab_name;
	idx_rootid	= idxbld->idx_root_sstab;
	idx_meta	= idxbld->idx_meta;

	
	
	
	index_bld_root_name(tab_meta_dir, tabname, idx_meta->idxname, 
						idx_rootid, FALSE);

	
	index_root_rp = tablet_schm_srch_row(idx_meta->idx_id, idx_rootid,
					tab_meta_dir, keycol, keycolen);

	int namelen;

	
	char *name = row_locate_col(index_root_rp, 
				TABLETSCHM_TABLETNAME_COLOFF_INROW,
				ROW_MINLEN_IN_TABLETSCHM, &namelen);

	index_bld_root_dir(tab_meta_dir, idxbld->idx_tab_name,
				idxbld->idx_meta->idxname,
				idxbld->idx_root_sstab);

	str1_to_str2(tab_meta_dir, '/', name);	

	
	Assert(STRLEN(tab_meta_dir) < SSTABLE_NAME_MAX_LEN);

	
	index_sstab_num = *(int *)row_locate_col(index_root_rp, 
				TABLETSCHM_TABLETID_COLOFF_INROW,
				ROW_MINLEN_IN_TABLETSCHM, &namelen);	

	
	TABINFO_INIT(&tabinfo, tab_meta_dir, NULL, 0, &sinfo,
					ROW_MINLEN_IN_INDEXBLK,
					TAB_DEL_INDEX, 
					idx_meta->idx_id, 
					index_sstab_num);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, INDEXBLK_KEYCOL_COLID_INROW,
			tabinfo.t_key_coltype, tabinfo.t_key_coloff);	
	
	index__del_row(&tabinfo, idxbld->idx_rp);
	
	session_close( &tabinfo);
	tabinfo_pop();

	tss->topid &= ~TSS_OP_INDEX_CASE;

	return TRUE;
}


static int
index__del_row(TABINFO *tabinfo, char *delrow)
{	
	LOCALTSS(tss);
	BUF	*bp;
	int	rnum;
	int	minlen;
	int	rlen;
	int	i;
	int	*offtab;
	char	*rp;
	int	del_stat;
	RID	*delrid;
	RID	*mother_rid;
	int	tot_ridnum;
	int	result;
	int	ign;
	


	minlen = tabinfo->t_row_minlen;
	del_stat = TRUE;
	
	tabinfo->t_sinfo->sistate |= SI_DEL_DATA;
	
	bp = blkget(tabinfo);

	if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
	{
		bufunkeep(bp->bsstab);
		return FALSE;
	}


	Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);
	rnum = tabinfo->t_rowinfo->rnum;

	if (tabinfo->t_sinfo->sistate & SI_NODATA)
	{
		traceprint("We can not find the row to be deleted.\n"); 
		bufunkeep(bp->bsstab);
		return FALSE;
	}

	bufpredirty(bp->bsstab);

	rp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, rnum);
	rlen = ROW_GET_LENGTH(rp, minlen);
	
	tot_ridnum = *(int *)row_locate_col(rp, 
					INDEXBLK_RIDNUM_COLOFF_INROW,
					ROW_MINLEN_IN_INDEXBLK, &ign);

	if (tot_ridnum > 1)
	{

		delrid = (RID *)row_locate_col(delrow, 
					IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
					ROW_MINLEN_IN_INDEXBLK, &ign);

		mother_rid = (RID *)row_locate_col(rp, 
					IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
					ROW_MINLEN_IN_INDEXBLK, &ign);
	
			
		for (i = 0; i < tot_ridnum; i++)
		{	
			result = index_rid_cmp((char *)delrid, 
						(char *)&(mother_rid[i]));
		
			if (result == EQ)
			{
				break;
			}
		}

		Assert(i < tot_ridnum);

		index_rmrid(bp->bblk, rnum, i);

		goto finish;

	}
	

	ROW_SET_STATUS(rp, ROW_DELETED);	

	offtab = ROW_OFFSET_PTR(bp->bblk);

	
	for(i = rnum; i < (bp->bblk->bnextrno - 1); i++)
	{
		offtab[-i] = offtab[-(i + 1)];	
	}

	BLK_GET_NEXT_ROWNO(bp->bblk)--;

	if (bp->bblk->bnextrno == 0)
	{
		blk_compact(bp->bblk);
	}

finish:
	bufdirty(bp->bsstab);
	bufunkeep(bp->bsstab);

	if (   (tss->topid & TSS_OP_RANGESERVER) 
	    && (!(tss->tstat & TSS_OP_RECOVERY))
	    && (!(tabinfo->t_stat & TAB_NOLOG_MODEL)))
	{
		tabinfo->t_insdel_old_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
		
		bp->bsstab->bblk->bsstab_insdel_ts_lo = mtts_increment(
					bp->bsstab->bblk->bsstab_insdel_ts_lo);

		tabinfo->t_insdel_new_ts_lo = 
					bp->bsstab->bblk->bsstab_insdel_ts_lo;
		
		LOGREC		logrec;

		log_build(&logrec, LOG_INDEX_DELETE, 
				tabinfo->t_insdel_old_ts_lo,
				tabinfo->t_insdel_new_ts_lo,
				tabinfo->t_sstab_name, NULL, 
				tabinfo->t_row_minlen, 
				tabinfo->t_tabid, 
				tabinfo->t_sstab_id,
				bp->bblk->bblkno, rnum, NULL, NULL);

		if (tot_ridnum > 1)
		{
			(&logrec)->loginsdel.status |= LOGINSDEL_RID_UPD;
			log_put(&logrec, (char *)delrid, sizeof(RID));
		}
		else
		{
			log_put(&logrec, delrow, rlen);
		}
	}	
		
	tabinfo->t_sinfo->sistate &= ~SI_DEL_DATA;

	return TRUE;

}


int
index_upd_rid(IDXBLD *idxbld, RID *oldrid, RID *newrid)
{
	LOCALTSS(tss);
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		index_sstab_num;
	char 		*index_root_rp;
	char		*keycol;
	int		keycolen;
	char		*tabname;
	int		idx_rootid;
	IDXMETA		*idx_meta;
	int		ign;
	

	
	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;
	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);

	
	tabinfo.t_key_coloff = IDXBLK_KEYCOL_FAKE_COLOFF_INROW;
	tabinfo.t_key_coltype = VARCHAR;
	
	

	tss->topid |= TSS_OP_INDEX_CASE;

	
	keycol		= idxbld->idx_rp;
	keycolen	= idxbld->idx_rlen;
	
	tabname		= idxbld->idx_tab_name;
	idx_rootid	= idxbld->idx_root_sstab;
	idx_meta	= idxbld->idx_meta;

	
	
	
	index_bld_root_name(tab_meta_dir, tabname, idx_meta->idxname, 
						idx_rootid, FALSE);

	
	index_root_rp = tablet_schm_srch_row(idx_meta->idx_id, idx_rootid,
					tab_meta_dir, keycol, keycolen);

	if (!index_root_rp )
	{
		return FALSE;
	}
	
	int namelen;

	
	char *name = row_locate_col(index_root_rp, 
				TABLETSCHM_TABLETNAME_COLOFF_INROW,
				ROW_MINLEN_IN_TABLETSCHM, &namelen);
			
	index_bld_root_dir(tab_meta_dir, tabname, idx_meta->idxname,
				idx_rootid);
	
	str1_to_str2(tab_meta_dir, '/', name);
	

	
	Assert(STRLEN(tab_meta_dir) < SSTABLE_NAME_MAX_LEN);

	
	index_sstab_num = *(int *)row_locate_col(index_root_rp, 
				TABLETSCHM_TABLETID_COLOFF_INROW,
				ROW_MINLEN_IN_TABLETSCHM, &namelen);	
	
	tabinfo_push(&tabinfo);

	
	TABINFO_INIT(&tabinfo, tab_meta_dir, NULL, 0, &sinfo,
					ROW_MINLEN_IN_INDEXBLK,
					TAB_DEL_DATA, 
					idx_meta->idx_id, 
					index_sstab_num);
	SRCH_INFO_INIT(&sinfo, keycol, keycolen, INDEXBLK_KEYCOL_COLID_INROW,
			tabinfo.t_key_coltype, tabinfo.t_key_coloff);	

	
	tabinfo.t_sinfo->sistate |= SI_UPD_DATA;

	BUF	*bp;
	int	rnum;
	char	*index_rp;
	int	index_rlen;
	RID	*ridp;
	int	ridnum;
	int	i;
	int	result;
		
	bp = blkget(&tabinfo);

	if (tabinfo.t_stat & TAB_RETRY_LOOKUP)
	{
		goto exit;
	}

//	offset = blksrch(tabinfo, bp);

	Assert(tabinfo.t_rowinfo->rblknum == bp->bblk->bblkno);
	Assert(tabinfo.t_rowinfo->rsstabid == bp->bsstab->bsstabid);
	rnum = tabinfo.t_rowinfo->rnum;

	if (tabinfo.t_sinfo->sistate & SI_NODATA)
	{
		traceprint("We can not find the row to be deleted.\n"); 

		Assert(0);
		goto exit;
	}

	index_rp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, rnum);
	index_rlen = ROW_GET_LENGTH(index_rp, ROW_MINLEN_IN_INDEXBLK);

	
	ridp = (RID *)row_locate_col(index_rp, IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
				ROW_MINLEN_IN_INDEXBLK, &ign);
	ridnum = *(int *)row_locate_col(index_rp, INDEXBLK_RIDNUM_COLOFF_INROW,
				ROW_MINLEN_IN_INDEXBLK, &ign);

	traceprint("RID count is %d -- state 1.\n",ridnum);
	
	result = LE;
	
	for (i = 0; i < ridnum; i++)
	{
		result = index_rid_cmp((char *)oldrid, (char *)(&(ridp[i])));

		if (result == EQ)
		{
			tabinfo.t_insdel_old_ts_lo = 
				bp->bsstab->bblk->bsstab_insdel_ts_lo;
	
			bp->bsstab->bblk->bsstab_insdel_ts_lo = 
				mtts_increment(bp->bsstab->bblk->bsstab_insdel_ts_lo);

			tabinfo.t_insdel_new_ts_lo = 
				bp->bsstab->bblk->bsstab_insdel_ts_lo;
			
			LOGREC		logrec;
			int		logid;

			logid = LOG_UPDRID;

			log_build(&logrec, logid, tabinfo.t_insdel_old_ts_lo,
						tabinfo.t_insdel_new_ts_lo,
						tabinfo.t_sstab_name, NULL, 
						tabinfo.t_row_minlen,
						tabinfo.t_tabid,
						tabinfo.t_sstab_id, 
						bp->bblk->bblkno,
						rnum,
						(char *)(&(ridp[i])),
						(char *)newrid);
			
			log_put(&logrec, NULL, 0);
		
			bufpredirty(bp->bsstab);
			
			MEMCPY((char *)(&(ridp[i])), (char *)newrid, sizeof(RID));
			
			bufdirty(bp->bsstab);

			break;
		}
	}

	if (result!= EQ)
	{
		Assert(0);
		traceprint("old rid (%d:%d:%d) is not found.\n", oldrid->sstable_id, oldrid->block_id, oldrid->roffset);
	}
exit:
	tabinfo.t_sinfo->sistate &= ~SI_UPD_DATA;
	bufunkeep(bp->bsstab);
	
	session_close( &tabinfo);
	tabinfo_pop();

	tss->topid &= ~TSS_OP_INDEX_CASE;

	return TRUE;
}

int
index_srch_root(IDX_ROOT_SRCH *root_srchctx)

{
	int		coltype;
	int		indexid;
	int		rootid;
	char		*rootname;
	int		key_is_expand;
	char		*keycol;
	int		keycolen;
	char		*rp;


	key_is_expand = FALSE;
	coltype = root_srchctx->coltype;
	
	keycol = root_srchctx->keycol;
	keycolen = root_srchctx->keycolen;

	rootid = root_srchctx->rootid;
	indexid = root_srchctx->indexid;
	rootname = root_srchctx->rootname;

	if ((keycolen == 1) && (!strncasecmp("*", keycol, keycolen)))
	{
		
		key_is_expand = TRUE;
		
	}
	
	if (key_is_expand)
	{
		
		if (root_srchctx->stat & IDX_ROOT_SRCH_1ST_KEY)
		{
			rp = tablet_get_1st_or_last_row(indexid, rootid, 
							rootname, TRUE, FALSE);
		}
		else if (root_srchctx->stat & IDX_ROOT_SRCH_LAST_KEY)
		{
			rp = tablet_get_1st_or_last_row(indexid, rootid,
							rootname, FALSE, FALSE);
		}
		else
		{
			Assert(0);
		}
	}
	else
	{
		rp = tablet_schm_srch_row(indexid, rootid, rootname, keycol, 
					keycolen);
	}

	int	namelen;
	root_srchctx->leafname = row_locate_col(rp, 
					TABLETSCHM_TABLETNAME_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);
			
	root_srchctx->sstab_id = *(int *)row_locate_col(rp,
					TABLETSCHM_TABLETID_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);
	
	return TRUE;
}



static int
index__range_sstab_scan(TABLET_SCANCTX *tablet_scanctx, IDXMETA *idxmeta, 
			IDX_RANGE_CTX *idx_range_ctx, char *tabdir)
{
	char		*resp;
	int		resp_size;
	BUF		*bp;
	int		rnum;
	char		last_sstab[SSTABLE_NAME_MAX_LEN];
	B_SRCHINFO	srchinfo;
	char		tab_left_sstab_dir[TABLE_NAME_MAX_LEN];
	char		tab_right_sstab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	char		*keyleft;
	int		keyleftlen;
	TABINFO		*tabinfo;
	TABINFO		t_tabinfo;
	SSTAB_SCANCTX	scanctx;
	RANGE_QUERYCTX	rgsel_cont_data;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		start_scan_row;


	bp = NULL;
	rtn_stat = TRUE;

	
	
	index_bld_root_dir(tab_left_sstab_dir, tabdir, idxmeta->idxname,
				idx_range_ctx->tabletid);
	str1_to_str2(tab_left_sstab_dir, '/', idx_range_ctx->sstab_left);

	
	index_bld_root_dir(tab_right_sstab_dir, tabdir, idxmeta->idxname,
				idx_range_ctx->tabletid);
	str1_to_str2(tab_right_sstab_dir, '/', idx_range_ctx->sstab_right);	

	keyleft = idx_range_ctx->key_left;
	keyleftlen = idx_range_ctx->keylen_left;

	MEMSET(&t_tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));
	

	tabinfo = &t_tabinfo;
	tabinfo->t_sinfo = &sinfo;
	tabinfo->t_rowinfo = &blk_rowinfo;
	

	TABINFO_INIT(tabinfo, tab_left_sstab_dir, idxmeta->idxname,
			STRLEN(idxmeta->idxname), tabinfo->t_sinfo, 
			ROW_MINLEN_IN_INDEXBLK, 0, idxmeta->idx_id, 
			idx_range_ctx->sstabid_left);
	
	SRCH_INFO_INIT(tabinfo->t_sinfo, keyleft, keyleftlen, 1, VARCHAR, -1);

	if (idx_range_ctx->left_expand)
	{
		bp = blk_getsstable(tabinfo);

		rnum = 0;
	}
	else
	{
		bp = blkget(tabinfo);
	
	
		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			bufunkeep(bp->bsstab);
			return FALSE;
		}

		Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
		Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);

		if ((tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
		    || (tabinfo->t_rowinfo->rsstabid != bp->bsstab->bsstabid))
		{
			traceprint("Hit a buffer error!\n");
			bufunkeep(bp->bsstab);
			ex_raise(EX_ANY);
		}
		
		rnum = tabinfo->t_rowinfo->rnum;

//		Assert(rnum != BLOCK_EMPTY_ROWID);
	}
	
	MEMSET(&scanctx, sizeof(SSTAB_SCANCTX));
	MEMSET(&rgsel_cont_data, sizeof(RANGE_QUERYCTX));

	scanctx.rgsel = &rgsel_cont_data;

	scanctx.andplan	= tablet_scanctx->andplan;
	scanctx.orplan	= tablet_scanctx->orplan;
	scanctx.rminlen	= tablet_scanctx->rminlen;
	rgsel_cont_data.rowminlen = tablet_scanctx->rminlen;
	scanctx.stat	= 0;
	

	BLOCK	*datablk;
	
	
	datablk= (BLOCK *)(scanctx.rgsel->data);
	datablk->bfreeoff = BLKHEADERSIZE;
	datablk->bnextrno = 0;
	datablk->bstat = 0;
	MEMSET(datablk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);
	
	char	resp_cli[8];	
		
	start_scan_row = rnum;

//	Assert(rnum < bp->bblk->bnextrno);

	
	int	data_cont = TRUE;
	int	get_row = TRUE;
	

	if (bp->bblk->bnextrno == 0)
	{
		
		goto nextblk;
	}
	
	
	while (TRUE)
	{	
		
		if (idx_range_ctx->right_expand)
		{
			scanctx.currow = 0;
			scanctx.endrow = bp->bblk->bnextrno - 1;
		}
		else
		{	
			TABINFO_INIT(tabinfo, tabinfo->t_sstab_name,
					tabinfo->t_tab_name, 
					tabinfo->t_tab_namelen, 
					tabinfo->t_sinfo, tabinfo->t_row_minlen, 
					0, tabinfo->t_tabid, tabinfo->t_sstab_id);

			
			SRCH_INFO_INIT(tabinfo->t_sinfo, idx_range_ctx->key_right, 
					idx_range_ctx->keylen_right, 1, VARCHAR, -1);			

			MEMSET(&srchinfo, sizeof(B_SRCHINFO));
			SRCHINFO_INIT((&srchinfo), 0, 
					BLK_GET_NEXT_ROWNO(bp->bblk) - 1, 
					BLK_GET_NEXT_ROWNO(bp->bblk), LE);

			b_srch_block(tabinfo, bp, &srchinfo);

			scanctx.currow = start_scan_row;

			if (srchinfo.brownum < (bp->bblk->bnextrno - 1))
			{
				scanctx.endrow = srchinfo.brownum;
				data_cont = FALSE;
			}
			else
			{
				Assert(srchinfo.brownum == (bp->bblk->bnextrno - 1));

				if (srchinfo.bcomp == GR)
				{
					scanctx.endrow = srchinfo.brownum;
				}
				else
				{
					if (srchinfo.bcomp == LE)
					{
						scanctx.endrow = 
							srchinfo.brownum - 1;
					}
					else
					{
						scanctx.endrow = 
							srchinfo.brownum;
					}

					data_cont = FALSE;
				}
			}

		}

		scanctx.curblk = bp->bblk->bblkno;
		scanctx.ridnum = 0;

		
		scanctx.rminlen = bp->bblk->bminlen;
		scanctx.sstab = (char *)(bp->bblk);
		
		while(get_row)
		{
			index_get_datarow(&scanctx, idx_range_ctx->tabname, 
						idx_range_ctx->tab_namelen,
						idx_range_ctx->tabid);

			if (!(scanctx.stat & SSTABSCAN_BLK_IS_FULL))
			{
				if (data_cont)
				{
					
					goto nextblk;
				}
			}

			
			rgsel_cont_data.cur_rowpos = 0;
			rgsel_cont_data.first_rowpos = 0;
			rgsel_cont_data.end_rowpos =
					BLK_GET_NEXT_ROWNO(datablk) - 1;
			rgsel_cont_data.status = DATA_CONT;

			resp = conn_build_resp_byte(RPC_SUCCESS, 
						sizeof(RANGE_QUERYCTX), 
						(char *)&rgsel_cont_data);

			resp_size = conn_get_resp_size((RPCRESP *)resp);

			
			tcp_put_data(tablet_scanctx->connfd, resp, resp_size);			

			conn_destroy_resp_byte(resp);	

			
			MEMSET(resp_cli, 8);
			int n = conn_socket_read(tablet_scanctx->connfd, 
							resp_cli, 8);

			if (n != 8)
			{
				traceprint("Socket read error 1.\n");
				goto done;
			}

			if (   !data_cont 
			    && !(scanctx.stat & SSTABSCAN_BLK_IS_FULL))
			{
				goto done;
			}
			
			Assert(scanctx.stat & SSTABSCAN_BLK_IS_FULL);

			
			scanctx.stat = 0;
			datablk->bfreeoff = BLKHEADERSIZE;
			datablk->bnextrno = 0;
			datablk->bstat = 0;
			MEMSET(datablk->bdata, BLOCKSIZE - BLKHEADERSIZE - 4);
				
		}
					
nextblk:	
		if (bp->bblk->bnextblkno != -1)
		{
			bp++;
		}
		else if (bp->bsstab->bblk->bnextsstabnum != -1)
		{
			
			if (!row_col_compare(VARCHAR,tabinfo->t_sstab_name,
						STRLEN(tabinfo->t_sstab_name),
						tab_right_sstab_dir,
						STRLEN(tab_right_sstab_dir)))
			{
				
				if (scanctx.stat & SSTABSCAN_HIT_ROW)
				{
					
					rgsel_cont_data.cur_rowpos = 0;
					rgsel_cont_data.first_rowpos = 0;
					rgsel_cont_data.end_rowpos = 
						BLK_GET_NEXT_ROWNO(datablk) - 1;
					rgsel_cont_data.status = DATA_CONT;

				 	resp = conn_build_resp_byte(RPC_SUCCESS, 
							sizeof(RANGE_QUERYCTX), 
							(char *)&rgsel_cont_data);
				
					resp_size = conn_get_resp_size((RPCRESP *)resp);

					
					tcp_put_data(tablet_scanctx->connfd, resp,
									resp_size);			

					conn_destroy_resp_byte(resp);	

					
					
					MEMSET(resp_cli, 8);
					int n = conn_socket_read(tablet_scanctx->connfd,
								resp_cli, 8);

					if (n != 8)
					{
						traceprint("Socket read error 2.\n");
						goto done;
					}
				}
				
				goto done;
			}
			
			
			MEMSET(last_sstab, SSTABLE_NAME_MAX_LEN);
			MEMCPY(last_sstab, tabinfo->t_sstab_name, 
					STRLEN(tabinfo->t_sstab_name));
			
			MEMSET(tabinfo->t_sstab_name, SSTABLE_NAME_MAX_LEN);			
			
			sstab_namebyid(last_sstab, tabinfo->t_sstab_name, 
						bp->bsstab->bblk->bnextsstabnum);

			tabinfo->t_sstab_id = bp->bsstab->bblk->bnextsstabnum;

			bufunkeep(bp->bsstab);
			
			bp = blk_getsstable(tabinfo);			
		}
		else
		{
			
			if (scanctx.stat & SSTABSCAN_HIT_ROW)
			{
				
				rgsel_cont_data.cur_rowpos = 0;
				rgsel_cont_data.first_rowpos = 0;
				rgsel_cont_data.end_rowpos = 
						BLK_GET_NEXT_ROWNO(datablk) - 1;
				rgsel_cont_data.status = DATA_CONT;

			 	resp = conn_build_resp_byte(RPC_SUCCESS, 
							sizeof(RANGE_QUERYCTX), 
							(char *)&rgsel_cont_data);
			
				resp_size = conn_get_resp_size((RPCRESP *)resp);

				
				tcp_put_data(tablet_scanctx->connfd, resp,
								resp_size);			

				conn_destroy_resp_byte(resp);	

				
				
				MEMSET(resp_cli, 8);
				int n = conn_socket_read(tablet_scanctx->connfd,
							resp_cli, 8);

				if (n != 8)
				{
					traceprint("Socket read error 2.\n");
					goto done;
				}
			}
			
			break;
		}

		if (bp->bblk->bnextrno > 0)
		{
			start_scan_row = 0;
		}
		else
		{
			
			goto nextblk;
		}		
			
	}

done:
	bufunkeep(bp->bsstab);
	
	return rtn_stat;
}



static int
index__get_count_or_sum(SSTAB_SCANCTX *scanctx, char *tabname, int tab_name_len,
				int tabid)
{
	int		ridnum;
	int		colen;
	RID		*ridarry;
	int		i;
	TABINFO		*tabinfo;
	BLK_ROWINFO	blk_rowinfo;
	char		sstab_name[SSTABLE_NAME_MAX_LEN];
	BUF		*bp;
	BLOCK		*blk;
	int		*offtab;
	char		*rp;
	char		*indexrp;
	int		datarow_minlen;
	char		sstab_full_path[TABLE_NAME_MAX_LEN];
	char		*colval;
	int		ign;
	
	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	

	tabinfo->t_rowinfo = &blk_rowinfo;

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	while(TRUE)
	{
		if (scanctx->currow > scanctx->endrow)
		{
			goto finish;
		}
		
		offtab = ROW_OFFSET_PTR((BLOCK *)(scanctx->sstab));
		indexrp = (char *)(scanctx->sstab) + offtab[-(scanctx->currow)];

		ridnum = *(int *)row_locate_col(indexrp, 
						INDEXBLK_RIDNUM_COLOFF_INROW,
						ROW_MINLEN_IN_INDEXBLK, &colen);
		
		traceprint("RID count is %d.\n -- state 2",ridnum);

		ridarry = (RID *)row_locate_col(indexrp, 
					IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
					ROW_MINLEN_IN_INDEXBLK, &colen);

		MEMSET(sstab_full_path, TABLE_NAME_MAX_LEN);
		MEMCPY(sstab_full_path, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

		for (i = scanctx->ridnum; i < ridnum; i++)
		{
			
			sstab_bld_name(sstab_name, tabname, tab_name_len,
					ridarry[i].sstable_id);

			str1_to_str2(sstab_full_path, '/', tabname);
			str1_to_str2(sstab_full_path, '/', sstab_name);
			
			TABINFO_INIT(tabinfo, sstab_full_path, tabname,
					tab_name_len, tabinfo->t_sinfo, -1, 0, 
					tabid, ridarry[i].sstable_id);

			
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, 1, 
					VARCHAR, -1);

			bp = blk_getsstable(tabinfo);

			datarow_minlen = bp->bblk->bminlen;

			blk = bp->bblk + ridarry[i].block_id;

			offtab = ROW_OFFSET_PTR(blk);

			rp = (char *)blk + ridarry[i].roffset;

			if (   par_process_orplan(scanctx->orplan, rp,
						datarow_minlen)
			    && par_process_andplan(scanctx->andplan, rp, 
			    			datarow_minlen))
			{
				switch (scanctx->querytype)
				{
				    case SELECTCOUNT:
				
					
					(scanctx->rowcnt)++;
					break;
					
				    case SELECTSUM:
					
					colval = row_locate_col(rp,
							scanctx->sum_coloff,
							datarow_minlen, &ign);
					
					scanctx->sum_colval += *(int *)colval;
					break;
					
				    default:
					traceprint("No any counting.\n");
					break;
				}

			}

			bufunkeep(bp);

			(scanctx->ridnum)++;
		}

		(scanctx->currow)++;
		scanctx->ridnum = 0;

	};

finish:

	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

	return TRUE;
}


static int
index__count_sstab_scan(TABLET_SCANCTX *tablet_scanctx, IDXMETA *idxmeta, 
			IDX_RANGE_CTX *idx_range_ctx, char *tabdir)
{
	BUF		*bp;
	int		rnum;
	char		last_sstab[SSTABLE_NAME_MAX_LEN];
	B_SRCHINFO	srchinfo;
	char		tab_left_sstab_dir[TABLE_NAME_MAX_LEN];
	char		tab_right_sstab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	char		*keyleft;
	int		keyleftlen;
	TABINFO		*tabinfo;
	TABINFO		t_tabinfo;
	SSTAB_SCANCTX	scanctx;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		start_scan_row;


	bp = NULL;
	rtn_stat = TRUE;

	
	
	index_bld_root_dir(tab_left_sstab_dir, tabdir, idxmeta->idxname,
				idx_range_ctx->tabletid);
	str1_to_str2(tab_left_sstab_dir, '/', idx_range_ctx->sstab_left);

	
	index_bld_root_dir(tab_right_sstab_dir, tabdir, idxmeta->idxname,
				idx_range_ctx->tabletid);
	str1_to_str2(tab_right_sstab_dir, '/', idx_range_ctx->sstab_right);	

	keyleft = idx_range_ctx->key_left;
	keyleftlen = idx_range_ctx->keylen_left;

	MEMSET(&t_tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));
	

	tabinfo = &t_tabinfo;
	tabinfo->t_sinfo = &sinfo;
	tabinfo->t_rowinfo = &blk_rowinfo;
	

	TABINFO_INIT(tabinfo, tab_left_sstab_dir, idxmeta->idxname,
			STRLEN(idxmeta->idxname), tabinfo->t_sinfo, 
			ROW_MINLEN_IN_INDEXBLK, 0, idxmeta->idx_id, 
			idx_range_ctx->sstabid_left);
	
	SRCH_INFO_INIT(tabinfo->t_sinfo, keyleft, keyleftlen, 1, VARCHAR, -1);

	if (idx_range_ctx->left_expand)
	{
		bp = blk_getsstable(tabinfo);

		rnum = 0;
	}
	else
	{
		bp = blkget(tabinfo);
	
	
		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			bufunkeep(bp->bsstab);
			return FALSE;
		}

		Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
		Assert(tabinfo->t_rowinfo->rsstabid == bp->bsstab->bsstabid);

		if ((tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
		    || (tabinfo->t_rowinfo->rsstabid != bp->bsstab->bsstabid))
		{
			traceprint("Hit a buffer error!\n");
			bufunkeep(bp->bsstab);
			ex_raise(EX_ANY);
		}
		
		rnum = tabinfo->t_rowinfo->rnum;

//		Assert(rnum != BLOCK_EMPTY_ROWID);
	}
	
	MEMSET(&scanctx, sizeof(SSTAB_SCANCTX));

	scanctx.andplan	= tablet_scanctx->andplan;
	scanctx.orplan	= tablet_scanctx->orplan;
	scanctx.rminlen	= tablet_scanctx->rminlen;
	scanctx.querytype = tablet_scanctx->querytype;
	scanctx.stat	= 0;	
	start_scan_row	= rnum;
	scanctx.sum_coloff = tablet_scanctx->sum_coloff;

//	Assert(rnum < bp->bblk->bnextrno);

	if (bp->bblk->bnextrno == 0)
	{
		
		goto nextblk;
	}
	
	
	while (TRUE)
	{	
		
		if (idx_range_ctx->right_expand)
		{
			scanctx.currow = 0;
			scanctx.endrow = bp->bblk->bnextrno - 1;
		}
		else
		{	
			TABINFO_INIT(tabinfo, tabinfo->t_sstab_name,
					tabinfo->t_tab_name, 
					tabinfo->t_tab_namelen, 
					tabinfo->t_sinfo, tabinfo->t_row_minlen, 
					0, tabinfo->t_tabid, tabinfo->t_sstab_id);

			
			SRCH_INFO_INIT(tabinfo->t_sinfo, idx_range_ctx->key_right, 
					idx_range_ctx->keylen_right, 1, VARCHAR, -1);			

			MEMSET(&srchinfo, sizeof(B_SRCHINFO));
			SRCHINFO_INIT((&srchinfo), 0, 
					BLK_GET_NEXT_ROWNO(bp->bblk) - 1, 
					BLK_GET_NEXT_ROWNO(bp->bblk), LE);

			b_srch_block(tabinfo, bp, &srchinfo);

			scanctx.currow = start_scan_row;

			if (srchinfo.brownum < (bp->bblk->bnextrno - 1))
			{
				scanctx.endrow = srchinfo.brownum;
			}
			else
			{
				Assert(srchinfo.brownum == (bp->bblk->bnextrno - 1));

				if (srchinfo.bcomp == GR)
				{
					scanctx.endrow = srchinfo.brownum;
				}
				else
				{
					if (srchinfo.bcomp == LE)
					{
						scanctx.endrow = 
							srchinfo.brownum - 1;
					}
					else
					{
						scanctx.endrow = 
							srchinfo.brownum;
					}

				}
			}

		}

		scanctx.curblk = bp->bblk->bblkno;
		scanctx.ridnum = 0;

		
		scanctx.rminlen = bp->bblk->bminlen;
		scanctx.sstab = (char *)(bp->bblk);
				
		index__get_count_or_sum(&scanctx, idx_range_ctx->tabname, 
						idx_range_ctx->tab_namelen,
						idx_range_ctx->tabid);		
					
nextblk:	
		if (bp->bblk->bnextblkno != -1)
		{
			bp++;
		}
		else if (bp->bsstab->bblk->bnextsstabnum != -1)
		{
			
			if (!row_col_compare(VARCHAR,tabinfo->t_sstab_name,
						STRLEN(tabinfo->t_sstab_name),
						tab_right_sstab_dir,
						STRLEN(tab_right_sstab_dir)))
			{
				goto done;
			}
			
			
			MEMSET(last_sstab, SSTABLE_NAME_MAX_LEN);
			MEMCPY(last_sstab, tabinfo->t_sstab_name, 
					STRLEN(tabinfo->t_sstab_name));
			
			MEMSET(tabinfo->t_sstab_name, SSTABLE_NAME_MAX_LEN);			
			
			sstab_namebyid(last_sstab, tabinfo->t_sstab_name, 
						bp->bsstab->bblk->bnextsstabnum);

			tabinfo->t_sstab_id = bp->bsstab->bblk->bnextsstabnum;

			bufunkeep(bp->bsstab);
			
			bp = blk_getsstable(tabinfo);			
		}
		else
		{

			break;
		}

		if (bp->bblk->bnextrno > 0)
		{
			start_scan_row = 0;
		}
		else
		{
			
			goto nextblk;
		}		
			
	}

done:
	
	switch (tablet_scanctx->querytype)
	{
	    case SELECTCOUNT:
		tablet_scanctx->rowcnt += scanctx.rowcnt;
		break;

	    case SELECTSUM:
	    	tablet_scanctx->sum_colval += scanctx.sum_colval;
	    	break;

	    default:
	    	traceprint("No any counting.\n");
		Assert(0);
	    	break;
	}
	
	bufunkeep(bp->bsstab);
	
	return rtn_stat;
}

void
index_range_sstab_scan(TABLET_SCANCTX * tablet_scanctx,IDXMETA * idxmeta,
				char *tabname, int tabnamelen, int tabletid)
{
	IDX_RANGE_CTX	*idx_range_ctx;
	IDX_RANGE_CTX	t_idx_range_ctx;

	IDX_ROOT_SRCH	*root_srchctx;
	IDX_ROOT_SRCH	t_root_srchctx;
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];


	MEMSET(&t_idx_range_ctx, sizeof(IDX_RANGE_CTX));
	idx_range_ctx = &t_idx_range_ctx;

	MEMSET(&t_root_srchctx, sizeof(IDX_ROOT_SRCH));
	root_srchctx = &t_root_srchctx;

	index_bld_root_name(tab_meta_dir, tablet_scanctx->tabdir, 
				idxmeta->idxname, tabletid, FALSE);

	index_fill_rangectx_andplan(tablet_scanctx->andplan, 
					idxmeta->idx_col_map, idx_range_ctx);

	root_srchctx->indexid = idxmeta->idx_id;
	root_srchctx->rootname = tab_meta_dir;
	root_srchctx->rootid = tabletid;
	
	root_srchctx->coltype = idx_range_ctx->coltype;

	traceprint("Using Index.\n");
	
	int k;
	
	for (k = 0; k < 2; k++)
	{
		 
		root_srchctx->keycol = (k == 0) ? idx_range_ctx->key_left 
						: idx_range_ctx->key_right;
		
		root_srchctx->keycolen = (k == 0) ? idx_range_ctx->keylen_left
						: idx_range_ctx->keylen_right;
	

		index_srch_root(root_srchctx);

		if (k == 0)
		{
			idx_range_ctx->sstab_left = root_srchctx->leafname;
			idx_range_ctx->sstabid_left = root_srchctx->sstab_id;
		}
		else
		{
			idx_range_ctx->sstab_right = root_srchctx->leafname;
			idx_range_ctx->sstabid_right = root_srchctx->sstab_id;
		}
	}

	
	idx_range_ctx->tabid = tablet_scanctx->tabid;
	idx_range_ctx->tabname = tabname;
	idx_range_ctx->tab_namelen = tabnamelen;
	idx_range_ctx->tabletid = root_srchctx->rootid;

	if (tablet_scanctx->querytype == SELECTWHERE)
	{
		index__range_sstab_scan(tablet_scanctx, idxmeta, idx_range_ctx,
					tablet_scanctx->tabdir);
	}
	else
	{
		Assert(   (tablet_scanctx->querytype == SELECTCOUNT)
		       || (tablet_scanctx->querytype == SELECTSUM));

		index__count_sstab_scan(tablet_scanctx, idxmeta, idx_range_ctx,
					tablet_scanctx->tabdir);
	}
	
	return;

}




int
index_bld_row(char *index_rp, int index_rlen, RID *rid, char *keycol, 
					int keycolen, int keycol_type)
{
	int	index_ridx;
	int	ridnum = 1;


	index_ridx = 0;

	
	row_build_hdr((index_rp + index_ridx), 0, 0, 2);

	
	index_ridx += sizeof(ROWFMT);

	PUT_TO_BUFFER(index_rp, index_ridx, &ridnum, sizeof(int));

	
	PUT_TO_BUFFER(index_rp, index_ridx, &index_rlen, sizeof(int));

	
	PUT_TO_BUFFER(index_rp, index_ridx, keycol, keycolen);

	traceprint("index row key is %s.\n", keycol);

	PUT_TO_BUFFER(index_rp, index_ridx, rid, sizeof(RID));
	
	
	*(int *)(index_rp + index_ridx) = index_ridx - sizeof(RID);

	index_ridx += COLOFFSETENTRYSIZE;

	
	*(int *)(index_rp + index_ridx) = index_ridx - keycolen - 
					COLOFFSETENTRYSIZE - sizeof(RID);
	
	index_ridx += COLOFFSETENTRYSIZE;	

	Assert(index_ridx == index_rlen);

	return TRUE;
}


int
index_tab_has_index(META_SYSINDEX *meta_sysidx, int tabid)
{
	IDXMETA		*idx_meta;
	int		meta_num;


	idx_meta = meta_sysidx->idx_meta;
	
	for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
	{
		if (tabid == idx_meta->idx_tabid)
		{
			return TRUE;
		}

		idx_meta++;
	}

	
	return FALSE;
}




int
index_tab_check_index(META_SYSINDEX *meta_sysidx, int tabid)
{
	IDXMETA		*idx_meta;
	int		meta_num;
	char		idx_statinfo[256];
	

	idx_meta = meta_sysidx->idx_meta;

	traceprint("Table (id = %d) index information, as follows:\n", tabid);
	traceprint("====== BEGIN ======\n");
	for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
	{
		if (tabid == idx_meta->idx_tabid)
		{
			traceprint("Table ID:	%d. \n", idx_meta->idx_tabid);
			traceprint("Index name:	%s. \n", idx_meta->idxname);
			traceprint("Index ID:	%d. \n", idx_meta->idx_id);
			
			traceprint("Index column map: %d. \n", idx_meta->idx_col_map);

			MEMSET(idx_statinfo, 256);
			
			sprintf(idx_statinfo, "%s %s %s %s", 
				(idx_meta->idx_stat & IDX_IN_CREATE) ? "IDX_IN_CREATE": "",
				(idx_meta->idx_stat & IDX_IN_DROP) ? "IDX_IN_DROP": "",
				(idx_meta->idx_stat & IDX_IN_WORKING) ? "IDX_IN_WORKING": "",
				(idx_meta->idx_stat & IDX_IN_SUSPECT) ? "IDX_IN_SUSPECT": "");

			traceprint("Index state: %s. \n", idx_statinfo);			
		}

		idx_meta++;
	}

	traceprint("====== END ======\n");

	
	return TRUE;
}



int
index_get_meta_by_colmap(int tabid, int colmap, META_SYSINDEX *meta_sysidx)
{
	IDXMETA		*idx_meta;
	int		meta_num;


	idx_meta = meta_sysidx->idx_meta;
	
	for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
	{
		if (tabid == idx_meta->idx_tabid)
		{
			if (colmap & idx_meta->idx_col_map)
			{
				return meta_num;
			}
		}

		idx_meta++;
	}

	
	return -1;
	
}



int
index_get_meta_by_idxname(int tabid, char *idxname, META_SYSINDEX *meta_sysidx)
{
	IDXMETA		*idx_meta;
	int		meta_num;


	idx_meta = meta_sysidx->idx_meta;
	
	for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
	{
		if (tabid == idx_meta->idx_tabid)
		{
			if (!strcmp(idxname, idx_meta->idxname))
			{
				return meta_num;
			}
		}

		idx_meta++;
	}

	
	return -1;
	
}

int
index_ins_meta(IDXMETA *idxmeta, META_SYSINDEX *meta_sysidx)
{
	MEMCPY(&(meta_sysidx->idx_meta[meta_sysidx->idx_num]), idxmeta, 
				sizeof(IDXMETA));
	
	(meta_sysidx->idx_num)++;
	(meta_sysidx->idx_ver)++;

	return TRUE;
}

int
index_del_meta(int tabid, char *idxname, META_SYSINDEX *meta_sysidx)
{
	int	meta_num;


	meta_num = index_get_meta_by_idxname(tabid, idxname, meta_sysidx);

	if (meta_num == -1)
	{
		traceprint("This index(%d, %s) is not exist!\n", tabid, idxname);

		return FALSE;
	}

	
	Assert(meta_sysidx->idx_meta[meta_num].idx_stat != IDX_IN_WORKING);

	BACKMOVE((char *)(&(meta_sysidx->idx_meta[meta_num + 1])), 
			(char *)(&(meta_sysidx->idx_meta[meta_num])), 
			(meta_sysidx->idx_num - meta_num) * sizeof(IDXMETA));

	(meta_sysidx->idx_num)--;
	(meta_sysidx->idx_ver)++;

	return TRUE;
}

int
index_bld_meta(IDXMETA *idxmeta, TABLEHDR *tabhdr, COLINFO *colinfo, 
			TREE *command, int idxid)
{
	TREE	*col_tree;
	int	col_num;
	int	colcnt;


	colcnt = 0;
	idxmeta->idx_stat = IDX_IN_CREATE;
	idxmeta->idx_tabid = tabhdr->tab_id;
//	idxmeta->idx_root_sstab = -1;
	idxmeta->idx_id = idxid;

	
	col_tree = command->right->left;

	while (col_tree)
	{
		for (col_num = 0; col_num < tabhdr->tab_col; col_num++)
		{
		        if (!strcmp(col_tree->sym.resdom.colname, 
					colinfo[col_num].col_name))
		        {
		        	TAB_COL_SET_INDEX(idxmeta->idx_col_map,
						colinfo[col_num].col_id);
		        	break;
		        }
		}

		colcnt ++;

		if (colcnt > COL_MAX_NUM)
		{
			traceprint("The # of column (%d) expands the limit.\n", colcnt);
			return FALSE;
		}
		
		col_tree = col_tree->left;
	}
	
	return TRUE;
}


static int
index_bld_sstabnum(int tablet_num, int sstab_index)
{
	int	sstabnum;


	sstabnum = tablet_num;
	sstabnum <<= 16;

        sstabnum |= sstab_index;

	return sstabnum;
}



int
index_bld_root_dir(char *tab_meta_dir, char *tab_name, char *idx_name, 
			int tablet_id)
{
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);

	sprintf(tab_meta_dir, "%s/%s/%s_%d_ptn", tab_name, idx_name, idx_name, tablet_id);
	
	return TRUE;
}

int
index_bld_root_name(char *tab_meta_dir, char *tab_name, char *idx_name, 
			int tablet_id, int mk_dir)
{
	char	idxname[64];
	int	status;

	
	index_bld_root_dir(tab_meta_dir, tab_name, idx_name, tablet_id);

	if (mk_dir)
	{
		MKDIR(status, tab_meta_dir, 0755);
	}

	MEMSET(idxname, 64);

	sprintf(idxname, "%s_%d_root", idx_name, tablet_id);

	str1_to_str2(tab_meta_dir, '/', idxname);
	
	return TRUE;
}

int
index_bld_leaf_name(char *tab_meta_dir, char *index_sstab_name, char *tab_name, 
			char *idx_name, int tablet_num, int sstab_num)
{
	int	index_sstab_num;
	
	MEMSET(index_sstab_name, 64);
	
	index_bld_root_dir(tab_meta_dir, tab_name, idx_name, tablet_num);

	index_sstab_num = index_bld_sstabnum(tablet_num, sstab_num);

	sprintf(index_sstab_name, "%s_%d_sstable%d", idx_name, tablet_num, index_sstab_num);

	str1_to_str2(tab_meta_dir, '/', index_sstab_name);

	return TRUE;
}

int
index_get_datarow(SSTAB_SCANCTX *scanctx, char *tabname, int tab_name_len, int tabid)
{
	int		ridnum;
	int		colen;
	RID		*ridarry;
	int		i;
	TABINFO		*tabinfo;
	BLK_ROWINFO	blk_rowinfo;
	char		sstab_name[SSTABLE_NAME_MAX_LEN];
	BUF		*bp;
	BLOCK		*blk;
	int		*offtab;
	char		*rp;
	char		*indexrp;
	int		datarow_minlen;
	char		sstab_full_path[TABLE_NAME_MAX_LEN];

	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	

	tabinfo->t_rowinfo = &blk_rowinfo;

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	while(TRUE)
	{
		if (scanctx->currow > scanctx->endrow)
		{
			goto finish;
		}
		
		offtab = ROW_OFFSET_PTR((BLOCK *)(scanctx->sstab));
		indexrp = (char *)(scanctx->sstab) + offtab[-(scanctx->currow)];

		ridnum = *(int *)row_locate_col(indexrp, 
						INDEXBLK_RIDNUM_COLOFF_INROW,
						ROW_MINLEN_IN_INDEXBLK, &colen);
		
		traceprint("RID count is %d.\n -- state 2",ridnum);

		ridarry = (RID *)row_locate_col(indexrp, 
					IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
					ROW_MINLEN_IN_INDEXBLK, &colen);

		MEMSET(sstab_full_path, TABLE_NAME_MAX_LEN);
		MEMCPY(sstab_full_path, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

		for (i = scanctx->ridnum; i < ridnum; i++)
		{
			
			sstab_bld_name(sstab_name, tabname, tab_name_len,
					ridarry[i].sstable_id);

			str1_to_str2(sstab_full_path, '/', tabname);
			str1_to_str2(sstab_full_path, '/', sstab_name);
			
			TABINFO_INIT(tabinfo, sstab_full_path, tabname,
					tab_name_len, tabinfo->t_sinfo, -1, 0, 
					tabid, ridarry[i].sstable_id);

			
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, 1, 
					VARCHAR, -1);

			bp = blk_getsstable(tabinfo);

			datarow_minlen = bp->bblk->bminlen;

			blk = bp->bblk + ridarry[i].block_id;

			offtab = ROW_OFFSET_PTR(blk);

			rp = (char *)blk + ridarry[i].roffset;

			if (   par_process_orplan(scanctx->orplan, rp,
						datarow_minlen)
			    && par_process_andplan(scanctx->andplan, rp, 
			    			datarow_minlen))
			{
				if (!(scanctx->stat & SSTABSCAN_HIT_ROW))
				{
					scanctx->stat |= SSTABSCAN_HIT_ROW;
				}
				
				if (!(blk_appendrow(
					(BLOCK *)(scanctx->rgsel->data),
					rp, ROW_GET_LENGTH(rp, datarow_minlen))))
				{
					scanctx->stat |= SSTABSCAN_BLK_IS_FULL;

					bufunkeep(bp);
					goto finish;
				}
			}

			bufunkeep(bp);

			(scanctx->ridnum)++;
		}

		(scanctx->currow)++;
		scanctx->ridnum = 0;

	};

finish:

	session_close(tabinfo);

	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();

	return TRUE;
}

int
index_fill_rangectx_andplan(ORANDPLAN *cmd, int col_map, 
				IDX_RANGE_CTX *idx_range_ctx)
{
	int		rtn_stat;
	SRCHCLAUSE	*srchclause;
	TREE		*tmptree;
	int		colid;
	int		colmap;


	rtn_stat = FALSE;
	
	if (cmd == NULL)
	{
		goto finish;
	}	
		
	while(cmd)
	{
		colmap = 0;

		srchclause = &(cmd->orandsclause);

		colid = srchclause->scterms->left->sym.resdom.colid;
		tmptree = srchclause->scterms->left;
	
		TAB_COL_SET_INDEX(colmap, colid);		

		if (colmap & col_map)
		{		
			idx_range_ctx->coltype = 
					tmptree->sym.resdom.coltype;

			idx_range_ctx->key_left = 
					tmptree->right->sym.constant.value;

			idx_range_ctx->keylen_left =
					tmptree->right->sym.constant.len;

			idx_range_ctx->key_right =
					tmptree->right->sym.constant.rightval;

			idx_range_ctx->keylen_right =
					tmptree->right->sym.constant.rightlen;

			if (strncasecmp("*", idx_range_ctx->key_left, 
					idx_range_ctx->keylen_left) == 0)
			{
				idx_range_ctx->left_expand = TRUE;
			}

			if (strncasecmp("*", idx_range_ctx->key_right, 
					idx_range_ctx->keylen_right) == 0)
			{
				idx_range_ctx->right_expand = TRUE;
			}

			rtn_stat = TRUE;

			goto finish;
		}
		
		cmd = cmd->orandplnext;
		
	}

finish:
	return rtn_stat;
}


int
index_insert(IDXBLD *idxbld, TABINFO *tabinfo,	META_SYSINDEX *meta_sysidx)
{
	int		index_rlen;
	BLOCK		*index_rp;
	IDXMETA		*idx_meta;
	int		meta_num;
	int		idx_stat;
	int		col_idx;	
	COLINFO		*colinfo;
	int		rminlen;
	char		*datarp;
	int		datarlen;
	char		*datacol;
	int		data_colen;
	RID		*rid;
	int		idx_sstab_split;
		

	
	colinfo		= tabinfo->t_colinfo;
	rminlen		= tabinfo->t_row_minlen;
	datarp		= tabinfo->t_cur_rowp;
	datarlen	= tabinfo->t_cur_rowlen;
	rid		= &(tabinfo->t_currid);
	
	idx_meta	= meta_sysidx->idx_meta;
	idx_stat	= idxbld->idx_stat;
	idx_sstab_split	= FALSE;

	BUF_GET_RESERVED(index_rp);
	
	
	for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
	{
		if (tabinfo->t_tabid == idx_meta->idx_tabid)
		{
			idxbld->idx_meta = idx_meta;

			col_idx = 0;
				
			INDEX_MAP_GET_COLUMN_NUM(idx_meta->idx_col_map,
							col_idx);

			
			datacol = row_locate_col(datarp, 
						colinfo[col_idx].col_offset,
						rminlen, &data_colen);

			
			index_rlen = sizeof(ROWFMT) + 2 * sizeof(int)
						+ data_colen + sizeof(RID)
						+ 2 * COLOFFSETENTRYSIZE;

			Assert(index_rlen < BLOCKSIZE);

			MEMSET((char *)index_rp, index_rlen);			

			index_bld_row((char *)index_rp, index_rlen, rid, datacol,
					data_colen, colinfo[col_idx].col_type);

			idxbld->idx_rp = (char *)index_rp;
			idxbld->idx_rlen = index_rlen;

			index_ins_row(idxbld);

			
			if (idxbld->idx_stat & IDXBLD_SSTAB_SPLIT)
			{
				idx_sstab_split = TRUE;
			}
			
			idxbld->idx_stat = idx_stat;
		}

		idx_meta++;
	}	
	
	BUF_RELEASE_RESERVED(index_rp);

	
	if (idx_sstab_split)
	{
		hkgc_wash_sstab(TRUE);
	}
	
	return TRUE;
}

int
index_delete(IDXBLD *idxbld, TABINFO *tabinfo, META_SYSINDEX *meta_sysidx)
{
	int		index_rlen;
	BLOCK		*index_rp;
	IDXMETA		*idx_meta;
	int		meta_num;
	int		idx_stat;
	int		col_idx;	
	COLINFO		*colinfo;
	int		rminlen;
	char		*datarp;
	int		datarlen;
	char		*datacol;
	int		data_colen;
	RID		*rid;
	int		idx_sstab_split;
	
		

	
	colinfo		= tabinfo->t_colinfo;
	rminlen		= tabinfo->t_row_minlen;
	datarp		= tabinfo->t_cur_rowp;
	datarlen	= tabinfo->t_cur_rowlen;
	rid		= &(tabinfo->t_currid);
	
	idx_meta	= meta_sysidx->idx_meta;
	idx_stat	= idxbld->idx_stat;
	idx_sstab_split	= FALSE;

	BUF_GET_RESERVED(index_rp);
	
	
	for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
	{
		if (tabinfo->t_tabid == idx_meta->idx_tabid)
		{
			idxbld->idx_meta = idx_meta;

			col_idx = 0;
				
			INDEX_MAP_GET_COLUMN_NUM(idx_meta->idx_col_map,
							col_idx);

			
			datacol = row_locate_col(datarp, 
						colinfo[col_idx].col_offset,
						rminlen, &data_colen);

			
			
			index_rlen = sizeof(ROWFMT) + 2 * sizeof(int)
						+ data_colen + sizeof(RID)
						+ 2 * COLOFFSETENTRYSIZE;

			Assert(index_rlen < BLOCKSIZE);

			MEMSET((char *)index_rp, index_rlen);			

			index_bld_row((char *)index_rp, index_rlen, rid, datacol,
					data_colen, colinfo[col_idx].col_type);

			idxbld->idx_rp = (char *)index_rp;
			idxbld->idx_rlen = index_rlen;			

			index_del_row(idxbld);

			idxbld->idx_stat = idx_stat;
		}

		idx_meta++;
	}	

	BUF_RELEASE_RESERVED(index_rp);
	
	return TRUE;
}



int
index_update(IDXBLD *idxbld, IDXUPD *idxupd, TABINFO *tabinfo,
				META_SYSINDEX *meta_sysidx)
{
	LOCALTSS(tss);
	int		i;
	RID		oldrid;
	RID		newrid;
	IDXMETA		*idx_meta;
	int		idx_stat;
	int		meta_num;
	int		*old_offtab;
	int		*new_offtab;
	BLOCK		*oldblk;
	BLOCK		*newblk;
	int		split_row;
	COLINFO		*colinfo;
	int		col_idx;


	MEMSET(&oldrid, sizeof(RID));
	MEMSET(&newrid, sizeof(RID));
	
	oldblk			= idxupd->oldblk;
	newblk			= idxupd->newblk;		
	old_offtab		= ROW_OFFSET_PTR(oldblk);
	new_offtab		= ROW_OFFSET_PTR(newblk);	
	oldrid.sstable_id 	= idxupd->old_sstabid;
	oldrid.block_id		= oldblk->bblkno;	
	newrid.sstable_id	= idxupd->new_sstabid;
	newrid.block_id		= newblk->bblkno;
	split_row		= idxupd->start_row;
	colinfo			= tabinfo->t_colinfo;

	if (DEBUG_TEST(tss))
	{
		traceprint("Index Update.\n");
	}
		
	for (i = 0; i < newblk->bnextrno; i++)
	{
		oldrid.roffset = old_offtab[-split_row];

		split_row++;

		newrid.roffset = new_offtab[-i];

		idx_meta = meta_sysidx->idx_meta;
		
		idx_stat = idxbld->idx_stat;		
		
		
		for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
		{
			if (tabinfo->t_tabid == idx_meta->idx_tabid)
			{
				idxbld->idx_meta = idx_meta;

				col_idx = 0;
				
				INDEX_MAP_GET_COLUMN_NUM(idx_meta->idx_col_map,
								col_idx);

				
				idxbld->idx_rp = row_locate_col(
						(char *)oldblk + oldrid.roffset,
						colinfo[col_idx].col_offset,
						tabinfo->t_row_minlen, 
						&(idxbld->idx_rlen));
		
				if (!index_upd_rid(idxbld, &oldrid, &newrid))
				{
					
					Assert(0);
				}
		
				idxbld->idx_stat = idx_stat;
			}
		
			idx_meta++;
		}	
	}
	
	return TRUE;
}


int
index_rid_cmp(char *rid1, char *rid2)
{
	int	i;


	for (i = 0; i < sizeof(RID); i++)
	{
		if (rid1[i] < rid2[i])
		{
			return (LE);
		}
		
		if (rid1[i] > rid2[i])
		{
			return (GR);
		}
	}
	
	return (EQ);

}
		


int
index_root_crt_empty(int tabid, char *tabname, int tabletid, 
			int ovflow_tablet, META_SYSINDEX *meta_sysidx)
{
	int		meta_num;
	IDXMETA		*idx_meta;
	char		indexroot[TABLE_NAME_MAX_LEN];
	char		tabletname[TABLE_NAME_MAX_LEN];
	int		blkno;
	char		*blk, *filehdr;
	int		fd;
	int		rtn_stat;
	char		rg_tab_name[TABLE_NAME_MAX_LEN];
	
	TABINFO		*tabinfo;
	int		minrowlen;
	BUF		*bp;
	BLK_ROWINFO	blk_rowinfo;
	BLOCK		*tabletblk;


	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo; 
	
	tabinfo_push(tabinfo);

	minrowlen = ROW_MINLEN_IN_TABLET;


	MEMSET(rg_tab_name, TABLE_NAME_MAX_LEN);
	
	MEMCPY(rg_tab_name, MT_RANGE_TABLE, STRLEN(MT_RANGE_TABLE));

	str1_to_str2(rg_tab_name, '/', tabname);
		

	MEMSET(tabletname, TABLE_NAME_MAX_LEN);

	sprintf(tabletname, "%s/%s/tablet%d", MT_META_TABLE, tabname, tabletid);
	

	TABINFO_INIT(tabinfo, tabletname, NULL, 0, tabinfo->t_sinfo, 
			minrowlen, TAB_SCHM_SRCH, tabid, tabletid);
	
	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, 
			TABLET_KEY_COLID_INROW, VARCHAR, -1);			
	
	
	bp = blk_getsstable(tabinfo);

	tabletblk = bp->bblk;
	
	rtn_stat = FALSE;
	
	idx_meta = meta_sysidx->idx_meta;
	

	filehdr = blk = malloc(SSTABLE_SIZE);

	for (blkno = 0; blkno < BLK_CNT_IN_SSTABLE; blkno++)
	{
		ca_init_blk((BLOCK *)blk, blkno, 1, BLK_CNT_IN_SSTABLE);
		
		
		if (blkno > (BLK_CNT_IN_SSTABLE / 2 - 2))
		{
			MEMCPY(((BLOCK *)blk)->bdata, tabletblk->bdata, 
					BLOCKSIZE - BLKHEADERSIZE - 4);

			((BLOCK *)blk)->bnextrno = tabletblk->bnextrno;					\
			((BLOCK *)blk)->bfreeoff = tabletblk->bfreeoff;					\
			((BLOCK *)blk)->bminlen  = tabletblk->bminlen;				

			tabletblk++;
		}

		blk = (char *)blk + BLOCKSIZE;
	}

	bufunkeep(bp->bsstab);
	session_close(tabinfo);


	MEMFREEHEAP(tabinfo->t_sinfo);
	MEMFREEHEAP(tabinfo);

	tabinfo_pop();
	

	
	((BLOCK *)filehdr)->bnextsstabnum = ovflow_tablet;
	((BLOCK *)filehdr)->bsstabnum = tabletid;
	((BLOCK *)filehdr)->bstat = BLK_INDEX_ROOT | BLK_CRT_EMPTY;


	int 	status;
	
	for(meta_num = 0; meta_num < meta_sysidx->idx_num; meta_num++)
	{
		if (tabid == idx_meta->idx_tabid)
		{
			index_bld_root_dir(indexroot, rg_tab_name, 
					idx_meta->idxname, tabletid);
			
			MKDIR(status, indexroot, 0755);
			
			index_bld_root_name(indexroot, rg_tab_name, 
				idx_meta->idxname, tabletid, FALSE);

			((BLOCK *)filehdr)->btabid = idx_meta->idx_id;
			
			OPEN(fd, indexroot, (O_CREAT|O_WRONLY|O_TRUNC));
				
			WRITE(fd, filehdr, SSTABLE_SIZE);
		
			CLOSE(fd);

			rtn_stat = TRUE;
		}

		idx_meta++;
	}

	free (filehdr);

	return rtn_stat;
}


int
index_root_move(IDXBLD *idxbld, BLOCK *srcblk, BLOCK *destblk, int indexid, char *src_root_dir, 
			char * dest_rootname,int dest_rootid)
{
	int		i;
	BLOCK		*metablk;
	char		*rp;
	int		sstabid;
	int		ign;

	
	Assert(destblk->bstat & BLK_INDEX_ROOT);


	metablk = destblk + (BLK_CNT_IN_SSTABLE / 2 - 1);

	Assert(metablk->bfreeoff > BLKHEADERSIZE);

		
	while(metablk->bfreeoff > BLKHEADERSIZE)
	{		
		for (i = 0; i < metablk->bnextrno; i++)
		{			
			rp = ROW_GETPTR_FROM_OFFTAB(metablk, i);
			
			sstabid = *(int *)row_locate_col(rp, 
					TABLET_SSTABID_COLOFF_INROW,
					ROW_MINLEN_IN_TABLET, &ign);

			index_root_sstabmov(idxbld, srcblk, indexid, src_root_dir,
						dest_rootname,
						dest_rootid, sstabid);
		}

		if (metablk->bnextblkno == -1)
		{
			break;
		}

		metablk++;
	}


	return TRUE;
	
}



int
index_root_sstabmov(IDXBLD *idxbld, BLOCK *srcblk, int indexid, char *src_root_dir, char *dest_rootname, 
			int dest_rootid,int sstabid)
{
	
	char		*rp;
	int		rlen;
	int		i,j,k,rnum;

	int		ign;
	int		*offtab;
	int		idx_sstabid;
	BUF		*src_idxsstab_bp;
	BLOCK		*src_idxsstab_blk;
	char		idxsstab_name[TABLE_NAME_MAX_LEN];
	char		*pidxsstab_name;
	TABINFO		ptabinfo;
	TABINFO		*tabinfo;
	SINFO		psinfo;
	char		*index_rp;
	int		index_rlen;
	RID		*ridp;
	int		ridnum;
	char		*keycol;
	int		keycolen;


	
	while(TRUE)
	{
		
		
		for (i = 0; i < srcblk->bnextrno; i++)
		{
			rp = ROW_GETPTR_FROM_OFFTAB(srcblk, i);
			rlen = ROW_GET_LENGTH(rp, srcblk->bminlen);

			idx_sstabid = *(int *)row_locate_col(rp, 
					TABLETSCHM_TABLETID_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &ign);

			pidxsstab_name  =row_locate_col(rp, 
					TABLETSCHM_TABLETNAME_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &ign);

			
			MEMSET(&ptabinfo,sizeof(TABINFO));
			MEMSET(&psinfo, sizeof(SINFO));
		
			tabinfo = &ptabinfo;
			tabinfo->t_sinfo = &psinfo;			

			MEMSET(idxsstab_name, TABLE_NAME_MAX_LEN);
			sprintf(idxsstab_name, "%s/%s", src_root_dir, pidxsstab_name);

			traceprint("idxsstab_name - %s is be scanning.\n", idxsstab_name);
			
			
			TABINFO_INIT(tabinfo, idxsstab_name, NULL, 0, 
						tabinfo->t_sinfo, 
						ROW_MINLEN_IN_INDEXBLK,
						0, indexid, idx_sstabid);

			
			SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, 1, 
						VARCHAR, -1);

			src_idxsstab_bp = blk_getsstable(tabinfo);
					
			src_idxsstab_blk = src_idxsstab_bp->bsstab->bblk;

			bufpredirty(src_idxsstab_bp->bsstab);
			
			
			while(TRUE)
			{
				offtab = ROW_OFFSET_PTR(src_idxsstab_blk);
				
				
				for (rnum = 0; rnum < src_idxsstab_blk->bnextrno;
									rnum++)
				{
					index_rp = ROW_GETPTR_FROM_OFFTAB(src_idxsstab_blk,
									rnum);
					index_rlen = ROW_GET_LENGTH(index_rp, 
								ROW_MINLEN_IN_INDEXBLK);			
					
					ridp = (RID *)row_locate_col(index_rp, 
							IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
							ROW_MINLEN_IN_INDEXBLK, &ign);
					ridnum = *(int *)row_locate_col(index_rp, 
							INDEXBLK_RIDNUM_COLOFF_INROW,
							ROW_MINLEN_IN_INDEXBLK, &ign);			

					Assert(ridnum == 1);
					
					
					for (k = 0; k < ridnum; k++)
					{
						if (ridp[k].sstable_id != sstabid)
						{
							continue;
						}
						
						
						if (ridnum == 1)
						{
							idxbld->idx_rp = index_rp;
							idxbld->idx_rlen = index_rlen;

							
							idxbld->idx_root_sstab
									= dest_rootid;

							index_ins_row(idxbld);
							
							
							ROW_SET_STATUS(index_rp, ROW_DELETED);

							
							for(j = rnum; 
								j < (src_idxsstab_blk->bnextrno - 1);
								j++)
							{
								offtab[-j] = offtab[-(j + 1)];	
							}

							BLK_GET_NEXT_ROWNO(src_idxsstab_blk)--;

							if (src_idxsstab_blk->bnextrno == 0)
							{
								blk_compact(src_idxsstab_blk);
							}

							
							rnum--;
						}
						else
						{
							keycol = row_locate_col(index_rp,
							  IDXBLK_KEYCOL_FAKE_COLOFF_INROW,
							  ROW_MINLEN_IN_INDEXBLK, &keycolen);

							BLOCK	*index_rowblk;
							BUF_GET_RESERVED(index_rowblk);

							index_bld_row((char *)index_rowblk,
									index_rlen,
									&(ridp[k]),
									keycol, 
									keycolen, 
									VARCHAR);

							idxbld->idx_rp = (char *)index_rowblk;
							idxbld->idx_rlen = index_rlen;

							
							idxbld->idx_root_sstab
									= dest_rootid;

							index_ins_row(idxbld);

							BUF_RELEASE_RESERVED(index_rowblk);
							
							index_rmrid(src_idxsstab_blk, rnum, k);

							k--;
							ridnum--;
						}				
							
						
							
					}

				}

				if (src_idxsstab_blk->bnextblkno == -1)
				{
					break;
				}

				src_idxsstab_blk++;

			}

			bufdirty(src_idxsstab_bp->bsstab);
			bufunkeep(src_idxsstab_bp->bsstab);
		}

		if (srcblk->bnextblkno == -1)
		{
			break;
		}

		srcblk++;
	}


	
	return TRUE;
}


void
index_rmrid(BLOCK *blk, int rnum, int del_ridnum)
{
	int		*offtab;
	int		freespace;   	
	
	char		*rp;
	RID		*ridp;
	int		ridnum;
	int		*ridnump;
	int		ign;

	char		*dataend;	
	int		i;

	
	rp  = ROW_GETPTR_FROM_OFFTAB(blk, rnum);
	
	ridp = (RID *)row_locate_col(rp, 
				IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
				ROW_MINLEN_IN_INDEXBLK, &ign);
	ridnump = (int *)row_locate_col(rp, 
				INDEXBLK_RIDNUM_COLOFF_INROW,
				ROW_MINLEN_IN_INDEXBLK, &ign);

	ridnum = *ridnump;

	Assert(ridnum > 1);

	
	dataend = (char *)blk + blk->bfreeoff;

	char	*dest;
	char	*src;
	
	
	dest = (char *)ridp + del_ridnum * sizeof(RID);
	src = (char *)ridp + (del_ridnum + 1) * sizeof(RID);
	
	BACKMOVE(src, dest, dataend - src);

	
	ridnum--;
	
	*ridnump = ridnum;
	
	freespace = sizeof(RID);

	offtab = ROW_OFFSET_PTR(blk);

	
	for (i = 0; i < blk->bnextrno; i++)
	{
		if (offtab[-i] > offtab[-rnum])
		{
			offtab[-i] -= freespace;
		}
	}

	
	blk->bfreeoff -= freespace;
}


void
index_addrid(BLOCK *blk,int rnum, RID *newridp)
{
	int		*offtab;
	char		*rp;		
	RID		*ridp;		
	int		*ridnump;
	int		ridnum;
	int		ridpos;		
	int		ign;
	
	char		*shiftstart;	
	int		movelen;
	int		i;
	int		result;


	
	rp  = ROW_GETPTR_FROM_OFFTAB(blk, rnum);
	
	ridp = (RID *)row_locate_col(rp, 
				IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
				ROW_MINLEN_IN_INDEXBLK, &ign);
	ridnump = (int *)row_locate_col(rp, 
				INDEXBLK_RIDNUM_COLOFF_INROW,
				ROW_MINLEN_IN_INDEXBLK, &ign);
	ridnum = *ridnump;

	
	Assert(ridnum);

	for (i = 0; i < ridnum; i++)
	{
		result = index_rid_cmp((char *)newridp, (char *)&(ridp[i]));
		
		if (result == LE)
		{
			ridpos = i;
			break;
		}
	}

	shiftstart = (char *)ridp + ridpos * sizeof(RID);
	
	
	movelen = ((char *)blk + blk->bfreeoff) - shiftstart;
	
	BACKMOVE(shiftstart, shiftstart + sizeof(RID), movelen);

	offtab = ROW_OFFSET_PTR(blk);
	
	
	for (i = 0; i < blk->bnextrno; i++)
	{
		if (offtab[-i] > offtab[-rnum])
		{
			offtab[-i] += sizeof(RID);
		}
	}
	
	
	MEMCPY(shiftstart, newridp, sizeof(RID));
	
	
	(*ridnump)++;
	
	
	blk->bfreeoff += sizeof(RID);
}

