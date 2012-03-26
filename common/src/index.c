/*
** index.c 2012-03-07 xueyingfei
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

#include "global.h"
#include "utils.h"
#include "list.h"
#include "master/metaserver.h"
#include "parser.h"
#include "memcom.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "strings.h"
#include "tabinfo.h"
#include "row.h"
#include "tablet.h"
#include "type.h"
#include "session.h"
#include "b_search.h"
#include "tss.h"
#include "ranger/rangeserver.h"
#include "index.h"
#include "m_socket.h"
#include "sstab.h"


extern	TSS	*Tss;

static int
index_bld_sstabnum(int tablet_num, int sstab_index);


int
index_crt(char *index_name, TABLEHDR *tablehdr, char *tabledir, int column_num)
{
	
	return TRUE;
}

int
index__crt_empty(char *index_name, TABLEHDR *tablehdr, char *tabledir, int column_num)
{
	return TRUE;
}

int
inex__crt_data()
{
	return TRUE;
}


int
index_ins_row(IDXBLD *idxbld)
{
	LOCALTSS(tss);
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
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


	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;

	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);

	
	tabinfo.t_key_coloff = TABLE_KEYCOL_FAKE_COLOFF_INROW;
	tabinfo.t_key_coltype = VARCHAR;
	
	tabinfo_push(&tabinfo);


	first_row_hit = (idxbld->idx_stat & IDXBLD_FIRST_DATAROW_IN_TABLET) 
			? TRUE : FALSE;

	
	keycol = row_locate_col(idxbld->idx_rp, -1, ROW_MINLEN_IN_INDEXBLK,
					&keycolen);
	
	if (first_row_hit)
	{
		
		idxbld->idx_index_sstab_cnt = 1;

		index_bld_leaf_name(tab_meta_dir, idx_sstab_name,
						idxbld->idx_tab_name,
						idxbld->idx_meta->idxname,
						idxbld->idx_tablet_name,
						idxbld->idx_root_sstab,
						idxbld->idx_index_sstab_cnt);

		index_sstab_num = index_bld_sstabnum(idxbld->idx_root_sstab,
						idxbld->idx_index_sstab_cnt);
	
	}
	else
	{
		
		index_bld_root_name(tab_meta_dir, idxbld->idx_tab_name, 
					idxbld->idx_meta->idxname, 
					idxbld->idx_tablet_name);
	
		index_root_rp = tablet_schm_srch_row(idxbld->idx_meta->idx_id, 
					idxbld->idx_root_sstab,
					tab_meta_dir, keycol, keycolen);

		idxbld->idx_index_sstab_cnt = tablet_schm_get_totrow(
					idxbld->idx_meta->idx_id, 
					idxbld->idx_root_sstab,
					tab_meta_dir, keycol, keycolen);


		int namelen;
		char *name = row_locate_col(index_root_rp, 
					TABLETSCHM_TABLETNAME_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);
				
		
		MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
		MEMCPY(tab_meta_dir, idxbld->idx_tab_name,
					STRLEN(idxbld->idx_tab_name));
		str1_to_str2(tab_meta_dir, '/', name);

		index_sstab_num = *(int *)row_locate_col(index_root_rp, 
					TABLETSCHM_TABLETID_COLOFF_INROW,
					ROW_MINLEN_IN_TABLETSCHM, &namelen);

	}
	
	TABINFO_INIT(&tabinfo, tab_meta_dir, NULL, 0, &sinfo,
			ROW_MINLEN_IN_INDEXBLK,
			first_row_hit ? TAB_CRT_NEW_FILE : TAB_INS_INDEX, 
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

	
	Assert(STRLEN(tab_meta_dir) < SSTABLE_NAME_MAX_LEN);
	MEMCPY(insmeta.sstab_name, tab_meta_dir, STRLEN(tab_meta_dir));
	
	tabinfo.t_insmeta = &insmeta;

	tss->topid |= TSS_OP_INDEX_CASE;
	
	blkins(&tabinfo, idxbld->idx_rp);

	
	idx_sstab_split = (tabinfo.t_stat & TAB_SSTAB_SPLIT)? TRUE : FALSE;

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
		
		index_bld_root_name(tab_meta_dir, idxbld->idx_tab_name,
					idxbld->idx_meta->idxname,
					idxbld->idx_tablet_name);
		
		int rlen = ROW_MINLEN_IN_TABLETSCHM + keycolen + sizeof(int) +
								sizeof(int);
		char *temprp = (char *)MEMALLOCHEAP(rlen);
		
		char	rg_addr[RANGE_ADDR_MAX_LEN];
		tablet_schm_bld_row(temprp, rlen, index_sstab_num,
					pidx_sstab_name, rg_addr, keycol, 
					keycolen, 1981);
		
		tablet_schm_ins_row(idxbld->idx_meta->idx_id, 
					idxbld->idx_root_sstab, tab_meta_dir,
					temprp, first_row_hit? 0 : 1);

				

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
							rootname, TRUE);
		}
		else if (root_srchctx->stat & IDX_ROOT_SRCH_LAST_KEY)
		{
			rp = tablet_get_1st_or_last_row(indexid, rootid,
							rootname, FALSE);
		}
		else
		{
			Assert(0);
		}
	}
	else
	{
		rp = tablet_srch_row(NULL, indexid, rootid, rootname,
					keycol, keycolen);
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



int
index_range_sstab_scan(TABLET_SCANCTX *tablet_scanctx, IDXMETA *idxmeta, 
			IDX_RANGE_CTX *idx_range_ctx, char *tabdir)
{
	char		*resp;
	int		resp_size;
	BUF		*bp;
	int		offset;
	char		last_sstab[SSTABLE_NAME_MAX_LEN];
	B_SRCHINFO	srchinfo;
	char		tab_left_sstab_dir[TABLE_NAME_MAX_LEN];
	char		tab_right_sstab_dir[TABLE_NAME_MAX_LEN];
	int		rtn_stat;
	int 		*offtab;
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

	
	
	MEMSET(tab_left_sstab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_left_sstab_dir, tabdir, STRLEN(tabdir));
	str1_to_str2(tab_left_sstab_dir, '/', idx_range_ctx->sstab_left);

	
	MEMSET(tab_right_sstab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_right_sstab_dir, tabdir, STRLEN(tabdir));
	str1_to_str2(tab_right_sstab_dir, '/', idx_range_ctx->sstab_right);	
	keyleft = idx_range_ctx->key_left;
	keyleftlen = idx_range_ctx->keylen_left;

	MEMSET(&t_tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));
	
again:
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
	}
	else
	{
		bp = blkget(tabinfo);
	
	
		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			return FALSE;
		}
	}
	

	offtab = ROW_OFFSET_PTR(bp->bblk);
	
	if (idx_range_ctx->left_expand)
	{
		offset = BLKHEADERSIZE;

		
		if (ROW_IS_DELETED((char *)(bp->bblk) + offset))
		{
			keyleft = row_locate_col((char *)(bp->bblk) + offset,
						-1, tabinfo->t_row_minlen,
						&keyleftlen);
			idx_range_ctx->left_expand = FALSE;
			goto again;
		}
	}
	else
	{
		Assert(tabinfo->t_rowinfo->rblknum == bp->bblk->bblkno);
		Assert(tabinfo->t_rowinfo->rsstabid == bp->bblk->bsstabid);

		if ((tabinfo->t_rowinfo->rblknum != bp->bblk->bblkno)
		    || (tabinfo->t_rowinfo->rsstabid != bp->bblk->bsstabid))
		{
			traceprint("Hit a buffer error!\n");
			bufunkeep(bp->bsstab);
			ex_raise(EX_ANY);
		}
		
		offset = tabinfo->t_rowinfo->roffset;
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
	int	i;
		
	for (i = 0; i < bp->bblk->bnextrno; i++)
	{
		if (offtab[-i] == offset)
		{
			break;
		}
	}

	start_scan_row = i;

	Assert(i < bp->bblk->bnextrno);

	
	int	data_cont = TRUE;
	int	get_row = TRUE;
	

	
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
				
		};
					
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

		if (bp->bblk->bfreeoff > BLKHEADERSIZE)
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
index_tab_has_index(META_SYSINDEX *meta_sysindex, int tab_id)
{
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

	BACKMOVE((char *)(&(meta_sysidx->idx_meta[meta_num + 1])), 
			(char *)(&(meta_sysidx->idx_meta[meta_num])), 
			(meta_sysidx->idx_num - meta_num) * sizeof(IDXMETA));

	(meta_sysidx->idx_num)--;

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
index_bld_root_name(char *tab_meta_dir, char *tab_name, char *idx_name, 
			char *tablet_name)
{
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_name, STRLEN(tab_name));
	str1_to_str2(tab_meta_dir, '/', idx_name);
	str1_to_str2(tab_meta_dir, '_', tablet_name);
	str1_to_str2(tab_meta_dir, '_', "root");

	return TRUE;
}

int
index_bld_leaf_name(char *tab_meta_dir, char *index_sstab_name, char *tab_name, 
			char *idx_name, char *tablet_name, int tablet_num, 
			int sstab_num)
{
	int	index_sstab_num;
	
	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMSET(index_sstab_name, 64);
	MEMCPY(tab_meta_dir, tab_name, STRLEN(tab_name));
	MEMCPY(index_sstab_name, idx_name, STRLEN(idx_name));
	str1_to_str2(index_sstab_name, '_', tablet_name);

	index_sstab_num = index_bld_sstabnum(tablet_num, sstab_num);

	build_file_name("sstable", index_sstab_name, index_sstab_num);
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
	int		nextrow;

	
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));
	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	

	tabinfo->t_rowinfo = &blk_rowinfo;

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	tabinfo_push(tabinfo);

	nextrow = TRUE;

	while(nextrow)
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

		ridarry = (RID *)row_locate_col(indexrp, 
					TABLE_RIDARRAY_FAKE_COLOFF_INROW,
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

			rp = (char *)blk + offtab[-(ridarry[i].row_id)];

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

