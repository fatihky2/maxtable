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
#include "strings.h"
#include "master/metaserver.h"
#include "tabinfo.h"
#include "rpcfmt.h"
#include "parser.h"
#include "ranger/rangeserver.h"
#include "memcom.h"
#include "buffer.h"
#include "block.h"
#include "file_op.h"
#include "utils.h"
#include "tss.h"
#include "log.h"
#include "row.h"
#include "type.h"
#include "timestamp.h"
#include "session.h"
#include "rginfo.h"
#include "index.h"
#include "tablet.h"


extern TSS	*Tss;

int
undo_split(LOGREC *logrec, char *rgip, int rgport)
{

	char	backup[TABLE_NAME_MAX_LEN];
	int	status;
	
	char	cmd_str[64];
	char	tmpsstab[TABLE_NAME_MAX_LEN];


	status = TRUE;
	
	MEMSET(cmd_str, 64);	

	MEMSET(tmpsstab, TABLE_NAME_MAX_LEN);

	log_get_rgbackup(backup, rgip, rgport);
	
	int i = strmnstr(logrec->logsplit.oldsstabname, "/", 
			STRLEN(logrec->logsplit.oldsstabname));

	MEMCPY(tmpsstab, logrec->logsplit.oldsstabname + i, 
			STRLEN(logrec->logsplit.oldsstabname + i));

	
	char	new_sstab[TABLE_NAME_MAX_LEN];

	MEMSET(new_sstab, TABLE_NAME_MAX_LEN);
	MEMCPY(new_sstab, logrec->logsplit.newsstabname,
			STRLEN(logrec->logsplit.newsstabname));

	char	srcfile[TABLE_NAME_MAX_LEN];

	MEMSET(srcfile, TABLE_NAME_MAX_LEN);
	sprintf(srcfile, "%s/%s", backup, tmpsstab);				


	char	tab_dir[TABLE_NAME_MAX_LEN];

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, logrec->logsplit.oldsstabname, i);			
							
	if (STAT(tab_dir, &st) != 0)
	{
		status = FALSE;
		goto exit;
	}

#ifdef MT_KFS_BACKEND
	i = strmnstr(srcfile, "/", STRLEN(srcfile));
	sprintf(cmd_str, "%s", tab_dir);
	sprintf(cmd_str, "%s", srcfile + i);

	if (COPYFILE(srcfile, cmd_str) != 0)
	{
		status = FALSE;
		goto exit;
	}

	int	rtn_stat;

	RMFILE(rtn_stat,srcfile);

	if (rtn_stat < 0)


#else			
	sprintf(cmd_str, "cp %s %s", srcfile, tab_dir);

	if (system(cmd_str))
	{
		traceprint("UNDO_SPLIT: File copying hit error!\n");
		status = FALSE;
		goto exit;
	}

	sprintf(cmd_str, "rm %s", srcfile);

	if (system(cmd_str))
#endif
	{
		status = FALSE;
		goto exit;
	}

	if (((LOGHDR *)logrec)->opid == LOG_DATA_SSTAB_SPLIT)
	{
		
		char	rgstate[TABLE_NAME_MAX_LEN];

		ri_get_rgstate(rgstate, rgip, rgport);

		ri_rgstat_deldata(rgstate, new_sstab);
	}

	BUF *bp = buf_search(logrec->logsplit.newsstabno,
				logrec->logsplit.tabid);

	if (bp)
	{
		bufdestroy(bp);
	}
	
exit:			
	return status;
}



int
undo_updrid(LOGREC *logrec)
{
	LOCALTSS(tss);
	TABINFO 	tabinfo;
	SINFO		sinfo;
	BLK_ROWINFO	blk_rowinfo;
	LOGUPDRID	*logupdrid;
	RID		*oldrid;
	RID		*newrid;	
	int		ign;
	int		status;
	BUF		*bp;
	char		*index_rp;
	int		index_rlen;
	RID		*ridp;
	int		ridnum;
	int		i;
	int		result;
	int		rnum;
	

	status = FALSE;
	logupdrid = &(logrec->logupdrid);
	oldrid = &(logupdrid->oldrid);
	newrid = &(logupdrid->newrid);
	
	
	MEMSET(&tabinfo, sizeof(TABINFO));
	MEMSET(&sinfo, sizeof(SINFO));
	MEMSET(&blk_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo.t_rowinfo = &blk_rowinfo;
	tabinfo.t_dold = tabinfo.t_dnew = (BUF *) (&tabinfo);

	
	tabinfo.t_key_coloff = IDXBLK_KEYCOL_FAKE_COLOFF_INROW;
	tabinfo.t_key_coltype = VARCHAR;
	
	tss->topid |= TSS_OP_INDEX_CASE;
	
	tabinfo_push(&tabinfo);

	
	TABINFO_INIT(&tabinfo, logupdrid->sstabname, NULL, 0, &sinfo,
					ROW_MINLEN_IN_INDEXBLK,
					TAB_DEL_DATA, 
					logupdrid->idx_id, 
					logupdrid->sstabid);
	
	SRCH_INFO_INIT(&sinfo, NULL, 0, INDEXBLK_KEYCOL_COLID_INROW,
			tabinfo.t_key_coltype, tabinfo.t_key_coloff);	
	

	
		
	bp = blk_getsstable(&tabinfo);

	if (tabinfo.t_stat & TAB_RETRY_LOOKUP)
	{
		goto exit;
	}

	if (bp->bsstab->bblk->bsstab_insdel_ts_lo > logupdrid->newts)
	{
		traceprint("Some logs has not been recovery.\n");
		goto exit;
	}
	else if (bp->bsstab->bblk->bsstab_insdel_ts_lo < logupdrid->newts)
	{
		Assert(   (bp->bsstab->bblk->bsstab_insdel_ts_lo == logupdrid->oldts)
		       || (bp->bsstab->bblk->bsstab_insdel_ts_lo < logupdrid->oldts));

		status = TRUE;

		goto exit;
	}

	

	rnum = logupdrid->rnum;
	bp += logupdrid->blockid;
	
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
		result = index_rid_cmp((char *)newrid, (char *)(&(ridp[i])));

		if (result == EQ)
		{
			bp->bsstab->bblk->bsstab_insdel_ts_lo = logupdrid->oldts;

			bufpredirty(bp->bsstab);
			
			MEMCPY((char *)(&(ridp[i])), (char *)oldrid, sizeof(RID));
			
			bufdirty(bp->bsstab);

			break;
		}
	}

	if (result!= EQ)
	{
		traceprint("old rid (%d:%d:%d) is not found.\n", newrid->sstable_id, newrid->block_id, newrid->roffset);

		Assert(0);
	}
exit:

	bufunkeep(bp->bsstab);
	
	session_close(&tabinfo);
	tabinfo_pop();

	tss->topid &= ~TSS_OP_INDEX_CASE;

	return TRUE;
}



int
undo_data_insdel(LOGREC *logrec, char *rp)
{
	TABINFO		*tabinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		status;
	LOGINSDEL	*loginsdel;
	BUF		*bp;
	int		rnum;
	int		minlen;
	int		rlen;
	int		i;
	int		*offtab;
	char		*tmprp;
	int		blkid;
	int		ign;


	status		= FALSE;
	bp		= NULL;
	ign		= 0;
	loginsdel	= &(logrec->loginsdel);
	tabinfo		= MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	TABINFO_INIT(tabinfo, loginsdel->sstabname, NULL, 0, tabinfo->t_sinfo,
						loginsdel->minrowlen, 0,
						loginsdel->tabid,
						loginsdel->sstabid);
	
	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, 1, VARCHAR, -1);	
	
	rnum = loginsdel->rnum;
	blkid = loginsdel->blockid;
	minlen = tabinfo->t_row_minlen;
	rlen = ROW_GET_LENGTH(rp, minlen);
	
	if (((LOGHDR *)logrec)->opid == LOG_DATA_DELETE)
	{		
		tabinfo->t_sinfo->sistate |= SI_INS_DATA;
		
		bp = blk_getsstable(tabinfo);

		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			goto exit;
		}

		if (bp->bsstab->bblk->bsstab_insdel_ts_lo > loginsdel->newts)
		{
			traceprint("Some logs has not been recovery.\n");
			goto exit;
		}
		else if (bp->bsstab->bblk->bsstab_insdel_ts_lo < loginsdel->newts)
		{
			Assert(   (bp->bsstab->bblk->bsstab_insdel_ts_lo == loginsdel->oldts)
			       || (bp->bsstab->bblk->bsstab_insdel_ts_lo < loginsdel->oldts));

			status = TRUE;

			goto exit;
		}
		
		

		bufpredirty(bp->bsstab);
		
		bp += blkid;
		
		Assert(rnum > -1);
		
		offtab = ROW_OFFSET_PTR(bp->bblk);
		
		
		for (i = bp->bblk->bnextrno; i > rnum; i--)
		{
			offtab[-i] = offtab[-(i-1)];
		}

		bp->bsstab->bblk->bsstab_insdel_ts_lo = 
			mtts_increment(bp->bsstab->bblk->bsstab_insdel_ts_lo);
		
		PUT_TO_BUFFER((char *)bp->bblk + bp->bblk->bfreeoff, ign, rp,
				rlen);
			
		
		offtab[-i] = bp->bblk->bfreeoff;
			
		bp->bblk->bfreeoff += rlen;
		bp->bblk->bminlen = minlen;
		
		BLK_GET_NEXT_ROWNO(bp->bblk)++;

		bufdirty(bp->bsstab);
			
		tabinfo->t_sinfo->sistate &= ~SI_INS_DATA;
	}
	else if (   (((LOGHDR *)logrec)->opid == LOG_DATA_INSERT)
		 && (!(((LOGHDR *)logrec)->status & LOG_NOT_UNDO)))
	{
		tabinfo->t_sinfo->sistate |= SI_DEL_DATA;
		
		bp = blk_getsstable(tabinfo);

		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			goto exit;
		}

		if (bp->bsstab->bblk->bsstab_insdel_ts_lo > loginsdel->newts)
		{
			traceprint("Some logs has not been recovery.\n");
			goto exit;
		}
		else if (bp->bsstab->bblk->bsstab_insdel_ts_lo < loginsdel->newts)
		{
			Assert(   (bp->bsstab->bblk->bsstab_insdel_ts_lo == loginsdel->oldts)
			       || (bp->bsstab->bblk->bsstab_insdel_ts_lo < loginsdel->oldts));

			status = TRUE;

			goto exit;
		}
		

		bufpredirty(bp->bsstab);

		bp += blkid;
		
		Assert(rnum > -1);

		tmprp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, rnum);
		rlen = ROW_GET_LENGTH(tmprp, minlen);
	
		ROW_SET_STATUS(tmprp, ROW_DELETED);

		offtab = ROW_OFFSET_PTR(bp->bblk);
	
		for(i = rnum; i < (bp->bblk->bnextrno - 1); i++)
		{
			offtab[-i] = offtab[-(i + 1)];	
		}

		bp->bsstab->bblk->bsstab_insdel_ts_lo = loginsdel->oldts;
		
		BLK_GET_NEXT_ROWNO(bp->bblk)--;

		if (bp->bblk->bnextrno == 0)
		{
			blk_compact(bp->bblk);
		}

		bufdirty(bp->bsstab);
			
		tabinfo->t_sinfo->sistate &= ~SI_DEL_DATA;
		
	}


	status = TRUE;
exit:
	if (bp)
	{
		bufunkeep(bp->bsstab);
	}
	
	session_close(tabinfo);

	if (tabinfo!= NULL)
	{
		MEMFREEHEAP(tabinfo->t_sinfo);

		if (tabinfo->t_insrg)
		{
			Assert(0);

		}
		
		MEMFREEHEAP(tabinfo);
	}
	
	

	return status;

}


int
undo_index_insdel(LOGREC *logrec, char *rp)
{
	TABINFO		*tabinfo;
	BLK_ROWINFO	blk_rowinfo;
	int		status;
	LOGINSDEL	*loginsdel;
	BUF		*bp;
	int		rnum;
	int		minlen;
	int		ign;
	int		rlen;
	int		i;
	int		*offtab;
	int		blkid;
	int		upd_in_place;


	status = FALSE;
	bp = NULL;
	loginsdel = &(logrec->loginsdel);
	tabinfo = MEMALLOCHEAP(sizeof(TABINFO));
	MEMSET(tabinfo, sizeof(TABINFO));

	tabinfo->t_sinfo = (SINFO *)MEMALLOCHEAP(sizeof(SINFO));
	MEMSET(tabinfo->t_sinfo, sizeof(SINFO));

	tabinfo->t_rowinfo = &blk_rowinfo;
	MEMSET(tabinfo->t_rowinfo, sizeof(BLK_ROWINFO));

	tabinfo->t_dold = tabinfo->t_dnew = (BUF *) tabinfo;

	TABINFO_INIT(tabinfo, loginsdel->sstabname, NULL, 0, tabinfo->t_sinfo,
			loginsdel->minrowlen, 0, loginsdel->tabid, loginsdel->sstabid);

	SRCH_INFO_INIT(tabinfo->t_sinfo, NULL, 0, 1, VARCHAR, -1);

	rnum = loginsdel->rnum;
	blkid = loginsdel->blockid;
	minlen = tabinfo->t_row_minlen;
	
	if (((LOGHDR *)logrec)->opid == LOG_INDEX_DELETE)
	{
		
		tabinfo->t_sinfo->sistate |= SI_INS_DATA;
	
		bp = blk_getsstable(tabinfo);
		
		if (bp->bsstab->bblk->bsstab_insdel_ts_lo > loginsdel->newts)
		{
			traceprint("Some logs has not been recovery.\n");
			goto exit;
		}
		else if (bp->bsstab->bblk->bsstab_insdel_ts_lo < loginsdel->newts)
		{
			Assert(   (bp->bsstab->bblk->bsstab_insdel_ts_lo == loginsdel->oldts)
			       || (bp->bsstab->bblk->bsstab_insdel_ts_lo < loginsdel->oldts));

			status = TRUE;

			goto exit;
		}
		

	
		bufpredirty(bp->bsstab);

		bp += blkid;
		
		Assert(rnum > -1);
		
		
		upd_in_place = (loginsdel->status & LOGINSDEL_RID_UPD) 
			      ? TRUE : FALSE; 
	
		if (upd_in_place)
		{
			index_addrid(bp->bblk, rnum, (RID *)rp,
				IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
				INDEXBLK_RIDNUM_COLOFF_INROW,
				ROW_MINLEN_IN_INDEXBLK);
			
		}
		else
		{
			offtab = ROW_OFFSET_PTR(bp->bblk);
			
			
			for (i = bp->bblk->bnextrno; i > rnum; i--)
			{
				offtab[-i] = offtab[-(i-1)];
			}
			
			rlen = ROW_GET_LENGTH(rp, minlen); 
			PUT_TO_BUFFER((char *)bp->bblk + bp->bblk->bfreeoff,
					ign, rp, rlen);
				
			
			offtab[-i] = bp->bblk->bfreeoff;
				
			bp->bblk->bfreeoff += rlen;
			bp->bblk->bminlen = minlen;

			BLK_GET_NEXT_ROWNO(bp->bblk)++;
		}
					
		bp->bsstab->bblk->bsstab_insdel_ts_lo = loginsdel->oldts;

		bufdirty(bp);
			
		tabinfo->t_sinfo->sistate &= ~SI_INS_DATA;
	}
	else if (((LOGHDR *)logrec)->opid == LOG_INDEX_INSERT)
	{
		tabinfo->t_sinfo->sistate |= SI_DEL_DATA;
		
		bp = blk_getsstable(tabinfo);

		if (tabinfo->t_stat & TAB_RETRY_LOOKUP)
		{
			bufunkeep(bp->bsstab);
			return FALSE;
		}

		if (bp->bsstab->bblk->bsstab_insdel_ts_lo > loginsdel->newts)
		{
			traceprint("Some logs has not been recovery.\n");
			goto exit;
		}
		else if (bp->bsstab->bblk->bsstab_insdel_ts_lo < loginsdel->newts)
		{
			Assert(   (bp->bsstab->bblk->bsstab_insdel_ts_lo == loginsdel->oldts)
			       || (bp->bsstab->bblk->bsstab_insdel_ts_lo < loginsdel->oldts));

			status = TRUE;

			goto exit;
		}

		bufpredirty(bp->bsstab);

		bp += blkid;

		Assert(rnum > -1);

		char	*mother_rp;
		int	mother_rlen;
		int	tot_ridnum;
		RID	*delrid;
		RID	*mother_rid;
		int	result;

		
		mother_rp = ROW_GETPTR_FROM_OFFTAB(bp->bblk, rnum);
		mother_rlen = ROW_GET_LENGTH(mother_rp, minlen);
		
		tot_ridnum = *(int *)row_locate_col(mother_rp, 
						INDEXBLK_RIDNUM_COLOFF_INROW,
						ROW_MINLEN_IN_INDEXBLK, &ign);

		if (tot_ridnum > 1)
		{

			delrid = (RID *)row_locate_col(rp, 
						IDXBLK_RIDARRAY_FAKE_COLOFF_INROW,
						ROW_MINLEN_IN_INDEXBLK, &ign);

			mother_rid = (RID *)row_locate_col(mother_rp, 
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
		}
		else
		{
			ROW_SET_STATUS(mother_rp, ROW_DELETED);	

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
		}

		bufdirty(bp->bsstab);

		bp->bsstab->bblk->bsstab_insdel_ts_lo = loginsdel->oldts;
			
		tabinfo->t_sinfo->sistate &= ~SI_DEL_DATA;	
		
	}


	status = TRUE;
exit:
	if (bp)
	{
		bufunkeep(bp->bsstab);
	}
	
	session_close(tabinfo);

	if (tabinfo!= NULL)
	{
		MEMFREEHEAP(tabinfo->t_sinfo);

		if (tabinfo->t_insrg)
		{
			Assert(0);

		}
		
		MEMFREEHEAP(tabinfo);
	}
	
	return status;
}


