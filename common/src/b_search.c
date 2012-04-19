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
#include "utils.h"
#include "row.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "tss.h"
#include "b_search.h"

extern TSS	*Tss;



void
b_srch_block(TABINFO *tabinfo, BUF *bp, B_SRCHINFO *srchinfo)
{
	LOCALTSS(tss);

	int	keylen_in_blk;	
	char    *key_in_blk;	
	char    *key;		
	int     keylen;		
	int     result;		
	int	coloffset;	
	int	coltype;	


	
	coloffset = tabinfo->t_sinfo->sicoloff;
	key = tabinfo->t_sinfo->sicolval;
	coltype = tabinfo->t_sinfo->sicoltype;
	keylen = tabinfo->t_sinfo->sicollen;		


nextrow:
	
	srchinfo->brow = ROW_GETPTR_FROM_OFFTAB(bp->bblk, srchinfo->brownum);
//	srchinfo->boffset = ROW_OFFSET_PTR(bp->bblk)[-((int)(srchinfo->brownum))];

	if ((tss->topid & TSS_OP_RANGESERVER) && (ROW_IS_DELETED(srchinfo->brow)))
	{	
		
		if (   (tabinfo->t_sinfo->sistate & SI_INS_DATA) 
		    || (tabinfo->t_stat & TAB_SRCH_RANGE))
		{
			result = GR;
			goto do_gr_case;
		}
		
		else if (tabinfo->t_stat & (TAB_SRCH_DATA | TAB_DEL_DATA))
		{				
			result = LE;

			
			if (srchinfo->bhigh > srchinfo->blow)
			{
				BSRCH_MOVE_LEFT(bp->bblk, srchinfo);

				
				goto nextrow;
			}
		}

		goto finish;
	}		

	key_in_blk = row_locate_col(srchinfo->brow, coloffset, bp->bblk->bminlen, 
				    &keylen_in_blk);

	result = row_col_compare(coltype, key, keylen, key_in_blk, keylen_in_blk);

	switch (result)
	{
	    case EQ:
	    	
	    	break;
		
	    case LE:
	    	if (   (srchinfo->blow == srchinfo->bhigh)
		    || (srchinfo->brownum == srchinfo->blow))
	    	{
	    		

			
	    		if (tabinfo->t_sinfo->sistate & SI_INS_DATA) 
	    		{
	    			break;
	    		}

			if (tabinfo->t_stat & TAB_SCHM_SRCH)
			{
				
				if (   (tabinfo->t_sstab_id == 1) 
				    && (srchinfo->brownum == 0) 
			    	    && (bp->bblk->bblkno == 0))
				{
					tabinfo->t_stat |= TAB_TABLET_KEYROW_CHG;
				}

				
				if (srchinfo->brownum > 0)
				{
					(srchinfo->brownum)--;
				}
			}

			break;
	    	}
		else
		{
			BSRCH_MOVE_RIGHT(bp->bblk, srchinfo);

			goto nextrow;
		}
	    	
	    	break;
		
	    case GR:
do_gr_case:
	    	if (   (srchinfo->blow == srchinfo->bhigh) 
		    || (srchinfo->brownum == srchinfo->bhigh))
	    	{
	    		
	    		if (tabinfo->t_sinfo->sistate & SI_INS_DATA) 
	    		{
	    			Assert(srchinfo->brownum < srchinfo->btotrows);

				
				(srchinfo->brownum)++;
				
				
	    		}
	    	}
		else
		{
			BSRCH_MOVE_LEFT(bp->bblk, srchinfo);

			goto nextrow;
			
		}
		
	    	break;
		
	    default:
	    	break;
	}

finish:
	srchinfo->bcomp = result;

	return;
}
