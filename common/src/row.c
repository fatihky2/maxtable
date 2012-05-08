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
#include "master/metaserver.h"
#include "row.h"
#include "tss.h"
#include "parser.h"
#include "netconn.h"
#include "type.h"
#include "strings.h"
#include "utils.h"


extern TSS	*Tss;




char *
row_locate_col(char * rowptr, int coloffset, int minrowlen, int * length)
{
	int		collen;			
	int		varcolno;	
	int		varcount;	

	if (coloffset > 0)
	{
		return (rowptr + coloffset);
	}

	varcolno = -coloffset;

	varcount = ROW_GET_VARCNT(rowptr);

	if (varcolno > varcount)
	{
		
		*length = 0;
		return NULL;
	}

	ROW_GET_VARCOL_LEN(rowptr, minrowlen, varcolno, collen);

	
	*length = collen;
	return (rowptr + ROW_GET_VARCOL_OFFSET(ROW_GET_END(rowptr, minrowlen), varcolno));
}

void
row_build_hdr(char *rp, int rowno, int status, int vattcnt)
{
	int	rp_idx;

	rp_idx = 0;

	PUT_TO_BUFFER(rp, rp_idx, &rowno, sizeof(int));
	PUT_TO_BUFFER(rp, rp_idx, &status, sizeof(int));
	PUT_TO_BUFFER(rp, rp_idx, &vattcnt, sizeof(int));

	return;
}


void
row_rebld_row(char *oldrp, char *newrp, int newrlen, COLINFO *colinfo,
			int colnum, int minrlen)
{
	LOCALTSS(tss);
	int		col_offset;
	int		rp_idx;
	char		*col_val;
	int		col_len;
	int		col_off_idx;
	char		col_off_tab[COL_OFFTAB_MAX_SIZE];
	int		col_num;
	TREE		*cmd;
	int		i;
	int		hit_varcol;


	MEMCPY(newrp, oldrp, sizeof(ROWFMT));
	
	cmd = tss->tcmd_parser;

	col_num = colnum;

	col_offset = sizeof(ROWFMT);
	rp_idx = sizeof(ROWFMT);
	col_off_idx = COL_OFFTAB_MAX_SIZE;

	par_fill_resd(cmd, colinfo, col_num);
	
	hit_varcol = FALSE;

	
	while(col_num)
	{
		col_val = par_get_colval_by_coloff(cmd, col_offset,	
							&col_len);
		if(!col_val)
		{
			for (i = 0; i < colnum; i++)
			{
				if (colinfo[i].col_offset == col_offset)
				{
					col_val = row_locate_col(oldrp, col_offset,
								minrlen, &col_len);
					if (col_offset > 0)
					{
						col_len = colinfo[i].col_len;
					}

					break;
				}
			}

			if (col_offset && (i == colnum))
			{
				
				hit_varcol = TRUE;
			}
		}
		
		if(col_val)
		{
			if (col_offset < 0)
			{
				col_off_idx -= sizeof(int);
				*((int *)(col_off_tab + col_off_idx)) = rp_idx;
			}

			if ((rp_idx + col_len) > newrlen)
			{
				traceprint("The row to be inserted expand the max size %d of one row.\n", newrlen);
				goto exit;
			}
			
			PUT_TO_BUFFER(newrp, rp_idx, col_val, col_len);
			if (col_offset > 0)
			{
				col_offset += col_len;
			}
			else
			{				
				col_offset--;
			}

			col_num--;
		}
		else
		{
			Assert((col_offset > 0) && hit_varcol);

			if (!((col_offset > 0) && hit_varcol))
			{
				traceprint("Hit a row error!\n");
				goto exit;
			}

			hit_varcol = FALSE;
			
			
			
				

			if (col_num > 0)
			{
				
				rp_idx += sizeof(int);
				col_offset = -1;
			}
			
		}		
		
	}

	if (COL_OFFTAB_MAX_SIZE > col_off_idx)
	{
		if ((rp_idx + (COL_OFFTAB_MAX_SIZE - col_off_idx)) > newrlen)
		{
			traceprint("The row to be inserted expand the max size %d of one row.\n", newrlen);
			goto exit;
		}
		
		PUT_TO_BUFFER(newrp, rp_idx, (col_off_tab + col_off_idx), 
					(COL_OFFTAB_MAX_SIZE - col_off_idx));
		*(int *)(newrp + minrlen) = rp_idx;
	}
exit:

	return;

}

void
rebld_row(char *oldrp, char *newrp, int newrlen, COLINFO *colinfo,
			int colnum, int minrlen)
{
	LOCALTSS(tss);
	int		col_offset;
	int		rp_idx;
	char		*col_val;
	int		col_len;
	int		col_off_idx;
	char		col_off_tab[COL_OFFTAB_MAX_SIZE];
	int		col_num;
	TREE		*cmd;


	MEMCPY(newrp, oldrp, sizeof(ROWFMT));
	
	cmd = tss->tcmd_parser;

	col_num = colnum;

	col_offset = sizeof(ROWFMT);
	rp_idx = sizeof(ROWFMT);
	col_off_idx = COL_OFFTAB_MAX_SIZE;

	par_fill_resd(cmd, colinfo, col_num);

	
	
	while(col_num)
	{
		col_val = par_get_colval_by_coloff(cmd, col_offset, &col_len);

		if (!col_val)
		{
			
			int	i;
			
			for (i = 0; i < colnum; i++)
			{
				if (colinfo[i].col_id == (colnum - col_num + 1))
				{
					if (colinfo[i].col_offset == -1)
					{
						col_offset = colinfo[i].col_offset;
						
						
						rp_idx += sizeof(int);
					}
									
					col_val = row_locate_col(oldrp, col_offset,
								minrlen, &col_len);
					if (col_offset > 0)
					{
						col_len = colinfo[i].col_len;
					}
					
					break;					
				}
			}
		}
		
		Assert(col_val);
		
			
		
		
		if (col_offset < 0)
		{
			
			col_off_idx -= sizeof(int);
			*((int *)(col_off_tab + col_off_idx)) = rp_idx;
		}

		if ((rp_idx + col_len) > newrlen)
		{
			traceprint("The row to be inserted expand the max size %d of one row.\n", newrlen);
			goto exit;
		}
		
		PUT_TO_BUFFER(newrp, rp_idx, col_val, col_len);
		
		if (col_offset > 0)
		{
			col_offset += col_len;
		}
		else
		{				
			col_offset--;
		}

		col_num--;

	}

	if (COL_OFFTAB_MAX_SIZE > col_off_idx)
	{
		if ((rp_idx + (COL_OFFTAB_MAX_SIZE - col_off_idx)) > newrlen)
		{
			traceprint("The row to be inserted expand the max size %d of one row.\n", newrlen);
			goto exit;
		}
		
		PUT_TO_BUFFER(newrp, rp_idx, (col_off_tab + col_off_idx), 
					(COL_OFFTAB_MAX_SIZE - col_off_idx));
		*(int *)(newrp + minrlen) = rp_idx;
	}

exit:

	return;
}


int
row_col_compare(int coltype, char *colval1, int colen1, char *colval2, int colen2)
{
        int     result;
        int     colen;


        switch (coltype)
        {
	    case CHAR:
		Assert((colen1 == 1) && (colen2 == 1));
		result = MT_COMPARE(*colval1, *colval2);

		break;

	    case VARCHAR:
		colen = MIN(colen1, colen2);
		result= strncmp(colval1, colval2, colen);
				
		if (result == 0)
		{
		        result = MT_COMPARE(colen1, colen2);
		}
		else if (result > 0)
		{
			result = GR;
		}
		else
		{
			result = LE;
		}
					
		break;

	    case INT4:
		//Assert((colen1 == 4) && (colen2 == 4));

		result = MT_COMPARE(type_char2int(colval1), type_char2int(colval2));

	    	break;

	    case INVALID_TYPE:
	    	Assert(0);
	    	break;

        }   
	
	return result;
}

void
row_prt_allcols(char *rp, int minlen)
{
	int	rlen;
	

	rlen = ROW_GET_LENGTH(rp,minlen);
	
}


void
row_prt_offtab(int *offtab, int n)
{
	int	i;


	for(i = 0; i < n; i++)
	{
		traceprint("offtab[-%d] : %d \n", i, offtab[-i]);
	}
}


