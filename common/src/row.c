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
#include "row.h"
#include "netconn.h"
#include "type.h"
#include "strings.h"
#include "utils.h"





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
row_build_row()
{
	;
	
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


