/*
** row.c 2010-11-21 xueyingfei
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
#include "row.h"
#include "netconn.h"
#include "type.h"
#include "strings.h"
#include "utils.h"


/*
** Following is the row format in MaxTable:
** | row# | status | #varlen | data fixed | row length | data for var-column | column offset  |
**
** Var-column offset table
** | last var-col (int) | second var-col (int) | first var-col (int) |
**
*/

/*
** Split Strategy -- tail row split
**
** Usage Note: the paraneter length will be valid when the 'coloffset' is less than 0.
**
*/

char *
row_locate_col(char * rowptr, int coloffset, int minrowlen, int * length)
{
	int		collen;		/* working length of column */
	int		varcolno;	/* varlen column's relative ID */
	int		varcount;	/* # varying-len columns */

	if (coloffset > 0)
	{
		return (rowptr + coloffset);
	}

	varcolno = -coloffset;

	varcount = ROW_GET_VARCNT(rowptr);

	if (varcolno > varcount)
	{
		/* Column doesn't exist in this row */
		*length = 0;
		return NULL;
	}

	ROW_GET_VARCOL_LEN(rowptr, minrowlen, varcolno, collen);

	/* Store the column length, return a pointer to the column. */
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
	/*
	** Delay this implementation till the introduction of buffer manager. 
	*/

}


int
row_col_compare(int coltype, char *colval1, int colen1, char *colval2, int colen2)
{
        int     result;
        int     colen;


        switch (coltype)
        {
	    case CHAR:
		assert((colen1 == 1) && (colen2 == 1));
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
		//assert((colen1 == 4) && (colen2 == 4));

		result = MT_COMPARE(type_char2int(colval1), type_char2int(colval2));

	    break;

        }   /* end of switch on data type */
	
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
		printf("offtab[-%d] : %d \n", i, offtab[-i]);
	}
}
/* Defination of the type of column. */
