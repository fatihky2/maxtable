/*
** metadata.c 2011-02-15 xueyingfei
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


#include "metadata.h"
#include "master/metaserver.h"
#include "row.h"
#include "buffer.h"
#include "block.h"

COLINFO *
meta_get_colinfor(int colid, int totcol, COLINFO *colinfor)
{
	int	i;


	i = 0;
	while(i < totcol)
	{
		if (colinfor[i].col_id == colid)
		{
			return &(colinfor[i]);
		}

		i++;
	}

	return NULL;
}


char *
meta_get_coldata(BUF *bp, int rowoffset, int coloffset)
{
	int ign;
	int minrowlen;
	BLOCK	*blk;
	

	blk = bp->bblk;
	minrowlen = blk->bminlen;
	
	return row_locate_col((char *)(bp->bblk) + rowoffset, coloffset, minrowlen, &ign);
}

