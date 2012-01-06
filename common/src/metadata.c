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
#include "strings.h"
#include "file_op.h"
#include "utils.h"


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



int
meta_save_sysobj(char *tab_dir, char *tab_hdr)
{
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];
	int	fd;
	int	status;
	int	rtn_stat;


	rtn_stat = TRUE;
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));

	str1_to_str2(tab_meta_dir, '/', "sysobjects");


	OPEN(fd, tab_meta_dir, (O_RDWR));

	if (fd < 0)
	{
		return FALSE;
	}


	status = WRITE(fd, tab_hdr, sizeof(TABLEHDR));

	Assert(status == sizeof(TABLEHDR));

	if (status != sizeof(TABLEHDR))
	{
		traceprint("Table %s sysobjects hit error!\n", tab_dir);
		rtn_stat = FALSE;
		
	}

	CLOSE(fd);

	return rtn_stat;
}


