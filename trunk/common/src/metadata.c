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

#include "metadata.h"
#include "master/metaserver.h"
#include "row.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "strings.h"
#include "file_op.h"
#include "utils.h"


COLINFO *
meta_get_colinfor(int colid, char *col_name, int totcol, COLINFO *colinfor)
{
	int	i;


	i = 0;
	while(i < totcol)
	{
		if(col_name != NULL)
		{
			if (!strcmp(colinfor[i].col_name, col_name))
			{
				return &(colinfor[i]);
			}
		}
		else if (colinfor[i].col_id == colid)
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



int
meta_save_sysindex(char *sysindex)
{
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];
	int	fd;
	int	status;
	int	rtn_stat;


	rtn_stat = TRUE;
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	
	MEMCPY(tab_meta_dir, MT_META_INDEX, STRLEN(MT_META_INDEX));
	str1_to_str2(tab_meta_dir, '/', "sysindex");

	OPEN(fd, tab_meta_dir, (O_RDWR));

	if (fd < 0)
	{
		return FALSE;
	}


	status = WRITE(fd, sysindex, sizeof(META_SYSINDEX));

	Assert(status == sizeof(META_SYSINDEX));

	if (status != sizeof(META_SYSINDEX))
	{
		traceprint("Save sysindex hit error!\n");
		rtn_stat = FALSE;
		
	}

	CLOSE(fd);

	return rtn_stat;
}

int
meta_load_sysindex(char *sysindex)
{
	int	fd;
	char	tab_meta_dir[TABLE_NAME_MAX_LEN];
	int	status;
	int	rtn_stat;


	rtn_stat= TRUE;
	
	
	MEMSET(tab_meta_dir, 256);
	MEMCPY(tab_meta_dir, MT_META_INDEX, STRLEN(MT_META_INDEX));
	str1_to_str2(tab_meta_dir, '/', "sysindex");

	OPEN(fd, tab_meta_dir, (O_RDONLY));

	if (fd < 0)
	{
		return FALSE;
	}

	status = READ(fd, sysindex, sizeof(META_SYSINDEX));

	Assert(status == sizeof(META_SYSINDEX));

	if (status != sizeof(META_SYSINDEX))
	{
		traceprint("Save sysindex hit error!\n");
		rtn_stat = FALSE;
		
	}

	CLOSE(fd);

	return rtn_stat;
}

