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
#include "strings.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "cache.h"
#include "memcom.h"
#include "file_op.h"
#include "utils.h"
#include "rginfo.h"
#include "netconn.h"

RANGEINFO *Range_infor = NULL;

int
ri_rgstat_putdata(char *statefile, char *new_sstab, int state,
			SSTAB_SPLIT_INFO *split_value)
{
	int			fd;
	RG_STATE		*rgstate;	/* Ptr to the buffer for the
						** rg_state file.
						*/
	int			statelen;
	int			rtn_state;
	SSTAB_SPLIT_INFO	*split_info;	/* Ptr to one slot in the
						** rg_state.
						*/


	rtn_state = TRUE;
	statelen = sizeof(RG_STATE);
	rgstate = (RG_STATE *)malloc(statelen);
	
	OPEN(fd, statefile, (O_RDWR));
	
	if (fd < 0)
	{
		rtn_state = FALSE;
		goto exit;
	}

	MEMSET(rgstate, statelen);
	READ(fd,rgstate, statelen);

	/* 
	** It will set this flag after that the meta server fullfill tablet
	** split and before removing the tablet from backup directory. 
	*/
	if (state & RG_SSTABLE_DELETED)
	{
		/* Update the old data. */
		split_info = ri_rgstat_getdata(rgstate, new_sstab, state);

		if (split_info)
		{
			split_info->sstab_state |= state;
			rtn_state = TRUE;			
		}
		else
		{
			traceprint("It can not get the data(%s) for this delete() in the rgstate file.", new_sstab);
			rtn_state = FALSE;
			goto exit;	
		}
	}
	else
	{
		Assert (split_value);
		
		if ((rgstate->sstab_split_num + 1) > SSTABLE_MAX_COUNT)
		{
			traceprint("The number of splitted sstable(%d) expand the limited number", rgstate->sstab_split_num);
			rtn_state = FALSE;

			goto exit;			
		}
		
		/* Put new data. */ 
		int	len = rgstate->offset;
		
		PUT_TO_BUFFER((char *)rgstate, len, (char *)split_value, 
						SSTAB_SPLIT_INFO_HEADER);
		PUT_TO_BUFFER((char *)rgstate, len, split_value->sstab_key,
						split_value->sstab_keylen);

		rgstate->sstab_split_num++;
		rgstate->offset = len;
	}


#ifdef MT_KFS_BACKEND
	CLOSE(fd);
	
	OPEN(fd, statefile, (O_RDWR));
	
	if (fd < 0)
	{		
		goto exit;
	}
#else
	
	LSEEK(fd, 0, SEEK_SET);
#endif
	WRITE(fd, rgstate, sizeof(RG_STATE));

	
		
exit:
	CLOSE(fd);
	
	free (rgstate);
	return rtn_state;
}

int
ri_rgstat_deldata(char *statefile, char *new_sstab)
{
	int			fd;
	RG_STATE		*rgstate;	/* Ptr to the buffer for the
						** rg_state file.
						*/
	int			statelen;
	int			rtn_state;
	SSTAB_SPLIT_INFO	*split_info;	/* Ptr to one slot in the
						** rg_state.
						*/


	rtn_state = FALSE;
	statelen = sizeof(RG_STATE);
	rgstate = (RG_STATE *)malloc(statelen);
	
	OPEN(fd, statefile, (O_RDWR));
	
	if (fd < 0)
	{		
		goto exit;
	}

	MEMSET(rgstate, statelen);
	READ(fd, rgstate, statelen);
	
	int	i;
	
	split_info = (SSTAB_SPLIT_INFO *)rgstate->sstab_state;

	for (i = 0; i < rgstate->sstab_split_num; i++)
	{	
		/* Must be less than the valid storage zone. */
		Assert((char *)split_info < ((char *)rgstate + 
			rgstate->offset - SSTAB_SPLIT_INFO_HEADER));
		if (!strcmp(split_info->newsstabname, new_sstab))
		{			
			rtn_state = TRUE;

			char	*next_split_info = (char *)split_info + 
					(split_info->sstab_keylen +
					SSTAB_SPLIT_INFO_HEADER);

			/* Forward memory move. */
			int forward_len = rgstate->offset - 
					(next_split_info - (char *)rgstate);
			
			Assert(forward_len + 1);
			
			MEMCPY((char *)split_info, next_split_info, forward_len);

			/* Update the header of rgstate file. */
			(rgstate->sstab_split_num)--;
			rgstate->offset -= (split_info->sstab_keylen +
					SSTAB_SPLIT_INFO_HEADER);
		}

		split_info = (SSTAB_SPLIT_INFO *)((char *)split_info + 
			SSTAB_SPLIT_INFO_HEADER + split_info->sstab_keylen);
	}

	if (!rtn_state)
	{
		goto exit;
	}
	
#ifdef MT_KFS_BACKEND
	CLOSE(fd);
	
	OPEN(fd, statefile, (O_RDWR));
	
	if (fd < 0)
	{		
		goto exit;
	}
#else
	
	LSEEK(fd, 0, SEEK_SET);
#endif
	WRITE(fd, rgstate, sizeof(RG_STATE));
	
exit:
	free (rgstate);
	CLOSE(fd);
	return rtn_state;
}


/*
** Side Effects: split_info->sstab_key pointer to the fake value.
*/

SSTAB_SPLIT_INFO *
ri_rgstat_getdata(RG_STATE *rgstate, char *new_sstab, int state)
{
	SSTAB_SPLIT_INFO	*split_info;
	int			i;


	split_info = (SSTAB_SPLIT_INFO *)rgstate->sstab_state;

	for (i = 0; i < rgstate->sstab_split_num; i++)
	{	
		/* Must be less than the valid storage zone. */
		Assert((char *)split_info < ((char *)rgstate + 
			rgstate->offset - SSTAB_SPLIT_INFO_HEADER));
		if (!strcmp(split_info->newsstabname, new_sstab))
		{	
			/* Adjust the sstab key pointer. */
			//split_info->sstab_key = (char *)split_info + 
			//			SSTAB_SPLIT_INFO_HEADER;
			goto exit;
		}

		split_info = (SSTAB_SPLIT_INFO *)((char *)split_info + 
			SSTAB_SPLIT_INFO_HEADER + split_info->sstab_keylen);
	}

	split_info = NULL;
exit:	
		
	return split_info;
}

int
ri_get_rgstate(char *rgstate, char *rgip, int rgport)
{
	MEMSET(rgstate, 256);
	MEMCPY(rgstate, MT_RANGE_STATE, STRLEN(MT_RANGE_STATE));

	char	rgname[64];
	
	MEMSET(rgname, 64);
	sprintf(rgname, "%s%d", rgip, rgport);

	str1_to_str2(rgstate, '/', rgname);

	if (!(STAT(rgstate, &st) == 0))
	{
		traceprint("Log file %s is not exist.\n", rgstate);
		return FALSE;
	}	

	return TRUE;
}

