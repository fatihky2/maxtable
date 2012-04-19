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
#include "utils.h"
#include "dskio.h"
#include "file_op.h"
#include "buffer.h"
#include "tss.h"


extern	TSS	*Tss;

int
dopen(char * name, long flags)
{
	int	mode;		
	int	fd;
	char	*tempname;
	
	if (flags & D_READ_ONLY)
	{
		mode = O_RDONLY;
	}
	else
	{
		mode = O_RDWR;

	}

	tempname = name;
	
	OPEN(fd, tempname, mode);

	return fd;
}

int
dclose(int fd)
{
	return (CLOSE(fd));
}

int
dstartio(BLKIO * blkiop)
{
	LOCALTSS(tss);
	int	status;
	int	fd;
	int	flags;


	flags = (blkiop->bioflags & DBREAD) ? D_READ_ONLY : 0;
	fd = dopen(blkiop->biobp->bsstab_name, flags);

	if (DEBUG_TEST(tss))
	{
		traceprint(" Enter into the dstartio. fd = %d\n", fd);
		traceprint(" Enter into the dstartio. bsstab_name = %s\n", blkiop->biobp->bsstab_name);
	}
	
	if (blkiop->bioflags & DBREAD) 
	{
		status = READ(fd, blkiop->biomaddr, blkiop->biosize);
	}
	else
	{
		status = WRITE(fd, blkiop->biomaddr, blkiop->biosize);
	}

	dclose(fd);

	
	if ((blkiop->bioflags & DBREAD) && (blkiop->biobp->bstat & BUF_READ_EMPTY) && (status == 0))
	{
		return TRUE;
	}
	
	if (status != blkiop->biosize)
	{
		return FALSE;
	}

	return TRUE;
}



