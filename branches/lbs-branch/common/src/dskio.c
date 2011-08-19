/*
** diskio.c 2010-07-18 xueyingfei
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


/* Only for I/O that only include write and read */
#include "global.h"
#include "dskio.h"
#include "file_op.h"
#include "buffer.h"

int
dopen(char * name, long flags)
{
	int	mode;	/* Mode to be passed to open() call. */
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
	int	status;
	int	fd;
	int	flags;


	flags = (blkiop->bioflags & DBREAD) ? D_READ_ONLY : 0;
	fd = dopen(blkiop->biobp->bsstab_name, flags);

	printf(" Enter into the dstartio. fd = %d\n", fd);
	printf(" Enter into the dstartio. bsstab_name = %s\n", blkiop->biobp->bsstab_name);

	if (blkiop->bioflags & DBREAD) 
	{
		status = READ(fd, blkiop->biomaddr, blkiop->biosize);
	}
	else
	{
		status = WRITE(fd, blkiop->biomaddr, blkiop->biosize);
	}

	dclose(fd);
	/*
	** Check that the requested number of bytes have been
	** read or written
	*/
	if ((blkiop->biobp->bstat & BUF_READ_EMPTY) && (status == 0))
	{
		return TRUE;
	}
	
	if (status != blkiop->biosize)
	{
		return FALSE;
	}

	return TRUE;
}



