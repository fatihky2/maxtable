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

#ifndef	DDBLKIO_H_
#define	DDBLKIO_H_

struct buf;


typedef struct blkio
{
 	char		*biomaddr;	
	int		biosize;	
	int		bioflags;
	struct buf	*biobp;

} BLKIO;



#define	DBREAD		0x0001		
#define	DBWRITE		0x0002		


#define	D_READ_ONLY	0x1		
#define	D_O_DIRECT	0x2 		
#define	D_CREATE	0x4
#define	D_WRITE_ONLY	0x8


int
dstartio(BLKIO * blkiop);

#endif	
