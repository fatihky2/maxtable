/*
** diskio.h 2010-11-25 xueyingfei
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
