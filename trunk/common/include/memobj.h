/*
** memobj.h 2010-09-07 xueyingfei
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


#ifndef MEMOBJ_H_
#define MEMOBJ_H_

#include "memcom.h"

/* Per Object Information */
typedef struct objinfo
{
	MEMBLK	*oi_memblk;
	int	oi_slotid;
	int	oi_flags;
} MEM_OBJINFO;

/* Generic Freelist */
typedef struct memobject 
{
	MEMCOM		f_mc;		/* must be the first field */
	LINK		f_block;	/* heading memblock list */
	LINK		f_free;		/* Free items */
	LINK		f_used;		/* Used items */
	size_t		f_itemsize;	/* size of items */
	size_t		f_maxengines;	/* maximum number of engines */
	int		f_status;	/* properties of an object pool */
	long		f_count;	/* # of free items in central list */
	long		f_destr_count;	/* # of objects in destroy state */
	size_t		f_security;	/* zero memory brfore release? */
	long		f_spares[2];	/* for development and debugging */
} MEMOBJECT;


#define	MOBJ_CALC_OBJECT_SIZE(_objsize)						\
	ROUNDSIZE((_objsize) + sizeof(MEM_OBJINFO), sizeof(ALIGNDATATYPE))	\

/* Following are for object memory pool. */
MEMOBJECT *
mp_obj_crt(size_t itemsize, size_t minitems, size_t maxitems);

void *
mp_obj_alloc(MEMOBJECT *fp);

int
mp_obj_grow(MEMOBJECT * fp);

int
mp_obj_free(MEMOBJECT * fp, void * item);

int
mp_obj_destroy(MEMOBJECT * fp);


#endif
