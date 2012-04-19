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

#ifndef MEMOBJ_H_
#define MEMOBJ_H_

#include "memcom.h"


typedef struct objinfo
{
	MEMBLK	*oi_memblk;
	int	oi_slotid;
	int	oi_flags;
} MEM_OBJINFO;


typedef struct memobject 
{
	MEMCOM		f_mc;				
	LINK		f_block;		
	LINK		f_free;			
//	LINK		f_used;			
	size_t		f_itemsize;		
	size_t		f_maxengines;		
	int		f_status;		
	long		f_count;		
	long		f_destr_count;		
	size_t		f_security;		
	long		f_spares[2];		
} MEMOBJECT;


#define	MOBJ_CALC_OBJECT_SIZE(_objsize)						\
	ROUNDSIZE((_objsize) + sizeof(MEM_OBJINFO), sizeof(ALIGNDATATYPE))	\


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
