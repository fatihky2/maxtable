/*
** memobj.c 2010-09-17 xueyingfei
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
#include "global.h"
#include "memcom.h"
#include "memobj.h"
#include "strings.h"


MEMOBJECT *
mp_obj_crt(size_t itemsize, size_t minitems, size_t maxitems)
{
	char 		*memptr;	
	char 		*free_memptr;	
	char 		*top;		
	char 		*itemp;		
	size_t		size;		
	size_t		seg_size;	
	MEMBLK		*mbp;		
	MEMOBJECT	*fp;		
	size_t		total_itemsize;	

	
	size = sizeof(MEMOBJECT) + sizeof(MEMBLK);

  	total_itemsize = MOBJ_CALC_OBJECT_SIZE(itemsize);

	
	seg_size = size + minitems * total_itemsize;

	
	seg_size = ROUNDSIZE(seg_size, MY_MEMPAGESIZE);

//	memptr = memallocheap(B2P(seg_size));
//	B2P(seg_size);
	memptr = mem_os_malloc(seg_size);
	free_memptr = memptr;

	

	fp = (MEMOBJECT *) free_memptr;
	free_memptr += sizeof(MEMOBJECT);

	
	MEMSET(fp, sizeof(MEMOBJECT));

	fp->f_itemsize = itemsize;
	MEMCOM_MAXITEMS(fp) = maxitems;
	MEMCOM_GROWSIZE(fp) = MY_MEMPAGESIZE;
	strncpy(MEMCOM_POOLNAME(fp), "Object Pool", sizeof(MEMCOM_POOLNAME(fp)-1));
	MEMCOM_POOLTYPE(fp) = MEMPOOL_OBJECT;

	
	INITQUE(&(fp->f_free));

	
	INITQUE(&(fp->f_block));

	
	INITQUE(&(fp->f_used));

	itemsize = total_itemsize;
	size = ROUNDSIZE(size, sizeof(ALIGNDATATYPE));
	if ( (seg_size - size) >= itemsize )
	{
		
		top = memptr + seg_size;

		
		mbp = (MEMBLK *)(free_memptr);
		mbp->mb_begin = (void *)free_memptr;
		mbp->mb_end = (void *)top;

		free_memptr += sizeof(MEMBLK);

		
		itemp = free_memptr;

		
		INSQTAIL(&(fp->f_block), mbp);

		
	
		while (itemp <= (top - itemsize))
		{
			
			INSQTAIL(&(fp->f_free), itemp);
			fp->f_count++;
			MEMCOM_TOTAL(fp)++;
	
			
			itemp += itemsize;

		}
	}

	
	MEMCOM_MINITEMS(fp) = fp->f_count;

	
	return (fp);
}


void *
mp_obj_alloc(MEMOBJECT *fp)
{
	int	grow_status;	
	void	*item;		
	
	item = NULL;

retry:
	if (!EMPTYQUE(&fp->f_free))
	{
		
		REMQHEAD(&fp->f_free, item, void);
		fp->f_count--;

		
		INSQTAIL(&(fp->f_used), item);

		
		MEMCOM_UPDATE_USED(fp, 1);
		
	}
	else if ( MEMCOM_TOTAL(fp) < MEMCOM_MAXITEMS(fp))
	{
		grow_status = mp_obj_grow(fp); 
		if (grow_status == MEMPOOL_SUCCESS)
		{
			
			goto retry;
		}
	}

	return item;
}

int
mp_obj_grow(MEMOBJECT * fp)
{
	int     n;              
	int     growsize;       
	char    *top;           
	char    *space;         
	char    *itemp;         
	char    *last_one;      
	MEMBLK  *mbp;           
	size_t  itemsize;       

	
	growsize = MEMCOM_GROWSIZE(fp) ? MEMCOM_GROWSIZE(fp) : MY_MEMPAGESIZE;
	
//	space = (char *)memallocheap(B2P(growsize));
	space = (char *)mem_os_malloc(B2P(growsize));
	if ( space == (char *)NULL )
	{
	        
	        return MEMPOOL_FAIL;
	}

	
	top = space + growsize;

	
	mbp = (MEMBLK *)space;
	mbp->mb_begin = (void *)space;
	mbp->mb_end = (void *)top;

	
	itemsize = ROUNDSIZE(fp->f_itemsize, sizeof(ALIGNDATATYPE));

	
	itemp = space + sizeof(MEMBLK);

	last_one = top - itemsize;
	if ( itemp > last_one )
	{
	        
	        return MEMPOOL_OBJECT2BIG;
	}

	if ( MEMCOM_TOTAL(fp) >= MEMCOM_MAXITEMS(fp) )
	{
	       
	//        memfreeheap(space);
		free(space);
	        return MEMPOOL_MAXEXCEEDED;
	}

	n = 0;
	while ( itemp < last_one )
	{
	        
	        INSQTAIL(&(fp->f_free), itemp);
	        itemp += itemsize;
	        n++;
	}

	
	fp->f_count += n;

	MEMCOM_TOTAL(fp) += n;
	
	
	INSQUE(&(fp->f_block), mbp);

	
	return n;
}
int
mp_obj_free(MEMOBJECT * fp, void * item)
{
	int     growsize;       
	LINK    *list;          

	if ( fp == NULL )
	{
	        return MEMPOOL_FAIL;
	}

	if ( item == NULL )
	{
	        return MEMPOOL_INVALIDADDR;
	}

	
	if ( fp->f_security == TRUE )
	{
	        MEMSET(item, fp->f_itemsize);
	}

	growsize = MEMCOM_GROWSIZE(fp);


	
	INSQTAIL(&(fp->f_free), item);
	fp->f_count++;

	
	list = &(((MEMOBJECT *)item)->f_used);
	REMQUE(list, list, LINK);

	
	MEMCOM_UPDATE_USED(fp, -1);


	return MEMPOOL_SUCCESS;
}


int
mp_obj_destroy(MEMOBJECT * fp)
{
	MEMBLK  *mbp;
	MEMBLK  memblock;



	if ( fp->f_count != MEMCOM_TOTAL(fp) )
	{
		return MEMPOOL_BUSY;
	}

	
	FOR_QUEUE(MEMBLK, &(fp->f_block), mbp)
	{
		if (   (char *)mbp >= (char *)fp 
		    && (char *)mbp < (char *)fp + MY_MEMPAGESIZE )
		{
		        
		        continue;
		}
		
		
		MEMCPY(mbp, &memblock, sizeof(MEMBLK));
	//	memfreeheap(mbp);
		free(mbp);
		mbp = &memblock;
	}

	
	MEMSET(fp, sizeof(MEMOBJECT));

	//memfreeheap((void *)fp);
	free((void *)fp);
	fp = NULL;

	return MEMPOOL_SUCCESS;
}



