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
	char 		*memptr;	/* memory pointer */
	char 		*free_memptr;	/* for carving out memory */
	char 		*top;		/* the top address of the 1st block */
	char 		*itemp;		/* pointer to an object */
	size_t		size;		/* memory needed for pool management */
	size_t		seg_size;	/* round up of size */
	MEMBLK		*mbp;		/* MEMBLK pointer */
	MEMOBJECT	*fp;		/* The new object pool */
	size_t		total_itemsize;	/* includes overhead */

	/* space needed for managing an object pool */
	size = sizeof(MEMOBJECT) + sizeof(MEMBLK);

  	total_itemsize = MOBJ_CALC_OBJECT_SIZE(itemsize);

	/* plus the amount of minitems objects */
	seg_size = size + minitems * total_itemsize;

	/* make the inital segment MY_MEMPAGESIZE-aligned */
	seg_size = ROUNDSIZE(seg_size, MY_MEMPAGESIZE);

//	memptr = memallocheap(B2P(seg_size));
//	B2P(seg_size);
	memptr = mem_os_malloc(seg_size);
	free_memptr = memptr;

	/*
	** The first memory block has the following layout:
	**	MEMOBJECT struct
	**	MEMBLK struct
	**	n objects, where value of n depends on the object size
	*/

	fp = (MEMOBJECT *) free_memptr;
	free_memptr += sizeof(MEMOBJECT);

	/*
	** Initialize the MEMOBJECT struct. Copy the name, itemsize, 
	** maxitems, minitems, and spinlock into the MEMOBJECT struct.
	*/
	MEMSET(fp, sizeof(MEMOBJECT));

	fp->f_itemsize = itemsize;
	MEMCOM_MAXITEMS(fp) = maxitems;
	MEMCOM_GROWSIZE(fp) = MY_MEMPAGESIZE;
	strncpy(MEMCOM_POOLNAME(fp), "Object Pool", sizeof(MEMCOM_POOLNAME(fp)-1));
	MEMCOM_POOLTYPE(fp) = MEMPOOL_OBJECT;

	/* initialize the free object queue */
	INITQUE(&(fp->f_free));

	/* initialize the memblock queue */
	INITQUE(&(fp->f_block));

	/* initialize the used object queue */
	INITQUE(&(fp->f_used));

	itemsize = total_itemsize;
	size = ROUNDSIZE(size, sizeof(ALIGNDATATYPE));
	if ( (seg_size - size) >= itemsize )
	{
		/* top address of the block */
		top = memptr + seg_size;

		/* initialize the memblock */
		mbp = (MEMBLK *)(free_memptr);
		mbp->mb_begin = (void *)free_memptr;
		mbp->mb_end = (void *)top;

		free_memptr += sizeof(MEMBLK);

		/* get the starting address of the first object */
		itemp = free_memptr;

		/* Add the memblock to the memblock list */
		INSQTAIL(&(fp->f_block), mbp);

		/*
		** Allocated only the number of objects requested for
		** by the client and not more. Some clients of memory 
		** manager has this requirement.
		*/
	
		while (itemp <= (top - itemsize))
		{
			/* add the object to the free list */
			INSQTAIL(&(fp->f_free), itemp);
			fp->f_count++;
			MEMCOM_TOTAL(fp)++;
	
			/* Move to the next object */
			itemp += itemsize;

		}
	}

	/*
	** The min size of the pool is the size when the pool
	** was created.
	*/
	MEMCOM_MINITEMS(fp) = fp->f_count;

	/* Return new object pool handle */
	return (fp);
}


void *
mp_obj_alloc(MEMOBJECT *fp)
{
	int	grow_status;	/* return status of ubogrow() */
	void	*item;		/* address of allocated object */
	
	item = NULL;

retry:
	if (!EMPTYQUE(&fp->f_free))
	{
		/* Pool not empty, take an object from the pool.*/
		REMQHEAD(&fp->f_free, item, void);
		fp->f_count--;

		/* Add the object to the used object list */
		INSQTAIL(&(fp->f_used), item);

		/* Keep track of resource usage statistics */
		MEMCOM_UPDATE_USED(fp, 1);
		
	}
	else if ( MEMCOM_TOTAL(fp) < MEMCOM_MAXITEMS(fp))
	{
		grow_status = mp_obj_grow(fp); 
		if (grow_status == MEMPOOL_SUCCESS)
		{
			/* there are more free objects. Retry */
			goto retry;
		}
	}

	return item;
}

int
mp_obj_grow(MEMOBJECT * fp)
{
	int     n;              /* number of objects added */
	int     growsize;       /* how much to grow the object pool */
	char    *top;           /* end of allocated memory block */
	char    *space;         /* address of allocated memory block */
	char    *itemp;         /* address of an object */
	char    *last_one;      /* address of the last object in a block */
	MEMBLK  *mbp;           /* a memblock pointer */
	size_t  itemsize;       /* rounded object size */

	
	growsize = MEMCOM_GROWSIZE(fp) ? MEMCOM_GROWSIZE(fp) : MY_MEMPAGESIZE;
	
//	space = (char *)memallocheap(B2P(growsize));
	space = (char *)mem_os_malloc(B2P(growsize));
	if ( space == (char *)NULL )
	{
	        /* set error code */
	        return MEMPOOL_FAIL;
	}

	/* top address of the block */
	top = space + growsize;

	/* initialize the memblock */
	mbp = (MEMBLK *)space;
	mbp->mb_begin = (void *)space;
	mbp->mb_end = (void *)top;

	/* each object is aligned to sizeof(double) boundary */
	itemsize = ROUNDSIZE(fp->f_itemsize, sizeof(ALIGNDATATYPE));

	/* get the starting address of the first object */
	itemp = space + sizeof(MEMBLK);

	last_one = top - itemsize;
	if ( itemp > last_one )
	{
	        /* itemsize is too big */
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
	        /* Add the object to the pool */
	        INSQTAIL(&(fp->f_free), itemp);
	        itemp += itemsize;
	        n++;
	}

	/* Free object list. */
	fp->f_count += n;

	MEMCOM_TOTAL(fp) += n;
	
	/* Add the memblock to the memblock list */
	INSQUE(&(fp->f_block), mbp);

	/* Return the number of objects added. */
	return n;
}
int
mp_obj_free(MEMOBJECT * fp, void * item)
{
	int     growsize;       /* work variable, growsize of object pool */
	LINK    *list;          /* Listhead for moving items */

	if ( fp == NULL )
	{
	        return MEMPOOL_FAIL;
	}

	if ( item == NULL )
	{
	        return MEMPOOL_INVALIDADDR;
	}

	/* erase the content of struct if necessary */
	if ( fp->f_security == TRUE )
	{
	        MEMSET(item, fp->f_itemsize);
	}

	growsize = MEMCOM_GROWSIZE(fp);


	/* Return the item to the object pool. */
	INSQTAIL(&(fp->f_free), item);
	fp->f_count++;

	/* Remove this item from the used object list */
	list = &(((MEMOBJECT *)item)->f_used);
	REMQUE(list, list, LINK);

	/* Keep track of resource usage statistics */
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

	/*
	** The object pool is not used by anybody. Destroy it!
	*/
	FOR_QUEUE(MEMBLK, &(fp->f_block), mbp)
	{
		if (   (char *)mbp >= (char *)fp 
		    && (char *)mbp < (char *)fp + MY_MEMPAGESIZE )
		{
		        /* skip the first memory block */
		        continue;
		}
		
		/* release all the memblocks */
		MEMCPY(mbp, &memblock, sizeof(MEMBLK));
	//	memfreeheap(mbp);
		free(mbp);
		mbp = &memblock;
	}

	/* erase the MEMOBJECT struct before release */
	MEMSET(fp, sizeof(MEMOBJECT));

	//memfreeheap((void *)fp);
	free((void *)fp);
	fp = NULL;

	return MEMPOOL_SUCCESS;
}



