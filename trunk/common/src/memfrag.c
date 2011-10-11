/*
** memfrag.c 2010-08-12 xueyingfei
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
#include "tss.h"
#include "strings.h"
#include "buffer.h"
#include "utils.h"
#include "trace.h"

KERNEL *Kernel;

MEMPOOL *Globle_mp;	


#define	MEMFRAG_INIT_SIZE	(1024 * 1024)

static int
mp_frag_grow(MEMPOOL * mp, size_t grow_size);
static int
mp_list_insert(void * pool_hdl, int type);



void * 
mem_os_malloc(unsigned long size)
{
	int             rnd;
	unsigned long   alloc_size;
	void		*start_addr;

	
	rnd = MY_MEMPAGESIZE;

	if (size % rnd)
	{
		alloc_size = (size - (size % rnd) + rnd);
	}
	else
	{
		alloc_size = size;
	}

	
	alloc_size += (unsigned long) MY_MEMPAGESIZE;
	start_addr = (void *)malloc((unsigned int)alloc_size);

	Assert(start_addr != NULL);

	
	if (0 && ((long) start_addr % MY_MEMPAGESIZE))
	{
		start_addr = (start_addr + MY_MEMPAGESIZE) -
			    ((long) start_addr % MY_MEMPAGESIZE);
	}

	if (start_addr != NULL)
	{
		
		MEMSET(start_addr, size);
	}

	return(start_addr);
}


int
mem_init_alloc_regions()
{
	void *		kmem_ptr;	
	size_t		size;

	size = MY_KERNEL_MEM_SIZE;

	
	kmem_ptr = mem_os_malloc(size);

	Kernel = (KERNEL *)kmem_ptr;
	MEMSET(Kernel,sizeof(KERNEL));
	kmem_ptr += sizeof(KERNEL);
	size -= sizeof(KERNEL);

	
	Kernel->ke_fragpool_list = (MEMPLIST *)kmem_ptr;
	Kernel->ke_fragpool_list->mpl_count = 0;
	INITQUE(&(Kernel->ke_fragpool_list->mpl_link));

	kmem_ptr += sizeof(MEMPLIST);
	size -= sizeof(MEMPLIST);

	
	Kernel->ke_objpool_list = (MEMPLIST *)kmem_ptr;
	Kernel->ke_objpool_list->mpl_count = 0;
	INITQUE(&(Kernel->ke_objpool_list->mpl_link));

	kmem_ptr += sizeof(MEMPLIST);
	size -= sizeof(MEMPLIST);
	
	
	Kernel->ke_mp_frag = mp_frag_crt(MEMFRAG_INIT_SIZE, MAXSIZE_FRAGPOOL);

	
	mp_list_insert(Kernel->ke_mp_frag, MEMPOOL_FRAG);

	
	Kernel->ke_tss_objpool = (void *)mp_obj_crt(sizeof(TSSOBJ), 
						TSS_MIN_ITEMS, TSS_MAX_ITEMS);
	mp_list_insert((MEMOBJECT *)Kernel->ke_tss_objpool, MEMPOOL_OBJECT);

        
        Kernel->ke_buf_objpool = (void *)mp_obj_crt(sizeof(BUF), 
						TSS_MIN_ITEMS, TSS_MAX_ITEMS);
	mp_list_insert((MEMOBJECT *)Kernel->ke_buf_objpool, MEMPOOL_OBJECT);

	return TRUE;
}


int
mem_free_alloc_regions()
{
	if (Kernel->ke_mp_frag)
	{
		mp_frag_destroy(Kernel->ke_mp_frag);
	}

	if (Kernel->ke_tss_objpool)
	{
		mp_obj_destroy(Kernel->ke_tss_objpool);
	}

	if (Kernel->ke_buf_objpool)
	{
		mp_obj_destroy(Kernel->ke_buf_objpool);
	}

	free (Kernel);

	return TRUE;
}


MEMPOOL *
mp_frag_crt(size_t min_size, size_t max_size)
{
	size_t		init_size;
	size_t		grow_size;	
	MEMPOOL		*mp;		
	MEMBLK	   	*mbp;
	MEMFRAG		*mfp;

	
	if ( min_size < MY_MEMPAGESIZE )
	{
		min_size = MY_MEMPAGESIZE;
	}

	if ( max_size < min_size )
	{
		max_size = min_size;
	}

	min_size = ROUNDSIZE(min_size, MY_MEMPAGESIZE);
	max_size = ROUNDSIZE(max_size, MY_MEMPAGESIZE);

	if ( min_size <= MAX_FRAG_SIZE )
	{
		init_size = min_size;
	}
	else
	{
		init_size = MAX_FRAG_SIZE;
	}

	
	if ( Kernel && !Kernel->ke_mp_frag )
	{
		mp = mem_os_malloc(init_size);
		Globle_mp = mp;
	}
	else
	{
		return NULL;
	}

	if ( mp == (MEMPOOL *)NULL )
	{
		return (MEMPOOL *)NULL;
	}

	MEMSET(mp, sizeof(MEMPOOL));
	INITQUE(&mp->mp_link);
	INITQUE(&mp->mp_frags);
	INITQUE(&mp->mp_free_frags);                              
	mp->mp_free_frags.start_addr = (char *)&mp->mp_free_frags;

	
	MEMCOM_MINSIZE(mp) = min_size;
	MEMCOM_MAXSIZE(mp) = max_size;
	MEMCOM_GROWSIZE(mp) = MY_MEMPAGESIZE;
	MEMCOM_POOLTYPE(mp) = MEMPOOL_FRAG;
	MEMCOM_TOTAL(mp) = init_size;
	MEMCOM_UPDATE_USED(mp, sizeof(MEMPOOL));
	mp->mp_ovhd = sizeof(MEMPOOL);
	strcpy(MEMCOM_POOLNAME(mp), "noname_fragpool");

	
	mbp = (MEMBLK *)(mp + 1);
	mbp->mb_frags = 1;
	mbp->mb_begin = (void *)(mbp + 1);
	mbp->mb_end = (void *)((char *)mp + init_size);

	
	mfp = (MEMFRAG *)mbp->mb_begin;
	mfp->mf_flags = 0;
	mfp->mf_block = mbp;
	
	mfp->mf_size = (mbp->mb_end - (void *)mfp)/sizeof(MEMFRAG) - 1;
//	mfp->mf_size = ((MEMFRAG *)mbp->mb_end - mfp) - 1;

	mp->mp_nfrags = 1;

	
	MEMCOM_UPDATE_USED(mp, (sizeof(MEMBLK) + sizeof(MEMFRAG)));
	mp->mp_ovhd += (sizeof(MEMBLK) + sizeof(MEMFRAG));

	
	INSQUE(&(mp->mp_link), mbp);
	INSQUE(&(mp->mp_frags), mfp);
	MF_INS_FREEQUE(mp, mfp);

	
	while ( init_size < min_size )
	{
		if ( min_size - init_size > MAX_FRAG_SIZE )
		{
			grow_size = MAX_FRAG_SIZE;
		}
		else
		{
			grow_size = min_size - init_size;
		}

		init_size += grow_size;
		if (mp_frag_grow(mp, grow_size) != TRUE)
		{
			return NULL;
		}
	}

	return mp;
}


void *
mp_frag_alloc(MEMPOOL *mp, size_t size, char *file, int line)
{
	int 	ret_stat;	
	MEMFRAG	*mfp;		
	MEMFRAG *nextmfp;	
	MEMFRAG *addr;		
	FLINK	*free_link;	
	size_t	alloc_size;	
	size_t	alloced;	
	size_t  grow_size;

	
	if ((size > MAX_FRAG_ALLOCSIZE) || !mp)
	{
		return (void *)NULL;
	}

	addr = NULL;

//	int yxue_test = ROUNDSIZE(size, sizeof(MEMFRAG));
	alloc_size = ROUNDSIZE(size, sizeof(MEMFRAG)) / sizeof(MEMFRAG);

retry:
	FOR_QUEUE(FLINK, &mp->mp_free_frags, free_link)
	{
		mfp = MF_GET_MFP(free_link);
		if (mfp == NULL)
		{
			continue;
		}

		
		if (mfp->mf_size >= alloc_size)
        	{
			addr = mfp + 1;
			mfp->mf_flags |= MEMPOOL_USED;

			
			if (mfp->mf_size > alloc_size + 2)
            		{
				nextmfp = mfp + (alloc_size + 1);

				
				INSQUE(mfp, nextmfp);
				nextmfp->mf_size = mfp->mf_size - (alloc_size + 1);
				nextmfp->mf_flags = 0;

				
				MF_INS_FREEQUE(mp, nextmfp);

				
				MF_REM_FREEQUE(mfp);

				
				nextmfp->mf_block = mfp->mf_block;

				
				mfp->mf_block->mb_frags++;

				mfp->mf_size = alloc_size;

				alloced = (alloc_size + 1) * sizeof(MEMFRAG);
				MEMCOM_UPDATE_USED(mp, alloced);
				mp->mp_ovhd += sizeof(MEMFRAG);
				mp->mp_nfrags++;
			}
			else
			{
				alloced = mfp->mf_size * sizeof(MEMFRAG);
				MEMCOM_UPDATE_USED(mp, alloced);
				MF_REM_FREEQUE(mfp);
			}

			
			break;
		}
	}

	if ( addr == NULL )
	{
//		size_t	grow_size;

		
		grow_size = MEMPOOL_FRAG_GROWSIZE(size);

		if ( MEMCOM_GROWSIZE(mp) > grow_size )
        	{
			grow_size = MEMCOM_GROWSIZE(mp);
		}

		ret_stat = mp_frag_grow(mp, grow_size);

		if ( ret_stat == TRUE )
        	{
			goto retry;
		}
	}

	
	if (addr == NULL)
	{
		Assert(addr);
	}

	if (TRACECMDLINE(MEM_USAGE))
	{
		traceprint("MEMORY ALLOCATED BY ADDRESS == 0x%x, file = %s, line = %d\n", addr,file, line);
	}
	
	return ((void *)addr);
}



void *
mp_frag_realloc(MEMPOOL *mp, void * addr, size_t size)
{
	int	alloc_size;	
	int	comb_size;	
	void	*newaddr;	
	MEMFRAG	*nextmfp;	
	MEMFRAG	*mfp;		
	MEMFRAG	*newmfp;	

	
	if ((size > MAX_FRAG_ALLOCSIZE) || !mp)
    	{
		return (void *)NULL;
	}

	
	mfp = (MEMFRAG *)addr - 1;

	
	alloc_size = ROUNDSIZE(size, sizeof(MEMFRAG)) / sizeof(MEMFRAG);

	if (alloc_size <= mfp->mf_size)
	{
		traceprint("MEMORY REALLOCATED BY ADDRESS == 0x%x\n", addr);
		return addr;	
	}

	nextmfp = (MEMFRAG *)(QUE_NEXT(mfp));
	comb_size = (int)(mfp->mf_size + nextmfp->mf_size + 1);

	if ( MF_FREE(nextmfp) && comb_size >= alloc_size &&
		nextmfp->mf_block == mfp->mf_block )
    	{
		if ( comb_size == alloc_size )
        	{
			
			mfp->mf_size = alloc_size;
			mfp->mf_flags |= MEMPOOL_USED;
			
			REMQUE(nextmfp, nextmfp, MEMFRAG);

			
			MF_REM_FREEQUE(nextmfp);

			
			MEMCOM_UPDATE_USED(mp, (sizeof(MEMFRAG) * nextmfp->mf_size));
			mp->mp_ovhd -= sizeof(MEMFRAG);
			mp->mp_nfrags--;

			
			MEMSET(nextmfp, sizeof(MEMFRAG));

		}
	        else
	        {
			newmfp = nextmfp + (alloc_size - mfp->mf_size); 
			nextmfp->mf_flags |= MEMPOOL_USED;
			
			REMQUE(nextmfp, nextmfp, MEMFRAG);

			
			MF_REM_FREEQUE(nextmfp);

			
			newmfp->mf_size = comb_size - alloc_size - 1;
			newmfp->mf_block = nextmfp->mf_block;
			newmfp->mf_flags = 0;
			INSQUE(mfp, newmfp);

			
			MF_INS_FREEQUE(mp, newmfp);

			
			MEMCOM_UPDATE_USED(mp, 
				(sizeof(MEMFRAG) * (alloc_size - mfp->mf_size)));

			
			MEMSET(nextmfp, sizeof(MEMFRAG));

			
			mfp->mf_size = alloc_size;

			
		}

		traceprint("MEMORY REALLOCATED BY ADDRESS == 0x%x\n", addr);
		return addr;
	}

	
	newaddr = mp_frag_alloc(mp, size, NULL,0);
//	MEMSET(newaddr, 0, size);

	if ( newaddr != NULL )
    	{
		MEMCPY(newaddr, addr, mfp->mf_size);
		mp_frag_free(mp, addr,NULL,0);
	}

	if (TRACECMDLINE(MEM_USAGE))
	{
		traceprint("MEMORY REALLOCATED BY ADDRESS == 0x%x\n", addr);
	}
	
	return newaddr;
}



int
mp_frag_free(MEMPOOL *mp, void *addr, char *file, int line)
{
	MEMFRAG	*mfp;		
	MEMFRAG *temp_mfp;	

	if (addr == NULL)
	{
		return TRUE;
	}

	
	if ( mp == (MEMPOOL *)NULL )
    	{
		return FALSE;
	}

	mfp = (MEMFRAG *) addr;
//	printf("\n print NULL list ---1 \n");
//	prLINK((LINK *)mfp);
	--mfp;

//	printf("\n print NULL list ---2 \n");
//	prLINK((LINK *)mfp);
	
	mfp->mf_flags = 0;

	
	MEMCOM_UPDATE_USED(mp, -(sizeof(MEMFRAG) * mfp->mf_size));
	

	
	MF_INS_FREEQUE(mp, mfp);
	
	
	
	temp_mfp = (MEMFRAG *)(QUE_PREV(mfp));

	if (   temp_mfp != (MEMFRAG *)(&(mp->mp_frags))	
	    && MF_FREE(temp_mfp) 
	    && temp_mfp->mf_block == mfp->mf_block )
	{
		
		MEMCOM_UPDATE_USED(mp, -(sizeof(MEMFRAG)));
		mp->mp_ovhd -= sizeof(MEMFRAG);
		mp->mp_nfrags--;

		
		temp_mfp->mf_size += (mfp->mf_size + 1);
		REMQUE(mfp, mfp, MEMFRAG);
		MF_REM_FREEQUE(mfp);

		mfp = temp_mfp;
		mfp->mf_block->mb_frags--;
	}

	
	temp_mfp = (MEMFRAG *)(QUE_NEXT(mfp));

	if (   temp_mfp != (MEMFRAG *)(&(mp->mp_frags))	
	    && MF_FREE(temp_mfp)			
	    && temp_mfp->mf_block == mfp->mf_block )
    	{
		
		MEMCOM_UPDATE_USED(mp, -(sizeof(MEMFRAG)));
		mp->mp_ovhd -= sizeof(MEMFRAG);
		mp->mp_nfrags--;

		mfp->mf_size += temp_mfp->mf_size + 1;
	
		REMQUE(temp_mfp, temp_mfp, MEMFRAG); 

		MF_REM_FREEQUE(temp_mfp);

		mfp->mf_block->mb_frags--;
	}

	if (TRACECMDLINE(MEM_USAGE))
	{
		traceprint("MEMORY FREEED BY ADDRESS == 0x%x, file = %s, line = %d \n", addr,file,line);
	}

	return TRUE;
}


static int
mp_frag_grow(MEMPOOL * mp, size_t grow_size)
{
	char		*mem_star_addr;	
	MEMBLK	*mbp;		
	MEMFRAG		*mfp;		
	int		ret;		

	
	ret = FALSE;

	
	if ((grow_size > MAX_FRAG_SIZE) || (grow_size < MY_MEMPAGESIZE))
	{
		goto cleanup;
	}

	grow_size = ROUNDSIZE(grow_size, MY_MEMPAGESIZE);

	
	if (   ((MEMCOM_TOTAL(mp) + grow_size) > MEMCOM_MAXSIZE(mp)
	    || (grow_size > MAX_FRAG_SIZE)))
	{
			goto cleanup;
	}

	mem_star_addr = mem_os_malloc(grow_size);

	if ( mem_star_addr == NULL )
    	{
		goto cleanup;
	}

	
	mbp = (MEMBLK *)mem_star_addr;
	mbp->mb_frags = 1;
	mbp->mb_begin = (void *)(mbp + 1);
	mbp->mb_end = (void *)((char *)mbp + grow_size);

	
	mfp = (MEMFRAG *)mbp->mb_begin;
	mfp->mf_flags = 0;
	mfp->mf_block = mbp;
	mfp->mf_size = ((MEMFRAG *)mbp->mb_end - mfp) - 1;

	
	INSQUE(&(mp->mp_link), mbp);

	
	INSQTAIL(&(mp->mp_frags), mfp);

	MF_INS_FREEQUE(mp, mfp);

	
	MEMCOM_TOTAL(mp) += grow_size;
	MEMCOM_UPDATE_USED(mp, (sizeof(MEMFRAG) + sizeof(MEMBLK)));
	mp->mp_ovhd += (sizeof(MEMFRAG) + sizeof(MEMBLK));
	mp->mp_nfrags++;

	ret = TRUE;

  cleanup:
	return (ret);
}


int
mp_frag_destroy(MEMPOOL *mp)
{
	MEMBLK	*mbp;		
	void 		*temp_ptr;
	int 		status = TRUE;
	LINK		freeblocks;

	
	if (mp == NULL)
    	{
		return FALSE;
	}

	
	FOR_QUEUE(MEMBLK, &(mp->mp_link), mbp)
    	{
		if (!MF_UNUSED_BLOCK(mbp))
        	{
			return FALSE;
		}
	}

	
	INITQUE(&freeblocks);
	MOVEQUE(&freeblocks, &mp->mp_link);

	
	while ( (char *)QUE_NEXT(&freeblocks) != (char *)(mp+1) )
    	{
		REMQHEAD(&freeblocks, temp_ptr, MEMBLK);
		free(temp_ptr);
	}

	
	free(mp);
	return status;
}


static int
mp_list_insert(void * pool_hdl, int type) 
{
	MEMPLIST	*mpl;	
	
	
	mpl = MEMPOOL_TYPE_TO_LIST(type);

	mpl->mpl_count++;
	INSQUE(&(mpl->mpl_link), pool_hdl);

	return TRUE;
}

void * 
memallocheap(size_t size, char *file, int line)
{
	void* ptr;

	ptr = mp_frag_alloc(Kernel->ke_mp_frag, size,file,line);

	if(ptr == NULL) 
	{
		;
	}

	MEMSET(ptr, size);
//	printf("\nMALLOC MEMORY == %d\n", size);
	return ptr;
}

void 
*memreallocheap(void * addr, size_t size)
{
	return  mp_frag_realloc(Kernel->ke_mp_frag, addr, size);
}

int
memfreeheap(void *addr, char *file, int line)
{
//	printf("\nFREE MEMORY == %d\n", addr);
	return mp_frag_free(Kernel->ke_mp_frag, addr,file,line);
}

#ifdef MEMMGR_UNIT_TEST


static void
prLINK(LINK *link)
{
	traceprint("\n LinkAddr = 0x%p \n", link);
	traceprint("\t prev=0x%p \t next=0x%p \n", link->prev, link->next);
}



static void
mem_prt_fragmp(MEMPOOL *mp)
{
	FLINK   *free_link;
	MEMFRAG *mfp;

	traceprint("\n Pint Frag List : BEGIN \n");
	traceprint("\n Free  Frag LIST \n");

	FOR_QUEUE(FLINK, &mp->mp_free_frags, free_link)
    	{
		mfp = MF_GET_MFP(free_link);
		prLINK((LINK *)mfp);
	}

	traceprint("\n All  Frag LIST \n");

	FOR_QUEUE(MEMFRAG, &(mp->mp_frags), mfp)
    	{
		prLINK((LINK *)mfp);
	}

	traceprint("\n Pint Frag List : END \n");

}

int main()
{
	char *test_char1;
	char *test_char2;
	char *test_char3;

	traceprint(" \n-------------Create MemPool \n");
	mem_init_alloc_regions();
	mem_prt_fragmp(Globle_mp);

	traceprint(" \n-------------MemPool After Allocting\n");
	test_char1 = MEMALLOCHEAP(1600);

	test_char2 = MEMALLOCHEAP(1600);
	
	test_char3 = MEMALLOCHEAP(1600);

	mem_prt_fragmp(Globle_mp);

	traceprint(" \n-------------MemPool After Free\n");
	MEMFREEHEAP(test_char2);		
	mem_prt_fragmp(Globle_mp);
	return 1;
}
#endif
