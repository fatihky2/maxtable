/*
** memcom.h 2010-07-17 xueyingfei
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


#ifndef MEMCOM_H_
#define MEMCOM_H_

# ifdef __cplusplus
extern "C" {
# endif

#include "spinlock.h"
#include "list.h"





#define MEMPOOL_BLOCK	1	
#define MEMPOOL_FRAG	2	
#define MEMPOOL_OBJECT	3	
#define MEMPOOL_STACK	4	

#define	ALIGNDATATYPE	long

struct buf;



typedef struct free_frag_link
{
	LINK f_link;	
	void * start_addr; 
} FLINK;


typedef struct memplist
{
	LINK 	mpl_link;	
	size_t	mpl_count;	
} MEMPLIST;



typedef struct memcom
{
	LINK		mc_link;	
	int		mc_pooltype;	
	size_t		mc_total;	
	size_t		mc_used;	
	size_t		mc_maxsize;	
	size_t		mc_minsize;	
	size_t		mc_growsize;	
	char		mc_name[32];	
} MEMCOM;


#define MEMCOM_POOLNAME(_mc)	(((struct memcom *) (_mc))->mc_name)
#define MEMCOM_POOLTYPE(_mc)	(((struct memcom *) (_mc))->mc_pooltype)
#define MEMCOM_MINSIZE(_mc)	(((struct memcom *) (_mc))->mc_minsize)
#define MEMCOM_MAXSIZE(_mc)	(((struct memcom *) (_mc))->mc_maxsize)
#define MEMCOM_GROWSIZE(_mc)	(((struct memcom *) (_mc))->mc_growsize)
#define MEMCOM_TOTAL(_mc)	(((struct memcom *) (_mc))->mc_total)
#define MEMCOM_USED(_mc)	(((struct memcom *) (_mc))->mc_used)
#define MEMCOM_MAXITEMS(_mc)	MEMCOM_MAXSIZE(_mc)
#define MEMCOM_MINITEMS(_mc)	MEMCOM_MINSIZE(_mc)



typedef struct mempool
{
	MEMCOM	mp_mc;		
	LINK	mp_link;	
	LINK	mp_frags;	
	FLINK   mp_free_frags;  
	size_t	mp_ovhd;	
	size_t	mp_nfrags;	
} MEMPOOL;


typedef struct memblk
{
	LINK 	mb_link;	
	void 	*mb_begin;	
	void	*mb_end;	
	int	mb_frags;	
} MEMBLK;


typedef struct memfrag
{
	LINK	mf_link;	
	MEMBLK *mf_block;	
	int	mf_size;	
	short	mf_flags;	
	char	pad[2];
} MEMFRAG;


#define MEMPOOL_SUCCESS		0	
#define MEMPOOL_FAIL		-1	
#define MEMPOOL_INVALIDADDR 	-2	
#define MEMPOOL_BUSY		-3	
#define MEMPOOL_MAXEXCEEDED 	-4	
#define MEMPOOL_OBJECT2BIG	-5	



#define MEMPOOL_USED	0x1	


#define MF_FREE(mfp) (((mfp)->mf_flags & MEMPOOL_USED) == 0)
#define MF_USED(mfp) (((mfp)->mf_flags & MEMPOOL_USED) != 0)


#define MF_UNUSED_BLOCK(_mbp) (((_mbp)->mb_frags == 1) && \
	(MF_FREE((MEMFRAG *)((char *)(_mbp) + sizeof(MEMBLK)))))

#define MY_MEMPAGESIZE		4096L	
#define MY_KERNEL_MEM_SIZE	(32 * MY_MEMPAGESIZE + 1)



#define ROUNDSIZE(size, round)	(((size) + ((round) - 1)) & ~((round) - 1))


#define MAXSIZE_FRAGPOOL	MY_MEMPAGESIZE * 1024
#define MAX_FRAG_SIZE		(65536 * sizeof(MEMFRAG))
#define MAX_FRAG_ALLOCSIZE (MAX_FRAG_SIZE - sizeof(MEMFRAG) - sizeof(MEMBLK))


#define MAXSIZE_BLKPOOL		MY_MEMPAGESIZE * 1024


#define B2P(size)	((size) % MY_MEMPAGESIZE			\
				? ((size) / MY_MEMPAGESIZE + 1)	\
				: ((size) / MY_MEMPAGESIZE)) 


#define MF_GET_MFP(link)	(MEMFRAG *)((FLINK *)(link))->start_addr


#define  MEMPOOL_FRAG_GROWSIZE(_growsize)				\
	ROUNDSIZE((ROUNDSIZE((_growsize), sizeof(MEMFRAG)) +		\
		(sizeof(MEMBLK) + sizeof(MEMFRAG))), MY_MEMPAGESIZE)


#define MEMCOM_UPDATE_USED(_mc, _chg)		\
	do {					\
		MEMCOM_USED(_mc) += (_chg);	\
	} while (0)



#define MEMPOOL_TYPE_TO_LIST(_type) \
	(((_type) == MEMPOOL_BLOCK) ? Kernel->ke_fragpool_list : \
	 (((_type) == MEMPOOL_OBJECT) ? Kernel->ke_objpool_list : \
	  (((_type) == MEMPOOL_FRAG) ? Kernel->ke_fragpool_list : \
	   (((_type) == MEMPOOL_STACK) ? Kernel->ke_fragpool_list : NULL))))



typedef struct kernel
{
	MEMPLIST	*ke_fragpool_list;	
	MEMPLIST	*ke_objpool_list;	
	MEMPLIST	*ke_blkpool_list;
	MEMPOOL		*ke_mp_block;
	MEMPOOL 	*ke_mp_object;
	MEMPOOL		*ke_mp_frag;
	MEMPOOL 	*ke_mp_stack;
	void		*ke_tss_objpool;
        void            *ke_buf_objpool;
	SPINLOCK	ke_buf_spinlock;
	struct buf	*ke_buflru;		
	struct buf	**ke_bufhash;		
	struct buf	*ke_bufwash;		
} KERNEL;

# define	BUF_SPIN	(Kernel->ke_buf_spinlock)





#define TSS_MIN_ITEMS   32
#define TSS_MAX_ITEMS   256

#define BUF_MIN_ITEMS   32
#define BUF_MAX_ITEMS   256




#define MF_INS_FREEQUE(mp, mfp)						\
	do{								\
		INITQUE(((mfp)+1));					\
		((FLINK *)((mfp) + 1))->start_addr = (char *)(mfp);	\
		INSQUE(&((mp)->mp_free_frags), (FLINK *)((mfp)+1));	\
	}while(0);


#define MF_REM_FREEQUE(mfp)					\
	do{							\
		FLINK *tmplink;					\
		REMQUE(((mfp)+1), tmplink,  FLINK);		\
	}while(0);						


void *
mem_os_malloc(unsigned long size);

int
mem_init_alloc_regions();

MEMPOOL *
mp_frag_crt(size_t minsize, size_t maxsize);

void *
mp_frag_alloc(MEMPOOL *mp, size_t size, char *file, int line);

void *
mp_frag_realloc(MEMPOOL *mp, void * addr, size_t size);

int
mp_frag_free(MEMPOOL *mp, void *addr, char *file, int line);

int
mp_frag_destroy(MEMPOOL *mp);

void *
memallocheap(size_t size, char *file, int line);

void
*memreallocheap(void * addr, size_t size);

int
memfreeheap(void *addr, char *file, int line);

//# ifdef MEMMGR_TEST

#define MEMALLOCHEAP(size)		memallocheap(size, __FILE__, __LINE__)
#define MEMFREEHEAP(addr)		memfreeheap(addr, __FILE__, __LINE__)
#define MEMREALLOCHEAP(addr, size)	memreallocheap(addr, size)

//# else

//#define MEMALLOCHEAP(size)		malloc(size)
//#define MEMFREEHEAP(addr)		free(addr)
//#define MEMREALLOCHEAP(addr, size)	realloc(addr, size)

//# endif


# ifdef __cplusplus
}
# endif

#endif 

