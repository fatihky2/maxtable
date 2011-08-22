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


/**
 *     		The picture of memory in the region server.
 *
 *        MEMPLIST: the list of memory pool that includes Object Pool, Fragment Pool, 
 *		    Block Pool, Stack Pool.
 *        MEMPOOL:  only implement the fragment pool in the region server.
 *        MEMBLK: the basic unit of memory allocation in the memory pool mechanism.
 *        MEMFRAG:  tracks the address and size of each fragment.
 *
 *             |------------------|
 *             |     KERNEL       |
 *             |------------------|
 *     +---|     MEMPLIST     |
 *        |   |------------------|   +--->mp_link
 *     +-->|     MEMPOOL               |---+--->mp_frags
 *             |------------------|   +--->mp_freefrags--->|----------|
 *             |     MEMBLK                  |                        |  LINK    |
 *             |------------------|                        |----------|
 *             |     MEMFRAG                |                        |start_addr|
 *             |     the 1st                    |			   ------------
 *             |   memory fragment      |
 *             |------------------|
 *             |     MEMFRAG      |
 *             |     the 2nd      |
 *             |   memory fragment|
 *             |------------------|
 *             |       nth        |
 **/

/* Hold place to code for memory pool type */
#define MEMPOOL_BLOCK	1	/* block memory pool */
#define MEMPOOL_FRAG	2	/* fragment memory pool */
#define MEMPOOL_OBJECT	3	/* object memory pool */
#define MEMPOOL_STACK	4	/* stak memory pool */

#define	ALIGNDATATYPE	long

struct buf;


/* Define for the fragment memory pool */
typedef struct free_frag_link
{
	LINK f_link;	
	void * start_addr; 
} FLINK;

/* Head global list of mempools */
typedef struct memplist
{
	LINK 	mpl_link;	/* head the global mempool list */
	size_t	mpl_count;	/* # of mempool in use */
} MEMPLIST;


/* Memory Common Structure */
typedef struct memcom
{
	LINK		mc_link;	/* link to the global list. */
	int		mc_pooltype;	/* type of the pool */
	size_t		mc_total;	/* total size of the pool */
	size_t		mc_used;	/* currently used size of the pool */
	size_t		mc_maxsize;	/* max size of the pool */
	size_t		mc_minsize;	/* min size of the pool */
	size_t		mc_growsize;	/* grow size of the pool */
	char		mc_name[32];	/* pool name */
} MEMCOM;

/* Macros to get the field in the MEMCOM structure. */
#define MEMCOM_POOLNAME(_mc)	(((struct memcom *) (_mc))->mc_name)
#define MEMCOM_POOLTYPE(_mc)	(((struct memcom *) (_mc))->mc_pooltype)
#define MEMCOM_MINSIZE(_mc)	(((struct memcom *) (_mc))->mc_minsize)
#define MEMCOM_MAXSIZE(_mc)	(((struct memcom *) (_mc))->mc_maxsize)
#define MEMCOM_GROWSIZE(_mc)	(((struct memcom *) (_mc))->mc_growsize)
#define MEMCOM_TOTAL(_mc)	(((struct memcom *) (_mc))->mc_total)
#define MEMCOM_USED(_mc)	(((struct memcom *) (_mc))->mc_used)
#define MEMCOM_MAXITEMS(_mc)	MEMCOM_MAXSIZE(_mc)
#define MEMCOM_MINITEMS(_mc)	MEMCOM_MINSIZE(_mc)


/* Describes a memory pool. */
typedef struct mempool
{
	MEMCOM	mp_mc;		/* Must be the first field */
	LINK	mp_link;	/* list of blocks */
	LINK	mp_frags;	/* list of fragments */
	FLINK   mp_free_frags;  /* list of free fragments */
	size_t	mp_ovhd;	/* # of bytes wasted due headers */
	size_t	mp_nfrags;	/* # of fragments in the pool */
} MEMPOOL;

/* Memory block header */
typedef struct memblk
{
	LINK 	mb_link;	/* link all blocks in a pool */
	void 	*mb_begin;	/* start addr of this block. */
	void	*mb_end;	/* end addr of this block. */
	int	mb_frags;	/* # of frags in this block. */
} MEMBLK;

/* memory fragment header */
typedef struct memfrag
{
	LINK	mf_link;	/* the list of free frags or all frags */
	MEMBLK *mf_block;	/* ptr to the containing block */
	int	mf_size;	/* size of this fragment */
	short	mf_flags;	/* Free, Used */
	char	pad[2];
} MEMFRAG;

/* Return codes for the memmgr functions */
#define MEMPOOL_SUCCESS		0	/* success */
#define MEMPOOL_FAIL		-1	/* failed for unknow reason */
#define MEMPOOL_INVALIDADDR 	-2	/* passed in an invalid address */
#define MEMPOOL_BUSY		-3	/* the memory pool is busy */
#define MEMPOOL_MAXEXCEEDED 	-4	/* the maximum pool size is exceeded */
#define MEMPOOL_OBJECT2BIG	-5	/* object size is too big */


/* Values for memfrag.mf_flags. */
#define MEMPOOL_USED	0x1	/* this memfrag or mempool is used. */

/* Macros to check for used/free memfrags */
#define MF_FREE(mfp) (((mfp)->mf_flags & MEMPOOL_USED) == 0)
#define MF_USED(mfp) (((mfp)->mf_flags & MEMPOOL_USED) != 0)

/*
** If the memblk has just one fragment and if that fragment is free,
** then it is unused.
*/
#define MF_UNUSED_BLOCK(_mbp) (((_mbp)->mb_frags == 1) && \
	(MF_FREE((MEMFRAG *)((char *)(_mbp) + sizeof(MEMBLK)))))

#define MY_MEMPAGESIZE		4096L	/* bytes */
#define MY_KERNEL_MEM_SIZE	(32 * MY_MEMPAGESIZE + 1)

/* rounding size upto the minimum multiple of parameter "round" */
/* NOTE: the value of the round must be the power of 2. It's the root reason for some memory issue. */
#define ROUNDSIZE(size, round)	(((size) + ((round) - 1)) & ~((round) - 1))

/* Frag Pool */
#define MAXSIZE_FRAGPOOL	MY_MEMPAGESIZE * 1024
#define MAX_FRAG_SIZE		(65536 * sizeof(MEMFRAG))
#define MAX_FRAG_ALLOCSIZE (MAX_FRAG_SIZE - sizeof(MEMFRAG) - sizeof(MEMBLK))

/* Block Pool */
#define MAXSIZE_BLKPOOL		MY_MEMPAGESIZE * 1024


#define B2P(size)	((size) % MY_MEMPAGESIZE			\
				? ((size) / MY_MEMPAGESIZE + 1)	\
				: ((size) / MY_MEMPAGESIZE)) 


#define MF_GET_MFP(link)	(MEMFRAG *)((FLINK *)(link))->start_addr

/* Returns the size needed for a given growsize with overheads */
#define  MEMPOOL_FRAG_GROWSIZE(_growsize)				\
	ROUNDSIZE((ROUNDSIZE((_growsize), sizeof(MEMFRAG)) +		\
		(sizeof(MEMBLK) + sizeof(MEMFRAG))), MY_MEMPAGESIZE)

/* Update the used counter in a memory pool. */
#define MEMCOM_UPDATE_USED(_mc, _chg)		\
	do {					\
		MEMCOM_USED(_mc) += (_chg);	\
	} while (0)


/* Get the pool list by the given type. */
#define MEMPOOL_TYPE_TO_LIST(_type) \
	(((_type) == MEMPOOL_BLOCK) ? Kernel->ke_fragpool_list : \
	 (((_type) == MEMPOOL_OBJECT) ? Kernel->ke_objpool_list : \
	  (((_type) == MEMPOOL_FRAG) ? Kernel->ke_fragpool_list : \
	   (((_type) == MEMPOOL_STACK) ? Kernel->ke_fragpool_list : NULL))))


/* 
** Manage the global resource. Only implement the Fragment Pool 
** in the current region server. 
*/
typedef struct kernel
{
	MEMPLIST	*ke_fragpool_list;	/* Fragment pool list */
	MEMPLIST	*ke_objpool_list;	/* Object mempool list */
	MEMPLIST	*ke_blkpool_list;
	MEMPOOL		*ke_mp_block;
	MEMPOOL 	*ke_mp_object;
	MEMPOOL		*ke_mp_frag;
	MEMPOOL 	*ke_mp_stack;
	void		*ke_tss_objpool;
        void            *ke_buf_objpool;
	SPINLOCK	ke_buf_spinlock;
	struct buf	*ke_buflru;		/* Buffer LRU/MRU list */
	struct buf	**ke_bufhash;		/* Buffer hash list that contains the 
						** valid buffer but not used by any
						** process.
						*/
} KERNEL;

# define	BUF_SPIN	(Kernel->ke_buf_spinlock)



/* Object Pool Section*/

#define TSS_MIN_ITEMS   32
#define TSS_MAX_ITEMS   256

#define BUF_MIN_ITEMS   32
#define BUF_MAX_ITEMS   256



/* Insert into the head of free queue at maintained at the Memory pool */
#define MF_INS_FREEQUE(mp, mfp)						\
	do{								\
		INITQUE(((mfp)+1));					\
		((FLINK *)((mfp) + 1))->start_addr = (char *)(mfp);	\
		INSQUE(&((mp)->mp_free_frags), (FLINK *)((mfp)+1));	\
	}while(0);

/* Remove the fragment entry from the free queue */
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

#endif /* MEMCOM_H_ */

