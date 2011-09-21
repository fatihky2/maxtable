/*
** hkgc.c 2011-03-17 xueyingfei
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
#include <pthread.h>
#include "master/metaserver.h"
#include "utils.h"
#include "buffer.h"
#include "hkgc.h"
#include "spinlock.h"
#include "tss.h"
#include "file_op.h"


#define	HKGC_WORK_INTERVAL	10


typedef struct hkgc_info
{
	int		hk_stat;
	int		buf_num;
	BUF		*hk_dirty_buf;
	SPINLOCK	hk_sstabmap_mutex;	/* mutex for the sstabmap */
	SIGNAL		hk_sstabmap_cond;
	TAB_SSTAB_MAP	*hk_sstabmap;		/* sstabmap: just one item. */
	
} HKGC_INFO;


#define	HKGC_SSTAB_MAP_DIRTY	0x0001	/* Trigger for the sstab map writing. */
#define HKGC_SSTAB_BUF_DIRTY	0x0002	/* This feature if it need to */


void
hkgc_write_block()
{
	return;
}

void
hkgc_grab_dirty_resource()
{
	;
}

void
hkgc_get_sstabmap(TAB_SSTAB_MAP *sstabmap)
{
	HKGC_INFO	*hkgc = NULL;


//	hkgc = &(Kernel->hkgc_info);

	
	P_SPINLOCK(hkgc->hk_sstabmap_mutex);

	if (!(hkgc->hk_stat & HKGC_SSTAB_MAP_DIRTY))
	{
		
		hkgc->hk_sstabmap= sstabmap;
		hkgc->hk_stat |= HKGC_SSTAB_MAP_DIRTY;
	}
	else
	{
		Assert(hkgc->hk_sstabmap == sstabmap);
	}
	
	V_SPINLOCK(hkgc->hk_sstabmap_mutex);
}

int
hkgc_flush_sstabmap(TAB_SSTAB_MAP *sstabmap)
{	
	int		fd;
	HKGC_INFO	*hkgc = NULL;


//	hkgc = &(Kernel->hkgc_info);


	if (sstabmap->stat & SSTABMAP_CHG)
	{
		P_SPINLOCK(hkgc->hk_sstabmap_mutex);

		hkgc->hk_stat &= ~HKGC_SSTAB_MAP_DIRTY;

		V_SPINLOCK(hkgc->hk_sstabmap_mutex);

		OPEN(fd, sstabmap->sstabmap_path, (O_RDWR));

		if (fd < 0)
		{
			return FALSE;
		}
		
		WRITE(fd, sstabmap->sstab_map, SSTAB_MAP_SIZE);

		CLOSE(fd);
	}

	return TRUE;
}


void
hkgc_init(int opid)
{
	HKGC_INFO	*hkgc = NULL;


//	hkgc = &(Kernel->hkgc_info);
	
	switch (opid)
	{
	    case TSS_OP_METASERVER:

//		hkgc->hk_sstabmap_mutex = PTHREAD_MUTEX_INITIALIZER;
//		hkgc->hk_sstabmap_cond = PTHREAD_COND_INITIALIZER;

		hkgc->hk_sstabmap = NULL;
		

		break;

	    case TSS_OP_RANGESERVER:
		
		break;

	    default:
		break;		
	}

}

void
hkgc_boot(void *opid)
{
	while(TRUE)
	{
		switch (*(int *)opid)
		{
		    case TSS_OP_METASERVER:
		    	
		    	sleep(HKGC_WORK_INTERVAL);
		    	break;
			
		    case TSS_OP_RANGESERVER:
		    	sleep(HKGC_WORK_INTERVAL);
			hkgc_write_block();
		    	break;
			
		    default:
		    	break;	
		}
	}

	return;
}


