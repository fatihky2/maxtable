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
#include "memcom.h"
#include <pthread.h>
#include "master/metaserver.h"
#include "utils.h"
#include "buffer.h"
#include "hkgc.h"
#include "spinlock.h"
#include "tss.h"
#include "file_op.h"
#include "log.h"


extern KERNEL *Kernel;


void
hkgc_wash_sstab(int force)
{
	int		i;
	BUF		*sstab;
	HKGC_INFO	*hk_info;
	LOGREC		logrec;


	hk_info = (HKGC_INFO *)(Kernel->hk_info);

	if (!force)
	{
		P_SPINLOCK(BUF_SPIN);	
	}
	
	for (i = 0; i < HK_BATCHSIZE; i++)
	{
		sstab = Kernel->ke_bufwash->bdnew;

		if (sstab == Kernel->ke_bufwash)
		{
			break;
		}

		(hk_info->buf_num)++;

		hk_info->hk_dirty_buf[i] = sstab;

		sstab->bstat |= (BUF_IN_HKWASH | BUF_WRITING);

		DIRTYUNLINK(sstab);
	}

	if (hk_info->buf_num)
	{
		log_build(&logrec, CHECKPOINT_BEGIN, 0, 0, NULL, NULL, 0, 0, 0);
		
		log_insert_insdel(&logrec, NULL, 0);
	}

	if (!force)
	{
		V_SPINLOCK(BUF_SPIN);
	}
	
	BUF	*bp;
	for (i = 0; i < hk_info->buf_num; i++)
	{
		bp = hk_info->hk_dirty_buf[i];
		
		bufawrite(bp);

		hk_info->hk_dirty_buf[i] = NULL;

//		bufunkeep(bp);
	}

	if (hk_info->buf_num)
	{
		if (!force)
		{
			P_SPINLOCK(BUF_SPIN);
		}
		
		log_build(&logrec, CHECKPOINT_COMMIT, 0, 0, NULL, NULL, 0, 0, 0);
		
		log_insert_insdel(&logrec, NULL, 0);

		if (!force)
		{
			V_SPINLOCK(BUF_SPIN);
		}
	}
	
	hk_info->buf_num = 0;

	return;
}

void
hkgc_wash()
{
	while(TRUE)
	{
		sleep(HKGC_WORK_INTERVAL);
		hkgc_wash_sstab(FALSE);
	}
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

void *
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
		    	hkgc_wash();
		    	break;
			
		    default:
		    	break;	
		}
	}

	return NULL;
}


