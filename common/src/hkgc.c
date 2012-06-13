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

#include "global.h"
#include "memcom.h"
#include <pthread.h>
#include "master/metaserver.h"
#include "utils.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "cache.h"
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
	BUF		*washlink;
	HKGC_INFO	*hk_info;
	LOGREC		logrec;


	// ca_chk_cache();
	
	if (!force)
	{
		P_SPINLOCK(BUF_SPIN);	
	}

	P_SPINLOCK(HKGC_SPIN);
	
	hk_info = (HKGC_INFO *)(Kernel->hk_info);

	washlink = Kernel->ke_bufwash->bdnew;

	for (i = 0; i < HK_BATCHSIZE; i++)
	{
		sstab = washlink;

		if (sstab == Kernel->ke_bufwash)
		{
			break;
		}

		if (sstab->bkeep == 0)
		{
			(hk_info->buf_num)++;

			hk_info->hk_dirty_buf[i] = sstab;

			sstab->bstat |= (BUF_IN_HKWASH | BUF_WRITING);

			washlink = sstab->bdnew;
			
			DIRTYUNLINK(sstab);
		}
		else
		{
			Assert(sstab->bkeep);

			traceprint("HK Flush(%d) - HK index: %d - Buf(0x%x) still has been kept.\n", force, i, (char *)sstab);

			i--;

			washlink = sstab->bdnew;
		}
	}

	if (hk_info->buf_num)
	{
		log_build(&logrec, CHECKPOINT_BEGIN, 0, 0, NULL, NULL,
					0, 0, 0, 0, 0, NULL, NULL);
		
		log_put(&logrec, NULL, 0);
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

		bp->bstat &= ~BUF_IN_HKWASH;

//		bufunkeep(bp);
	}

	if (hk_info->buf_num)
	{
		log_build(&logrec, CHECKPOINT_COMMIT, 0, 0, NULL, NULL,
					0, 0, 0, 0, 0, NULL, NULL);
		
		log_put(&logrec, NULL, 0);
	}
	
	hk_info->buf_num = 0;

	V_SPINLOCK(HKGC_SPIN);

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

void
hk_prt_bufinfo()
{
	int		i;
	HKGC_INFO	*hk_info;

	
	hk_info = (HKGC_INFO *)(Kernel->hk_info);

	for (i = 0; i < hk_info->buf_num; i++)
	{
		traceprint("hk_info->hk_dirty_buf[%d] = 0x%x", i, (char *)(hk_info->hk_dirty_buf[i]));
	}

}

