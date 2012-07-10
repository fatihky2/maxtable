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

#include "tss.h"
#include "memcom.h"
#include "memobj.h"
#include "exception.h"
#include "utils.h"


TSS		*Tss;
int Trace;

extern	KERNEL	*Kernel;


TSS *
tss_alloc(void)
{
	register TSS	*tss;
	TSSOBJ          *tssobj;

	tssobj = (TSSOBJ *) mp_obj_alloc(Kernel->ke_tss_objpool);

	if (tssobj == NULL)
	{
	        return NULL;
	}

	tss = &(tssobj->to_tssp);

	tss->tssobjp = tssobj;

	tss_init(tss);

	return tss;
}

void
tss_init(TSS *tss)
{
	tss->texcptr = &tss->texcproc;

	
	tss->tstat = TSS_BEGIN_LOGGING;
	
#ifdef DEBUG
	DEBUG_SET(tss);
#endif

	if (tss->topid & TSS_OP_METASERVER)
	{
		tss->topid = TSS_OP_METASERVER;
	}
	else if (tss->topid & TSS_OP_RANGESERVER)
	{
		tss->topid = TSS_OP_RANGESERVER;
	}
	else if (tss->topid & TSS_OP_CLIENT)
	{
		tss->topid = TSS_OP_CLIENT;
	}

	tss->ttabinfo = tss->toldtabinfo = NULL;
	tss->tcmd_parser = NULL;
	tss->tmeta_hdr = NULL;
	tss->ttab_hdr = NULL;

	ex_init(tss);

	Tss = tss;
}


void
tss_release()
{
	int ret;

	ret = mp_obj_free(Kernel->ke_tss_objpool, (void *)Tss->tssobjp);
	Assert(ret == MEMPOOL_SUCCESS);

	return;
}

int
tss_setup(int opid)
{
	TSS	*new_tss;

	
	if ((new_tss = tss_alloc()) == NULL)
	{
		return FALSE;
	}

	new_tss->topid = opid;
	Tss = new_tss;

	return TRUE;
}
