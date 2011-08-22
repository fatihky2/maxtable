/*
** tssmgr.c 2010-12-10 xueyingfei
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


#include "tss.h"
#include "memcom.h"
#include "memobj.h"
#include "exception.h"


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
	tss->sstat = 0;
	tss->ttabinfo = tss->toldtabinfo = NULL;
	tss->tcmd_parser = NULL;
	tss->tmeta_hdr = NULL;

	ex_init(tss);

	Tss = tss;
}


void
tss_release()
{
	int ret;

	ret = mp_obj_free(Kernel->ke_tss_objpool, (void *)Tss->tssobjp);
	assert(ret == MEMPOOL_SUCCESS);

	return;
}

int
tss_setup(int opid)
{
	TSS	*new_tss;

	/* allocate a TSS for it */
	if ((new_tss = tss_alloc()) == NULL)
	{
		return FALSE;
	}

	new_tss->topid = opid;
	Tss = new_tss;

	return TRUE;
}
