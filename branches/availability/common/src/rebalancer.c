/*
** rebalancer.c 2011-09-01 xueyingfei
**
** Copyright Transoft Corp.
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
#include "master/metaserver.h"
#include "memcom.h"
#include "buffer.h"
#include "block.h"
#include "cache.h"
#include "tss.h"

extern TSS		*Tss;

RANGE_PROF *
rebalan_get_rg_prof_by_addr(char *rg_addr, int rg_port)
{
	LOCALTSS(tss);
	RANGE_PROF	*rg_prof;
	SVR_IDX_FILE	*rglist;
	int		found;
	int		i;

	rglist = &(tss->tmaster_infor->rg_list);
	found = FALSE;
	rg_prof = (RANGE_PROF *)(rglist->data);

	for(i = 0; i < rglist->nextrno; i++)
	{
		if (!strncasecmp(rg_addr, rg_prof[i].rg_addr, RANGE_ADDR_MAX_LEN)
			&& (rg_prof[i].rg_port == rg_port)
		)
		{
			found = TRUE;
			break;
		}
	}

	return (found ? &(rg_prof[i]) : NULL);
}

