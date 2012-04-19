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

#include "master/metaserver.h"
#include "heartbeat.h"
#include "masterinfo.h"
#include "memcom.h"
#include "buffer.h"
#include "rpcfmt.h"
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
		if (   !strncasecmp(rg_addr, rg_prof[i].rg_addr, RANGE_ADDR_MAX_LEN)
		    && (rg_prof[i].rg_port == rg_port)
		)
		{
			found = TRUE;
			break;
		}
	}

	return (found ? &(rg_prof[i]) : NULL);
}

