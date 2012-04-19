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
#include "tabinfo.h"
#include "utils.h"
#include "memcom.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "cache.h"
#include "tss.h"


extern	TSS	*Tss;
extern KERNEL *Kernel;

int
session_close(TABINFO *tabinfo)
{
	LOCALTSS(tss);
	BUF	*bp;
	BUF	*tmp_bp;

	bp = tabinfo->t_dnew;

	if (DEBUG_TEST(tss))
	{
		traceprint("Enter into close table\n");
	}

	if ((tabinfo->t_stat & TAB_RESERV_BUF)&& (tabinfo->t_resbuf))
	{
		tabinfo->t_resbuf->bstat &= ~BUF_RESERVED;
	}

	for (bp = tabinfo->t_dnew; bp != (BUF *)tabinfo;)
	{
		Assert(bp->bstat & BUF_DIRTY);

		if (!(bp->bstat & BUF_DIRTY))
		{
			traceprint("Buffer should be DIRTY!\n");
		}
		
		bufwrite(bp);

		tmp_bp= bp;

		bp = bp->bdnew;

		
		DIRTYUNLINK(tmp_bp);

		tmp_bp->bstat &= ~BUF_DIRTY;

		bufunkeep(tmp_bp);
		
		//LRUUNLINK(tmp_bp);

		//LRULINK(tmp_bp, Kernel->ke_buflru);
	}

	return TRUE;
}

