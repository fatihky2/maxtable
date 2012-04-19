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
#include "strings.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "cache.h"
#include "memcom.h"
#include "utils.h"
#include "row.h"
#include "tablet.h"
#include "tss.h"
#include "type.h"
#include "session.h"


extern	TSS	*Tss;

void
tabinfo_push(TABINFO *tabinfo)
{
	LOCALTSS(tss);
	TABINFO	*tmptab;

	tmptab = tss->toldtabinfo;
	tss->toldtabinfo = tabinfo;
	tabinfo->t_nexttab = tmptab;

	tss->ttabinfo = tabinfo;	
}

void
tabinfo_pop()
{
	LOCALTSS(tss);
	TABINFO	*tmptab;

	tmptab = tss->toldtabinfo->t_nexttab;
	tss->toldtabinfo = tmptab;

	tss->ttabinfo = tmptab;
}

