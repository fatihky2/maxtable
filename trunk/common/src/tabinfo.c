/*
** tabinfo.c 2011-08-20 xueyingfei
**
** Copyright flying/xueyingfei..
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

