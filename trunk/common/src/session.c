/*
** session.c 2011-08-01 xueyingfei
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


extern KERNEL *Kernel;

int
session_close(TABINFO *tabinfo)
{
	BUF	*bp;
	BUF	*tmp_bp;

	bp = tabinfo->t_dnew;

	printf("Enter into close table\n");

	for (bp = tabinfo->t_dnew; bp != (BUF *)tabinfo;)
	{
		bufwrite(bp);

		tmp_bp= bp;

		bp = bp->bdnew;
		
		DIRTYUNLINK(tmp_bp);

		bufunkeep(tmp_bp);
		
		//LRUUNLINK(tmp_bp);

		//LRULINK(tmp_bp, Kernel->ke_buflru);
	}

	return TRUE;
}

