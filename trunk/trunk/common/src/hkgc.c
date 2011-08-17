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
#include "buffer.h"


#define	HKGC_WORK_INTERVAL	600


typedef struct hkgc_que
{
	int	buf_num;
	BUF	*hk_dirty_buf;
} HKGC_QUE;

void
hkgc_write_block()
{
	return;
}

void
hkgc_boot()
{
	while(TRUE)
	{
		sleep(HKGC_WORK_INTERVAL);
		hkgc_write_block();
	}

	return;
}


