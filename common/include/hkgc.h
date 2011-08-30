/*
** hkgc.h 2011-03-17 xueyingfei
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
#ifndef	HKGC_H_
#define HKGC_H_

#include "buffer.h"

typedef struct _io_data
{
	//int	buf_num;
	BUF	*hk_dirty_buf;
	struct _io_data *next;
} io_data;


extern io_data * io_list_head;
extern io_data * io_list_tail;

extern pthread_mutex_t io_mutex;
extern pthread_mutex_t bufkeep_mutex;
extern pthread_mutex_t io_list_mutex;


typedef struct hkgc_info
{
	int	stat;
}HKGC_INFO;

#define	HKGC_SSTAB_MAP_DIRTY	0x0001	/* Trigger for the sstab map writing. */
#define HKGC_SSTAB_BUF_DIRTY	0x0002	/* This feature if it need to */

void * hkgc_boot(void *args);

void put_io_list(BUF * buffer);

#endif
