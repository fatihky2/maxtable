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

struct buf;


#define	HKGC_WORK_INTERVAL	20
#define	HK_BATCHSIZE		16

typedef struct hkgc_info
{
	int		hk_stat;
	int		buf_num;
	struct buf	*hk_dirty_buf[HK_BATCHSIZE];
	
	SPINLOCK	hk_sstabmap_mutex;	/* mutex for the sstabmap */
	SIGNAL		hk_sstabmap_cond;
	TAB_SSTAB_MAP	*hk_sstabmap;		/* sstabmap: just one item. */
	
} HKGC_INFO;


#define	HKGC_SSTAB_MAP_DIRTY	0x0001	/* Trigger for the sstab map writing. */
#define HKGC_SSTAB_BUF_DIRTY	0x0002	/* This feature if it need to */


void *
hkgc_boot(void *opid);

#endif
