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

void
hkgc_wash_sstab(int force);

void
hk_prt_bufinfo();



#endif
