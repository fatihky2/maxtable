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

#ifndef CACHE_H_
#define CACHE_H_

#define	MINHASHSIZE		128
#define	MAXHASHSIZE		4096

#define CACHE_CAL_HASHSIZE(bufnum, hashsize)		\
		(hashsize) = MINHASHSIZE;		\
		while ((bufnum) > (hashsize))		\
			(hashsize) <<= 1;		\
		(hashsize) <<= 1;


void
ca_init_buf(BUF *bp, BLOCK *blk, BUF *sstab);

void
ca_init_sstab(BUF *sstab, BLOCK *blk, int sstabid, int bp_cnt);

int
ca_setup_pool();

void
ca_prt_bp();

void
ca_init_blk(BLOCK *blk, int blkno, int sstabid, int bp_cnt);

void
ca_prt_cache();

void
ca_prt_dirty();

void
ca_prt_hash();



#endif
