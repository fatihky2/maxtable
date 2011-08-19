/*
** cache.h 2010-11-25 xueyingfei
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


#endif
