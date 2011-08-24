/*
** sstab.h 2011-07-25 xueyingfei
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
#ifndef	SSTAB_H_
#define SSTAB_H_


void
sstab_namebyname(char *old_sstab, char *new_sstab);

void
sstab_split(TABINFO *srctabinfo, BUF *srcbp, char *rp);

void
sstab_namebyid(TABINFO *tabinfo, char *new_sstab);

int *
sstab_map_get(int tabid, char *tab_dir, TAB_SSTAB_MAP **tab_sstab_map);

void
sstab_map_release(int tabid, int flag, TAB_SSTAB_MAP *tab_sstab_map);

int
sstab_map_put(int tabid, TAB_SSTAB_MAP *tab_sstab_map);


#endif
