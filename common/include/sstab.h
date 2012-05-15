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

#ifndef	SSTAB_H_
#define SSTAB_H_


void
sstab_namebyname(char *old_sstab, char *new_sstab);

void
sstab_split(TABINFO *srctabinfo, BUF *srcbp, char *rp, int data_insert_needed);

void
sstab_namebyid(char *old_sstab, char *new_sstab, int new_sstab_id);

SSTAB_INFOR *
sstab_map_get(int tabid, char *tab_dir, TAB_SSTAB_MAP **tab_sstab_map);

void
sstab_map_release(int tabid, int flag, TAB_SSTAB_MAP *tab_sstab_map);

int
sstab_map_put(int tabid, TAB_SSTAB_MAP *tab_sstab_map);

int
sstab_bld_name(char *sstab_name, char *tab_name, int tab_name_len, 
				int sstabid);

int
sstab_shuffle(BLOCK *sstab);


#endif
