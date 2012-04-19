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

#ifndef METADATA_H_
#define METADATA_H_


struct col_info;
struct buf;

struct col_info *
meta_get_colinfor(int colid, char *col_name, int totcol, struct col_info *colinfor);

char *
meta_get_coldata(struct buf *bp, int rowoffset, int coloffset);

int
meta_save_sysobj(char *tab_dir, char *tab_hdr);

int
meta_save_sysindex(char *sysindex);

int
meta_load_sysindex(char *sysindex);

#endif

