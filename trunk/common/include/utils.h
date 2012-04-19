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

#ifndef UTILS_H
#define UTILS_H

#ifdef __cplusplus
extern "c" {
#endif

#include "list.h"


#define MIN(a, b) ((a) < (b))?(a):(b)
#define MAX(a, b) ((a) > (b))?(a):(b)

#ifdef DEBUG
#define Assert(cond) assert(cond)
#else
#define Assert(cond)
#endif


/* Following definition is for the method of read dir, it sames with the definition in the kfsfacer.cc. */
#define TABLE_READDIR_MAX_NUM		1024
#define TABLE_NAME_READDIR_MAX_LEN	128
typedef struct mt_entries
{
	int	ent_num;
	char	tabname[TABLE_READDIR_MAX_NUM][TABLE_NAME_READDIR_MAX_LEN];
	
}MT_ENTRIES;


char * 
trim(char *str, char deli);

int 
match(char* dest, char *src);

void
str1_to_str2(char *des, char split, char *src);

void
traceprint (char *fmt, ...);

void
backmove(register long *from, register long *to, register int length);

void
backmove1(register char *from, register char *to, register int length);

void
build_file_name(char	*filehdr, char *filename, int fileno);

#define BACKMOVE(s, d, l)	backmove((long *)(s), (long *)(d), l)

#define BACKMOVE1(s, d, l)	backmove1((s), (d), l)

unsigned long
hashstring(char *bytes, int nbytes, int seed);

int
m_atoi(char *strval, int strlen);


#ifdef __cplusplus
}
#endif

#endif /* UTILS_H_ */

