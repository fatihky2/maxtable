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

#ifndef STRINGS_H_
#define STRINGS_H_H


#include <string.h> 
#define MEMSET(target,size)     memset((void *)(target), 0, (size_t)(size)) 
#define MEMCPY(dest,src,size)	memcpy((void *)(dest), (const void *)(src), (size_t)(size)) 
#define STRLEN(s)               strlen(s)


int
str1nstr (char *buf, const char *sub, int len);

int
str01str (char *buf, const char *sub, int len);

int
str0n_trunc_0t(char *buf, int len, int *star, int *end);

int
strmnstr (char *buf, char *sub, int len);


#endif
