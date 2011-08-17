/*
** strings.h 2010-12-06 xueyingfei
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
