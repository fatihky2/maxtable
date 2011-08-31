/*
** utils.h 2010-11-02 xueyingfei
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


#ifndef UTILS_H
#define UTILS_H

#ifdef __cplusplus
extern "c" {
#endif

#include "list.h"


#define MIN(a, b) ((a) < (b))?(a):(b)
#define MAX(a, b) ((a) > (b))?(a):(b)


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

#define BACKMOVE1(s, d, l)	backmove((long *)(s), (long *)(d), l)

#define BACKMOVE(s, d, l)	backmove1((s), (d), l)

unsigned long
hashstring(char *bytes, int nbytes, int seed);

int
m_atoi(char *strval, int strlen);


#ifdef __cplusplus
}
#endif

#endif /* UTILS_H_ */

