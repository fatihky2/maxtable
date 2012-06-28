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

#ifndef EXCEPTION_H_
#define EXCEPTION_H_

# ifdef __cplusplus
extern "C" {
# endif


#include <stdlib.h>
#include <stdio.h>
#include <setjmp.h>



#define EX_BACKOUT_STACKSIZE    32

typedef int (* EXC_FUNC_PTR)(int );

typedef struct mt_except
{
	int             exc_number;	   
	EXC_FUNC_PTR    exc_func;       
	jmp_buf         exc_buf;	
} EXCEPT;

typedef struct exc_manage
{
	EXCEPT  *ex_start;      	
	EXCEPT  *ex_top;        	
	EXCEPT  *ex_end;        	
} EXC_MANAGE;


typedef struct exc_proc
{
	EXC_MANAGE      exp_manage;     
	EXCEPT          exp_stack[EX_BACKOUT_STACKSIZE];    
} EXC_PROC;

struct tss;

void
ex_init(struct tss * new_tss);

int
ex_handle(int exce_num, EXC_FUNC_PTR handler);

int
ex_raise(int exce_num);

void
ex_delete();

int 
yxue_handler(int exce_num);


#define	EX_ANY		0
#define	EX_BUFERR	1 
#define	EX_TABLETERR	2
#define	EX_SSTABERR	3


# ifdef __cplusplus
}
# endif


#endif

