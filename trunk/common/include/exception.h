/*
** exception.h 2011-04-11 xueyingfei
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


# define	EX_ANY		0


# ifdef __cplusplus
}
# endif


#endif

