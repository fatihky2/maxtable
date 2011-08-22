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

/* number of EXCEPT structs for backout on process stack */
#define EX_BACKOUT_STACKSIZE    32

typedef int (* EXC_FUNC_PTR)(int );

typedef struct mt_except
{
	int             exc_number;     /* exception number */
	EXC_FUNC_PTR    exc_func;       /* handler to call upon exception */
	jmp_buf         exc_buf;        /* saved state for returning control to
                                        			** point following ex_handle call */
} EXCEPT;

typedef struct exc_manage
{
	EXCEPT  *ex_start;      /* set to bottom of stack */
	EXCEPT  *ex_top;        /* current top of stack */
	EXCEPT  *ex_end;        /* maximum top of stack */
} EXC_MANAGE;

/* Exception stack */
typedef struct exc_proc
{
	EXC_MANAGE      exp_manage;     /* ex stack bounds for this process */
	EXCEPT          exp_stack[EX_BACKOUT_STACKSIZE];     /* ex stack for this process */
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

