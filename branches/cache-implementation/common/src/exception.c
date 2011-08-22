/*
** exception.c 2011-04-11 xueyingfei
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

// exception_handler.cpp : Defines the entry point for the console application.
//

#include "exception.h" 
#include "tss.h"

extern	TSS	*Tss;
EXC_PROC mt_except_yxue;

void
ex_init(TSS * new_tss)
{
	EXC_MANAGE	*exp1;
 
	exp1 = &(new_tss->texcproc.exp_manage);
	exp1->ex_start = new_tss->texcproc.exp_stack;

	exp1->ex_end = new_tss->texcproc.exp_stack + EX_BACKOUT_STACKSIZE;
	exp1->ex_top = new_tss->texcproc.exp_stack - 1;
}

int
ex_handle(int exce_num, EXC_FUNC_PTR handler)
{
	LOCALTSS(tss);
	int 		setjmp_ret;
	EXC_MANAGE	*exp1;
			
	setjmp_ret = -1;
	exp1 = &(tss->texcproc.exp_manage);

	if ((exp1->ex_top + 1) < (tss->texcproc.exp_stack + EX_BACKOUT_STACKSIZE))
	{
		(exp1->ex_top)++;
		exp1->ex_top->exc_number = exce_num;
		exp1->ex_top->exc_func = handler;
		setjmp_ret = setjmp(exp1->ex_top->exc_buf);
	}

	return setjmp_ret;
}

int 
yxue_handler(int exce_num)
{
	printf("We are handling the exception\n");
	return EX_ANY;
}

static int
ex_backout_cleanup(EXCEPT *ex_elem)
{
	LOCALTSS(tss);
	EXC_MANAGE	*exp1;

	exp1 = &(tss->texcproc.exp_manage);
	exp1->ex_top = ex_elem;

	longjmp(ex_elem->exc_buf,1);
}

int
ex_raise(int exce_num)
{
	LOCALTSS(tss);
	register int 		ex_num;
	register EXCEPT		*ex_elem;
	register EXC_MANAGE	*exp1;

	exp1 = &(tss->texcproc.exp_manage);

	
	for(ex_elem = exp1->ex_top; ex_elem >= exp1->ex_start; ex_elem--)
	{
		ex_num = ex_elem->exc_number;

		
		if(ex_num == exce_num)
		{
			switch ((int)(*ex_elem->exc_func)(exce_num))
			{
			    case EX_ANY:
				ex_backout_cleanup(ex_elem);
				break;
			    default:
				break;
			}
		}
	}

	return 0;
}

void
ex_delete()
{
	Tss->texcproc.exp_manage.ex_top--;
}


