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

// exception_handler.cpp : Defines the entry point for the console application.
//

#include "utils.h"
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
	traceprint("We are handling the exception\n");
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


