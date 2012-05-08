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

#include "global.h"
#include "utils.h"
#include "token.h"

TOKENS  MT_Tokens[] =
{
	{TABCREAT,	"create table"},
	{INSERT,	"insert into"},
	{CRTINDEX,	"create index"},
	{SELECT,	"select"},
	{DELETE,	"delete"},
	{ADDSERVER,	"add server"},
	{ADDSSTAB,	"addsstab into"},
	{DROPTAB,	"drop table"},
	{REMOVETAB,	"remove table"},
	{REBALANCE,	"rebalance"},
	{SELECTRANGE,	"selectrange"},
	{MCCTABLE,	"mcc checktable"},
	{SELECTWHERE,	"selectwhere"},
	{MCCRANGER,	"mcc checkranger"},
	{SHARDING,	"sharding"},
	{SELECTCOUNT,	"selectcount"},
	{SELECTSUM,	"selectsum"},
	{DROPINDEX,	"drop index"},
	{REMOVEINDEX,	"remove index"},
	{DELETEWHERE,	"deletewhere"},
	{UPDATE,	"update set"}
};

int
token_validate(char *token)
{
	int i;
	int tok_num;

	tok_num = 0;

	for (i = 0;  i < MAXSI_FIXED_TOKENS; i++)
	{
		if (!strcmp(token, MT_Tokens[i].tokstring))
		{
			tok_num = MT_Tokens[i].toknum;
			break;
		}
	}

	return tok_num;
}


