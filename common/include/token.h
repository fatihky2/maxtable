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

#ifndef TOKEN_H_
#define TOKEN_H_

# ifdef __cplusplus
extern "C" {
# endif


#define TOKEN_MAX_NUM	32
#define TOKEN_NAME_MAX_LEN  64

typedef struct tokens
{
	int     toknum;
	char    *tokstring;
} TOKENS;

#define INVALID_TOK	0
#define TABCREAT	1
#define INSERT		2
#define CRTINDEX	3
#define SELECT		4
#define DELETE		5
#define ADDSERVER	6
#define ADDSSTAB	7	/* Add sstable row into tabletN. */
#define DROPTAB		8
#define REMOVETAB	9	/* Remove the whole table file in the metadata server. */
#define REBALANCE	10
#define SELECTRANGE	11
#define	MCCTABLE	12
#define SELECTWHERE	13
#define MCCRANGER	14
#define SHARDING	15
#define	SELECTCOUNT	16
#define SELECTSUM	17
#define	IDXROOTSPLIT	18
#define	DROPINDEX	19
#define	REMOVEINDEX	20
	

#define MAXSI_FIXED_TOKENS	20

int
token_validate(char *token);

# ifdef __cplusplus
}
# endif


#endif

