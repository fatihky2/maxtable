/*
** token.h 2010-12-17 xueyingfei
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
#define DROP		8
#define REMOVE		9	/* Remove the whole table file in the metadata server. */
#define REBALANCE	10
#define SELECTRANGE	11
#define	MCCTABLE	12
#define SELECTWHERE	13
#define MCCRANGER	14
#define SHARDING	15
	

#define MAXSI_FIXED_TOKENS	15

int
token_validate(char *token);

# ifdef __cplusplus
}
# endif


#endif

