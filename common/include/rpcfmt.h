/*
** rpcfmt.h 2012-03-22 xueyingfei
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


#ifndef	RPCFMT_H_
#define RPCFMT_H_


#ifndef	BLOCKSIZE
#define	BLOCKSIZE		(64 * 1024)
//#define	BLOCKSIZE		512
#endif

/* The context of range query. */
typedef struct range_query_contex
{
	int	status;			/* Status for the range query. */
	int	first_rowpos;		/* The index of first row in the current 
					** query context. 
					*/
	int	end_rowpos;		/* The index of last row in the current 
					** query context. 
					*/
	int	cur_rowpos;		/* The index of current row in the 
					** current  query context. 
					*/
	int	rowminlen;		/* The min-length of row. */
	char	data[BLOCKSIZE];	/* The data context from ranger server. */
}RANGE_QUERYCTX;


#define	DATA_CONT	0x0001		/* There're still some data in the ranger
					** and The ranger is waitting for the 
					** response of data sending.
					*/
#define DATA_DONE	0x0002		/* No data need to be read from the 
					** ranger.
					*/
#define DATA_EMPTY	0x0004		/* There're no data in the current query
					** context. 
					*/

#endif
