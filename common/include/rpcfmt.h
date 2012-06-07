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

#ifndef	RPCFMT_H_
#define RPCFMT_H_


#ifndef	BLOCKSIZE
#define	BLOCKSIZE		(1024 * 1024)
//#define	BLOCKSIZE		(2 * 1024)
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
