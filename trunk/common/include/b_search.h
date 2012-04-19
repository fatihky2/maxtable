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

#ifndef B_SEARCH_H_
#define B_SEARCH_H_



typedef struct b_srchinfo
{
	char	        *brow;				
	int	        brownum;	
	int 	        blow;		
	int	        bhigh;		
	int   	        bcomp;		
	int		btotrows;	 
	int		boffset;	
	int		bcoltype;	
	int		bcoloffset;	

} B_SRCHINFO;


#define	SRCHINFO_INIT(srchinfo, low, high, total, result)	\
do								\
{								\
	(srchinfo)->brow = NULL;				\
	(srchinfo)->brownum = (low + high)>>1;			\
	(srchinfo)->blow = low;					\
	(srchinfo)->bhigh = high;				\
	(srchinfo)->bcomp = result;				\
	(srchinfo)->btotrows = total;				\
	(srchinfo)->boffset = 0;				\
}								\
while(0)



#define BSRCH_MOVE_LEFT(blkptr, srchinfo)			\
do                                                             	\
{                                                              	\
	srchinfo->blow = srchinfo->brownum + 1;      		\
	srchinfo->brownum = (srchinfo->blow + 			\
					srchinfo->bhigh) >> 1;	\
	srchinfo->brow = ROW_GETPTR_FROM_OFFTAB(blkptr,		\
					srchinfo->brownum);	\
} while (0)

#define BSRCH_MOVE_RIGHT(blkptr, srchinfo)      		\
do                                                             	\
{                                                               \
	srchinfo->bhigh = srchinfo->brownum - 1;        	\
	srchinfo->brownum = (srchinfo->blow + 			\
					srchinfo->bhigh) >> 1;	\
	srchinfo->brow = ROW_GETPTR_FROM_OFFTAB(blkptr,		\
					srchinfo->brownum);	\
} while (0)

void
b_srch_block(TABINFO *tabinfo, BUF *bp, B_SRCHINFO *srchinfo);


#endif
