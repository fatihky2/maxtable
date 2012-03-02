/*
** b_search.h 2010-10-08 xueyingfei
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
	(srchinfo)->bhigh	= high;				\
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
