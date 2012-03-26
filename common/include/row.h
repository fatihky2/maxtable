/*
** row.h 2010-11-21 xueyingfei
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


#ifndef	ROW_H_
#define ROW_H_


typedef struct rowfmt
{
	int		rowno;  	
	int		status;		
	int		vattcnt;	
}ROWFMT;


#define	ROW_DELETED	0x0001			



#define	ROW_SET_STATUS(rp, flag)	((((ROWFMT *)(rp))->status) |= flag)
#define	ROW_GET_STATUS(rp)	((int) ((ROWFMT *)(rp))->status)

#define ROW_IS_DELETED(rp)	(ROW_GET_STATUS(rp) & ROW_DELETED)


#define GETINT(p)	(*(int *) (p))

#define COLOFFSETENTRYSIZE	4	
#define	ROWVATTCNT_OFF		8	

#define	VARCOL_MAX_NUM		16
#define	COL_OFFTAB_MAX_SIZE	(VARCOL_MAX_NUM * sizeof(int))

#define ROW_GET_MINLEN()	sizeof(ROWFMT)


#define ROW_GET_VARCNT(rp)	GETINT((rp) + ROWVATTCNT_OFF)

#define ROW_GET_ROWNO(rp)       GETINT(rp)


#define ROW_GET_END(rp, minlen)				\
	((rp) + GETINT((rp) + (minlen)) - 1)

#define ROW_GET_LENGTH(rp,minlen)                        \
	((ROW_GET_VARCNT(rp) == 0)                      \
		? (minlen)                               \
		: GETINT((char *)(rp) + (minlen)))


#define ROW_GET_VARCOL_OFFSET(endrowp, varcolno)			\
		(GETINT((endrowp) - (varcolno) * COLOFFSETENTRYSIZE + 1))


#define ROW_GET_ENDOF_OFFSET_TABLE(rp, minlen)			\
	(GETINT((rp) + (minlen))				\
	      - ROW_GET_VARCNT((rp)) * COLOFFSETENTRYSIZE)

#define	ROW_GETPTR_FROM_OFFTAB(blkptr, offset_idx)		\
	(((char *) (blkptr)) +					\
		(ROW_OFFSET_PTR((blkptr)))[-((int)(offset_idx))])


#define ROW_GET_VARCOL_LEN(rp, minlen, varcolid, collen)		\
do									\
{									\
	int		nextcoloff;					\
	int		thiscoloff;					\
	int		varcount;					\
	char		*endrowp;					\
									\
	varcount = ROW_GET_VARCNT((rp));				\
	endrowp = ROW_GET_END((rp), (minlen));				\
									\
	Assert ((int)(varcolid) <= (int)varcount);		 	\
	(thiscoloff) = ROW_GET_VARCOL_OFFSET(endrowp, (varcolid));	\
									\
	if ((varcolid) == varcount)					\
	{								\
			\
		(collen) = ROW_GET_ENDOF_OFFSET_TABLE((rp),		\
				      (minlen)) - (thiscoloff);		\
	}								\
	else								\
	{								\
		nextcoloff = ROW_GET_VARCOL_OFFSET(endrowp,		\
						     (varcolid) + 1);	\
		Assert(nextcoloff >= (int)(thiscoloff));	 	\
		(collen) = nextcoloff - (thiscoloff); 			\
	}								\
} while(0)

#define ROWNO(rp) ((ROWFMT *)(rp))->rowno
#define ROWSTAT(rp) ((ROWFMT *)(rp))->status
#define ROWVATTCNT(rp) ((ROWFMT *)(rp))->vattcnt
	
#define EQ      0	    
#define LE      -1	
#define GR      1       
	
#define MT_COMPARE(val1, val2) ((val1) > (val2)) ? GR : (((val1) < (val2)) ? LE : EQ)


char *
row_locate_col(char * rowptr, int coloffset, int minrowlen, int * length);

void
row_build_hdr(char *rp, int rowno, int status, int vattcnt);

int
row_col_compare(int coltype, char *colval1, int colen1, char *colval2, int colen2);

void
row_prt_offtab(int *offtab, int n);

#endif
