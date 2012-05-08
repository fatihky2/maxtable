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

#ifndef	ROW_H_
#define ROW_H_


typedef struct rowfmt
{
	int		rowno;  	
	int		status;		
	int		vattcnt;	
}ROWFMT;


#define	ROW_DELETED		0x0001		
#define	ROW_OVERFLOW		0x0002		
#define	ROW_HAS_OVERFLOW	0x0004		


#define	ROW_SET_STATUS(rp, flag)	((((ROWFMT *)(rp))->status) |= flag)
#define	ROW_GET_STATUS(rp)	((int) ((ROWFMT *)(rp))->status)

#define ROW_IS_DELETED(rp)	(ROW_GET_STATUS(rp) & ROW_DELETED)
#define	ROW_IS_OVERFLOW(rp)	(ROW_GET_STATUS(rp) & ROW_OVERFLOW)
#define	ROW_WITH_OVERFLOW(rp)	(ROW_GET_STATUS(rp) & ROW_HAS_OVERFLOW)



#define GETINT(p)	(*(int *) (p))

#define COLOFFSETENTRYSIZE	4	
#define	ROWVATTCNT_OFF		8	

#define	VARCOL_MAX_NUM		16
#define	COL_OFFTAB_MAX_SIZE	(VARCOL_MAX_NUM * sizeof(int))

#define ROW_GET_MINLEN()	sizeof(ROWFMT)


#define ROW_GET_VARCNT(rp)	GETINT((rp) + ROWVATTCNT_OFF)

#define ROW_SET_ROWNO(rp, rnum)	(GETINT(rp) = rnum)
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

void
row_rebld_row(char *oldrp, char *newrp, int newrlen, COLINFO *colinfo,
				int colnum, int minrlen);


#endif
