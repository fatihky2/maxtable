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

#define	TSL_OVERFLOW	0
#define	TSH_OVERFLOW	0
	
typedef struct mtts
{
	unsigned int	ts_high;	/* high part */
	unsigned int	ts_low; 	/* low part */
} MTTS;

unsigned int
mtts_increment(unsigned int ts_low);

void
mtts_inits(MTTS *tsptr);

int
mtts_tscomp(unsigned int ts1_high, unsigned int ts1_low, unsigned int ts2_high, unsigned int ts2_low);

int
mtts_blktscmp(BLOCK *blk, MTTS *tsptr);





