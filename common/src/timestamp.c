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

#include "master/metaserver.h"
#include "buffer.h"
#include "rpcfmt.h"
#include "block.h"
#include "timestamp.h"


/* TODO: the logic of high part*/
unsigned int
mtts_increment(unsigned int ts_low)
{
	if (++ts_low == TSL_OVERFLOW)
	{	
		ts_low = 0;		
	}	

	return(ts_low);
}

void
mtts_inits(MTTS *tsptr)
{
	tsptr->ts_high = 0;
	tsptr->ts_low = 0;
}

int
mtts_tscomp(unsigned int ts1_high, unsigned int ts1_low, unsigned int ts2_high, unsigned int ts2_low)
{

	register int	ret;


	if (ts1_high > ts2_high)
	{
			ret = 1;
	}
	else
	{
		if (ts1_high < ts2_high)
		{
			ret = -1;
		}
		else
		{
			if (ts1_low > ts2_low)
			{
				ret = 1;
			}
			else if (ts1_low < ts2_low)
			{
				ret = -1;
			}
			else
			{
				ret = 0;
			}
		}
	}
	
	return(ret);
}

int
mtts_blktscmp(BLOCK *blk, MTTS *tsptr)
{
	
	register int	ret;
	

	ret = mtts_tscomp(blk->bsstab_split_ts_hi, blk->bsstab_split_ts_lo, tsptr->ts_high, tsptr->ts_low);
	
	return (ret);
}

