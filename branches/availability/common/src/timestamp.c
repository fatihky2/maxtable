/*
** timestamp.c 2010-10-08 xueyingfei
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
#include "master/metaserver.h"
#include "buffer.h"
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
	

	ret = mtts_tscomp(blk->bts_hi, blk->bts_lo, tsptr->ts_high, tsptr->ts_low);
	
	return (ret);
}

