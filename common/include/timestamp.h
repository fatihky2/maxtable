/*
** timestamp.h 2010-10-08 xueyingfei
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





