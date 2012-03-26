/*
** qryoptimizer.c 2012-03-18 xueyingfei
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
#include "parser.h"


int
qryopt_get_colmap_by_cmd(TREE *command, COLINFO *colinfo, int colnum)
{
	int	i;
	int	colmap;


	colmap = 0;
	
	for (i = 0; i < colnum; i++)
	{
		if ((par_get_constant_by_colname(command, 
					colinfo[i].col_name)) != NULL)
		{
			TAB_COL_SET_INDEX(colmap, colinfo[i].col_id);
		}
	}

	return colmap;	 
}

int
qryopt_get_index_col(int colmap)
{
	int	colidx;
	int	tmp_colmap;


	colidx = 0;
	tmp_colmap = colmap;
	
	while(tmp_colmap)				
	{					
		tmp_colmap >>=1;			
		colidx++;			
	}

	return colidx;
}


