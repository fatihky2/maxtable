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


