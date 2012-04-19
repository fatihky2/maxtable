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


#ifndef COMPACT_H_
#define COMPACT_H_


/* The context of data compaction. */
typedef struct compact_data
{
	char	compact_magic[RPC_MAGIC_MAX_LEN];
	char	compact_magic_back[RPC_MAGIC_MAX_LEN];
	int	compact_opid;
	int	compact_tabid;
	char	compact_tabname[TABLE_NAME_MAX_LEN];
	char	compact_sstabname[TABLE_NAME_MAX_LEN];
	char	compact_tablet_rg[RANGE_ADDR_MAX_LEN];
	int 	compact_tablet_rgport;
	int	compact_key_colid;
	int	compact_key_coloff;
	int	compact_row_minlen;
	char	compact_data[SSTABLE_SIZE];
}COMPACT_DATA;

#endif /* COMPACT_H_ */

