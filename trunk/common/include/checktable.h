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

#ifndef CHECKTABLE_H_
#define CHECKTABLE_H_


/*
** The context of check table.
*/
typedef struct checktable_data
{
	char	chktab_magic[RPC_MAGIC_MAX_LEN];	/* the magic number. */
	char	chktab_magic_back[RPC_MAGIC_MAX_LEN];	/* place holder. */
	int	chktab_opid;				/* place holder. */
	int	chktab_tabid;
	char	chktab_tabname[TABLE_NAME_MAX_LEN];
	char	chktab_sstabname[TABLE_NAME_MAX_LEN];
	char	chktab_tablet_rg[RANGE_ADDR_MAX_LEN];
	int 	chktab_tablet_rgport;
	int	chktab_key_colid;
	int	chktab_key_coloff;
	int	chktab_row_minlen;
	char	chktab_data[SSTABLE_SIZE];
}CHECKTABLE_DATA;

#endif /* CHECKTABLE_H_ */

