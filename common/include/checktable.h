/*
** checktable.h 2011-10-18 xueyingfei
**
** Copyright Transoft Corp.
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

#ifndef CHECKTABLE_H_
#define CHECKTABLE_H_


typedef struct checktable_data
{
	char	chktab_magic[RPC_MAGIC_MAX_LEN];
	char	chktab_magic_back[RPC_MAGIC_MAX_LEN];
	int	chktab_opid;
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

