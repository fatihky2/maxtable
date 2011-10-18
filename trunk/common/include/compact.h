/*
** compact.h 2011-10-18 xueyingfei
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

#ifndef COMPACT_H_
#define COMPACT_H_


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

