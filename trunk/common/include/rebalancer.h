/*
** rebalancer.h 2011-09-01 xueyingfei
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

#ifndef REBALANCER_H_
#define REBALANCER_H_


typedef struct rebalance_data
{
	char	rbd_magic[RPC_MAGIC_MAX_LEN];
	char	rbd_magic_back[RPC_MAGIC_MAX_LEN];
	int	rbd_opid;
	char	rbd_tabname[TABLE_NAME_MAX_LEN];
	char	rbd_sstabname[TABLE_NAME_MAX_LEN];
	char	rbd_max_tablet_rg[RANGE_ADDR_MAX_LEN];
	char	rbd_min_tablet_rg[RANGE_ADDR_MAX_LEN];
	int	rbd_min_tablet_rgport;
	int 	rbd_max_tablet_rgport;
	char	rbd_data[SSTABLE_SIZE];
}REBALANCE_DATA;

#define		RBD_FILE_SENDER		0x0001
#define		RBD_FILE_RECVER		0x0002

typedef struct rebalance_statistics
{
	int	rbs_tablet_av_num;
	int	rbs_tablet_tot_num;
	int	rbs_rg_num;
	int	pad;
}REBALANCE_STATISTICS;

RANGE_PROF *
rebalan_get_rg_prof_by_addr(char *rg_addr, int rg_port);


#endif /* REBALANCER_H_ */

