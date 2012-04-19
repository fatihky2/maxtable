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

