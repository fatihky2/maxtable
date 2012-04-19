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

#ifndef	MASTERINFO_H_
#define MASTERINFO_H_

typedef struct master_infor
{
	char		conf_path[META_CONF_PATH_MAX_LEN];
	int		port;
	int		last_tabid;
	META_SYSTABLE	*meta_systab;
	META_SYSOBJECT	*meta_sysobj;
	META_SYSCOLUMN	*meta_syscol;	
	META_SYSINDEX	*meta_sysindex;
	LOCKATTR 	mutexattr;
	SPINLOCK	rglist_spinlock;	
	SVR_IDX_FILE	rg_list;
	HB_DATA		heart_beat_data[MAX_RANGER_NUM];
}MASTER_INFOR;

#endif
