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

#ifndef	HEARTBEAT_H_
#define HEARTBEAT_H_


#define MAX_RANGER_NUM	1024

#define HB_DATA_SIZE	128

typedef struct hb_data
{
	int	hb_stat;
	char	recv_data[HB_DATA_SIZE];
}HB_DATA;

/* Place holder: following definition is for the hb_stat. */
#define	HB_IS_OFF	0x0000		/* heartbeat is down. */
#define	HB_IS_ON	0x0001		/* heartbeat setup. */

#define HB_RANGER_IS_ON(hb_data)	((hb_data)->hb_stat & HB_IS_ON)

#define HB_SET_RANGER_ON(hb_data)	((hb_data)->hb_stat = (((hb_data)->hb_stat & ~HB_IS_OFF) | HB_IS_ON))

#define HB_SET_RANGER_OFF(hb_data)	((hb_data)->hb_stat = (((hb_data)->hb_stat & ~HB_IS_ON) | HB_IS_OFF))

#endif
