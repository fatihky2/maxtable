/*
** rginfo.h 2010-06-21 xueyingfei
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

#ifdef MAXTABLE_BENCH_TEST

#define	MT_RANGE_STATE	"./rg_state"

#else

#define	MT_RANGE_STATE	"/mnt/ranger/rg_state"

#endif


typedef struct sstab_split_info
{
	int	sstab_state;
	int	tab_name_len;
	char	tab_name[256];
	char	newsstabname[256];
	int	sstab_id;	/* New sstable id. */
	int	split_ts;	/* Time stamp, it can be used for the 
				** duplicated recovery.
				*/
	int	split_sstabid;	/* Old sstable id. */
	int	sstab_keylen;
	char	*sstab_key;	/* Ptr to the sstable key. */	
}SSTAB_SPLIT_INFO;

/* Following definition is for the sstab_state. */
#define	RG_SSTABLE_DELETED	0x0001	/* Flag that the tablet split is successful in meta server. */
#define	RG_SSTABLE_RECOVERED	0x0002	/* Flag this Add SStable has been recovered. */

typedef struct rg_state
{
	int	sstab_split_num;/* The # of splitted sstab in the file rg_state. */
	int	offset;		/* The offset of the Add SStable row. */
	char	tabletname[256];/* This tablet to be splitted in the Add SStable. */
	char	sstab_state[SSTABLE_MAX_COUNT * 1024];/* Save the SSTAB_SPLIT_INFO. */
}RG_STATE;

#define	RG_STATE_HEADER		(sizeof(int) *2 + 256)
#define	SSTAB_SPLIT_INFO_HEADER	(sizeof (int) * 6 + 512)


#define	SSTAB_SPLIT_INFO_INIT(split_info, sstabstate, tabname, tabname_len, newsstab_name,	\
		sstabid, splitts, splitsstabid, sstabkeylen, sstabkey)				\
do{												\
	(split_info)->sstab_state = sstabstate;							\
	MEMCPY((split_info)->tab_name, tabname, tabname_len);					\
	(split_info)->tab_name_len = tabname_len;						\
	MEMCPY((split_info)->newsstabname, newsstab_name, 256);					\
	(split_info)->sstab_id = sstabid;							\
	(split_info)->split_ts = splitts;							\
	(split_info)->split_sstabid = splitsstabid;						\
	(split_info)->sstab_keylen = sstabkeylen;						\
	(split_info)->sstab_key = sstabkey;							\
}while (0)

int
ri_rgstat_putdata(char *statefile, char *new_sstab, int state, SSTAB_SPLIT_INFO *split_value);

int
ri_rgstat_deldata(char * statefile,char *new_sstab);

SSTAB_SPLIT_INFO *
ri_rgstat_getdata(RG_STATE *rgstate, char *new_sstab, int state);

int
ri_get_rgstate(char *rgstate, char *rgip, int rgport);

