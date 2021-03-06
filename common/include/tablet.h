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

#ifndef	TABLET_H_
#define TABLET_H_


struct table_hdr;

#define	INVALID_TABLETID	1
#define TABLETSCHM_ID		0





#define ROW_MINLEN_IN_TABLET		(sizeof(ROWFMT) + sizeof(int) + SSTABLE_NAME_MAX_LEN + sizeof(int) )

#define TABLET_SSTABID_COLID_INROW	1
#define TABLET_SSTABNAME_COLID_INROW	2
#define TABLET_RESSSTABID_COLID_INROW	3
#define TABLET_KEY_COLID_INROW		4

#define TABLET_SSTABID_COLOFF_INROW	(sizeof(ROWFMT))
#define TABLET_SSTABNAME_COLOFF_INROW	(sizeof(ROWFMT) + sizeof(int))
#define	TABLET_RESSSTABID_COLOFF_INROW	(sizeof(ROWFMT) + sizeof(int) + SSTABLE_NAME_MAX_LEN)
#define TABLET_KEY_COLOFF_INROW		(ROW_MINLEN_IN_TABLET + sizeof(int))

#define TABLE_KEY_FAKE_COLOFF_INROW	-1





#define ROW_MINLEN_IN_TABLETSCHM	(sizeof(ROWFMT) + sizeof(int) + sizeof(int) + TABLET_NAME_MAX_LEN + RANGE_ADDR_MAX_LEN)

#define TABLETSCHM_TABLETID_COLID_INROW		1
#define	TABLETSCHM_RGPORT_COLID_INROW		2	
#define TABLETSCHM_TABLETNAME_COLID_INROW	3
#define TABLETSCHM_RGADDR_COLID_INROW		4
#define TABLETSCHM_KEY_COLID_INROW		5
#define	TABLETSCHM_RIDLIST_COLID_INROW		6	

#define TABLETSCHM_TABLETID_COLOFF_INROW	(sizeof(ROWFMT))
#define TABLETSCHM_RGPORT_COLOFF_INROW		(sizeof(ROWFMT) + sizeof(int))
#define TABLETSCHM_RIDNUM_COLOFF_INROW		(sizeof(ROWFMT) + sizeof(int))
#define TABLETSCHM_TABLETNAME_COLOFF_INROW	(sizeof(ROWFMT) + sizeof(int) + sizeof(int)) 
#define TABLETSCHM_RGADDR_COLOFF_INROW		(sizeof(ROWFMT) + sizeof(int) + sizeof(int) + TABLET_NAME_MAX_LEN)
#define TABLETSCHM_KEY_COLOFF_INROW		(ROW_MINLEN_IN_TABLETSCHM + sizeof(int))

#define TABLETSCHM_KEY_FAKE_COLOFF_INROW	-1



#define		TABLET_OK		0x0001		
#define		TABLET_FAIL		0x0002		
#define		TABLET_CRT_NEW		0x0004		


int
tablet_crt(TABLEHDR *tablehdr, char *tabledir, char *rg_addr, char *rp, int minlen, int port);

int
tablet_bld_row(char *sstab_rp, int sstab_rlen, char *tab_name, int tab_name_len,
			int sstab_id, int res_sstab_id, char *sstab_name, int sstab_name_len, 
			char *keycol, int keycolen, int keycol_type);

char *
tablet_srch_row(TABINFO *usertabinfo, int tabid, int sstabid, char *systab, char *key, int keylen);

char *
tablet_get_1st_or_last_row(int tabid, int sstabid, char *systab, int firstrow, int is_tablet);

int
tablet_ins_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen);

void
tablet_del_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen);

void 
tablet_schm_bld_row(char *rp, int rlen, int tabletid, char *tabletname, char *rang_addr, 
			char *keycol, int keycolen, int port);

int
tablet_schm_ins_row(int tabid, int sstabid, char *systab, char *row, int tabletnum, int flag);

int
tablet_schm_del_row(int tabid, int sstabid, char *systab, char *key, int keylen);

char *
tablet_schm_srch_row(int tabid, int sstabid, char *systab, char *key, int keylen);

char *
tablet_schm_get_row(int tabid, int sstabid, char *systab, int rowno);

void
tablet_namebyname(char *old_sstab, char *new_sstab);

void
tablet_namebyid(TABINFO *tabinfo, char *new_sstab);

int
tablet_split(TABINFO *srctabinfo, BUF *srcbp, char *rp);

int
tablet_upd_col(char *newrp, char *oldrp, int rlen, int colid, char *newcolval, int newvalen);

void
tablet_schm_upd_col(char *newrp, char *oldrp, int colid, char *newcolval, int newvalen);

int
tablet_sharding(TABLEHDR *tablehdr, char *rg_addr, int rg_port, char *tabdir, int tabid, char *tabletname, int tabletid);

int
tablet_upd_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name,
			char *oldrp, char *newrp, int minlen);

int
tablet_schm_get_totrow(int tabid, int sstabid, char *systab, char *key, int keylen);



#endif


