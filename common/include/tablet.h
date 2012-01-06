/*
** tablet.h 2011-08-08 xueyingfei
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

#define TABLETSCHM_TABLETID_COLOFF_INROW	(sizeof(ROWFMT))
#define TABLETSCHM_RGPORT_COLOFF_INROW		(sizeof(ROWFMT) + sizeof(int))
#define TABLETSCHM_TABLETNAME_COLOFF_INROW	(sizeof(ROWFMT) + sizeof(int) + sizeof(int)) 
#define TABLETSCHM_RGADDR_COLOFF_INROW		(sizeof(ROWFMT) + sizeof(int) + sizeof(int) + TABLET_NAME_MAX_LEN)
#define TABLETSCHM_KEY_COLOFF_INROW		(ROW_MINLEN_IN_TABLETSCHM + sizeof(int))

#define TABLETSCHM_KEY_FAKE_COLOFF_INROW	-1


void
tablet_crt(TABLEHDR *tablehdr, char *tabledir, char *rg_addr, char *rp, int minlen, int port);

int
tablet_bld_row(char *sstab_rp, int sstab_rlen, char *tab_name, int tab_name_len,
			int sstab_id, int res_sstab_id, char *sstab_name, int sstab_name_len, 
			char *keycol, int keycolen, int keycol_type);

char *
tablet_srch_row(TABINFO *usertabinfo, TABLEHDR *tablehdr, int tabid, int sstabid, char *systab, char *key, int keylen);

char *
tablet_get_1st_or_last_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *systab, int firstrow);

int
tablet_ins_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen);

void
tablet_del_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen);

void 
tablet_schm_bld_row(char *rp, int rlen, int tabletid, char *tabletname, char *rang_addr, 
			char *keycol, int keycolen, int port);

void
tablet_schm_ins_row(int tabid, int sstabid, char *systab, char *row, int tabletnum);

void
tablet_schm_del_row(int tabid, int sstabid, char *systab, char *row);

char *
tablet_schm_srch_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *systab, char *key, int keylen);

char *
tablet_schm_get_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *systab, int rowno);

void
tablet_namebyname(char *old_sstab, char *new_sstab);

void
tablet_namebyid(TABINFO *tabinfo, char *new_sstab);

void
tablet_split(TABINFO *srctabinfo, BUF *srcbp, char *rp);

int
tablet_upd_col(char *newrp, char *oldrp, int rlen, int colid, char *newcolval, int newvalen);

void
tablet_schm_upd_col(char *newrp, char *oldrp, int colid, char *newcolval, int newvalen);

int
tablet_sharding(TABLEHDR *tablehdr, char *rg_addr, int rg_port, char *tabdir, int tabid, char *tabletname, int tabletid);


#endif


