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

void
tablet_crt(TABLEHDR *tablehdr, char *tabledir, char *rp, int minlen);

int
tablet_bld_row(char *sstab_rp, int sstab_rlen, char *tab_name, int tab_name_len,
			int sstab_id, int res_sstab_id, char *sstab_name, int sstab_name_len, 
			char *rang_addr, char *keycol, int keycolen, int keycol_type);

char *
tablet_srch_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *systab, char *key, int keylen);

void
tablet_ins_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen);

void
tablet_del_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *tablet_name, char *rp, int minlen);

void 
tablet_schm_bld_row(char *rp, int rlen, int tabletid, char *tabletname, char *keycol, int keycolen);

void
tablet_schm_ins_row(int tabid, int sstabid, char *systab, char *row, int tabletnum);

char *
tablet_schm_srch_row(TABLEHDR *tablehdr, int tabid, int sstabid, char *systab, char *key, int keylen);

void
tablet_namebyname(char *old_sstab, char *new_sstab);

void
tablet_namebyid(TABINFO *tabinfo, char *new_sstab);

void
tablet_split(TABINFO *srctabinfo, BUF *srcbp, char *rp);


#endif


