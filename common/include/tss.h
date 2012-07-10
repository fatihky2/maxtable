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

#ifndef TSS_H_
#define TSS_H_

# ifdef __cplusplus
extern "C" {
# endif

#include "list.h"
#include "netconn.h"
#include "exception.h"

struct col_info;
struct tab_info;
struct insert_meta;
struct rg_prof;
struct master_infor;
struct tab_sstab_map;
struct table_hdr;





typedef struct tss
{
	
	EXC_PROC	*texcptr;		

	struct tssobj	*tssobjp;	

	struct mempool	*ttmemfrag;	

	
	int		tstat;  	
	int		tlogbeg_off;	
	struct tree     *tcmd_parser;

	RPCREQ		trecvbuf;	
	char		*tsendbuf;	

	short		topid;		
	char		pad[6];

	
	EXC_PROC	texcproc;	
	
	struct col_info	*tcol_info;	

	struct insert_meta
			*tmeta_hdr;	
	struct table_hdr
			*ttab_hdr;	

	struct tab_info	*ttabinfo;
	struct tab_info *toldtabinfo;
	struct rg_prof	*tcur_rgprof;
	struct master_infor
			*tmaster_infor;
	struct tab_sstab_map
			*ttab_sstabmap;
	char		*rglogfile;	
	char		*rgbackpfile;	
	char		*rgstatefile;	
	char		*metabackup;	
} TSS;


#define TSS_PARSER_ERR		0x0001
#define TSS_DEBUG		0x0002
#define	TSS_BEGIN_LOGGING	0x0004	
#define	TSS_LOGGING_SCOPE	0x0008	


#define DEBUG_TEST(tss)		((tss)->tstat & TSS_DEBUG)
#define DEBUG_SET(tss)		((tss)->tstat |= TSS_DEBUG)
#define DEBUG_CLR(tss)		((tss)->tstat &= ~TSS_DEBUG)



typedef struct tssobj
{
	LINK	to_link;	
	TSS	to_tssp;	
} TSSOBJ;

#ifndef LOCALTSS
#define	LOCALTSS(tss)	TSS	*tss = Tss
#endif 


#define		TSS_OP_METASERVER	0x0001
#define		TSS_OP_RANGESERVER	0x0002
#define		TSS_OP_CLIENT		0x0004
#define		TSS_OP_CRTTAB		0x0008
#define		TSS_OP_INSTAB		0x0010
#define		TSS_OP_SELDELTAB	0x0020
#define		TSS_OP_RECOVERY		0x0040
#define		TSS_OP_SELWHERE		0x0080
#define		TSS_OP_CRTINDEX		0x0100
#define		TSS_OP_INDEX_CASE	0x0200
#define		TSS_OP_IDXROOT_SPLIT	0x0400
#define		TSS_OP_UPDATE		0x0800

TSS *
tss_alloc(void);

void
tss_init(register TSS *tss);

void
tss_release();

int
tss_setup(int opid);

# ifdef __cplusplus
}
# endif

#endif

