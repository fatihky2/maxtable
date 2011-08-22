/*
** tss.h 2010-12-10 xueyingfei
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


typedef struct tss
{
	/* Warning: texcptr must be first field is tss */
	EXC_PROC	*texcptr;	/* ptr to exception handler data */

	struct tssobj	*tssobjp;	/* pointer to tssobj struct */

	struct mempool	*ttmemfrag;	/* handle to task's memory fragment */

	/* Status, flags, options */
	int		sstat;  	/* process status */
	struct tree     *tcmd_parser;

	RPCREQ		trecvbuf;	/* receive host buffer */
	char		*tsendbuf;	/* send host buffer */

	short		topid;		/* current opration */

	/* Exception stack */
	EXC_PROC	texcproc;	/* exception handler data */
	
	struct col_info	*tcol_info;	/* column information */

	struct insert_meta
			*tmeta_hdr;	/* metadata of table */

	struct tab_info	*ttabinfo;
	struct tab_info *toldtabinfo;

} TSS;

typedef struct tssobj
{
	LINK	to_link;	/* Must be first field as required by
				** memory object manager.
				*/
	TSS	to_tssp;	/* points to the actual tss */
} TSSOBJ;

#ifndef LOCALTSS
#define	LOCALTSS(tss)	TSS	*tss = Tss
#endif /* LOCALTSS */


/* topid definitions */

#define		TSS_OP_METASERVER	0x0001

#define		TSS_OP_RANGESERVER	0x0002

#define		TSS_OP_CLIENT		0x0004

#define		TSS_OP_CRTTAB		0x0008

#define		TSS_OP_INSTAB		0x0010


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

