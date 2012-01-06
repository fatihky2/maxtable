/*
** metadata.h 2011-02-15 xueyingfei
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


#ifndef METADATA_H_
#define METADATA_H_


struct col_info;
struct buf;

struct col_info *
meta_get_colinfor(int colid, int totcol, struct col_info *colinfor);

char *
meta_get_coldata(struct buf *bp, int rowoffset, int coloffset);

int
meta_save_sysobj(char *tab_dir, char *tab_hdr);



#endif

