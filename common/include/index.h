/*
** index.h 2012-03-06 xueyingfei
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
#ifndef	INDEX_H_
#define INDEX_H_





#define ROW_MINLEN_IN_INDEXBLK		(sizeof(ROWFMT) + sizeof(int))

#define INDEXBLK_RIDNUM_COLID_INROW	1
#define INDEXBLK_KEYCOL_COLID_INROW	2
#define	INDEXBLK_RIDARRAY_COLID_INROW	3


#define INDEXBLK_RIDNUM_COLOFF_INROW	(sizeof(ROWFMT))
//#define INDEXBLK_RIDARRAY_COLOFF_INROW	(sizeof(ROWFMT) + sizeof(int))
#define TABLE_KEYCOL_FAKE_COLOFF_INROW		-1
#define TABLE_RIDARRAY_FAKE_COLOFF_INROW	-2







typedef struct rid
{
	int	sstable_id;	
	int	block_id;	
	int	row_id;		
	int	pad;
}RID;



#define	IDX_IN_CREATE	0x0001	
#define	IDX_IN_WORKING	0x0002	
#define	IDX_IN_DROP	0x0004	



typedef struct idxbld
{
	int	idx_root_sstab;		
	int	idx_data_sstab;		
	int	idx_data_blk;		
	int	idx_data_row;		
	int	idx_stat;		
	char	*idx_rp;		
	int	idx_rlen;
	char	*idx_tab_name;		
	char	*idx_tablet_name;	
	IDXMETA	*idx_meta;		
	int	idx_index_sstab_cnt;	
	int	pad;
} IDXBLD;


#define	IDXBLD_FIRST_DATAROW_IN_TABLET	0x0001	


typedef struct idx_root_srch
{
	char	*keycol;
	int	keycolen;
	int 	coltype;
	int	indexid;
	int	rootid;
	char	*rootname;
	char	*leafname;
	int	sstab_id;		
	int	stat;
}IDX_ROOT_SRCH;


#define	IDX_ROOT_SRCH_1ST_KEY		0x0001	
#define	IDX_ROOT_SRCH_LAST_KEY		0x0002	



typedef struct idx_range_ctx
{
	int 	coltype;
	int	pad;
	char	*key_left;		
	char	*key_right;		
	int	keylen_left;		
	int	keylen_right;		
		
	char	*sstab_left;		
	char	*sstab_right;		
	int	sstabid_left;		
	int	sstabid_right;		
	int	left_expand;		
	int	right_expand;		

	
	char	*tabname;		
	int	tab_namelen;
	int	tabid;
	int	tabletid;
	int	pad1;
}IDX_RANGE_CTX;



int
index_bld_meta(IDXMETA *idxmeta, TABLEHDR *tabhdr, COLINFO *colinfo, 
					TREE *command, int idxid);

int
index_get_meta_by_idxname(int tabid, char *idxname, META_SYSINDEX *meta_sysidx);

int
index_ins_meta(IDXMETA *idxmeta, META_SYSINDEX *meta_sysidx);

int
index_del_meta(int tabid, char *idxname, META_SYSINDEX *meta_sysidx);

int
index_bld_row(char *index_rp, int index_rlen, RID *rid, char *keycol, 
					int keycolen, int keycol_type);

int
index_ins_row(IDXBLD *idxbld);

int
index_bld_root_name(char *tab_meta_dir, char *tab_name, char *idx_name, 
			char *tablet_name);

int
index_bld_leaf_name(char *tab_meta_dir, char *index_sstab_name, char *tab_name, 
			char *idx_name, char *tablet_name, int tablet_num, 
			int sstab_num);

int
index_range_sstab_scan(TABLET_SCANCTX *tablet_scanctx, IDXMETA *idxmeta , 
			IDX_RANGE_CTX *idx_range_ctx, char *tabdir);

int
index_get_datarow(SSTAB_SCANCTX *scanctx, char *tabname, int tab_name_len, 
			int tabletid);

int
index_get_meta_by_colmap(int tabid, int colmap, META_SYSINDEX *meta_sysidx);

int
index_fill_rangectx_andplan(ORANDPLAN *cmd, int col_map, 
				IDX_RANGE_CTX *idx_range_ctx);

int
index_srch_root(IDX_ROOT_SRCH *root_srchctx);


#endif
