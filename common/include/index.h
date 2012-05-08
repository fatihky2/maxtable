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

#ifndef	INDEX_H_
#define INDEX_H_


struct tree;
struct tablet_scancontext;
struct sstab_scancontext;
struct orandplan;







#define ROW_MINLEN_IN_INDEXBLK		(sizeof(ROWFMT) + sizeof(int))

#define INDEXBLK_RIDNUM_COLID_INROW	1
#define INDEXBLK_KEYCOL_COLID_INROW	2
#define	INDEXBLK_RIDARRAY_COLID_INROW	3


#define INDEXBLK_RIDNUM_COLOFF_INROW	(sizeof(ROWFMT))
//#define INDEXBLK_RIDARRAY_COLOFF_INROW	(sizeof(ROWFMT) + sizeof(int))
#define IDXBLK_KEYCOL_FAKE_COLOFF_INROW		-1
#define IDXBLK_RIDARRAY_FAKE_COLOFF_INROW	-2








typedef struct idxbld
{
	int	idx_root_sstab;		
	int	idx_stat;		
	char	*idx_rp;		
	int	idx_rlen;
	char	*idx_tab_name;		

	IDXMETA	*idx_meta;		
	int	idx_index_sstab_cnt;	
} IDXBLD;


#define	IDXBLD_FIRST_DATAROW_IN_TABLET	0x0001	
#define IDXBLD_NOLOG			0x0002	
#define	IDXBLD_IDXROOT_SPLIT		0x0004	
#define	IDXBLD_SSTAB_SPLIT		0x0008	

typedef struct idxupd
{
	BLOCK	*oldblk;		
	BLOCK	*newblk;		
	int	old_sstabid;		
	int	new_sstabid;		
	int	start_row;		
	int	pad;
}IDXUPD;


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
					struct tree *command, int idxid);

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
index_bld_root_dir(char *tab_meta_dir, char *tab_name, char *idx_name, 
			int tablet_id);

int
index_bld_root_name(char *tab_meta_dir, char *tab_name, char *idx_name, 
			int tablet_id, int mk_dir);

int
index_bld_leaf_name(char *tab_meta_dir, char *index_sstab_name, char *tab_name, 
			char *idx_name, int tablet_num, int sstab_num);

void
index_range_sstab_scan(TABLET_SCANCTX * tablet_scanctx,IDXMETA * idxmeta,
				char *tabname, int tabnamelen, int tabletid);

int
index_get_datarow(struct sstab_scancontext *scanctx, char *tabname,
				int tab_name_len, int tabletid);

int
index_get_meta_by_colmap(int tabid, int colmap, META_SYSINDEX *meta_sysidx);

int
index_fill_rangectx_andplan(struct orandplan *cmd, int col_map, 
				IDX_RANGE_CTX *idx_range_ctx);

int
index_srch_root(IDX_ROOT_SRCH *root_srchctx);

int
index_del_row(IDXBLD *idxbld);

int
index_update(IDXBLD *idxbld, IDXUPD *idxupd, TABINFO *tabinfo,
				META_SYSINDEX *meta_sysidx);

int
index_insert(IDXBLD *idxbld, TABINFO *tabinfo,	META_SYSINDEX *meta_sysidx);

int
index_delete(IDXBLD *idxbld, TABINFO *tabinfo, META_SYSINDEX *meta_sysidx);

int
index_root_crt_empty(int tabid, char *tabname, int tabletid,
			int ovflow_tablet, META_SYSINDEX *meta_sysidx);

int
index_root_sstabmov(IDXBLD *idxbld, BLOCK *srcblk, int indexid,
				char *src_rootname, char *dest_rootname, 
				int dest_rootid,int sstabid);

int
index_root_move(IDXBLD *idxbld, BLOCK *srcblk, BLOCK *destblk, int indexid,
			char *src_root_name, char * dest_rootname,int dest_rootid);

void
index_rmrid(BLOCK *blk, int rnum, int del_ridnum);

void
index_addrid(BLOCK *blk,int rnum, RID *newridp, int ridlist_off, int ridnum_off,
		int rminlen);

int
index_rid_cmp(char *rid1, char *rid2);

int
index_tab_has_index(META_SYSINDEX *meta_sysidx, int tabid);

int
index_tab_check_index(META_SYSINDEX *meta_sysidx, int tabid);

void
index_prt_ridinfo(RID *ridp, int ridnum);


#endif
