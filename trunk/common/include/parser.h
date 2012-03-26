/*
** parser.h 2010-08-11 xueyingfei
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


#ifndef	PARSER_H_
#define PARSER_H_




struct command
{
	int		querytype;  	
	long            rootstat;
	int             tabname_len;
	char            tabname[128];
};

typedef struct resdom
{
	int		colid;   	
	char		colname[64];
	short		colstat; 	
	int		coltype; 	
	int		coloffset;		
	short		resdstat;
	int		colen;   	
}RESDOM;

#define	RESDOM_SELECTSUM_COL	0x0001	

typedef struct constant
{
	int            	len;
	char            *value;  	
	int		rightlen;	
	char		*rightval;

	short	       	constat;	
	short           pad;
} CONSTANT;


#define	CONSTANT_SELECTWHERE	0x0001	


typedef union symbol
{	
	struct command	command;
	struct constant	constant;
	struct resdom	resdom;	
} SYMBOL; 

typedef struct tree
	{
	struct tree	*left;
	struct tree	*right;
	int		type;
	union symbol	sym;
} TREE;

#define PAR_CMD_NODE		1	
#define PAR_RESDOM_NODE		2	
#define PAR_CONSTANT_NODE	3	


#define PAR_NODE_IS_COMMAND(type)       (type == PAR_CMD_NODE)

#define PAR_NODE_IS_RESDOM(type)        (type == PAR_RESDOM_NODE)

#define PAR_NODE_IS_CONSTANT(type)      (type == PAR_CONSTANT_NODE)


#define	OR	1
#define	AND	2
#define WHERE	3


typedef struct srchclause
{	struct tree	*scterms;	
} SRCHCLAUSE;


typedef struct orandplan
{
	struct srchclause	orandsclause;	
	struct orandplan	*orandplnext;	
} ORANDPLAN;


TREE*
par_bld_cmd(char *data, int data_len, int querytype);

void
par_destroy_cmd(TREE *node);

TREE*
par_bld_const(char *data, int datalen, int datatype, char *rightdata, int rightdatalen);

void
par_destroy_const(TREE *node);

TREE*
par_bld_resdom(char *colname, char *coltype, int col_id, short resdstat);

void
par_destroy_resdom(TREE * node);

int 
parser_open(char *s_str);

void
parser_close(void);

char *
par_get_colval_by_coloff(TREE *command, int coloff, int *col_len);

char *
par_get_colval_by_colid(TREE *command, int colid, int *colen);

int 
par_add_server(char *s_str, int querytype);

int 
par_crtins_tab(char *s_str, int querytype);

int 
par_seldel_tab(char *s_str, int querytype);

int 
par_dropremovrebalanmcc_tab(char *s_str, int querytype);

int 
par_get_query(char *s_str, int *s_idx);

int
par_col_info(char *cmd, int cmd_len, int querytype);

void
par_prt_tree(TREE *command);

int 
par_addsstab(char *s_str, int querytype);

int
par_name_check(char *name, int len);

int 
par_selrange_tab(char *s_str, int querytype);

int
par_col_info4where(char *cmd, int cmd_len, int querytype, char *colname);

int 
par_selwherecnt_tab(char *s_str, int querytype);

int
par_op_where(char *cmd, int len);

CONSTANT *
par_get_constant_by_colname(TREE *command, char *colname);

RESDOM *
par_get_resdom_by_colname(TREE *command, char *colname);

int
par_fill_colinfo(int colnum, COLINFO* col_buf, TREE *command);

ORANDPLAN *
par_get_orplan(TREE *command);

ORANDPLAN *
par_get_andplan(TREE *command);

void
par_release_orandplan(ORANDPLAN *orandplan);

int
par_chk_fill_resdom(TREE *command, int colnum, COLINFO* col_buf);

int 
par_crt_idx_tab(char *s_str, int querytype);

int
par_process_orplan(ORANDPLAN *cmd, char *rp, int minrowlen);

int
par_process_andplan(ORANDPLAN *cmd, char *rp, int minrowlen);

#endif
