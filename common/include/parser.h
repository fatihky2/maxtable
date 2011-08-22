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

struct resdom
{
	int		colid;   	
	char		colname[64];
	short		colstat; 	
	int		coltype; 	
	int		coloffset;		
	char		pad[2];
	int		colen;   	
} ;

typedef struct constant
{
	int            	len;
	char            *value;  
	short	       	constat;	
	short           pad;
} CONSTANT;

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


TREE*
par_bld_cmd(char *data, int data_len, int querytype);

void
par_destroy_cmd(TREE *node);

TREE*
par_bld_const(char *data, int datalen, int datatype);

void
par_destroy_const(TREE *node);

TREE*
par_bld_resdom(char *colname, char *coltype, int col_id);

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
par_sel_tab(char *s_str, int querytype);

int 
par_get_query(char *s_str, int *s_idx);

int
par_col_info(char *cmd, int cmd_len, int querytype);

void
par_prt_tree(TREE *command);

int 
par_addsstab(char *s_str, int querytype);


#endif
