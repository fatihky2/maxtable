/*
** parser.c 2010-08-11 xueyingfei
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
#include "master/metaserver.h"
#include "region/rangeserver.h"
#include "utils.h"
#include "parser.h"
#include "tss.h"
#include "token.h"
#include "memcom.h"
#include "type.h"
#include "strings.h"
#include "metadata.h"

extern	TSS	*Tss;


TREE*
par_bld_cmd(char *data, int data_len, int querytype)
{
	TREE    *node;
	struct command *cmd_node;

	node = (TREE *)MEMALLOCHEAP(sizeof(TREE));

	MEMSET(node, sizeof(TREE));

	node->type = PAR_CMD_NODE;
	cmd_node = (struct command *)(&node->sym);
	node->left = node->right = NULL;

	cmd_node->querytype = querytype;
	cmd_node->tabname_len = data_len;
	MEMCPY(cmd_node->tabname, data, data_len);

	return node;
}

void
par_destroy_cmd(TREE *node)
{
	assert(PAR_NODE_IS_COMMAND(node->type));

	MEMFREEHEAP(node);
	return;
}


TREE*
par_bld_const(char *data, int datalen, int datatype)
{
	TREE    *node;
        int     len;
	struct constant *const_node;

	node = (TREE *)MEMALLOCHEAP(sizeof(TREE));
	MEMSET(node, sizeof(TREE));

	node->type = PAR_CONSTANT_NODE;
	const_node = (struct constant *)(&node->sym);
	node->left = node->right = NULL;

	len = datalen;

	if (!TYPE_IS_INVALID(datatype) && TYPE_IS_FIXED(datatype))
	{
		
		len = TYPE_GET_LEN(datatype);
	}
      
	const_node->len = len;        
	const_node->value = (char *)MEMALLOCHEAP(len);
	MEMSET(const_node->value, len);

	if(!TYPE_IS_FIXED(datatype))
	{
		MEMCPY(const_node->value, data, MIN(datalen, len));
	}
	else
	{
		int tmp_val = m_atoi(data, datalen);
		MEMCPY(const_node->value, &tmp_val, len);
	}
	
	return node;    
}

void
par_destroy_const(TREE *node)
{
	assert(PAR_NODE_IS_CONSTANT(node->type));

	MEMFREEHEAP(node->sym.constant.value);
	MEMFREEHEAP(node);
	
	return;
}


TREE*
par_bld_resdom(char *colname, char *coltype, int col_id)
{
	TREE    *node;
	struct resdom *resd_node;
	int	col_idx;
	int	len_tmp;
	int	col_len;

	len_tmp = 0;
	
	node = (TREE *)MEMALLOCHEAP(sizeof(TREE));
	MEMSET(node, sizeof(TREE));

	node->type = PAR_RESDOM_NODE;
	resd_node = (struct resdom *)(&node->sym);
	node->left = node->right = NULL;

	resd_node->colid = col_id;

	if (coltype)
	{
		
		col_idx = type_get_index_by_name(coltype);

		assert(!TYPE_IS_INVALID(col_idx));
	
        	resd_node->coltype = TYPE_GET_TYPE_NUM(col_idx);
		resd_node->colen = TYPE_GET_LEN(col_idx);
	}
	
	if (colname)
	{
		col_len = STRLEN(colname);

		assert(col_len < 256);
		MEMCPY(resd_node->colname, colname, col_len);
	}


	
	

	return node;
}

void
par_destroy_resdom(TREE * node)
{
	assert(PAR_NODE_IS_RESDOM(node->type));

	if(node->right)
	{
		
		par_destroy_const(node->right);
	}
	
	MEMFREEHEAP(node);

	return;
}

int 
parser_open(char *s_str)
{
	LOCALTSS(tss);
	int 	querytype;
	int	s_idx;
	
	
	querytype = par_get_query(s_str, &s_idx);
	

	switch (querytype)
	{
	    case ADDSERVER:
		par_add_server((s_str + s_idx), ADDSERVER);
		break;
	    case TABCREAT:
	    	tss->topid |= TSS_OP_CRTTAB;
		par_crtins_tab((s_str + s_idx), TABCREAT);
		break;
		
	    case INSERT:
	    	tss->topid |= TSS_OP_INSTAB;
		par_crtins_tab((s_str + s_idx), INSERT);
	        break;

	    case CRTINDEX:
	        
	        break;

	    case SELECT:
	        
		
		par_seldel_tab((s_str + s_idx), SELECT);

	        break;

	    case DELETE:
	    	
	    	par_seldel_tab((s_str + s_idx), DELETE);
	        break;
	    case ADDSSTAB:
	    	par_addsstab(s_str + s_idx, ADDSSTAB);
	    	break;

	    default:
	        break;
	}



	return TRUE;
}

void
parser_close(void)
{
	LOCALTSS(tss);
	TREE    *tree;
	TREE    *tree_left;
	TREE    *tree_left_tmp;
	TREE    *tree_right;

	
	tree = tss->tcmd_parser;

	if (tree == NULL)
	{
		return;
	}

	while(tree && PAR_NODE_IS_COMMAND(tree->type))
	{
		
		tree_left = tree->left;
		while(tree_left && PAR_NODE_IS_RESDOM(tree_left->type))
		{            
			tree_left_tmp = tree_left->left;
			par_destroy_resdom(tree_left);   
			tree_left = tree_left_tmp;
		}

		
		tree_right = tree->right;
		par_destroy_cmd(tree);

		tree = tree_right;
	}

	return;
}


char *
par_get_colval_by_coloff(TREE *command, int coloff, int *col_len)
{
	while(command)
	{
		if (   (PAR_NODE_IS_RESDOM(command->type))
		    && (command->sym.resdom.coloffset == coloff))
		{
			if (col_len)
			{
				*col_len = command->right->sym.constant.len;
			}
			
			return (command->right->sym.constant.value);
		}

		command = command->left;
	}

	return NULL;
	
}


char *
par_get_colval_by_colid(TREE *command, int colid, int *colen)
{
	while(command)
	{
		if (   (PAR_NODE_IS_RESDOM(command->type))
		    && (command->sym.resdom.colid == colid))
		{
			if (colen)
			{
				*colen = command->right->sym.constant.len;
			}

			return (command->right->sym.constant.value);
		}

		command = command->left;
	}

	return NULL;
	
}


void
par_prt_tree(TREE *command)
{
	printf ("\n");
	
	if(command)
	{
		if (PAR_NODE_IS_COMMAND(command->type))
		{
			printf("Table Name : %s\n", command->sym.command.tabname);
			printf("------------------------------\n");
		}
		else if (PAR_NODE_IS_RESDOM(command->type))
		{
			printf("Column Name : %s \n", command->sym.resdom.colname);
			printf("Column ID : %d \n", command->sym.resdom.colid);
			printf("Column Offset : %d \n", command->sym.resdom.coloffset);
			printf("Column Type : %d \n", command->sym.resdom.coltype);
			printf("------------------------------\n");
		}
		else if (PAR_NODE_IS_CONSTANT(command->type))
		{
			printf("Column Value : %s \n", command->sym.constant.value);
			printf("Column ValueLength : %d \n", command->sym.constant.len);
			printf("------------------------------\n");
		}

		printf ("LEFT child\n");
		par_prt_tree (command->left);
		printf ("RIGHT child\n");
		par_prt_tree (command->right);
	}

	return;
	
}


int 
par_get_query(char *s_str, int *s_idx)
{
	char		start[64]; 
	int		len;
	char 		separator;
	int 		querytype;
	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	len = 0;
	separator = ' ';
	querytype = INVALID_TOK;
	MEMSET(start, 64);
	
double_parse:  

	
	while ((*s_str == ' ') || (*s_str == '\t'))
	{
		s_str++;
	}

	
	while(   (*s_str != separator) && (*s_str != ' ') 
	      && (*s_str != '\t'))
	{
		start[len++] = *s_str++;
	}

	*s_idx = len;

	
	if(   (!strncasecmp("create", start, len))
	   || (!strncasecmp("insert", start, len))
	   || (!strncasecmp("add", start, len))
	   || (!strncasecmp("addsstab", start, len)))
	{
		start[len++] = ' ';
		goto double_parse;
	}

	start[len] = '\0';
	
	querytype = token_validate(start);

	return querytype;
}



int 
par_add_server(char *s_str, int querytype)
{
	LOCALTSS(tss);
	int		len;
	char		tab_name[64];
	int		cmd_len;
	char		tab_name_len;
	int		start;
	int		end;
	char		*col_info;
	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	len = 0;

	cmd_len = STRLEN(s_str);

	len = str1nstr(s_str, "(\0", cmd_len);

	MEMSET(tab_name, 64);
		
	MEMCPY(tab_name, s_str, len - 1);

	
	str0n_trunc_0t(tab_name, len - 1, &start, &end);
	tab_name_len = end - start;

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	start = len;
	col_info = s_str + start;
	len =  str1nstr(col_info, ")\0", cmd_len - len);

	str0n_trunc_0t(col_info, len - 1, &start, &end);

	par_col_info((col_info + start), (end - start), querytype);

	return TRUE;
}


int 
par_crtins_tab(char *s_str, int querytype)
{
	LOCALTSS(tss);
	int		len;
	char		tab_name[64];
	int		cmd_len;
	char		tab_name_len;
	int		start;
	int		end;
	char		*col_info;
	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	len = 0;

	cmd_len = STRLEN(s_str);

	len = str1nstr(s_str, "(\0", cmd_len);

	MEMSET(tab_name, 64);
		
	MEMCPY(tab_name, s_str, len - 1);

	
	str0n_trunc_0t(tab_name, len - 1, &start, &end);
	tab_name_len = end - start;

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	start = len;
	col_info = s_str + start;
	len =  str1nstr(col_info, ")\0", cmd_len - len);

	str0n_trunc_0t(col_info, len - 1, &start, &end);

	par_col_info((col_info + start), (end - start), querytype);

	return TRUE;
}


int 
par_seldel_tab(char *s_str, int querytype)
{
	LOCALTSS(tss);
	int		len;
	char		tab_name[64];
	int		cmd_len;
	char		tab_name_len;
	int		start;
	int		end;
	char		*col_info;
	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	len = 0;

	cmd_len = STRLEN(s_str);

	len = str1nstr(s_str, "(\0", cmd_len);

	MEMSET(tab_name, 64);
		
	MEMCPY(tab_name, s_str, len - 1);

	
	str0n_trunc_0t(tab_name, len - 1, &start, &end);
	tab_name_len = end - start;

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	start = len;
	col_info = s_str + start;
	len =  str1nstr(col_info, ")\0", cmd_len - len);

	str0n_trunc_0t(col_info, len - 1, &start, &end);

	par_col_info((col_info + start), (end - start), querytype);

	return TRUE;
}




int 
par_addsstab(char *s_str, int querytype)
{
	LOCALTSS(tss);
	int		len;
	char		tab_name[64];
	int		cmd_len;
	char		tab_name_len;
	int		start;
	int		end;
	char		*col_info;
	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	len = 0;

	cmd_len = STRLEN(s_str);

	len = str1nstr(s_str, "(\0", cmd_len);

	MEMSET(tab_name, 64);
		
	MEMCPY(tab_name, s_str, len - 1);

	
	str0n_trunc_0t(tab_name, len - 1, &start, &end);
	tab_name_len = end - start;

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	start = len;
	col_info = s_str + start;
	len =  str1nstr(col_info, ")\0", cmd_len - len);

	str0n_trunc_0t(col_info, len - 1, &start, &end);

	par_col_info((col_info + start), (end - start), querytype);

	return TRUE;
}


int
par_col_info(char *cmd, int cmd_len, int querytype)
{
	LOCALTSS(tss);
	int	start;
	int	end;
	int	len;
	int	i;
	char	*coldata;
	TREE	*command;
	char	colname[256];
	char	coltype[64];
	int	colid;
	COLINFO	*colinfor;
	int	rg_insert;
	char	*col_info;


	colid = 0;
	len = 0;
	rg_insert = ((tss->topid & TSS_OP_RANGESERVER) && (tss->topid & TSS_OP_INSTAB)) ? TRUE : FALSE;

	while((cmd_len -= len) > 0 )
	{
		cmd = &(cmd[len]);
		len = str01str(cmd, ",\0", cmd_len);

		str0n_trunc_0t(cmd, len + 1, &start, &end);

		coldata = col_info = &cmd[start];

		if (querytype == TABCREAT)
		{
			i = 0;
			while((coldata[i] != ' ') && (coldata[i] != '\t'))
			{
				i++;
			}

			MEMSET(colname, 256);
			MEMCPY(colname, &(col_info[0]), i);

			str0n_trunc_0t(&(coldata[i]), (end - start - i), &start, &end);

			MEMSET(coltype, 64);
			MEMCPY(coltype, &(coldata[i + start]), (end - start));

			command = tss->tcmd_parser;

			while(command->left)
			{
				command = command->left;
			}

		        command->left = par_bld_resdom(colname, coltype, ++colid);
		}
		else if (   (querytype == INSERT) || (querytype == ADDSERVER) || (querytype == SELECT)
			 || (querytype == ADDSSTAB) || (querytype == DELETE))
		{
			command = tss->tcmd_parser;

			while(command->left)
			{
				command = command->left;
			}

			colid++;
			if (rg_insert)
			{
				colinfor = meta_get_colinfor(colid, 
						      tss->tmeta_hdr->col_num, 
						      tss->tcol_info);
			}

			command->left = par_bld_resdom(NULL, NULL, colid);

			command->left->right = par_bld_const(coldata, (end - start),
							rg_insert ? colinfor->col_type 
								      : INVALID_TYPE);
		}
		
		
		len += 2;
	}

	return TRUE;
}

