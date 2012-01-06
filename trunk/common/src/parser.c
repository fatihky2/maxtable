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
#include "ranger/rangeserver.h"
#include "utils.h"
#include "parser.h"
#include "tss.h"
#include "token.h"
#include "memcom.h"
#include "type.h"
#include "strings.h"
#include "metadata.h"
#include "file_op.h"


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
	Assert(PAR_NODE_IS_COMMAND(node->type));

	MEMFREEHEAP(node);
	return;
}


TREE*
par_bld_const(char *data, int datalen, int datatype, char *rightdata, int rightdatalen)
{
	TREE    *node;
        int     len;
	int	rightlen;
	struct constant *const_node;

	node = (TREE *)MEMALLOCHEAP(sizeof(TREE));
	MEMSET(node, sizeof(TREE));

	node->type = PAR_CONSTANT_NODE;
	const_node = (struct constant *)(&node->sym);
	node->left = node->right = NULL;

	len = datalen;
	rightlen = rightdatalen;

	if (!TYPE_IS_INVALID(datatype) && TYPE_IS_FIXED(datatype))
	{
		
		rightlen = len = TYPE_GET_LEN(datatype);
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

	if (rightdata)
	{
		rightlen = rightdatalen;

		if (!TYPE_IS_INVALID(datatype) && TYPE_IS_FIXED(datatype))
		{
			
			rightlen = TYPE_GET_LEN(datatype);
		}
		
		const_node->rightlen = rightlen;        
		const_node->rightval = (char *)MEMALLOCHEAP(rightlen);
		MEMSET(const_node->rightval, rightlen);

		if(!TYPE_IS_FIXED(datatype))
		{
			MEMCPY(const_node->rightval, rightdata, MIN(rightdatalen, rightlen));
		}
		else
		{
			int tmp_val = m_atoi(rightdata, rightdatalen);
			MEMCPY(const_node->rightval, &tmp_val, rightlen);
		}

		const_node->constat |= CONSTANT_SELECTWHERE;
	}
	
	return node;    
}

void
par_destroy_const(TREE *node)
{
	Assert(PAR_NODE_IS_CONSTANT(node->type));

	MEMFREEHEAP(node->sym.constant.value);

	if (node->sym.constant.constat & CONSTANT_SELECTWHERE)
	{
		MEMFREEHEAP(node->sym.constant.rightval);
	}
	
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

		Assert(!TYPE_IS_INVALID(col_idx));
	
        	resd_node->coltype = TYPE_GET_TYPE_NUM(col_idx);
		resd_node->colen = TYPE_GET_LEN(col_idx);
	}
	
	if (colname)
	{
		col_len = STRLEN(colname);

		Assert(col_len < 256);
		MEMCPY(resd_node->colname, colname, col_len);
	}


	
	
	
	return node;
}

void
par_destroy_resdom(TREE * node)
{
	Assert(PAR_NODE_IS_RESDOM(node->type));

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
	int	rtn_stat;
	

	rtn_stat = TRUE;
	querytype = par_get_query(s_str, &s_idx);
	

	switch (querytype)
	{
	    case ADDSERVER:
		rtn_stat= par_add_server((s_str + s_idx), ADDSERVER);
		break;
	    case TABCREAT:
	    	tss->topid |= TSS_OP_CRTTAB;
		rtn_stat = par_crtins_tab((s_str + s_idx), TABCREAT);
		break;
		
	    case INSERT:
	    	tss->topid |= TSS_OP_INSTAB;
		rtn_stat = par_crtins_tab((s_str + s_idx), INSERT);
	        break;

	    case CRTINDEX:
	        
	        break;

	    case SELECT:		
		tss->topid |= TSS_OP_SELDELTAB;
		rtn_stat = par_seldel_tab((s_str + s_idx), SELECT);

	        break;

	    case DELETE:
	    	
	    	tss->topid |= TSS_OP_SELDELTAB;
	    	rtn_stat = par_seldel_tab((s_str + s_idx), DELETE);
	        break;

	    case SELECTRANGE:
	    	rtn_stat = par_selrange_tab((s_str + s_idx), SELECTRANGE);
	    	break;
		
	    case ADDSSTAB:
	    	rtn_stat = par_addsstab(s_str + s_idx, ADDSSTAB);
	    	break;
		
	    case DROP:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, DROP);
	    	break;
		
	    case REMOVE:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, REMOVE);
	    	break;
		
	    case MCCTABLE:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, MCCTABLE);
	    	break;
		
	    case MCCRANGER:
	    	/* Fake table name "ranger" */
	    	rtn_stat = par_dropremovrebalanmcc_tab("ranger\0", MCCRANGER);
	    	break;
		
	    case REBALANCE:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, REBALANCE);
	    	break;
		
	    case SHARDING:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, SHARDING);
	    	break;

	    case SELECTWHERE:
	    	rtn_stat = par_selwhere_tab(s_str + s_idx, SELECTWHERE);
		break;

	    default:
	    	rtn_stat = FALSE;
	        break;
	}



	return rtn_stat;
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



CONSTANT *
par_get_constant_by_colname(TREE *command, char *colname)
{
	TREE	*tmpcommand;

	tmpcommand = command;
	
	while (tmpcommand)
	{
		command = tmpcommand;
		
		while(command)
		{
			if (   (PAR_NODE_IS_RESDOM(command->type))
			    && (!strcmp(command->sym.resdom.colname, colname)))
			{
				return &(command->right->sym.constant);
			}

			command = command->left;
		}

		tmpcommand = tmpcommand->right;
	}

	return NULL;
	
}


RESDOM *
par_get_resdom_by_colname(TREE *command, char *colname)
{
	TREE	*tmpcommand;

	tmpcommand = command;
	
	while (tmpcommand)
	{
		command = tmpcommand;
		
		while(command)
		{
			if (   (PAR_NODE_IS_RESDOM(command->type))
			    && (!strcmp(command->sym.resdom.colname, colname)))
			{
				return &(command->sym.resdom);
			}

			command = command->left;
		}

		tmpcommand = tmpcommand->right;
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
			traceprint("Table Name : %s\n", command->sym.command.tabname);
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

	
	while ((*s_str != '\0') && ((*s_str == ' ') || (*s_str == '\t')))
	{
		s_str++;
	}

	
	while(   (*s_str != '\0') && (*s_str != separator) && (*s_str != ' ') 
	      && (*s_str != '\t'))
	{
		start[len++] = *s_str++;
	}

	*s_idx = len;

	
	if(   (!strncasecmp("create", start, len))
	   || (!strncasecmp("insert", start, len))
	   || (!strncasecmp("add", start, len))
	   || (!strncasecmp("addsstab", start, len))
	   || (!strncasecmp("mcc", start, len)))
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
	
	if (!par_name_check(&(tab_name[start]), tab_name_len))
	{
		return FALSE;
	}

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	start = len;
	col_info = s_str + start;
	len =  str1nstr(col_info, ")\0", cmd_len - len);

	if (len < 2)
	{
		traceprint("Value is not allowed with NULL.\n");
		return FALSE;
	}

	str0n_trunc_0t(col_info, len - 1, &start, &end);

	
	int		rtn_stat;
	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype);

	return TRUE;
}


int
par_name_check(char *name, int len)
{
	int i = 0;

	while (i < len)
	{
		if (   (!(*(name + i) < '0') && (!(*(name + i) > '9')))
		     || (!(*(name + i) < 'a') && (!(*(name + i) > 'z')))
		     || (!(*(name + i) < 'A') && (!(*(name + i) > 'Z')))
		     || (*(name + i) == '_')
		     || (*(name + i) == '-')
		    )
		{
			i++;
			continue;
		}

		break;		
	}

	return (i == len) ? TRUE : FALSE;
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

	if (len < 2)
	{
		traceprint("Value is not allowed with NULL.\n");
		return FALSE;
	}

	MEMSET(tab_name, 64);


			
	MEMCPY(tab_name, s_str, len - 1);

	
	str0n_trunc_0t(tab_name, len - 1, &start, &end);
	tab_name_len = end - start;

	if (tab_name_len < 1)
	{
		traceprint("Table name not allowed with NULL.\n");
		return FALSE;
	}

	if (!par_name_check(&(tab_name[start]), tab_name_len))
	{
		return FALSE;
	}

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	start = len;
	col_info = s_str + start;

#ifdef MT_KEY_VALUE
	if (querytype == INSERT)
	{
		start = 0;

		end = *(int *)col_info;

		end += *(int *)(col_info + sizeof(int) + end + 1);

		end += (2 * sizeof(int) + 1);
	}
	else
	{
		len =  str1nstr(col_info, ")\0", cmd_len - start);

		if (len != (cmd_len - start))
		{
			
			return FALSE;
		}

		if (len < 2)
		{
			traceprint("Value is not allowed with NULL.\n");
			return FALSE;
		}

		str0n_trunc_0t(col_info, len - 1, &start, &end);
	}
#else
	len =  str1nstr(col_info, ")\0", cmd_len - start);

	if (len != (cmd_len - start))
	{
		
		return FALSE;
	}

	if (len < 2)
	{
		traceprint("Value is not allowed with NULL.\n");
		return FALSE;
	}

	str0n_trunc_0t(col_info, len - 1, &start, &end);

#endif	
	int		rtn_stat;
	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype);

	return rtn_stat;
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

	if (len < 2)
	{
		traceprint("Value is not allowed with NULL.\n");
		return FALSE;
	}

	MEMSET(tab_name, 64);
		
	MEMCPY(tab_name, s_str, len - 1);

	
	str0n_trunc_0t(tab_name, len - 1, &start, &end);
	tab_name_len = end - start;

	if (tab_name_len < 1)
	{
		traceprint("Table name not allowed with NULL.\n");
		return FALSE;
	}
	
	if (!par_name_check(&(tab_name[start]), tab_name_len))
	{
		return FALSE;
	}

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	start = len;
	col_info = s_str + start;
	len =  str1nstr(col_info, ")\0", cmd_len - len);

	if (len != (cmd_len - start))
	{
		
		return FALSE;
	}

	if (len < 2)
	{
		traceprint("Value is not allowed with NULL.\n");
		return FALSE;
	}
	
	str0n_trunc_0t(col_info, len - 1, &start, &end);

	
	int		rtn_stat;
	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype);

	return TRUE;
}




int 
par_selrange_tab(char *s_str, int querytype)
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

	if (len < 2)
	{
		traceprint("Value is not allowed with NULL.\n");
		return FALSE;
	}

	MEMSET(tab_name, 64);
		
	MEMCPY(tab_name, s_str, len - 1);

	
	str0n_trunc_0t(tab_name, len - 1, &start, &end);
	tab_name_len = end - start;

	if (tab_name_len < 1)
	{
		traceprint("Table name not allowed with NULL.\n");
		return FALSE;
	}
	
	if (!par_name_check(&(tab_name[start]), tab_name_len))
	{
		return FALSE;
	}

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	start = len;
	col_info = s_str + start;
	len =  str1nstr(col_info, ")\0", cmd_len - len);

	if (len != (cmd_len - start))
	{
		
		return FALSE;
	}

	if (len < 2)
	{
		traceprint("Value is not allowed with NULL.\n");
		return FALSE;
	}
	
	str0n_trunc_0t(col_info, len - 1, &start, &end);

	
	int		rtn_stat;
	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype);

	return TRUE;
}



int 
par_selwhere_tab(char *s_str, int querytype)
{
	LOCALTSS(tss);
	int		len;
	char		tab_name[64];
	int		cmd_len;
	char		tab_name_len;
	int		start;
	int		end;
	char		*col_info;
	TREE		*command;
	char		*cmd_str;
	char		colname[64];
	int		colnamelen;
	int		cmd_strlen;
	int		parser_result;
	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	len = 0;
	parser_result = FALSE;
	
	cmd_len = STRLEN(s_str);

	col_info = s_str;
	cmd_str = "where\0";
	cmd_strlen = STRLEN(cmd_str);
	
	len = str1nstr(s_str, cmd_str, cmd_len);

	if (len < 0)
	{
		traceprint("Value is not allowed with NULL.\n");
		return parser_result;
	}

	MEMSET(tab_name, 64);
		
	MEMCPY(tab_name, s_str, len - cmd_strlen);

	
	str0n_trunc_0t(tab_name, len - cmd_strlen, &start, &end);
	tab_name_len = end - start;

	if (tab_name_len < 1)
	{
		traceprint("Table name not allowed with NULL.\n");
		return parser_result;
	}
	
	if (!par_name_check(&(tab_name[start]), tab_name_len))
	{
		return parser_result;
	}

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);

	while (cmd_len)
	{
		
		command = tss->tcmd_parser;

		while(command->right)
		{
			command = command->right;
		}

		cmd_strlen = STRLEN(cmd_str);
		command->right = par_bld_cmd(cmd_str, cmd_strlen, querytype);
		
		
		
		start = len;
		col_info = &(col_info[start]);
		cmd_len -= start;

		len = str1nstr(col_info, "(\0", cmd_len);

		if (len < 2)
		{
			traceprint("Value is not allowed with NULL.\n");
			return parser_result;
		}

		MEMSET(colname, 64);
			
		MEMCPY(colname, col_info, len - 1);
		
		str0n_trunc_0t(colname, len - 1, &start, &end);
		colnamelen = end - start;
		char	*tmpcolname = &(colname[start]);

		if (colnamelen < 1)
		{
			traceprint("Column name not allowed with NULL.\n");
			return parser_result;
		}

		col_info = &(col_info[len]);
		cmd_len -= len;

		
		len =  str1nstr(col_info, ")\0", cmd_len);

		if (len < 2)
		{
			traceprint("Value is not allowed with NULL.\n");
			return parser_result;
		}
		
		str0n_trunc_0t(col_info, len - 1, &start, &end);

		
		if (!par_col_info4where((col_info + start), (end - start), querytype, tmpcolname))
		{
			return parser_result;
		}


		cmd_len -= len;

		if (!(cmd_len > 0))
		{
			parser_result = TRUE;
			break;
		}

		col_info = &(col_info[len]);

		str0n_trunc_0t(col_info, cmd_len, &start, &end);

		cmd_len -= start;
		col_info = &(col_info[start]);

		len = str1nstr(col_info, " \0", end - start);

		str0n_trunc_0t(col_info, len - 1, &start, &end);

		int op = par_op_where(col_info + start, end - start);

		switch (op)
		{
		    case OR:
		    	cmd_str = "OR\0";
			break;
		    case AND:
		    	cmd_str = "AND\0";
			break;

		    default:
		    	break;
		}
	}


	if (!parser_result)
	{
		traceprint("selectwhere parser hit error, please type help for information.\n");
	}

	return parser_result;
}


int
par_op_where(char *cmd, int len)
{
	if (!strncasecmp("OR", cmd, len))
	{
		return OR;
	}
	else if (!strncasecmp("AND", cmd, len))
	{
		return AND;
	}
	else if (!strncasecmp("WHERE", cmd, len))
	{
		return WHERE;
	}
	

	return 0;
}

int 
par_dropremovrebalanmcc_tab(char *s_str, int querytype)
{
	LOCALTSS(tss);
	char		tab_name[64];
	int		cmd_len;
	char		tab_name_len;
	int		start;
	int		end;
	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	cmd_len = STRLEN(s_str);

	if (cmd_len < 1)
	{
		traceprint("Command is not allowed with NULL.\n");
		return FALSE;
	}
	
	MEMSET(tab_name, 64);
		
	MEMCPY(tab_name, s_str, cmd_len);

	
	str0n_trunc_0t(tab_name, cmd_len, &start, &end);
	tab_name_len = end - start;

	if (tab_name_len < 1)
	{
		traceprint("Table name is not allowed with NULL.\n");
		return FALSE;
	}
	
	if (!par_name_check(&(tab_name[start]), tab_name_len))
	{
		return FALSE;
	}

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
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

	
	if (!par_name_check(&(tab_name[start]), tab_name_len))
	{
		return FALSE;
	}

	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	start = len;
	col_info = s_str + start;
	len =  str1nstr(col_info, ")\0", cmd_len - len);

	if (len != (cmd_len - start))
	{
		
		return FALSE;
	}

	if (len < 2)
	{
		traceprint("Value is not allowed with NULL.\n");
		return FALSE;
	}

	str0n_trunc_0t(col_info, len - 1, &start, &end);

	
	int		rtn_stat;
	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype);

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

#ifdef MT_KEY_VALUE
		if (querytype == INSERT)
		{
			
			int	val_len;

			val_len = *(int *)cmd;

			cmd = &(cmd[sizeof(int)]);

			cmd_len -= sizeof(int);

			len = val_len;
		}

		len = str01str(cmd, ",\0", cmd_len);
#else
		len = str01str(cmd, ",\0", cmd_len);
#endif
		if (len == -1)
		{
			traceprint("Value is not allowed with NULL.\n");
			return FALSE;
		}

		str0n_trunc_0t(cmd, len + 1, &start, &end);

		coldata = col_info = &cmd[start];

		if (start == end)
		{
			traceprint("Value is not allowed with NULL.\n");
			return FALSE;
		}

		if (querytype == TABCREAT)
		{
			i = 0;
			while((coldata[i] != ' ') && (coldata[i] != '\t'))
			{
				i++;
			}

			MEMSET(colname, 256);

			MEMCPY(colname, &(col_info[0]), i);

			if (!par_name_check(colname, i))
			{
				return FALSE;
			}
			
			str0n_trunc_0t(&(coldata[i]), (end - start - i), &start, &end);

			if (start == end)
			{
				traceprint("Value is not allowed with NULL.\n");
				return FALSE;
			}
			
			MEMSET(coltype, 64);
			MEMCPY(coltype, &(coldata[i + start]), (end - start));

			command = tss->tcmd_parser;

			while(command->left)
			{
				command = command->left;
			}

		        command->left = par_bld_resdom(colname, coltype, ++colid);
		}
		else if (   (querytype == INSERT) || (querytype == ADDSERVER) 
			 || (querytype == SELECT) || (querytype == ADDSSTAB) 
			 || (querytype == DELETE))
		{
			command = tss->tcmd_parser;

			while(command->left)
			{
				command = command->left;
			}

			colid++;
			if (rg_insert)
			{
				if (colid > tss->tmeta_hdr->col_num)
				{
					traceprint("Colum number is invalid in the insertion.\n");
					return FALSE;
				}
				colinfor = meta_get_colinfor(colid, 
						      tss->tmeta_hdr->col_num, 
						      tss->tcol_info);
			}

			command->left = par_bld_resdom(NULL, NULL, colid);

			command->left->right = par_bld_const(coldata, (end - start),
							rg_insert ? colinfor->col_type 
								      : INVALID_TYPE, 
							NULL, 0);
		}
		else if (querytype == SELECTRANGE)
		{
			command = tss->tcmd_parser;

			while(command->left)
			{
				command = command->left;
			}

			
			colid++;
			
			command->left = par_bld_resdom(NULL, NULL, colid);

			command->left->right = par_bld_const(coldata, (end - start),
							INVALID_TYPE, NULL, 0);
		}
		
		len += 2;
	}

	return TRUE;
}




int
par_col_info4where(char *cmd, int cmd_len, int querytype, char *colname)
{
	LOCALTSS(tss);
	int	start;
	int	end;
	int	len;
	TREE	*command;
	int	rg_insert;
	int	leftlen;
	int	rightlen;
	int	left_context;
	char	*leftdata;
	char	*rightdata;


	Assert(querytype == SELECTWHERE);
	
	rg_insert = ((tss->topid & TSS_OP_RANGESERVER) && (tss->topid & TSS_OP_INSTAB)) ? TRUE : FALSE;

	rightdata = NULL;
	leftdata = NULL;
	command = tss->tcmd_parser;

	
	while(command->right)
	{
		command = command->right;
	}

	while(command->left)
	{
		command = command->left;
	}

	
	command->left = par_bld_resdom(colname, NULL, -1);

	len = 0;
	left_context = TRUE;
	
	while((cmd_len -= len) > 0 )
	{
		cmd = &(cmd[len]);

		len = str01str(cmd, ",\0", cmd_len);

		if (len == -1)
		{
			traceprint("Value is not allowed with NULL.\n");
			return FALSE;
		}

		str0n_trunc_0t(cmd, len + 1, &start, &end);

		
		if (left_context)
		{
			leftdata = &cmd[start];
			leftlen = (end - start);
			left_context= FALSE;
		}
		else
		{
			rightdata = &cmd[start];
			rightlen = (end - start);
		}
		
		len +=2;
	}

	
	command->left->right = par_bld_const(leftdata, leftlen, INVALID_TYPE,
						rightdata, rightlen);
	
	return TRUE;
}

int
par_fill_colinfo(char *tab_dir, int colnum, TREE *command)
{
	char		tab_meta_dir[TABLE_NAME_MAX_LEN];
	int		fd1;
	int		col_buf_len;
	char		*col_buf;
	RESDOM		*resdom;

	
	MEMSET(tab_meta_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_meta_dir, tab_dir, STRLEN(tab_dir));
	str1_to_str2(tab_meta_dir, '/', "syscolumns");

	OPEN(fd1, tab_meta_dir, (O_RDONLY));

	if (fd1 < 0)
	{
		return FALSE;
	}
	
	col_buf_len = colnum * (sizeof(COLINFO));
	col_buf = MEMALLOCHEAP(col_buf_len);
	
	READ(fd1, col_buf, col_buf_len);

	CLOSE(fd1);

	
	int i;

	for (i = 0; i < colnum; i++)
	{
		resdom = par_get_resdom_by_colname(command, 
				((COLINFO *)col_buf)[i].col_name);

		if (resdom != NULL)
		{
			resdom->colid = ((COLINFO *)col_buf)[i].col_id;
			resdom->coloffset = ((COLINFO *)col_buf)[i].col_offset;
			resdom->coltype = ((COLINFO *)col_buf)[i].col_type;
		}
	}

	MEMFREEHEAP(col_buf);

	return TRUE;
}


ORANDPLAN *
par_get_orplan(TREE *command)
{
	ORANDPLAN	*orplan;
	ORANDPLAN	*orplanhdr;
	TREE		*tmpcommand;
	

	orplanhdr = orplan = NULL;
	tmpcommand = command;
	
	while (tmpcommand)
	{
		command = tmpcommand;
		
		Assert(PAR_NODE_IS_COMMAND(command->type));
		
		if (par_op_where(command->sym.command.tabname, 
				command->sym.command.tabname_len) == OR)
		{
			orplan = (ORANDPLAN *)MEMALLOCHEAP(sizeof(ORANDPLAN));
			MEMSET(orplan, sizeof(ORANDPLAN));

			orplan->orandsclause.scterms = command;

			if (orplanhdr == NULL)
			{
				orplanhdr = orplan;					
			}
			else
			{
				orplanhdr->orandplnext = orplan;
			}
		}

		tmpcommand = tmpcommand->right;
	}


	return orplanhdr;
	
}


ORANDPLAN *
par_get_andplan(TREE *command)
{
	ORANDPLAN	*andplan;
	ORANDPLAN	*andplanhdr;
	TREE		*tmpcommand;
	int		opid;
	

	andplanhdr = andplan = NULL;
	tmpcommand = command;
	
	while (tmpcommand)
	{
		command = tmpcommand;
		
		Assert(PAR_NODE_IS_COMMAND(command->type));

		opid = par_op_where(command->sym.command.tabname, 
					command->sym.command.tabname_len);

		if ((opid == AND) || (opid == WHERE))
		{
			andplan = (ORANDPLAN *)MEMALLOCHEAP(sizeof(ORANDPLAN));
			MEMSET(andplan, sizeof(ORANDPLAN));

			andplan->orandsclause.scterms = command;

			if (andplanhdr == NULL)
			{
				andplanhdr = andplan;					
			}
			else
			{
				andplan->orandplnext = andplanhdr->orandplnext;
				andplanhdr->orandplnext = andplan;				
			}
		}

		tmpcommand = tmpcommand->right;
	}


	return andplanhdr;
	
}


void
par_release_orandplan(ORANDPLAN *orandplan)
{
	ORANDPLAN	*tmp;

	
	while (orandplan)
	{
		tmp = orandplan;
		orandplan = orandplan->orandplnext;

		MEMFREEHEAP(tmp);
	}
}
