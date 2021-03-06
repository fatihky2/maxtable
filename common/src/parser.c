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

#include "master/metaserver.h"
#include "rpcfmt.h"
#include "parser.h"
#include "ranger/rangeserver.h"
#include "utils.h"
#include "tss.h"
#include "token.h"
#include "memcom.h"
#include "type.h"
#include "strings.h"
#include "metadata.h"
#include "file_op.h"
#include "row.h"


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
par_bld_resdom(char *colname, char *coltype, int col_id, short resdstat)
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

	resd_node->resdstat = resdstat;	

	
	
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
parser_open(char *s_str, int s_strlen)
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
		rtn_stat = par_crtins_tab((s_str + s_idx), s_strlen - s_idx, TABCREAT);
		break;
		
	    case INSERT:
	    	tss->topid |= TSS_OP_INSTAB;
		rtn_stat = par_crtins_tab((s_str + s_idx), s_strlen - s_idx, INSERT);
	        break;

	    case CRTINDEX:
	    	tss->topid |= TSS_OP_CRTINDEX;
	    	rtn_stat = par_crt_idx_tab((s_str + s_idx), CRTINDEX);	        
	        break;

	    case DROPINDEX:
	    	rtn_stat = par_dropremov_idx_tab((s_str + s_idx), DROPINDEX);
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
		
	    case DROPTAB:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, DROPTAB);
	    	break;
		
	    case REMOVETAB:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, REMOVETAB);
	    	break;

	    case REMOVEINDEX:
	    	rtn_stat = par_dropremov_idx_tab(s_str + s_idx, REMOVEINDEX);
	    	break;
		
	    case MCCTABLE:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, MCCTABLE);
		break;
		
	    case MCCSSTAB:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, MCCSSTAB);
	    	break;
		
	    case MCCRANGER:
	    	
	    	rtn_stat = par_dropremovrebalanmcc_tab("ranger\0", MCCRANGER);
	    	break;
		
	    case REBALANCE:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, REBALANCE);
	    	break;
		
	    case SHARDINGTABLE:
	    	rtn_stat = par_dropremovrebalanmcc_tab(s_str + s_idx, SHARDINGTABLE);
	    	break;

	    
	    case SHARDINGTABLET:	   
		rtn_stat = par_seldel_tab((s_str + s_idx), SHARDINGTABLET);
	    
		break;

	    case SELECTWHERE:
	    	tss->topid |= TSS_OP_SELWHERE;
	    	rtn_stat = par_selwherecnt_tab(s_str + s_idx, SELECTWHERE);
		break;

	    case DELETEWHERE:
	    	rtn_stat = par_selwherecnt_tab(s_str + s_idx, DELETEWHERE);
	    	break;

	    case SELECTCOUNT:
	    	rtn_stat = par_selwherecnt_tab(s_str + s_idx, SELECTCOUNT);
		break;

	    case UPDATE:
	    	tss->topid |= TSS_OP_UPDATE;
	    	rtn_stat = par_selwherecnt_tab(s_str + s_idx, UPDATE);
		break;

	    case SELECTSUM:
	    	rtn_stat = par_selwherecnt_tab(s_str + s_idx, SELECTSUM);
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
				if (!(command->sym.resdom.resdstat & RESDOM_SELECTSUM_COL))
				{
					return &(command->right->sym.constant);
				}
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



int
par_chk_fill_resdom(TREE *command, int colnum, COLINFO* col_buf)
{
	int	i;


	if (!PAR_NODE_IS_RESDOM(command->type))
	{
		return FALSE;
	}

	for (i = 0; i < colnum; i++)
	{
		if (!strcmp(command->sym.resdom.colname, col_buf[i].col_name))
		{
			command->sym.resdom.colid = col_buf[i].col_id;
			command->sym.resdom.coloffset = col_buf[i].col_offset;
			command->sym.resdom.coltype = col_buf[i].col_type;
				
			return TRUE;
		}
	}
	
	
	return FALSE;
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
	   || (!strncasecmp("drop", start, len))
	   || (!strncasecmp("remove", start, len))
	   || (!strncasecmp("mcc", start, len))
	   || (!strncasecmp("sharding", start, len))
	   || (!strncasecmp("update", start, len)))
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
	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype,
				NULL, 0);

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
par_crtins_tab(char *s_str, int s_strlen, int querytype)
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
	int		blob_datalen;
	char		*blob_data;


	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	len = 0;

	cmd_len = s_strlen;

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

	if (querytype == CRTINDEX)
	{
		
		command = tss->tcmd_parser;
	
		
		while(command->right)
		{
			command = command->right;
		}

		
		command->right = par_bld_cmd(&(tab_name[start]), 
						tab_name_len, querytype);
	}
	else
	{
	
		tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
						tab_name_len, querytype);
	}
	
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

	blob_datalen = cmd_len - start - len;

	blob_data = (blob_datalen> 0)? &(col_info[len]) : NULL;

#if 0	
	if (len != (cmd_len - start))
	{
		
		return FALSE;
	}
#endif

	if (len < 2)
	{
		traceprint("Value is not allowed with NULL.\n");
		return FALSE;
	}

	str0n_trunc_0t(col_info, len - 1, &start, &end);
	
#endif	
	int		rtn_stat;
	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype,
				blob_data, blob_datalen);

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
	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype,
				NULL, 0);

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

	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype,
				NULL, 0);

	return TRUE;
}



int 
par_selwherecnt_tab(char *s_str, int querytype)
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
	char		*coldata;
	int		coldatalen;
	TREE		*upd_cmd;
	int		is_like;
	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	upd_cmd = NULL;
	parser_result = FALSE;
	

	
	if (querytype == SELECTSUM)
	{
		len = 0;

		cmd_len = STRLEN(s_str);

		len = str1nstr(s_str, "(\0", cmd_len);		
		
		start = len;

		s_str += start;
		
		col_info = s_str;

		len =  str1nstr(col_info, ")\0", cmd_len - start);

		s_str += len;
		
		if (len < 2)
		{
			traceprint("Value is not allowed with NULL.\n");
			goto exit;
		}

		str0n_trunc_0t(col_info, len - 1, &start, &end);

		coldata = col_info + start;
		coldatalen = end - start;
	}
	
	else if (querytype == UPDATE)
	{
		while (TRUE)
		{
			
			len = 0;

			cmd_len = STRLEN(s_str);

			
			len = str01str(s_str, "(\0", cmd_len);

			if (len == -1)
			{
				traceprint("Value is not allowed with NULL.\n");
				return FALSE;
			}

			str0n_trunc_0t(s_str, len + 1, &start, &end);
			

			MEMSET(colname, 64);

			MEMCPY(colname, &s_str[start], end - start);

			
			len += 2;

			start = len;

			s_str += start;
			
			col_info = s_str;

			len =  str1nstr(col_info, ")\0", cmd_len - start);

			s_str += len;
			
			if (len < 2)
			{
				traceprint("Value is not allowed with NULL.\n");
				goto exit;
			}

			str0n_trunc_0t(col_info, len - 1, &start, &end);

			coldata = col_info + start;
			coldatalen = end - start;
			
			if (upd_cmd == NULL)
			{
				
				upd_cmd = par_bld_resdom(colname, NULL, -1, 0);

				
				upd_cmd->right = par_bld_const(coldata, 
								(end - start),
							INVALID_TYPE, NULL, 0);
			}
			else
			{
				command = upd_cmd;

				while(command->left)
				{
					command = command->left;
				}
				
				
				command->left = par_bld_resdom(colname, NULL,
							-1, 0);

				
				command->left->right = par_bld_const(coldata, 
							(end - start),
							INVALID_TYPE, NULL, 0);
			}
			
						
			str0n_trunc_0t(s_str, len + 1, &start, &end);

			if (s_str[start] != ',')
			{
				break;
			}			
		}
	}

	len = 0;
	cmd_len = STRLEN(s_str);
	col_info = s_str;
	cmd_str = "where\0";
	cmd_strlen = STRLEN(cmd_str);
	
	len = str1nstr(s_str, cmd_str, cmd_len);

	if (len < 0)
	{
		traceprint("Value is not allowed with NULL.\n");
		goto exit;
	}

	MEMSET(tab_name, 64);
		
	MEMCPY(tab_name, s_str, len - cmd_strlen);

	
	str0n_trunc_0t(tab_name, len - cmd_strlen, &start, &end);
	tab_name_len = end - start;

	if (tab_name_len < 1)
	{
		traceprint("Table name not allowed with NULL.\n");
		goto exit;
	}
	
	if (!par_name_check(&(tab_name[start]), tab_name_len))
	{
		goto exit;
	}

	
	tss->tcmd_parser = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);

	if (querytype == SELECTSUM)
	{
		command = tss->tcmd_parser;
		
		while(command->left)
		{
			command = command->left;
		}
	
		MEMSET(colname, 64);

		MEMCPY(colname, coldata, coldatalen);
		
		
		command->left = par_bld_resdom(colname, NULL, 1,
					RESDOM_SELECTSUM_COL);
	}
	else if (querytype == UPDATE)
	{
		Assert((tss->tcmd_parser) && (tss->tcmd_parser->left == NULL));
		
		tss->tcmd_parser->left = upd_cmd;
	}

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
			goto exit;
		}

		MEMSET(colname, 64);
			
		MEMCPY(colname, col_info, len - 1);
		
		str0n_trunc_0t(colname, len - 1, &start, &end);
		colnamelen = end - start;

		is_like = FALSE;
		
		
		if (!strncasecmp("LIKE", &(colname[end -4]), 4))
		{
			is_like = TRUE;

			colnamelen -= 4;
			
			MEMCPY(colname, &(colname[start]), colnamelen);
			
			str0n_trunc_0t(colname, colnamelen, &start, &end);
			colnamelen = end - start;
		}
		
		char	tmpcolname[32];

		
		Assert (colnamelen < 32);
		MEMSET(tmpcolname, 32);
		MEMCPY(tmpcolname,  &(colname[start]), colnamelen);

		if (colnamelen < 1)
		{
			traceprint("Column name not allowed with NULL.\n");
			goto exit;
		}

		col_info = &(col_info[len]);
		cmd_len -= len;

		
		len =  str1nstr(col_info, ")\0", cmd_len);

		if (len < 2)
		{
			traceprint("Value is not allowed with NULL.\n");
			goto exit;
		}
		
		str0n_trunc_0t(col_info, len - 1, &start, &end);

		if (!par_col_info4where((col_info + start), (end - start),
					querytype, tmpcolname, is_like))
		{
			goto exit;
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

exit:
	if (!parser_result)
	{
		traceprint("selectwhere parser hit error, please type help for information.\n");
	}

	return parser_result;
}




int 
par_crt_idx_tab(char *s_str, int querytype)
{
	LOCALTSS(tss);
	int		len;
	char		idx_name[64];
	int		cmd_len;
	char		idx_name_len;
	int		start;
	int		end;
	char		*cmd_str;
	int		cmd_strlen;
	int		parser_result;

	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	parser_result = FALSE;
		
	len = 0;
	cmd_len = STRLEN(s_str);
	cmd_str = "on\0";
	cmd_strlen = STRLEN(cmd_str);
	
	len = str1nstr(s_str, cmd_str, cmd_len);

	if (len < 0)
	{
		traceprint("Value is not allowed with NULL.\n");
		goto exit;
	}

	MEMSET(idx_name, 64);

	
	MEMCPY(idx_name, s_str, len - cmd_strlen);
	
	str0n_trunc_0t(idx_name, len - cmd_strlen, &start, &end);
	idx_name_len = end - start;

	if (idx_name_len < 1)
	{
		traceprint("Index name not allowed with NULL.\n");
		goto exit;
	}
	
	if (!par_name_check(&(idx_name[start]), idx_name_len))
	{
		goto exit;
	}

	
	tss->tcmd_parser = par_bld_cmd(&(idx_name[start]), 
					idx_name_len, querytype);

	if (par_crtins_tab(s_str + len, cmd_len - len, querytype))
	{
		parser_result = TRUE;
	}

exit:	
	if (!parser_result)
	{
		traceprint("create index parser hit error, please type help for information.\n");
	}

	return parser_result;
}





int 
par_dropremov_idx_tab(char *s_str, int querytype)
{
	LOCALTSS(tss);
	int		len;
	char		idx_name[64];
	int		cmd_len;
	char		idx_name_len;
	int		start;
	int		end;
	char		*cmd_str;
	int		cmd_strlen;
	int		parser_result;
	char		tab_name[64];
	int		tab_name_len;
	TREE		*command;

	

	if (s_str == NULL || (STRLEN(s_str) == 0))
	{
		return FALSE;
	}

	parser_result = FALSE;
		
	len = 0;
	cmd_len = STRLEN(s_str);
	cmd_str = "on\0";
	cmd_strlen = STRLEN(cmd_str);
	
	len = str1nstr(s_str, cmd_str, cmd_len);

	if (len < 0)
	{
		traceprint("Value is not allowed with NULL.\n");
		goto exit;
	}

	MEMSET(idx_name, 64);

	
	MEMCPY(idx_name, s_str, len - cmd_strlen);
	
	str0n_trunc_0t(idx_name, len - cmd_strlen, &start, &end);
	idx_name_len = end - start;

	if (idx_name_len < 1)
	{
		traceprint("Index name not allowed with NULL.\n");
		goto exit;
	}
	
	if (!par_name_check(&(idx_name[start]), idx_name_len))
	{
		goto exit;
	}

	
	tss->tcmd_parser = par_bld_cmd(&(idx_name[start]), 
					idx_name_len, querytype);

	
	s_str += len;
	
	cmd_len = STRLEN(s_str);

	MEMSET(tab_name, 64);

					
	MEMCPY(tab_name, s_str, cmd_len);

	
	str0n_trunc_0t(tab_name, cmd_len, &start, &end);
	
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

	command = tss->tcmd_parser;

	
	while(command->right)
	{
		command = command->right;
	}
	
	command->right = par_bld_cmd(&(tab_name[start]), 
					tab_name_len, querytype);
	
	parser_result = TRUE;
exit:	
	if (!parser_result)
	{
		traceprint("drop index parser hit error, please type help for information.\n");
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
	else if (!strncasecmp("LIKE", cmd, len))
	{
		return LIKE;
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
	
	rtn_stat = par_col_info((col_info + start), (end - start), querytype,
				NULL, 0);

	return TRUE;
}


int
par_col_info(char *cmd, int cmd_len, int querytype, char *blob_data, int blob_datalen)
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
	rg_insert = (   (tss->topid & TSS_OP_RANGESERVER)
		     && (tss->topid & TSS_OP_INSTAB)) ? TRUE : FALSE;
	
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

			
		        command->left = par_bld_resdom(colname, coltype, ++colid, 0);
		}
		else if (   (querytype == INSERT) || (querytype == ADDSERVER) 
			 || (querytype == SELECT) || (querytype == ADDSSTAB) 
			 || (querytype == DELETE) || (querytype == SHARDINGTABLET))
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
				colinfor = meta_get_colinfor(colid, NULL, 
						      tss->tmeta_hdr->col_num, 
						      tss->tcol_info);
			}

			command->left = par_bld_resdom(NULL, NULL, colid, 0);

			if (rg_insert && TYPE_IS_BLOB(colinfor->col_type))
			{
				Assert((blob_data != NULL) && (blob_datalen > 0));
				
				char	*this_blob;
				int	this_blob_len;

				
				this_blob = &(blob_data[*(int *)(coldata + 1)]);

				this_blob_len = *(int *)(coldata + sizeof(int) + 1);
				
				command->left->right = par_bld_const(this_blob, 
								this_blob_len,
								colinfor->col_type,
								NULL, 0);	
		
			}
			else
			{
				command->left->right = par_bld_const(coldata, 
								(end - start),
								rg_insert ? colinfor->col_type 
								      : INVALID_TYPE, 
								NULL, 0);
			}
		}
		else if (querytype == SELECTRANGE)
		{
			command = tss->tcmd_parser;

			while(command->left)
			{
				command = command->left;
			}

			
			colid++;
			
			command->left = par_bld_resdom(NULL, NULL, colid, 0);

			command->left->right = par_bld_const(coldata, (end - start),
							INVALID_TYPE, NULL, 0);
		}
		else if (querytype == CRTINDEX)
		{
			command = tss->tcmd_parser;

			while(command->right)
			{
				command = command->right;
			}

			while(command->left)
			{
				command = command->left;
			}

			MEMSET(colname, 256);

			MEMCPY(colname, coldata, (end - start));
			
			command->left = par_bld_resdom(colname, NULL, -1, 0);
		}
		
		len += 2;
	}

	return TRUE;
}




int
par_col_info4where(char *cmd, int cmd_len, int querytype, char *colname,
			int is_like)
{
	LOCALTSS(tss);
	int	start;
	int	end;
	int	len;
	TREE	*command;
	int	leftlen;
	int	rightlen;
	int	left_context;
	char	*leftdata;
	char	*rightdata;
	int	rg_selwhere;
	COLINFO	*colinfor;


	Assert(   (querytype == SELECTWHERE) || (querytype == SELECTCOUNT)
	       || (querytype == SELECTSUM) || (querytype == DELETEWHERE)
	       || (querytype == UPDATE));
	
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

	
	command->left = par_bld_resdom(colname, NULL, -1, 0);

	len = 0;
	left_context = TRUE;
	
	while((cmd_len -= len) > 0 )
	{
		cmd = &(cmd[len]);

		if (is_like)
		{
			len = str01str(cmd, ")\0", cmd_len);
		}
		else
		{
			len = str01str(cmd, ",\0", cmd_len);
		}
		
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

		if (is_like)
		{
			break;
		}
		
		len +=2;
	}

	rg_selwhere = (   (tss->topid & TSS_OP_RANGESERVER) 
		       && (tss->topid & TSS_OP_SELWHERE)) ? TRUE : FALSE;

	if (rg_selwhere)
	{
		colinfor = meta_get_colinfor(0, colname, tss->ttab_hdr->tab_col,
						      tss->tcol_info);
	}
	
	command->left->right = par_bld_const(leftdata, leftlen, 
						rg_selwhere ? colinfor->col_type 
								: INVALID_TYPE,
						rightdata, rightlen);

	if (is_like)
	{
		struct constant *const_node = 
			(struct constant *)(&(command->left->right)->sym);

		const_node->constat |= CONSTANT_LIKE_OP;
	}
	
	return TRUE;
}


int
par_fill_colinfo(int colnum, COLINFO* col_buf, TREE *command)
{
	TREE	*tmpcommand;
	TREE	*rootcommand;
	

	tmpcommand = rootcommand = command;

	while (tmpcommand)
	{
		command = tmpcommand;
		
		while(command)
		{
			if (PAR_NODE_IS_RESDOM(command->type))
			{
				if (!par_chk_fill_resdom(command, colnum,
							col_buf))
				{
					
					return FALSE;
				}
			}			

			
			command = command->left;
		}

		
		tmpcommand = tmpcommand->right;
	}

	
	if (rootcommand->sym.command.querytype == SELECTSUM)
	{
		
		Assert (PAR_NODE_IS_RESDOM(rootcommand->left->type));

		if (!TYPE_IS_FIXED(rootcommand->left->sym.resdom.coltype))
		{
			traceprint("SELECTSUM can only be on the FIXED column.\n");

			return FALSE;
		}
	}	
	
	return TRUE;
}


ORANDPLAN *
par_get_orplan(TREE *command)
{
	ORANDPLAN	*orplan;
	ORANDPLAN	*orplanhdr;
	TREE		*tmpcommand;
	int		opid;
	int		first_cmd;
	

	orplanhdr = orplan = NULL;
	tmpcommand = command;
	first_cmd = FALSE;
	
	while (tmpcommand)
	{
		command = tmpcommand;
		
		Assert(PAR_NODE_IS_COMMAND(command->type));
		
		opid = par_op_where(command->sym.command.tabname, 
					command->sym.command.tabname_len);
		
		if ((opid == OR) || (opid == WHERE))
		{
			orplan = (ORANDPLAN *)MEMALLOCHEAP(sizeof(ORANDPLAN));
			MEMSET(orplan, sizeof(ORANDPLAN));

			orplan->orandsclause.scterms = command;

			if (orplanhdr == NULL)
			{
				orplanhdr = orplan;					

				if (opid == WHERE)
				{
					
					first_cmd = TRUE;
				}
			}
			else
			{
				if (first_cmd)
				{
					first_cmd = FALSE;
				}

				orplan->orandplnext = orplanhdr->orandplnext;
				orplanhdr->orandplnext = orplan;
				
			}
		}
		
		else if (opid == AND)
		{
			if (first_cmd)
			{
				MEMFREEHEAP(orplanhdr);

				orplanhdr = NULL;

				first_cmd = FALSE;
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
	int		first_cmd;
	

	andplanhdr = andplan = NULL;
	tmpcommand = command;
	first_cmd = FALSE;
	
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

				if (opid == WHERE)
				{
					
					first_cmd = TRUE;
				}
			}
			else
			{
				if (first_cmd)
				{
					first_cmd = FALSE;
				}
				
				andplan->orandplnext = andplanhdr->orandplnext;
				andplanhdr->orandplnext = andplan;				
			}
		}
		else if(opid == OR)
		{
			if (first_cmd)
			{
				MEMFREEHEAP(andplanhdr);

				andplanhdr = NULL;

				first_cmd = FALSE;
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

int
par_process_orplan(ORANDPLAN *cmd, char *rp, int minrowlen)
{
	int		coloffset;
	int		length;
	char		*colp;
	char		*leftval;
	int		leftvallen;
	char		*rightval;
	int		rightvallen;
	int		result;
	int		rtn_stat;
	int		coltype;
	SRCHCLAUSE	*srchclause;

	
	if (cmd == NULL)
	{
		return TRUE;
	}

	rtn_stat = FALSE;
	
	while(cmd)
	{
		srchclause = &(cmd->orandsclause);
		
		coloffset = srchclause->scterms->left->sym.resdom.coloffset;
		coltype = srchclause->scterms->left->sym.resdom.coltype;

		colp = row_locate_col(rp, coloffset, minrowlen, &length);

		leftval = srchclause->scterms->left->right->sym.constant.value;
		leftvallen = srchclause->scterms->left->right->sym.constant.len;
		rightval = srchclause->scterms->left->right->sym.constant.rightval;
		rightvallen = srchclause->scterms->left->right->sym.constant.rightlen;

		if (srchclause->scterms->left->right->sym.constant.constat 
							& CONSTANT_LIKE_OP)
		{	
			char	tmp[128];

			

			MEMCPY(tmp, leftval, leftvallen);
			tmp[leftvallen] = '\0';
			
			if (str01str(colp, tmp, length) == (length - 1))
			{
				
				cmd = cmd->orandplnext;
				continue;
			}

			
			rtn_stat = TRUE;
			
			break;
		}

		result = row_col_compare(coltype, colp, length, leftval, 
					leftvallen);

		if (result == LE)
		{
			
			cmd = cmd->orandplnext;
			continue;
		}

		result = row_col_compare(coltype, colp, length, rightval, 
					rightvallen);

		if (result == GR)
		{
			cmd = cmd->orandplnext;
			continue;
		}

		
		rtn_stat = TRUE;

		break;
		
	}

	return rtn_stat;
}


int
par_process_andplan(ORANDPLAN *cmd, char *rp, int minrowlen)
{
	int		coloffset;
	int		length;
	char		*colp;
	char		*leftval;
	int		leftvallen;
	char		*rightval;
	int		rightvallen;
	int		result;
	int		rtn_stat;
	int		coltype;
	SRCHCLAUSE	*srchclause;
	int		colid;

	
	if (cmd == NULL)
	{
		return PAR_ANDPLAN_HIT;
	}

	rtn_stat = PAR_ANDPLAN_NOHIT;
		
	while(cmd)
	{
		srchclause = &(cmd->orandsclause);
		
		coloffset = srchclause->scterms->left->sym.resdom.coloffset;
		coltype = srchclause->scterms->left->sym.resdom.coltype;
		colid = srchclause->scterms->left->sym.resdom.colid;

		colp = row_locate_col(rp, coloffset, minrowlen, &length);

		leftval = srchclause->scterms->left->right->sym.constant.value;
		leftvallen = srchclause->scterms->left->right->sym.constant.len;
		rightval = srchclause->scterms->left->right->sym.constant.rightval;
		rightvallen = srchclause->scterms->left->right->sym.constant.rightlen;

		if (srchclause->scterms->left->right->sym.constant.constat 
							& CONSTANT_LIKE_OP)
		{
			char	tmp[128];

			

			MEMCPY(tmp, leftval, leftvallen);
			tmp[leftvallen] = '\0';
			
			if (str01str(colp, tmp, length) == (length - 1))
			{
				
				break;
			}
		}
		else
		{		
			if (strncasecmp("*", leftval, leftvallen) != 0)
			{
				result = row_col_compare(coltype, colp, length,
							leftval, leftvallen);

				if (result == LE)
				{
					break;
				}
			}

			if (strncasecmp("*", rightval, rightvallen) != 0)
			{
				result = row_col_compare(coltype, colp, 
						length, rightval, rightvallen);

				if (result == GR)
				{
					
					if (colid == 1)
					{
						rtn_stat = PAR_ANDPLAN_HIT_BOUND;
					}
					
					break;
				}
			}
		}
		
		cmd = cmd->orandplnext;
		
	}

	if (cmd == NULL)
	{
		rtn_stat = PAR_ANDPLAN_HIT;
	}

	return rtn_stat;
}

int
par_fill_resd(TREE *command, COLINFO *colinfor, int totcol)
{
	COLINFO		*col_info;
	int		colid;

	while(command)
	{
		if (PAR_NODE_IS_RESDOM(command->type))
		{
			colid = command->sym.resdom.colid;
			col_info = meta_get_colinfor(colid, NULL, totcol, colinfor);

			Assert(col_info);

			command->sym.resdom.coloffset = col_info->col_offset;
			command->sym.resdom.coltype = col_info->col_type;
		}

		command = command->left;
	}

	return TRUE;

}

