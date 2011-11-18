#include "global.h"
#include "utils.h"
#include "master/metaserver.h"
#include "ranger/rangeserver.h"
#include "netconn.h"
#include "conf.h"
#include "token.h"
#include "tss.h"
#include "parser.h"
#include "memcom.h"
#include "strings.h"
#include "trace.h"
#include "row.h"
#include "type.h"
#include "interface.h"
#include "row.h"


extern	TSS	*Tss;

extern int 
sel_resp_rejoin(char * src_buf, char * dest_buf, int src_len, int * dest_len, char *index_buf, int querytype);

static int 
cli_prt_help(char *cmd);

int validation_request(char * request)
{
    //to be do.....
    return TRUE;
}


/*
** create one connection between cli and svr, return the connection
*/
int cli_connection(char * meta_ip, int meta_port, conn ** connection)
{
	mem_init_alloc_regions();
	tss_setup(TSS_OP_CLIENT);

	conn * new_conn = (conn *)MEMALLOCHEAP(sizeof(conn));
	new_conn->meta_server_port = meta_port;
	strcpy(new_conn->meta_server_ip, meta_ip);

	if((new_conn->connection_fd = conn_open(meta_ip, meta_port)) < 0)
	{
		perror("error in create connection: ");
		MEMFREEHEAP(new_conn);
		return FALSE;
	}
	
	new_conn->status = ESTABLISHED;
	new_conn->rg_list_len = 0;

	*connection = new_conn;

	return TRUE;
}

/*
** close one connection between cli and svr
*/
void cli_exit(conn * connection)
{
	int i;


	close(connection->connection_fd);
	
	for(i = 0; i < connection->rg_list_len; i++)
	{
		if(connection->rg_list[i]->status == ESTABLISHED)
		{
			close(connection->rg_list[i]->connection_fd);
		}
		
		MEMFREEHEAP(connection->rg_list[i]);
	}
	MEMFREEHEAP(connection);

	mem_free_alloc_regions();
}

/*
** commit one request
*/
int cli_commit(conn * connection, char * cmd, char * response, int * resp_len)
{
	LOCALTSS(tss);

	char		send_buf[LINE_BUF_SIZE];
	char		send_rg_buf[LINE_BUF_SIZE];
	char		tab_name[64];
	int		send_buf_size;

	RPCRESP		*resp;
	RPCRESP		*rg_resp;
	RPCRESP		*sstab_split_resp;
	RPCRESP		*remove_tab_resp;
	int		querytype;
	int		rtn_stat;
	int		sstab_split;
	int		remove_tab_hit;
	int		retry_cnt;
	

	if(!validation_request(cmd))
	{
		return FALSE;
	}

	if (cli_prt_help(cmd))
	{
		return TRUE;
	}

	rtn_stat = TRUE;
	sstab_split = FALSE;
	remove_tab_hit = FALSE;
	retry_cnt = 0;

	//querytype = par_get_query(cmd, &querytype_index);
	if(!parser_open(cmd))
	{
		parser_close();
		tss->tstat |= TSS_PARSER_ERR;
		traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
		return FALSE;
	}

	querytype = ((TREE *)(tss->tcmd_parser))->sym.command.querytype;
	MEMSET(tab_name, 64);
	MEMCPY(tab_name, ((TREE *)(tss->tcmd_parser))->sym.command.tabname,
	((TREE *)(tss->tcmd_parser))->sym.command.tabname_len);

retry:

#ifdef MT_KEY_VALUE
	if (querytype == INSERT)
	{		
		int cmd_len = STRLEN(cmd);

		cmd_len = str1nstr(cmd, "(\0", cmd_len);

		char	*col_info = cmd + cmd_len;
		send_buf_size = *(int *)col_info;

		send_buf_size += *(int *)(col_info + sizeof(int) + send_buf_size + 1);

		send_buf_size += (2 * sizeof(int) + 1) + cmd_len;
	}
	else
	{
		send_buf_size = strlen(cmd);
	}
#else
	send_buf_size = strlen(cmd);
#endif
	MEMSET(send_buf, LINE_BUF_SIZE);
	MEMCPY(send_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	MEMCPY(send_buf + RPC_MAGIC_MAX_LEN, cmd, send_buf_size);
			
	write(connection->connection_fd, send_buf, (send_buf_size + RPC_MAGIC_MAX_LEN));

	resp = conn_recv_resp(connection->connection_fd);
	if (resp->status_code != RPC_SUCCESS)
	{
		traceprint("\n ERROR in response \n");
		rtn_stat = FALSE;
		goto finish;

	}

	if(   (querytype == INSERT) || (querytype == SELECT) || (querytype == DELETE) 
	   || (querytype == SELECTRANGE) || (querytype == DROP) || (querytype == SELECTWHERE))
	{
		INSMETA		*resp_ins;
		SELRANGE	*resp_selrg;
		SELWHERE	*resp_selwh;
		
		if (querytype == SELECTRANGE)
		{			
			resp_selrg = (SELRANGE *)resp->result;
			resp_ins = &(resp_selrg->left_range);
		}
		else if (querytype == SELECTWHERE)
		{
			/*TODO: select where clause. */
			resp_selwh= (SELWHERE *)resp->result;
		}
		else
		{
			resp_ins = (INSMETA *)resp->result;
		}
		
		rg_conn * rg_connection;
		int i;
		//printf("rg server: %s/%d\n", resp_ins->i_hdr.rg_info.rg_addr, resp_ins->i_hdr.rg_info.rg_port);
		for(i = 0; i < connection->rg_list_len; i++)
		{
			if((resp_ins->i_hdr.rg_info.rg_port == connection->rg_list[i]->rg_server_port)
			     &&(!strcmp(resp_ins->i_hdr.rg_info.rg_addr, connection->rg_list[i]->rg_server_ip))
			     &&(connection->rg_list[i]->status == ESTABLISHED))
			{
				rg_connection = connection->rg_list[i];
				break;
			}
		}
		
		if(i == connection->rg_list_len)
		{
			rg_connection = (rg_conn *)MEMALLOCHEAP(sizeof(rg_conn));
			rg_connection->rg_server_port = resp_ins->i_hdr.rg_info.rg_port;
			strcpy(rg_connection->rg_server_ip, resp_ins->i_hdr.rg_info.rg_addr);

			if((rg_connection->connection_fd = conn_open(rg_connection->rg_server_ip, rg_connection->rg_server_port)) < 0)
			{
				perror("error in create connection with rg server: ");

				MEMFREEHEAP(rg_connection);
				rtn_stat = FALSE;
				goto finish;

			}
			
			rg_connection->status = ESTABLISHED;

			connection->rg_list[connection->rg_list_len] = rg_connection;
			connection->rg_list_len++;

		}

		if (querytype == SELECTRANGE)
		{
			MEMCPY(resp->result, RPC_SELECTRANGE_MAGIC, RPC_MAGIC_MAX_LEN);
		}
		else if (querytype == DROP)
		{
			MEMCPY(resp->result, RPC_DROP_TABLE_MAGIC, RPC_MAGIC_MAX_LEN);
		}
		else
		{
			MEMCPY(resp->result, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
		}
		
		MEMSET(send_rg_buf, LINE_BUF_SIZE);
		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);

		if (querytype == DROP)
		{
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, resp->result, RPC_MAGIC_MAX_LEN);
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + RPC_MAGIC_MAX_LEN, cmd, send_buf_size);

			write(rg_connection->connection_fd, send_rg_buf, 
		        		(RPC_MAGIC_MAX_LEN + send_buf_size + RPC_MAGIC_MAX_LEN));
		}
		else
		{
		
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, resp->result, resp->result_length);
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + resp->result_length, cmd, send_buf_size);

			write(rg_connection->connection_fd, send_rg_buf, 
					(resp->result_length + send_buf_size + RPC_MAGIC_MAX_LEN));
		}
		

		rg_resp = conn_recv_resp_abt(rg_connection->connection_fd);

		if (rg_resp->status_code == RPC_UNAVAIL)
		{
			traceprint("\n need to re-get rg meta \n");
			conn_destroy_resp(resp);

			rg_connection->status = CLOSED;
			conn_close(rg_connection->connection_fd, NULL, rg_resp);
			
			sleep(HEARTBEAT_INTERVAL + 1);
			goto retry;

		}

		if (rg_resp->status_code == RPC_RETRY)
                {
                        traceprint("\n need to try \n");
                        conn_destroy_resp(resp);
			conn_destroy_resp(rg_resp);
			retry_cnt++;

			if (retry_cnt > 5)
			{
				traceprint("\n Retry Fail\n");
				rtn_stat = FALSE;
				goto finish;
			}

			sleep(5);
					
                        goto retry;

                }
		
		if (rg_resp->status_code != RPC_SUCCESS)
		{
			traceprint("\n ERROR in rg_server response \n");
			rtn_stat = FALSE;
			goto finish;

		}

		if((querytype == INSERT) && (rg_resp->result_length))
		{
			char *cli_add_sstab = "addsstab into ";
			int new_size = rg_resp->result_length + 128 + STRLEN(cli_add_sstab);

			char * new_buf = MEMALLOCHEAP(new_size);
			MEMSET(new_buf, new_size);
			
			char newsstabname[SSTABLE_NAME_MAX_LEN];

			MEMSET(newsstabname, SSTABLE_NAME_MAX_LEN);

			MEMCPY(newsstabname, rg_resp->result, SSTABLE_NAME_MAX_LEN);

			int sstab_id = *(int *)(rg_resp->result + SSTABLE_NAME_MAX_LEN);

			int split_ts = *(int *)(rg_resp->result + SSTABLE_NAME_MAX_LEN + sizeof(int));

			int split_sstabid = *(int *)(rg_resp->result + SSTABLE_NAME_MAX_LEN + sizeof(int) + sizeof(int));

			int sstab_keylen = rg_resp->result_length - SSTABLE_NAME_MAX_LEN - sizeof(int) - sizeof(int) - sizeof(int) + 1;
			char *sstab_key = MEMALLOCHEAP(sstab_keylen);
			MEMSET(sstab_key, sstab_keylen);
			MEMCPY(sstab_key, rg_resp->result + SSTABLE_NAME_MAX_LEN + sizeof(int) + sizeof(int) + sizeof(int), 
				sstab_keylen - 1);

			sprintf(new_buf, "addsstab into %s (%s, %d, %d, %d, %s)", tab_name, newsstabname, 
									sstab_id, split_ts, split_sstabid, sstab_key);

			MEMSET(send_buf, LINE_BUF_SIZE);
			MEMCPY(send_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
			MEMCPY(send_buf + RPC_MAGIC_MAX_LEN, new_buf, new_size);

			MEMFREEHEAP(sstab_key);

			write(connection->connection_fd, send_buf, 
						(new_size + RPC_MAGIC_MAX_LEN));


			sstab_split_resp = conn_recv_resp(connection->connection_fd);
			sstab_split = TRUE;
			if (sstab_split_resp->status_code != RPC_SUCCESS)
			{
				traceprint("\n ERROR in meta_server response \n");
				rtn_stat = FALSE;
				MEMFREEHEAP(new_buf);
				goto finish;

			}

			MEMFREEHEAP(new_buf);

		}



		if (querytype == DROP)
		{	
			Assert(rg_resp->result_length);
			
			char *cli_remove_tab = "remove ";				    

			int new_size = TABLE_NAME_MAX_LEN + STRLEN(cli_remove_tab);
			char *new_buf = MEMALLOCHEAP(new_size);				    

			MEMSET(new_buf, new_size);

			sprintf(new_buf, "remove %s", tab_name);

			MEMSET(send_buf, LINE_BUF_SIZE);
			MEMCPY(send_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
			MEMCPY(send_buf + RPC_MAGIC_MAX_LEN, new_buf, new_size);

			write(connection->connection_fd, send_buf, 
						(new_size + RPC_MAGIC_MAX_LEN));


			remove_tab_resp = conn_recv_resp(connection->connection_fd);
			remove_tab_hit = TRUE;
			if (remove_tab_resp->status_code != RPC_SUCCESS)
			{
				traceprint("\n ERROR in meta_server response \n");
				rtn_stat = FALSE;
				MEMFREEHEAP(new_buf);
				goto finish;

			}

			MEMFREEHEAP(new_buf);
		}
			
	}

	if(querytype == SELECT)
	{
		//sel_resp_rejoin(rg_resp->result, response, rg_resp->result_length, resp_len, resp->result, querytype);
		*resp_len = rg_resp->result_length;
		strcpy(response, rg_resp->result);
	}
	else if (querytype == SELECTRANGE)
	{
		char	*rp;
		int	rlen;
		int	result_len = 0;
		int	rowcnt;
		int	row_idx = 0;
		char	rowbp[512];
		int	rlen_t;

		rowcnt = *(int *)(rg_resp->result);
		rp = rg_resp->result + sizeof(int);

		while(row_idx < rowcnt)
		{
			rlen = *(int *)rp;
			rp += sizeof(int);

			result_len += (rlen + sizeof(int));

			Assert(result_len < rg_resp->result_length);

			MEMSET(rowbp, 512);
			sel_resp_rejoin(rp, rowbp, rlen, &rlen_t, resp->result, querytype);

			printf(" %s\n", rowbp);
		
			rp += rlen;
			row_idx++;
		}
		
		*resp_len = sizeof(SUC_RET);
		strcpy(response, SUC_RET);
	}
	else
	{
		*resp_len = sizeof(SUC_RET);
		strcpy(response, SUC_RET);
	}

finish:

	conn_destroy_resp(resp);
	
	if(rg_resp && ((querytype == INSERT) || (querytype == SELECT) ||(querytype == DELETE)
		|| (querytype == SELECTRANGE)))
	{
		conn_destroy_resp(rg_resp);
	}

	if (sstab_split)
	{
		Assert(querytype == INSERT);
		conn_destroy_resp(sstab_split_resp);
	}

	
	if (remove_tab_hit)
	{
		Assert(querytype == DROP);
		conn_destroy_resp(remove_tab_resp);
	}
		

	parser_close();    

	return rtn_stat;
}


int sel_resp_rejoin(char * src_buf, char * dest_buf, int src_len, int * dest_len, char *index_buf, int querytype)
{
	char col_off_tab[COL_OFFTAB_MAX_SIZE];
	int col_off_idx = COL_OFFTAB_MAX_SIZE;
	INSMETA 	*ins_meta;
	SELRANGE	*resp_selrg;

	if (querytype == SELECTRANGE)
	{			
		resp_selrg = (SELRANGE *)index_buf;
		ins_meta = &(resp_selrg->left_range);
		index_buf += sizeof(SELRANGE);
	}
	else
	{
		ins_meta = (INSMETA *)index_buf;
		index_buf += sizeof(INSMETA);
	}	

	TABLEHDR *tab_hdr = (TABLEHDR *)index_buf;
	index_buf += sizeof(TABLEHDR);

	COLINFO *col_info = (COLINFO *)index_buf;

	int i, src_buf_index1, src_buf_index2, dest_buf_index;

	int volcol_count = 1;

	int offset = sizeof(ROWFMT);

	src_buf_index1 = 0;
	src_buf_index2 = src_len - sizeof(int);
	dest_buf_index = 0;

	col_off_idx -= sizeof(int);
	//column number
	*((int *)(col_off_tab + col_off_idx)) = tab_hdr->tab_col;

	for(i = 0; i < tab_hdr->tab_col; i++)
	{
		int col_type = (col_info+i)->col_type;
		
		if(TYPE_IS_FIXED(col_type))
		{
			MEMCPY(dest_buf + dest_buf_index, src_buf + src_buf_index1, TYPE_GET_LEN(col_type));

			col_off_idx -= sizeof(int);
			*((int *)(col_off_tab + col_off_idx)) = dest_buf_index;

			dest_buf_index += TYPE_GET_LEN(col_type);
			src_buf_index1 += TYPE_GET_LEN(col_type);
		}
		else
		{
			int valcol_len = (ins_meta->varcol_num == volcol_count)?
			           src_buf_index2 - (*((int *)(src_buf + src_buf_index2)) - offset):
			           *((int *)(src_buf + src_buf_index2 - sizeof(int))) - *((int *)(src_buf + src_buf_index2));
			MEMCPY(dest_buf + dest_buf_index, src_buf + *((int *)(src_buf + src_buf_index2)) - offset, valcol_len);

			col_off_idx -= sizeof(int);
			*((int *)(col_off_tab + col_off_idx)) = dest_buf_index;

			dest_buf_index += valcol_len;
			src_buf_index2 -= sizeof(int);
			volcol_count ++;
		}
	}

	if (COL_OFFTAB_MAX_SIZE > col_off_idx)
	{
		MEMCPY(dest_buf + dest_buf_index, col_off_tab + col_off_idx, COL_OFFTAB_MAX_SIZE - col_off_idx);
		dest_buf_index += (COL_OFFTAB_MAX_SIZE - col_off_idx);
	}

	*dest_len = dest_buf_index;

	return TRUE;
}


int
cli_rgsel_meta(conn * connection, char * cmd, SELCTX *resp_selctx)
{
	LOCALTSS(tss);

	char		send_buf[LINE_BUF_SIZE];
	char		tab_name[64];
	int		send_buf_size;
	RPCRESP		*resp;
	int		querytype;
	int		rtn_stat;
	

	if(!validation_request(cmd))
	{
		return FALSE;
	}

	if (cli_prt_help(cmd))
	{
		return FALSE;
	}

	rtn_stat = TRUE;

	if(!parser_open(cmd))
	{
		parser_close();
		tss->tstat |= TSS_PARSER_ERR;
		traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
		return FALSE;
	}

	querytype = ((TREE *)(tss->tcmd_parser))->sym.command.querytype;
	MEMSET(tab_name, 64);
	MEMCPY(tab_name, ((TREE *)(tss->tcmd_parser))->sym.command.tabname,
	((TREE *)(tss->tcmd_parser))->sym.command.tabname_len);

	send_buf_size = strlen(cmd);
	MEMSET(send_buf, LINE_BUF_SIZE);
	MEMCPY(send_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	MEMCPY(send_buf + RPC_MAGIC_MAX_LEN, cmd, send_buf_size);
			
	write(connection->connection_fd, send_buf, (send_buf_size + RPC_MAGIC_MAX_LEN));

	resp = conn_recv_resp(connection->connection_fd);
	if (resp->status_code != RPC_SUCCESS)
	{
		traceprint("\n ERROR in response \n");
		rtn_stat = FALSE;
		goto finish;

	}

	if (resp_selctx->stat == SELECT_RANGE_OP)
	{
		MEMCPY(&(resp_selctx->ctx.selrg), resp->result, sizeof(SELRANGE));
	}
	else if(resp_selctx->stat == SELECT_WHERE_OP)
	{
		MEMCPY(&(resp_selctx->ctx.selwh), resp->result, sizeof(SELWHERE));
		MEMCPY(&(resp_selctx->rglist), resp->result + sizeof(SELWHERE), 
					sizeof(SVR_IDX_FILE));		
	}
	
finish:

	conn_destroy_resp(resp);
	
	parser_close();    

	return TRUE;
}


rg_conn *
cli_rgsel_ranger(conn * connection, char * cmd, SELCTX *selctx, char *ip, int port)
{
	char		send_rg_buf[LINE_BUF_SIZE];
	int		rtn_stat;
	rg_conn		*rg_connection;
	int 		i;


	for(i = 0; i < connection->rg_list_len; i++)
	{
		if((port == connection->rg_list[i]->rg_server_port)
		     &&(!strcmp(ip, connection->rg_list[i]->rg_server_ip))
		     &&(connection->rg_list[i]->status == ESTABLISHED))
		{
			rg_connection = connection->rg_list[i];
			break;
		}
	}
	
	if(i == connection->rg_list_len)
	{
		rg_connection = (rg_conn *)MEMALLOCHEAP(sizeof(rg_conn));
		rg_connection->rg_server_port = port;
		strcpy(rg_connection->rg_server_ip, ip);

		if((rg_connection->connection_fd = conn_open(rg_connection->rg_server_ip,
				rg_connection->rg_server_port)) < 0)
		{
			perror("error in create connection with rg server: ");

			MEMFREEHEAP(rg_connection);
			rtn_stat = FALSE;
			return NULL;

		}
		
		rg_connection->status = ESTABLISHED;

		connection->rg_list[connection->rg_list_len] = rg_connection;
		connection->rg_list_len++;

	}

	int send_buf_size = strlen(cmd);

	if (selctx->stat == SELECT_RANGE_OP)
	{
		MEMCPY(&(selctx->ctx.selrg), RPC_SELECTRANGE_MAGIC, RPC_MAGIC_MAX_LEN);
				
		MEMSET(send_rg_buf, LINE_BUF_SIZE);
		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);


		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, &(selctx->ctx.selrg), 
			sizeof(SELRANGE));
		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(SELRANGE), cmd, 
			send_buf_size);
		
		write(rg_connection->connection_fd, send_rg_buf, 
			(sizeof(SELRANGE) + send_buf_size + RPC_MAGIC_MAX_LEN));
	}
	else if (selctx->stat == SELECT_WHERE_OP)
	{
		MEMCPY(&(selctx->ctx.selwh), RPC_SELECTWHERE_MAGIC, RPC_MAGIC_MAX_LEN);
				
		MEMSET(send_rg_buf, LINE_BUF_SIZE);
		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);


		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, &(selctx->ctx.selwh), 
			sizeof(SELWHERE));
		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(SELWHERE), cmd, 
			send_buf_size);
		
		write(rg_connection->connection_fd, send_rg_buf, 
			(sizeof(SELWHERE) + send_buf_size + RPC_MAGIC_MAX_LEN));
	}

	
	return rg_connection;
	

}


int
cli_rgsel_is_bigdata(rg_conn * rg_connection, int *bigdataport)
{
	RPCRESP		*rg_resp;
	int		rtn_stat;
	
	
	rg_resp = conn_recv_resp_abt(rg_connection->connection_fd);

	switch (rg_resp->status_code)
	{
	    case RPC_UNAVAIL:
	
		traceprint("\n need to re-get rg meta \n");
		
		rg_connection->status = CLOSED;
		conn_close(rg_connection->connection_fd, NULL, rg_resp);
		
		sleep(HEARTBEAT_INTERVAL + 1);

		rtn_stat = FALSE;

		break;

	    case RPC_RETRY:
	
	        traceprint("\n need to try \n");
		conn_destroy_resp(rg_resp);

		rtn_stat = FALSE;
		break;

	    case RPC_BIGDATA_CONN:
	    	*bigdataport = *(int *)rg_resp->result;
	    	rtn_stat = TRUE;
		break;
		
	    default:

		if (rg_resp->status_code != RPC_SUCCESS)
		{
			traceprint("\n ERROR in rg_server response \n");

			rtn_stat = FALSE;
		}
		else
		{
			rtn_stat = TRUE;
		}

		conn_destroy_resp(rg_resp);
			
		break;
	
	}

	return rtn_stat;
}


int
cli_open_range(conn * connection, char * cmd, int opid)
{
	SELCTX		*selctx;
	rg_conn		*rg_connection;
	int 		sockfd = -1;
	int		conn_cnt = 0;
	int		bigdataport;
	char		*ip;
	int		port;


	selctx = MEMALLOCHEAP(sizeof(SELCTX));

	if (opid == SELECT_RANGE_OP)
	{
		selctx->stat = SELECT_RANGE_OP;
	}
	else if (opid == SELECT_WHERE_OP)
	{
		selctx->stat = SELECT_WHERE_OP;
	}
	
	cli_rgsel_meta(connection, cmd, selctx);

	if (opid == SELECT_RANGE_OP)
	{
		ip = selctx->ctx.selrg.left_range.i_hdr.rg_info.rg_addr;
		port = selctx->ctx.selrg.left_range.i_hdr.rg_info.rg_port;
	}
	else if (opid == SELECT_WHERE_OP)
	{
		SVR_IDX_FILE	*temp_store;
		RANGE_PROF	*rg_prof;
	
		temp_store = &(selctx->rglist);
		rg_prof = (RANGE_PROF *)(temp_store->data);

		int	i;

		for(i = 0; i < temp_store->nextrno; i++)
		{
			if(rg_prof[i].rg_stat == RANGER_IS_ONLINE)
			{
				ip = rg_prof[i].rg_addr;
				port = rg_prof[i].rg_port;
				break;
			}
		}
	}

	rg_connection = cli_rgsel_ranger(connection, cmd, selctx, ip, port);

	if (!cli_rgsel_is_bigdata(rg_connection, &bigdataport))
	{
		MEMFREEHEAP(selctx);
		return -1;
	}
	
	MEMFREEHEAP(selctx);
	
	
	while ((sockfd < 0) && (conn_cnt < 1000))
	{
		sockfd = conn_open(rg_connection->rg_server_ip, bigdataport);

		conn_cnt++;
	}

	return sockfd;
}

int
cli_read_range(int sockfd, RANGE_QUERYCTX *rgsel_cont)
{
	RPCRESP		*rg_resp;
	int		rtn_stat;
	
	
	rg_resp = conn_recv_resp_abt(sockfd);

	switch (rg_resp->status_code)
	{
	    case RPC_UNAVAIL:
	
		traceprint("\n need to re-get rg meta \n");
		
		
		conn_close(sockfd, NULL, rg_resp);
		
		sleep(HEARTBEAT_INTERVAL + 1);

		rtn_stat = FALSE;

		break;

	    case RPC_RETRY:
	
	        traceprint("\n need to try \n");
		conn_destroy_resp(rg_resp);

		rtn_stat = FALSE;
		break;

	 		
	    default:

		if (rg_resp->status_code != RPC_SUCCESS)
		{
			traceprint("\n ERROR in rg_server response \n");
			rtn_stat = FALSE;
		}
		else
		{
			MEMCPY(rgsel_cont, rg_resp->result, sizeof(RANGE_QUERYCTX));
			rtn_stat = TRUE;
		}

		conn_destroy_resp(rg_resp);	
			
		break;
	
	}

	return rtn_stat;
}

void
cli_write_range(int sockfd)
{
	char	send_buf[8];


	MEMSET(send_buf, 8);

	MEMCPY(send_buf, "cont", 8);
	
	write(sockfd, send_buf, 8);
}


void
cli_close_range(int sockfd)
{
	close(sockfd);
}


#define	ROW_OFFSET_ENTRYSIZE	sizeof(int)
#define	BLK_TAILSIZE		sizeof(int)	
#define	ROW_OFFSET_PTR(blkptr)	((int *) (((char *)(blkptr)) +		\
                  (BLOCKSIZE - BLK_TAILSIZE - ROW_OFFSET_ENTRYSIZE)))

char *
cli_get_nextrow(RANGE_QUERYCTX *rgsel_cont)
{
	if (rgsel_cont->cur_rowpos > rgsel_cont->end_rowpos)
	{
		return NULL;
	}

	int *offtab = ROW_OFFSET_PTR(rgsel_cont->data);

	char *rp = rgsel_cont->data + offtab[-(rgsel_cont->cur_rowpos)];
	int rlen = ROW_GET_LENGTH(rp, rgsel_cont->rowminlen);

	char test[BLOCKSIZE];
	MEMSET(test, BLOCKSIZE);
	MEMCPY(test, rp, rlen);
	printf("next row: %s \n", (test + rgsel_cont->rowminlen + sizeof(int)));

	(rgsel_cont->cur_rowpos)++;
	
	return rp;
}

char *
cli_get_firstrow(RANGE_QUERYCTX *rgsel_cont)
{
	return NULL;
}

static int
cli_prt_help(char *cmd)
{
	if (!strncasecmp("help", cmd, 4))
	{
		printf("CREATE TABLE....create table table_name (col1_name col1_type, col2_name col2_type)\n");
		printf("INSERT DATA.....insert into table_name (col1_value, col2_value)\n");
		printf("SELECT DATA.....select table_name (col1_value)\n");
		printf("SELECT RANGE....selectrange table_name (col1_value1, col1_value2)\n");
		printf("DELETE DATA.....delete table_name (col1_value)\n");
		printf("DROP TABLE......drop table_name\n");

		return TRUE;
	}

	return FALSE;
}

