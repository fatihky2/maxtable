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
#include "m_socket.h"


MT_CLI_CONTEXT *cli_context = NULL;

extern	TSS	*Tss;

extern int 
sel_resp_rejoin(char * src_buf, char * dest_buf, int src_len, int * dest_len, char *index_buf, int querytype);

static int 
mt_cli_prt_help(char *cmd);

static rg_conn *
mt_cli_rgsel_ranger(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx, char *ip, int port);

static int
mt_cli_rgsel_meta(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx);

static int
mt_cli_rgsel_is_bigdata(rg_conn * rg_connection, int *bigdataport);

static void
mt_cli_write_range(int sockfd);

static void
mt_cli_close_range(int sockfd);

static char *
mt_cli_get__nextrow(RANGE_QUERYCTX *rgsel_cont, int *rlen);


int validation_request(char * request)
{
    //to be do.....
    return TRUE;
}

void
mt_cli_crt_context()
{
	assert (cli_context == NULL);
	
	mem_init_alloc_regions();
	tss_setup(TSS_OP_CLIENT);

	cli_context = MEMALLOCHEAP(sizeof(MT_CLI_CONTEXT));
	SPINLOCK_ATTR_INIT(cli_context->mutexattr);
	SPINLOCK_ATTR_SETTYPE(cli_context->mutexattr, PTHREAD_MUTEX_RECURSIVE);
	SPINLOCK_INIT(cli_context->mutex, &(cli_context->mutexattr));
}


void
mt_cli_destroy_context()
{
	SPINLOCK_DESTROY(cli_context->mutex);
	MEMFREEHEAP(cli_context);
	cli_context = NULL;
	tss_release();
	mem_free_alloc_regions();	
}


/*
** create one connection between cli and svr, return the connection
*/
int 
mt_cli_open_connection(char * meta_ip, int meta_port, conn ** connection)
{
	P_SPINLOCK(cli_context->mutex);
	
	conn * new_conn = (conn *)MEMALLOCHEAP(sizeof(conn));
	new_conn->meta_server_port = meta_port;
	strcpy(new_conn->meta_server_ip, meta_ip);

	if((new_conn->connection_fd = conn_open(meta_ip, meta_port)) < 0)
	{
		perror("error in create connection: ");
		MEMFREEHEAP(new_conn);
		V_SPINLOCK(cli_context->mutex);
		return FALSE;
	}

	new_conn->status = ESTABLISHED;
	new_conn->rg_list_len = 0;

	*connection = new_conn;

	V_SPINLOCK(cli_context->mutex);
	return TRUE;
}

/*
** close one connection between cli and svr
*/
void 
mt_cli_close_connection(conn * connection)
{
	int i;

	P_SPINLOCK(cli_context->mutex);
	
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

	V_SPINLOCK(cli_context->mutex);
}

/*
** commit one request
*/
int 
mt_cli_exec_crtseldel(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx)
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
	int		meta_retry;

	
	rtn_stat = CLI_SUCCESS;
	sstab_split_resp = NULL;
	remove_tab_resp = NULL;
	sstab_split = FALSE;
	remove_tab_hit = FALSE;
	retry_cnt = 0;
	meta_retry = 0;
	resp = NULL;
	rg_resp = NULL;

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
			
	tcp_put_data(connection->connection_fd, send_buf, (send_buf_size + RPC_MAGIC_MAX_LEN));

	resp = conn_recv_resp(connection->connection_fd);
	
	if (resp == NULL)
	{
		traceprint("\n ERROR in response \n");
		rtn_stat = CLI_FAIL;

		goto finish;
	}

	
	if (resp->status_code & RPC_RETRY)
	{
		traceprint("\n Waiting for the retry the meta server\n");
		
		if(meta_retry)
		{
			rtn_stat = CLI_FAIL;
			goto finish;
		}

		sleep(5);
		
		meta_retry++;

		conn_destroy_resp(resp);
		resp = NULL;

		goto retry;
	}
	
	if (!(resp->status_code & RPC_SUCCESS))
	{
		traceprint("\n ERROR in response \n");
		rtn_stat = CLI_FAIL;

		if (resp->status_code & RPC_TABLE_NOT_EXIST)
		{
			rtn_stat |= CLI_TABLE_NOT_EXIST;
		}
		goto finish;

	}

	if(   (querytype == INSERT) || (querytype == SELECT) || (querytype == DELETE) 
	   || (querytype == DROP) )
	{
		INSMETA		*resp_ins;
		
		resp_ins = (INSMETA *)resp->result;
		
		
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
				rtn_stat = CLI_FAIL;
				goto finish;

			}
			
			rg_connection->status = ESTABLISHED;

			connection->rg_list[connection->rg_list_len] = rg_connection;
			connection->rg_list_len++;

		}

		if (querytype == DROP)
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

			tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
		        		(RPC_MAGIC_MAX_LEN + send_buf_size + RPC_MAGIC_MAX_LEN));
		}
		else
		{
		
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, resp->result, resp->result_length);
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + resp->result_length, cmd, send_buf_size);

			tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
					(resp->result_length + send_buf_size + RPC_MAGIC_MAX_LEN));
		}
		

		rg_resp = conn_recv_resp_abt(rg_connection->connection_fd);

		if (rg_resp->status_code == RPC_UNAVAIL)
		{
			traceprint("\n need to re-get rg meta \n");
			conn_destroy_resp(resp);
			resp = NULL;

			rg_connection->status = CLOSED;
			conn_close(rg_connection->connection_fd, NULL, rg_resp);

			rg_resp = NULL;
			
			sleep(HEARTBEAT_INTERVAL + 1);
			goto retry;

		}

		if (rg_resp->status_code == RPC_RETRY)
                {
                        traceprint("\n need to try \n");

			conn_destroy_resp(resp);
			resp = NULL;

			conn_destroy_resp(rg_resp);
			rg_resp = NULL;
			
			retry_cnt++;

			if (retry_cnt > 5)
			{
				traceprint("\n Retry Fail\n");
				rtn_stat = CLI_FAIL | CLI_RPC_FAIL;
				goto finish;
			}

			sleep(5);
					
                        goto retry;

                }
		
		if (!(rg_resp->status_code & RPC_SUCCESS))
		{
			traceprint("\n ERROR in rg_server response \n");
			rtn_stat = CLI_FAIL;

			if (rg_resp->status_code & RPC_TABLE_NOT_EXIST)
			{
				rtn_stat |= CLI_TABLE_NOT_EXIST;
			}
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

			tcp_put_data(connection->connection_fd, send_buf, 
						(new_size + RPC_MAGIC_MAX_LEN));


			sstab_split_resp = conn_recv_resp(connection->connection_fd);
			sstab_split = TRUE;

			if (sstab_split_resp == NULL)
			{
				traceprint("\n ERROR on sstab_split in meta_server response \n");
				rtn_stat = CLI_FAIL;

				/**/
				MEMFREEHEAP(new_buf);
				goto finish;
			}
			
			if (!(sstab_split_resp->status_code & RPC_SUCCESS))
			{
				traceprint("\n ERROR on sstab_split in meta_server response \n");
				rtn_stat = CLI_FAIL;

				if (sstab_split_resp->status_code & RPC_TABLE_NOT_EXIST)
				{
					rtn_stat |= CLI_TABLE_NOT_EXIST;
				}
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

			tcp_put_data(connection->connection_fd, send_buf, 
						(new_size + RPC_MAGIC_MAX_LEN));


			remove_tab_resp = conn_recv_resp(connection->connection_fd);
			remove_tab_hit = TRUE;

			if (remove_tab_resp == NULL)
			{
				traceprint("\n ERROR on drop in meta_server response \n");
				rtn_stat = CLI_FAIL;

				MEMFREEHEAP(new_buf);
				goto finish;
			}
			
			if (!(remove_tab_resp->status_code & RPC_SUCCESS))
			{
				traceprint("\n ERROR on drop in meta_server response \n");
				rtn_stat = CLI_FAIL;

				if (remove_tab_resp->status_code & RPC_TABLE_NOT_EXIST)
				{
					rtn_stat |= CLI_TABLE_NOT_EXIST;
				}
				MEMFREEHEAP(new_buf);
				goto finish;

			}

			MEMFREEHEAP(new_buf);
		}
			
	}

finish:
	if (querytype == SELECT)
	{
		exec_ctx->meta_resp = (char *)resp;
		exec_ctx->rg_resp = (char *)rg_resp;
		exec_ctx->end_rowpos = 1;
	}
	else
	{
		conn_destroy_resp(resp);
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

int 
mt_cli_exec_selrang(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx)
{
	rg_conn		*rg_connection;
	int 		sockfd = -1;
	int		conn_cnt = 0;
	int		bigdataport;
	char		*ip;
	int		port;
	
	SELRANGE	*selrg;
	SELWHERE	*selwh;
	SVR_IDX_FILE	*rglist;
	int		rtn_state;


	rtn_state = mt_cli_rgsel_meta(connection, cmd, exec_ctx);

	if (rtn_state == FALSE)
	{
		return rtn_state;
	}

	Assert(((RPCRESP *)(exec_ctx->meta_resp))->status_code & RPC_SUCCESS);
	
	if (exec_ctx->querytype == SELECTRANGE)
	{
		selrg = (SELRANGE *)(((RPCRESP *)(exec_ctx->meta_resp))->result);

		ip = selrg->left_range.i_hdr.rg_info.rg_addr;
		port = selrg->left_range.i_hdr.rg_info.rg_port;
	}
	else if (exec_ctx->querytype == SELECTWHERE)
	{
		selwh = (SELWHERE *)(((RPCRESP *)(exec_ctx->meta_resp))->result);

		rglist =  (SVR_IDX_FILE *)(((RPCRESP *)(exec_ctx->meta_resp))->result + sizeof(SELWHERE));

		RANGE_PROF	*rg_prof;

		rg_prof = (RANGE_PROF *)(rglist->data);

		int	i;

		for(i = 0; i < rglist->nextrno; i++)
		{
			if(rg_prof[i].rg_stat & RANGER_IS_ONLINE)
			{
				ip = rg_prof[i].rg_addr;
				port = rg_prof[i].rg_port;
				break;
			}
		}
	}

	rg_connection = mt_cli_rgsel_ranger(connection, cmd, exec_ctx, ip, port);

	if (   (rg_connection == NULL) 
	    || (!mt_cli_rgsel_is_bigdata(rg_connection, &bigdataport)))
	{
		goto finish;
	}
	
	while ((sockfd < 0) && (conn_cnt < 1000))
	{
		sockfd = conn_open(rg_connection->rg_server_ip, bigdataport);

		conn_cnt++;
	}

	if (sockfd < 0)
	{
		rtn_state = FALSE;
		exec_ctx->status |= CLICTX_BAD_SOCKET;
		goto finish;
	}

finish:
	exec_ctx->socketid = sockfd;
	exec_ctx->status |= CLICTX_DATA_BUF_NO_DATA;
		
	return rtn_state;
}


int 
sel_resp_rejoin(char * src_buf, char * dest_buf, int src_len, int * dest_len, char *index_buf, int querytype)
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

		if (querytype == SELECT)
		{
			int 	collen;
			char	*col;
			col = row_locate_col(src_buf, (col_info + i)->col_offset,tab_hdr->tab_row_minlen, &collen);

			MEMCPY((char *)&dest_buf[i*32], col, collen);

			continue;
			
		}
	
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

	
	if (querytype == SELECT)
	{
		return TRUE;
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
mt_cli_rgsel_meta(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx)
{
	LOCALTSS(tss);
	char		send_buf[LINE_BUF_SIZE];
	char		tab_name[64];
	int		send_buf_size;
	RPCRESP		*resp;
	int		querytype;
	int		rtn_stat;
	int		meta_retry;

	
	rtn_stat = TRUE;
	meta_retry = 0;

	querytype = ((TREE *)(tss->tcmd_parser))->sym.command.querytype;
	MEMSET(tab_name, 64);
	MEMCPY(tab_name, ((TREE *)(tss->tcmd_parser))->sym.command.tabname,
		((TREE *)(tss->tcmd_parser))->sym.command.tabname_len);

retry:

	send_buf_size = strlen(cmd);
	MEMSET(send_buf, LINE_BUF_SIZE);
	MEMCPY(send_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	MEMCPY(send_buf + RPC_MAGIC_MAX_LEN, cmd, send_buf_size);
			
	tcp_put_data(connection->connection_fd, send_buf, (send_buf_size + RPC_MAGIC_MAX_LEN));

	resp = conn_recv_resp(connection->connection_fd);

	
	if ((resp != NULL) && (resp->status_code & RPC_RETRY))
	{
		traceprint("\n Waiting for the retry the meta server\n");
		
		if(meta_retry)
		{
			rtn_stat = FALSE;
			goto finish;
		}

		sleep(5);
		
		meta_retry++;

		conn_destroy_resp(resp);
		resp = NULL;

		goto retry;
	}
	
	if ((resp == NULL) || (!(resp->status_code & RPC_SUCCESS)))
	{
		traceprint("\n ERROR in response \n");
		rtn_stat = FALSE;
		goto finish;

	}
	
finish:

	exec_ctx->meta_resp = (char *)resp;
	parser_close();

	return rtn_stat;
}


rg_conn *
mt_cli_rgsel_ranger(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx, char *ip, int port)
{
	char		send_rg_buf[LINE_BUF_SIZE];
	int		rtn_stat;
	rg_conn		*rg_connection;
	int 		i;


	rg_connection = NULL;
	
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

	if (exec_ctx->querytype == SELECTRANGE)
	{
		MEMCPY(((RPCRESP *)(exec_ctx->meta_resp))->result, RPC_SELECTRANGE_MAGIC, RPC_MAGIC_MAX_LEN);
				
		MEMSET(send_rg_buf, LINE_BUF_SIZE);
		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);


		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, ((RPCRESP *)(exec_ctx->meta_resp))->result, 
			sizeof(SELRANGE));
		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(SELRANGE), cmd, 
			send_buf_size);
		
		tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
			(sizeof(SELRANGE) + send_buf_size + RPC_MAGIC_MAX_LEN));
	}
	else if (exec_ctx->querytype == SELECTWHERE)
	{
		MEMCPY(((RPCRESP *)(exec_ctx->meta_resp))->result, RPC_SELECTWHERE_MAGIC, RPC_MAGIC_MAX_LEN);
				
		MEMSET(send_rg_buf, LINE_BUF_SIZE);
		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);


		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, ((RPCRESP *)(exec_ctx->meta_resp))->result, 
			sizeof(SELWHERE));
		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(SELWHERE), cmd, 
			send_buf_size);
		
		tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
			(sizeof(SELWHERE) + send_buf_size + RPC_MAGIC_MAX_LEN));
	}

	return rg_connection;
	

}


int
mt_cli_rgsel_is_bigdata(rg_conn * rg_connection, int *bigdataport)
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
		rg_resp = NULL;
		
		sleep(HEARTBEAT_INTERVAL + 1);

		rtn_stat = FALSE;

		break;

	    case RPC_RETRY:
	
	        traceprint("\n need to try \n");
		conn_destroy_resp(rg_resp);
		rg_resp = NULL;

		rtn_stat = FALSE;
		break;

	    case RPC_BIGDATA_CONN:
	    	*bigdataport = *(int *)rg_resp->result;
	    	rtn_stat = TRUE;
		break;
		
	    default:

		if (!(rg_resp->status_code & RPC_SUCCESS))
		{
			traceprint("\n ERROR in rg_server response \n");

			rtn_stat = FALSE;
		}
		else
		{
			rtn_stat = TRUE;
		}

		
			
		break;
	
	}

	conn_destroy_resp(rg_resp);
	rg_resp = NULL;

	return rtn_stat;
}

int
mt_cli_read_range(MT_CLI_EXEC_CONTEX *exec_ctx)
{
	RPCRESP		*rg_resp;
	int		rtn_stat;
	int		retry_cnt = 0;
	
retry:
	rg_resp = conn_recv_resp_abt(exec_ctx->socketid);

	switch (rg_resp->status_code)
	{
	    case RPC_UNAVAIL:
	
		traceprint("\n need to re-get rg meta \n");
		
		
		conn_close(exec_ctx->socketid, NULL, rg_resp);
		
		sleep(HEARTBEAT_INTERVAL + 1);

		rtn_stat = FALSE;

		break;

	    case RPC_RETRY:
	
	        traceprint("\n need to try \n");
		conn_destroy_resp(rg_resp);
		
		if (retry_cnt < 1)
		{
			sleep(5);

			goto retry;
		}
		
		rtn_stat = FALSE;
		break;

	 		
	    default:

		if (!(rg_resp->status_code & RPC_SUCCESS))
		{
			traceprint("\n ERROR in rg_server response \n");
			conn_destroy_resp(rg_resp);
			rtn_stat = FALSE;
		}
		else
		{
			rtn_stat = TRUE;
			Assert(exec_ctx->rg_resp);
			exec_ctx->rg_resp = (char *)rg_resp;
		}

		break;
	
	}

	return rtn_stat;
}

void
mt_cli_write_range(int sockfd)
{
	char	send_buf[8];

	MEMSET(send_buf, 8);

	MEMCPY(send_buf, "cont", 8);
	
	write(sockfd, send_buf, 8);
}


void
mt_cli_close_range(int sockfd)
{
	close(sockfd);
}


#define	ROW_OFFSET_ENTRYSIZE	sizeof(int)
#define	BLK_TAILSIZE		sizeof(int)	
#define	ROW_OFFSET_PTR(blkptr)	((int *) (((char *)(blkptr)) +		\
                  (BLOCKSIZE - BLK_TAILSIZE - ROW_OFFSET_ENTRYSIZE)))

char *
mt_cli_get__nextrow(RANGE_QUERYCTX *rgsel_cont, int *rlen)
{
	if (rgsel_cont->cur_rowpos > rgsel_cont->end_rowpos)
	{
		return NULL;
	}

	int *offtab = ROW_OFFSET_PTR(rgsel_cont->data);

	char *rp = rgsel_cont->data + offtab[-(rgsel_cont->cur_rowpos)];
	*rlen = ROW_GET_LENGTH(rp, rgsel_cont->rowminlen);

	(rgsel_cont->cur_rowpos)++;
	
	return rp;
}

char *
mt_cli_get_nextrow(MT_CLI_EXEC_CONTEX *exec_ctx, int *rlen)
{
	char			*rp;
	RANGE_QUERYCTX		*rgsel_cont;


	rp = NULL;


retry:

	switch (exec_ctx->querytype)
	{
	    case SELECT:

		exec_ctx->cur_rowpos = 0;
		
		if (exec_ctx->cur_rowpos < exec_ctx->end_rowpos)
		{
			exec_ctx->cur_rowpos++;
			rp = ((RPCRESP *)(exec_ctx->rg_resp))->result;
		}

		break;
			
	    case SELECTRANGE:
	    case SELECTWHERE:

		if (exec_ctx->status & CLICTX_BAD_SOCKET)
		{
			traceprint("socket id (%d) is bad.\n", exec_ctx->socketid);
			break;
		}

		
		if (exec_ctx->status & CLICTX_RANGER_IS_UNCONNECT)
		{
			traceprint("ranger server is not connectable.\n");
			break;
		}

		if (exec_ctx->status & CLICTX_DATA_BUF_NO_DATA)
		{
			if (!mt_cli_read_range(exec_ctx))
			{
				exec_ctx->status |= CLICTX_RANGER_IS_UNCONNECT;
				break;
			}
			
			exec_ctx->status &= ~CLICTX_DATA_BUF_NO_DATA;
			exec_ctx->status |= CLICTX_DATA_BUF_HAS_DATA;
		}

		rgsel_cont = (RANGE_QUERYCTX *)(((RPCRESP *)(exec_ctx->rg_resp))->result);

		if (!(rgsel_cont->status & DATA_EMPTY))
		{
			
			rp = mt_cli_get__nextrow(rgsel_cont, rlen);

			if ((rp == NULL) && (rgsel_cont->status & DATA_CONT))
			{
				mt_cli_write_range(exec_ctx->socketid);
				
				conn_destroy_resp((RPCRESP *)(exec_ctx->rg_resp));

				exec_ctx->rg_resp = NULL;
				exec_ctx->status &= ~CLICTX_DATA_BUF_HAS_DATA;
				exec_ctx->status |= CLICTX_DATA_BUF_NO_DATA;
				goto retry;
			}
		}

		break;

	    default:
	    	break;

	}

	return rp;
}


int
mt_cli_get_rowcnt(MT_CLI_EXEC_CONTEX *exec_ctx)
{
	return exec_ctx->end_rowpos;
}

char *
mt_cli_get_colvalue(MT_CLI_EXEC_CONTEX *exec_ctx, char *rowbuf, int col_idx, int *collen)
{
	char		*meta_buf;


	meta_buf = ((RPCRESP *)(exec_ctx->meta_resp))->result;

	if (exec_ctx->querytype == SELECTRANGE)
	{			
		meta_buf += sizeof(SELRANGE);
	}
	else if (exec_ctx->querytype == SELECTWHERE)
	{
		meta_buf += (sizeof(SELWHERE) + sizeof(SVR_IDX_FILE));
	}
	else
	{
		meta_buf += sizeof(INSMETA);
	}	

	TABLEHDR *tab_hdr = (TABLEHDR *)meta_buf;

	if ((col_idx < 0) || ((col_idx + 1) > tab_hdr->tab_col))
	{
		traceprint("Can't find the column.\n");
		return FALSE;
	}
	
	meta_buf += sizeof(TABLEHDR);

	COLINFO *col_info = (COLINFO *)meta_buf;

	return row_locate_col(rowbuf, (col_info + col_idx)->col_offset,tab_hdr->tab_row_minlen, collen);
}


char *
mt_cli_get_firstrow(RANGE_QUERYCTX *rgsel_cont)
{
	return NULL;
}

int 
mt_cli_open_execute(conn *connection, char *cmd, MT_CLI_EXEC_CONTEX *exec_ctx)
{
	int 	querytype;
	int	s_idx;
	int	rtn_stat;

	if (exec_ctx == NULL)
	{
		return FALSE;
	}
	
	if (exec_ctx->status & CLICTX_IS_OPEN)
	{
		return FALSE;
	}
	
	if(!validation_request(cmd))
	{
		return FALSE;
	}

	if (mt_cli_prt_help(cmd))
	{
		return TRUE;
	}

	querytype = par_get_query(cmd, &s_idx);

	P_SPINLOCK(cli_context->mutex);

	LOCALTSS(tss);
	
	switch (querytype)
	{
	    case TABCREAT:
	    	tss->topid |= TSS_OP_CRTTAB;
		rtn_stat = par_crtins_tab((cmd + s_idx), TABCREAT);
		break;
		
	    case INSERT:
	    	tss->topid |= TSS_OP_INSTAB;
		rtn_stat = par_crtins_tab((cmd + s_idx), INSERT);
	        break;

	    case SELECT:		
		tss->topid |= TSS_OP_SELDELTAB;
		rtn_stat = par_seldel_tab((cmd + s_idx), SELECT);

	        break;

	    case DELETE:
	    	
	    	tss->topid |= TSS_OP_SELDELTAB;
	    	rtn_stat = par_seldel_tab((cmd + s_idx), DELETE);
	        break;

	    case SELECTRANGE:
	    	rtn_stat = par_selrange_tab((cmd + s_idx), SELECTRANGE);
	    	break;
		
	    case DROP:
	    	rtn_stat = par_dropremovrebalanmcc_tab(cmd + s_idx, DROP);
	    	break;
		
	    case SELECTWHERE:
	    	rtn_stat = par_selwhere_tab(cmd + s_idx, SELECTWHERE);
		break;

	    default:
	    	rtn_stat = FALSE;
	        break;
	}

	if (!rtn_stat)
	{
		parser_close();
		tss->tstat |= TSS_PARSER_ERR;
		traceprint("PARSER ERR: Please input the command again by the 'help' signed.\n");
		V_SPINLOCK(cli_context->mutex);
		return FALSE;
	}

	MEMSET(exec_ctx, sizeof(MT_CLI_EXEC_CONTEX));

	exec_ctx->querytype = querytype;

	switch (querytype)
	{
	    case TABCREAT:
	    case INSERT:
	    case DELETE:
	    case DROP:
	    case SELECT:
		rtn_stat = mt_cli_exec_crtseldel(connection, cmd, exec_ctx);
		break;
    		
	    case SELECTRANGE:
	    case SELECTWHERE:
	    	rtn_stat = mt_cli_exec_selrang(connection, cmd, exec_ctx);
	    	
		break;

	    default:
	    	rtn_stat = FALSE;
	        break;
	}

	V_SPINLOCK(cli_context->mutex);
	return rtn_stat;

}


void 
mt_cli_close_execute(MT_CLI_EXEC_CONTEX *exec_ctx)
{
	if (exec_ctx->meta_resp)
	{
		conn_destroy_resp((RPCRESP *)(exec_ctx->meta_resp));
		exec_ctx->meta_resp = NULL;
	}
	
	if(exec_ctx->rg_resp)
	{
		conn_destroy_resp((RPCRESP *)(exec_ctx->rg_resp));

		exec_ctx->rg_resp = NULL;
	}

	if ((exec_ctx->querytype == SELECTRANGE) || (exec_ctx->querytype == SELECTWHERE))
	{
		if (!(exec_ctx->status & CLICTX_BAD_SOCKET))
		{
			mt_cli_close_range(exec_ctx->socketid);
		}
	}
	
	exec_ctx->status  = 0;
		
}


static int
mt_cli_prt_help(char *cmd)
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

