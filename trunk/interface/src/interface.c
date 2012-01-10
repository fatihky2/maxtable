/*
** interface.c 2011-07-05 xueyingfei
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


MT_CLI_CONTEXT *Cli_context = NULL;

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


/* 
** Client context initialization. 
**
** Parameters:
** 	None.
** 
** Returns:
**	None.
** 
** Side Effects:
**	Allocate some memory to initialize the context for the client of Maxtable.
** 
*/
void
mt_cli_crt_context()
{
	assert (Cli_context == NULL);

	/* Initialize the memory context for the client. */
	mem_init_alloc_regions();
	tss_setup(TSS_OP_CLIENT);

	/* Initialize the client context. */
	Cli_context = MEMALLOCHEAP(sizeof(MT_CLI_CONTEXT));
	SPINLOCK_ATTR_INIT(Cli_context->mutexattr);
	SPINLOCK_ATTR_SETTYPE(Cli_context->mutexattr, PTHREAD_MUTEX_RECURSIVE);
	SPINLOCK_INIT(Cli_context->mutex, &(Cli_context->mutexattr));
}


/* 
** Client context destroy. 
**
** Parameters:
**	None.
** 
** Returns:
**	None.
** 
** Side Effects:
**	Free the memory allocated by the mt_cli_crt_context();
** 
*/
void
mt_cli_destroy_context()
{
	SPINLOCK_DESTROY(Cli_context->mutex);
	MEMFREEHEAP(Cli_context);
	Cli_context = NULL;
	tss_release();
	mem_free_alloc_regions();	
}


/*
** Create the connection between client and server and return the connection.
**
** Parameters:
**	meta_ip		- (IN) metaserver address.
**	meta_port	- (IN) metaserver port.
**	connection	- (OUT) the context of net connection.
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None
** 
*/
int 
mt_cli_open_connection(char * meta_ip, int meta_port, conn ** connection)
{
	P_SPINLOCK(Cli_context->mutex);

	/* Allocate the context for the meta connection. */
	conn * new_conn = (conn *)MEMALLOCHEAP(sizeof(conn));
	new_conn->meta_server_port = meta_port;
	strcpy(new_conn->meta_server_ip, meta_ip);

	if((new_conn->connection_fd = conn_open(meta_ip, meta_port)) < 0)
	{
		perror("error in create connection: ");
		MEMFREEHEAP(new_conn);
		V_SPINLOCK(Cli_context->mutex);
		return FALSE;
	}

	new_conn->status = ESTABLISHED;
	new_conn->rg_list_len = 0;

	*connection = new_conn;

	V_SPINLOCK(Cli_context->mutex);
	return TRUE;
}


/*
** Close the connection between client and server.
**
** Parameters:
**	connection	- (IN) the context of net connection.
** 
** Returns:
**	None.
** 
** Side Effects
**	None
**
*/
void 
mt_cli_close_connection(conn * connection)
{
	int i;

	P_SPINLOCK(Cli_context->mutex);
	
	close(connection->connection_fd);
	
	for(i = 0; i < connection->rg_list_len; i++)
	{
		if(connection->rg_list[i]->status == ESTABLISHED)
		{
			close(connection->rg_list[i]->connection_fd);
		}
		
		MEMFREEHEAP(connection->rg_list[i]);
	}

	/* Free the connection context. */
	MEMFREEHEAP(connection);

	V_SPINLOCK(Cli_context->mutex);
}


/*
** This routine processes the command of CREATE TABLE, SELECT and DELETE.
**
** Parameters:
**	connection	- (IN) the context of net connection.
**	cmd		- (IN) the command user specified.
**	exec_ctx		- (IN) the execution context.
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None
**
*/
int 
mt_cli_exec_crtseldel(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx)
{
	LOCALTSS(tss);
	char		send_buf[LINE_BUF_SIZE];/* The RPC buffer for sending
						** to meta server. 
						*/
	char		send_rg_buf[LINE_BUF_SIZE];/* The RPC buffer for sending
						** to ranger server. 
						*/
	char		tab_name[64];		/* Table name. */
	int		send_buf_size;		/* Buffer size. */
	RPCRESP		*resp;			/* Ptr to the response from the
						** meta server.
						*/			
	RPCRESP		*rg_resp;		/* Ptr to the response from the
						** ranger server. 
						*/
	RPCRESP		*sstab_split_resp;	/* Ptr to the response from the
						** meta server by the ADDSSTAB.
						*/
	RPCRESP		*remove_tab_resp;	/* Ptr to the response from the
						** meta server by the REMOVETAB.
						*/
	int		querytype;		/* Query type. */
	int		rtn_stat;		/* The return stat for the client. */	
	int		sstab_split;		/* Flag if sstab hit split issue. */
	int		remove_tab_hit;		/* Flag if this query is REMOVETAB. */
	int		retry_cnt;		/* The # of retry to connect meta if 
						** ranger server fail to response.
						*/
	int		meta_retry;		/* The # of retry to connect meta if 
						** meta server fail to response.
						*/


	/* Initialization. */
	rtn_stat = CLI_SUCCESS;
	sstab_split_resp = NULL;
	remove_tab_resp = NULL;
	sstab_split = FALSE;
	remove_tab_hit = FALSE;
	retry_cnt = 0;
	meta_retry = 0;
	resp = NULL;
	rg_resp = NULL;

	/* Aquire the query type. */
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

	/* Send the requirment to the meta server. */
	tcp_put_data(connection->connection_fd, send_buf, (send_buf_size + RPC_MAGIC_MAX_LEN));

	/* Accept the response from the meta server. */
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

	/* 
	** Here we have got the right response and the meta data from the meta 
	** server and then fire the query in the ranger server.
	*/
	if(   (querytype == INSERT) || (querytype == SELECT) 
	   || (querytype == DELETE) || (querytype == DROP) )
	{
		/* Ptr to the meta data information from the meta server. */
		INSMETA		*resp_ins;
		
		resp_ins = (INSMETA *)resp->result;
		
		/* Ptr to the context of ranger connection. */
		rg_conn * rg_connection;
		int i;
		//printf("rg server: %s/%d\n", resp_ins->i_hdr.rg_info.rg_addr, resp_ins->i_hdr.rg_info.rg_port);

		/* Find the right ranger from the ranger list. */
		for(i = 0; i < connection->rg_list_len; i++)
		{
			if(   (resp_ins->i_hdr.rg_info.rg_port == 
				connection->rg_list[i]->rg_server_port)
			   && (!strcmp(resp_ins->i_hdr.rg_info.rg_addr, 
				connection->rg_list[i]->rg_server_ip))
			   && (connection->rg_list[i]->status == ESTABLISHED))
			{
				rg_connection = connection->rg_list[i];
				break;
			}
		}
		
		if(i == connection->rg_list_len)
		{
			/* 
			** The ranger to be connected is not exist in the ranger 
			** list, we need to create the new connection to the 
			** ranger server.
			*/
			rg_connection = (rg_conn *)MEMALLOCHEAP(sizeof(rg_conn));
			rg_connection->rg_server_port = 
						resp_ins->i_hdr.rg_info.rg_port;
			strcpy(rg_connection->rg_server_ip, 
						resp_ins->i_hdr.rg_info.rg_addr);

			/* Connect to the ranger server. */
			if((rg_connection->connection_fd = conn_open(
						rg_connection->rg_server_ip, 
						rg_connection->rg_server_port)) < 0)
			{
				perror("error in create connection with rg server: ");

				MEMFREEHEAP(rg_connection);
				rtn_stat = CLI_FAIL;
				goto finish;

			}
			
			rg_connection->status = ESTABLISHED;

			/* Add the new connection into the connection context. */
			connection->rg_list[connection->rg_list_len] = rg_connection;
			connection->rg_list_len++;

		}

		/* 
		** Serilize the require data into the require buffer and send it
		** to ranger server.
		*/
		if (querytype == DROP)
		{
			/* Put the DROP's magic into the buffer. */
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
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, resp->result, 
							RPC_MAGIC_MAX_LEN);
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + RPC_MAGIC_MAX_LEN,
							cmd, send_buf_size);

			/* Send the query requirment to the ranger server. */
			tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
		        	(RPC_MAGIC_MAX_LEN + send_buf_size + RPC_MAGIC_MAX_LEN));
		}
		else
		{
		
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, resp->result,
							resp->result_length);
			MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + resp->result_length,
							cmd, send_buf_size);

			/* Send the query requirment to the ranger server. */
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

			/* It's a brifly solution and '5' is just temp value. */
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


		/* Check if this insertion hit the sstable split issue. */
		if((querytype == INSERT) && (rg_resp->result_length))
		{
			/* Working on the split issue. */
			char *cli_add_sstab = "addsstab into ";

			/*
			** TODO: 128 may need to be expanded.
			**
			** 128 must be greater than the sum of the length of  
			** tab_name, newsstabname, sstab_id, split_ts,
			** split_sstabid, sstab_key.
			*/
			int new_size = rg_resp->result_length + 128 + 
							STRLEN(cli_add_sstab);

			char * new_buf = MEMALLOCHEAP(new_size);
			MEMSET(new_buf, new_size);
			
			char newsstabname[SSTABLE_NAME_MAX_LEN];

			MEMSET(newsstabname, SSTABLE_NAME_MAX_LEN);

			/* Get the sstable name created newly from the response. */
			MEMCPY(newsstabname, rg_resp->result, SSTABLE_NAME_MAX_LEN);

			/* Get the old sstable id. */
			int sstab_id = *(int *)(rg_resp->result + SSTABLE_NAME_MAX_LEN);

			/* Split timestamp. */
			int split_ts = *(int *)(rg_resp->result + SSTABLE_NAME_MAX_LEN + 
								sizeof(int));

			/* New sstable id. */
			int split_sstabid = *(int *)(rg_resp->result + 
					SSTABLE_NAME_MAX_LEN + sizeof(int) + sizeof(int));

			/* The key length of sstable. */
			int sstab_keylen = rg_resp->result_length - SSTABLE_NAME_MAX_LEN - 
						sizeof(int) - sizeof(int) - sizeof(int) + 1;

			/* The key of sstable. */
			char *sstab_key = MEMALLOCHEAP(sstab_keylen);
			MEMSET(sstab_key, sstab_keylen);
			MEMCPY(sstab_key, rg_resp->result + SSTABLE_NAME_MAX_LEN +
				sizeof(int) + sizeof(int) + sizeof(int), sstab_keylen - 1);

			sprintf(new_buf, "addsstab into %s (%s, %d, %d, %d, %s)",
					tab_name, newsstabname, sstab_id, split_ts, 
					split_sstabid, sstab_key);

			MEMSET(send_buf, LINE_BUF_SIZE);
			MEMCPY(send_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
			MEMCPY(send_buf + RPC_MAGIC_MAX_LEN, new_buf, new_size);

			MEMFREEHEAP(sstab_key);

			/* Send the split requirment to the meta server. */
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


		/* 
		** Continue to do the operation if the query type is DROP, send 
		** the requirement to meta server and remove the meta  infor. 
		*/
		if (querytype == DROP)
		{	
			Assert(rg_resp->result_length);
			
			char *cli_remove_tab = "remove ";				    

			/* The command likes "remove tab_name". */
			int new_size = TABLE_NAME_MAX_LEN + STRLEN(cli_remove_tab);
			char *new_buf = MEMALLOCHEAP(new_size);				    

			MEMSET(new_buf, new_size);

			sprintf(new_buf, "remove %s", tab_name);

			MEMSET(send_buf, LINE_BUF_SIZE);
			MEMCPY(send_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
			MEMCPY(send_buf + RPC_MAGIC_MAX_LEN, new_buf, new_size);

			/* 
			** Send the requirment to the meta server and continue
			** to do the 'remove' operation.
			*/
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
		/* Save the response infor into the SELECT execution context. */
		exec_ctx->meta_resp = (char *)resp;
		exec_ctx->rg_resp = (char *)rg_resp;
		exec_ctx->end_rowpos = 1;
	}
	else
	{
		/* Destroy the response infor in the other cases. */
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


/*
** This routine processes the command of SELECTRANGE and SELECTWHERE.
**
** Parameters:
**	connection	- (IN) the context of net connection.
**	cmd		- (IN) the command user specified.
**	exec_ctx		- (IN) the execution context.
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None
**
*/
int 
mt_cli_exec_selrang(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx)
{
	rg_conn		*rg_connection;	/* The context of connection to the ranger. */
	int 		sockfd = -1;	/* Socket id. */
	int		conn_cnt = 0;	/* The # of connection. */
	int		bigdataport;	/* Big data port. */
	char		*ip;		/* Ranger address. */
	int		port;		/* Ranger port. */
	SELRANGE	*selrg;		/* Ptr to the context of the SELECTRANGE. */
	SELWHERE	*selwh;		/* Ptr to the context of the SELECTWHERE. */
	SVR_IDX_FILE	*rglist;	/* Ptr to the list of ranger. */
	int		rtn_state;	/* Return state. */


	/* Get the meta data information from the meta server. */
	rtn_state = mt_cli_rgsel_meta(connection, cmd, exec_ctx);

	if (rtn_state == FALSE)
	{
		return rtn_state;
	}

	Assert(((RPCRESP *)(exec_ctx->meta_resp))->status_code & RPC_SUCCESS);
	
	if (exec_ctx->querytype == SELECTRANGE)
	{
		/* Get the context of the selectrange. */
		selrg = (SELRANGE *)(((RPCRESP *)(exec_ctx->meta_resp))->result);

		/* 
		** TODO: we need to use the way of sql map-reduce to process the
		** selectrange.
		*/
		ip = selrg->left_range.i_hdr.rg_info.rg_addr;
		port = selrg->left_range.i_hdr.rg_info.rg_port;
	}
	else if (exec_ctx->querytype == SELECTWHERE)
	{
		/* Get the context of the selectwhere. */
		selwh = (SELWHERE *)(((RPCRESP *)(exec_ctx->meta_resp))->result);

		/* 
		** TODO: we need to use the way of sql map-reduce to process the
		** selectwhere.
		*/
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

	/* Switch the query to the range server. */
	rg_connection = mt_cli_rgsel_ranger(connection, cmd, exec_ctx, ip, port);

	if (   (rg_connection == NULL) 
	    || (!mt_cli_rgsel_is_bigdata(rg_connection, &bigdataport)))
	{
		goto finish;
	}
	
	while ((sockfd < 0) && (conn_cnt < 1000))
	{
		/* Connect to the range server by the big data port, default valuse is 1969. */
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
sel_resp_rejoin(char * src_buf, char * dest_buf, int src_len, int * dest_len, 
			char *index_buf, int querytype)
{
	char		col_off_tab[COL_OFFTAB_MAX_SIZE];
	int		col_off_idx = COL_OFFTAB_MAX_SIZE;
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
			col = row_locate_col(src_buf, (col_info + i)->col_offset,
						tab_hdr->tab_row_minlen, &collen);

			MEMCPY((char *)&dest_buf[i*32], col, collen);

			continue;
			
		}
	
		int col_type = (col_info+i)->col_type;
		
		if(TYPE_IS_FIXED(col_type))
		{
			MEMCPY(dest_buf + dest_buf_index, src_buf + src_buf_index1, 
						TYPE_GET_LEN(col_type));

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
		MEMCPY(dest_buf + dest_buf_index, col_off_tab + col_off_idx, 
					COL_OFFTAB_MAX_SIZE - col_off_idx);
		dest_buf_index += (COL_OFFTAB_MAX_SIZE - col_off_idx);
	}

	
	*dest_len = dest_buf_index;

	return TRUE;
}


/*
** Send the requirment to the meta server and receive the meta data information
** in the case of SELECTRANGE and SELECTWHERE.
**
** Parameters:
**	connection	- (IN) the context of net connection.
**	cmd		- (IN) the command user specified.
**	exec_ctx		- (IN) the execution context.
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None
**
*/
int
mt_cli_rgsel_meta(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx)
{
	LOCALTSS(tss);
	char		send_buf[LINE_BUF_SIZE];/* Buffer for the data sending. */
	char		tab_name[64];		/* Table name. */
	int		send_buf_size;		/* Buffer size. */
	RPCRESP		*resp;			/* Ptr to the response information. */
	int		querytype;		/* Query type. */
	int		rtn_stat;		/* Return state. */
	int		meta_retry;		/* The # of retrying to meta. */


	/* initialization. */
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

	/* Send the requirment to the meta server. */
	tcp_put_data(connection->connection_fd, send_buf, 
				(send_buf_size + RPC_MAGIC_MAX_LEN));

	/* Get the meta data information from the meta server. */
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

	/* Save the meta data infor into the execution context. */
	exec_ctx->meta_resp = (char *)resp;
	parser_close();

	return rtn_stat;
}


/*
** Send the requirment to the range server and receive the result of query
** in the case of SELECTRANGE and SELECTWHERE.
**
** Parameters:
**	connection	- (IN) the context of net connection.
**	cmd		- (IN) the command user specified.
**	exec_ctx		- (IN) the execution context.
**	ip		- (IN) the range address.
**	port		- (IN) the range port.
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None
**
*/
rg_conn *
mt_cli_rgsel_ranger(conn * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx, 
			char *ip, int port)
{
	char	send_rg_buf[LINE_BUF_SIZE];	/* Buffer for the data sending
						** to the range server.
						*/
	int	rtn_stat;			/* return state. */
	rg_conn	*rg_connection;			/* Ptr to the context of 
						** connection to the range 
						** server.
						*/
	int 	i;


	rg_connection = NULL;
	
	for(i = 0; i < connection->rg_list_len; i++)
	{
		if(   (port == connection->rg_list[i]->rg_server_port)
		   && (!strcmp(ip, connection->rg_list[i]->rg_server_ip))
		   && (connection->rg_list[i]->status == ESTABLISHED))
		{
			rg_connection = connection->rg_list[i];
			break;
		}
	}


	/*
	** While the input ranger server is not exist in the list of ranger ,
	** it will create the new connection to the ranger server and add
	** it into the list of range server.
	*/
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
		MEMCPY(((RPCRESP *)(exec_ctx->meta_resp))->result, 
				RPC_SELECTRANGE_MAGIC, RPC_MAGIC_MAX_LEN);
				
		MEMSET(send_rg_buf, LINE_BUF_SIZE);
		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);


		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, 
				((RPCRESP *)(exec_ctx->meta_resp))->result, 
				sizeof(SELRANGE));
		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(SELRANGE), cmd, 
				send_buf_size);

		/* Send the requirment to the range server. */
		tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
			(sizeof(SELRANGE) + send_buf_size + RPC_MAGIC_MAX_LEN));
	}
	else if (exec_ctx->querytype == SELECTWHERE)
	{
		MEMCPY(((RPCRESP *)(exec_ctx->meta_resp))->result, 
				RPC_SELECTWHERE_MAGIC, RPC_MAGIC_MAX_LEN);
				
		MEMSET(send_rg_buf, LINE_BUF_SIZE);
		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);


		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, 
				((RPCRESP *)(exec_ctx->meta_resp))->result, 
				sizeof(SELWHERE));
		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(SELWHERE), cmd, 
				send_buf_size);

		/* Send the requirment to the range server. */
		tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
			(sizeof(SELWHERE) + send_buf_size + RPC_MAGIC_MAX_LEN));
	}

	return rg_connection;
	

}


/*
** Get the port for the transmission of big data.
**
** Parameters:
**	rg_connection	- (IN) the context of connection to the range server.
**	bigdataport	- (OUT) the bigdata port.
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None
**
*/
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
	    	/* Successfully. */
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


/*
** Read data from the range server.
**
** Parameters:
**	exec_ctx		- (IN) the execution context.
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None
**
*/
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
			/* Successfully. */
			rtn_stat = TRUE;
			Assert(exec_ctx->rg_resp == NULL);
			exec_ctx->rg_resp = (char *)rg_resp;
		}

		break;
	
	}

	return rtn_stat;
}


/*
** Write the flag to the range server for the data fetching.
**
** Parameters:
**	sockfd		- (IN) socket id for the range server.
** 
** Returns:
**	None.
** 
** Side Effects
**	None
**
*/
void
mt_cli_write_range(int sockfd)
{
	char	send_buf[8];

	MEMSET(send_buf, 8);

	MEMCPY(send_buf, "cont", 8);
	
	write(sockfd, send_buf, 8);
}


/*
** Close the connection to the range server.
**
** Parameters:
**	sockfd		- (IN) socket id for the range server.
** 
** Returns:
**	None.
** 
** Side Effects
**	None
**
*/
void
mt_cli_close_range(int sockfd)
{
	close(sockfd);
}


#define	ROW_OFFSET_ENTRYSIZE	sizeof(int)
#define	BLK_TAILSIZE		sizeof(int)	
#define	ROW_OFFSET_PTR(blkptr)	((int *) (((char *)(blkptr)) +		\
                  (BLOCKSIZE - BLK_TAILSIZE - ROW_OFFSET_ENTRYSIZE)))

/*
** Get the row specified by the execution context.
**
** Parameters:
**	rgsel_cont	- (IN) the context of range query.
**	rlen		- (OUT) the length of row.
** 
** Returns:
**	The ptr of row.
** 
** Side Effects
**	The row position has been forwarded on.
**
*/
static char *
mt_cli_get__nextrow(RANGE_QUERYCTX *rgsel_cont, int *rlen)
{
	/* Check if we have got the end. */
	if (rgsel_cont->cur_rowpos > rgsel_cont->end_rowpos)
	{
		return NULL;
	}

	int *offtab = ROW_OFFSET_PTR(rgsel_cont->data);

	char *rp = rgsel_cont->data + offtab[-(rgsel_cont->cur_rowpos)];
	*rlen = ROW_GET_LENGTH(rp, rgsel_cont->rowminlen);

	/* Forward on the row. */
	(rgsel_cont->cur_rowpos)++;
	
	return rp;
}


/*
** Get the row specified by the execution context.
**
** Parameters:
**	rgsel_cont	- (IN) the context of range query.
**	rlen		- (OUT) the length of row.
** 
** Returns:
**	The ptr of row.
** 
** Side Effects
**	None.
**
*/
char *
mt_cli_get_nextrow(MT_CLI_EXEC_CONTEX *exec_ctx, int *rlen)
{
	char		*rp;		/* Ptr to the row. */	
	RANGE_QUERYCTX	*rgsel_cont;	/* Ptr to the context of range query. */


	rp = NULL;


retry:

	switch (exec_ctx->querytype)
	{
	    case SELECT:

		exec_ctx->cur_rowpos = 0;

		/* Just only get the first and only one row in the select case. */
		if (exec_ctx->cur_rowpos < exec_ctx->end_rowpos)
		{
			exec_ctx->cur_rowpos++;
			rp = ((RPCRESP *)(exec_ctx->rg_resp))->result;
		}

		break;
			
	    case SELECTRANGE:
	    case SELECTWHERE:

		/* Check if the query is right. */
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

		/* 
		** If the buffer has no data, we need to fetch the data from 
		** the range server. 
		*/
		if (exec_ctx->status & CLICTX_DATA_BUF_NO_DATA)
		{
			/* Fetch data from the range server. */
			if (!mt_cli_read_range(exec_ctx))
			{
				exec_ctx->status |= CLICTX_RANGER_IS_UNCONNECT;
				break;
			}
			
			exec_ctx->status &= ~CLICTX_DATA_BUF_NO_DATA;
			exec_ctx->status |= CLICTX_DATA_BUF_HAS_DATA;
		}

		rgsel_cont = (RANGE_QUERYCTX *)(((RPCRESP *)(exec_ctx->rg_resp))->result);

		/* Check if the data reading is done. */
		if (!(rgsel_cont->status & DATA_EMPTY))
		{
			/* Get the row. */
			rp = mt_cli_get__nextrow(rgsel_cont, rlen);

			/* 
			** If the reading of this round is done and the range
			** server still has some data to be read, we will send 
			** the requirment of reading to the range server for
			** the  further reading.
			*/
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

/*
** Get the row count in the execution context.
**
** Parameters:
**	exec_ctx		- (IN) the execution context.
** 
** Returns:
**	The # of row.
** 
** Side Effects
**	None.
**
*/
int
mt_cli_get_rowcnt(MT_CLI_EXEC_CONTEX *exec_ctx)
{
	return exec_ctx->end_rowpos;
}


/*
** Get the value of column user specified.
**
** Parameters:
**	exec_ctx		- (IN) the execution context.	
**	rowbuf		- (IN) the row pointer.
**	col_idx		- (IN) the index of column user specified.
**	collen		- (OUT) the length of column.
** 
** Returns:
**	The ptr to the column.  .
** 
** Side Effects
**	None.
**
*/
char *
mt_cli_get_colvalue(MT_CLI_EXEC_CONTEX *exec_ctx, char *rowbuf, int col_idx, 
			int *collen)
{
	char		*meta_buf;	/* Ptr to the meta data information. */


	/* Meta data information. */
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

	/* Validation. */
	if ((col_idx < 0) || ((col_idx + 1) > tab_hdr->tab_col))
	{
		traceprint("Can't find the column.\n");
		return FALSE;
	}
	
	meta_buf += sizeof(TABLEHDR);

	COLINFO *col_info = (COLINFO *)meta_buf;

	return row_locate_col(rowbuf, (col_info + col_idx)->col_offset,
				tab_hdr->tab_row_minlen, collen);
}


char *
mt_cli_get_firstrow(RANGE_QUERYCTX *rgsel_cont)
{
	return NULL;
}


/*
** Open the context of execution for the client.
**
** Parameters:
**	connection	- (IN) the context of connection to the meta server.
**	cmd		- (IN) the command user specified.
**	exec_ctx		- (IN) the execution context.	
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None.
**
*/
int 
mt_cli_open_execute(conn *connection, char *cmd, MT_CLI_EXEC_CONTEX *exec_ctx)
{
	int 	querytype;	/* Query type. */
	int	s_idx;		/* The char index of command string. */
	int	rtn_stat;	/* Return state. */

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

	/* Parse the input command and get the query type. */
	querytype = par_get_query(cmd, &s_idx);

	P_SPINLOCK(Cli_context->mutex);

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
		V_SPINLOCK(Cli_context->mutex);
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

	V_SPINLOCK(Cli_context->mutex);
	return rtn_stat;

}


/*
** Close the context of execution.
**
** Parameters:
**	connection	- (IN) the context of connection to the meta server.
**	cmd		- (IN) the command user specified.
**	exec_ctx		- (IN) the execution context.	
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None.
**
*/
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

	if (   (exec_ctx->querytype == SELECTRANGE) 
	    || (exec_ctx->querytype == SELECTWHERE))
	{
		if (!(exec_ctx->status & CLICTX_BAD_SOCKET))
		{
			mt_cli_close_range(exec_ctx->socketid);
		}
	}
	
	exec_ctx->status  = 0;
		
}


/*
** Print the help information.
**
** Parameters:
**	cmd		- (IN) the command user specified.
** 
** Returns:
**	True if success, or false.
** 
** Side Effects
**	None.
**
*/
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

