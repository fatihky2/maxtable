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

#include "global.h"
#include "utils.h"
#include "master/metaserver.h"
#include "rpcfmt.h"
#include "parser.h"
#include "ranger/rangeserver.h"
#include "netconn.h"
#include "conf.h"
#include "token.h"
#include "tss.h"
#include "memcom.h"
#include "strings.h"
#include "trace.h"
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

static RG_CONN *
mt_cli_rgsel__ranger(CONN * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx, char *ip, int port);

static int
mt_cli_rgsel_meta(CONN * connection, char * cmd, char **meta_resp);

static int
mt_cli_rgsel_is_bigdata(RG_CONN * rg_connection, int *bigdataport);

static void
mt_cli_write_range(int sockfd);

static void
mt_cli_close_range(int sockfd);

static char *
mt_cli_get__nextrow(RANGE_QUERYCTX *rgsel_cont, int *rlen);

static int
mt_cli_rgsel_ranger(CONN * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx, 
				char *ip, int port);


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
mt_cli_open_connection(char * meta_ip, int meta_port, CONN ** connection)
{
	P_SPINLOCK(Cli_context->mutex);

	/* Allocate the context for the meta connection. */
	CONN * new_conn = (CONN *)malloc(sizeof(CONN));

	MEMSET(new_conn, sizeof(CONN));
	
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
mt_cli_close_connection(CONN * connection)
{
	int i;

	P_SPINLOCK(Cli_context->mutex);
	
	close(connection->connection_fd);
	
	for(i = 0; i < connection->rg_list_len; i++)
	{
		if(connection->rg_list[i].status == ESTABLISHED)
		{
			close(connection->rg_list[i].connection_fd);
		}
		
//		MEMFREEHEAP(connection->rg_list[i]);
	}

	/* Free the connection context. */
	free(connection);

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
mt_cli_exec_crtseldel(CONN * connection, char * cmd, MT_CLI_EXEC_CONTEX **exec_ctx)
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
	int		querytype;		/* Query type. */
	int		rtn_stat;		/* The return stat for the client. */	
	int		sstab_split;		/* Flag if sstab hit split issue. */
	int		retry_cnt;		/* The # of retry to connect meta if 
						** ranger server fail to response.
						*/
	int		meta_retry;		/* The # of retry to connect meta if 
						** meta server fail to response.
						*/
	MT_CLI_EXEC_CONTEX	*t_exec_ctx;
	int		idx_root_split;


	/* Initialization. */
	rtn_stat = CLI_SUCCESS;
	sstab_split_resp = NULL;
	sstab_split = FALSE;
	retry_cnt = 0;
	meta_retry = 0;
	resp = NULL;
	rg_resp = NULL;
	idx_root_split = FALSE;

	/* Aquire the query type. */
	querytype = ((TREE *)(tss->tcmd_parser))->sym.command.querytype;
	MEMSET(tab_name, 64);
	MEMCPY(tab_name, ((TREE *)(tss->tcmd_parser))->sym.command.tabname,
	((TREE *)(tss->tcmd_parser))->sym.command.tabname_len);

	*exec_ctx = (MT_CLI_EXEC_CONTEX *)MEMALLOCHEAP(sizeof(MT_CLI_EXEC_CONTEX));
	MEMSET(*exec_ctx, sizeof(MT_CLI_EXEC_CONTEX));

	t_exec_ctx = *exec_ctx;
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

		if (resp->status_code & RPC_TAB_HAS_NO_DATA)
		{
			rtn_stat = CLI_HAS_NO_DATA;
				
		}
		else if (resp->status_code & RPC_TABLE_NOT_EXIST)
		{
			rtn_stat = CLI_TABLE_NOT_EXIST;
		}
		
		goto finish;

	}

rg_again:
	
	/* 
	** Here we have got the right response and the meta data from the meta 
	** server and then fire the query in the ranger server.
	*/
	if(   (querytype == INSERT) || (querytype == SELECT) 
	   || (querytype == DELETE) || idx_root_split)
	{
		/* Ptr to the meta data information from the meta server. */
		INSMETA		*resp_ins;
		
		resp_ins = (INSMETA *)resp->result;
		
		/* Ptr to the context of ranger connection. */
		RG_CONN * rg_connection;
		int i;
		int k = -1;
		//printf("rg server: %s/%d\n", resp_ins->i_hdr.rg_info.rg_addr, resp_ins->i_hdr.rg_info.rg_port);

		/* Find the right ranger from the ranger list. */
		for(i = 0; i < connection->rg_list_len; i++)
		{
			if(   (resp_ins->i_hdr.rg_info.rg_port == 
				connection->rg_list[i].rg_server_port)
			   && (!strcmp(resp_ins->i_hdr.rg_info.rg_addr, 
				connection->rg_list[i].rg_server_ip))
			   && (connection->rg_list[i].status == ESTABLISHED))
			{
				rg_connection = &(connection->rg_list[i]);
				break;
			}

			
			if (connection->rg_list[i].status == CLOSED)
			{
				k = i;
			}
		}
		
		if(i == connection->rg_list_len)
		{
			/* 
			** The ranger to be connected is not exist in the ranger 
			** list, we need to create the new connection to the 
			** ranger server.
			*/
			if (k != -1)
			{
				/* Re-use the context of zomb connection. */
				Assert(connection->rg_list[k].status == CLOSED);

				rg_connection = &(connection->rg_list[k]);

				MEMSET(rg_connection, sizeof(RG_CONN));
			}
			else
			{
				rg_connection = &(connection->rg_list[i]);
			}
			
			//rg_connection = (rg_conn *)MEMALLOCHEAP(sizeof(rg_conn));
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

//				MEMFREEHEAP(rg_connection);
				rtn_stat = CLI_FAIL;
				goto finish;

			}
			
			rg_connection->status = ESTABLISHED;

			/* Add the new connection into the connection context. */
			//connection->rg_list[connection->rg_list_len] = rg_connection;

			if (k == -1)
			{
				connection->rg_list_len++;
			}
		}

		/* 
		** Serilize the require data into the require buffer and send it
		** to ranger server.
		*/
		if (idx_root_split)
		{
			MEMCPY(resp->result, RPC_IDXROOT_SPLIT_MAGIC, RPC_MAGIC_MAX_LEN);
		}
		else
		{
			MEMCPY(resp->result, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
		}		
		MEMSET(send_rg_buf, LINE_BUF_SIZE);
		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
				
		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, resp->result,
						resp->result_length);
		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + resp->result_length,
						cmd, send_buf_size);

		/* Send the query requirment to the ranger server. */
		tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
			(resp->result_length + send_buf_size + RPC_MAGIC_MAX_LEN));
		
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
				rtn_stat = CLI_RPC_FAIL;
				goto finish;
			}

			sleep(5);
					
                        goto retry;

                }
		
		if (!(rg_resp->status_code & RPC_SUCCESS))
		{
			traceprint("\n ERROR in rg_server response \n");
			rtn_stat = CLI_FAIL;

			if (resp->status_code & RPC_TAB_HAS_NO_DATA)
			{
				rtn_stat = CLI_HAS_NO_DATA;
					
			}
			else if (resp->status_code & RPC_TABLE_NOT_EXIST)
			{
				rtn_stat = CLI_TABLE_NOT_EXIST;
			}
			
			goto finish;

		}		
		else if(querytype == SELECT)
		{
			if (rg_resp->result == NULL)
			{
				traceprint("The data user specified is not exist!\n");
				rtn_stat = CLI_HAS_NO_DATA;
				goto finish;
			}
		}

		if (idx_root_split)
		{
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

				if (resp->status_code & RPC_TAB_HAS_NO_DATA)
				{
					rtn_stat = CLI_HAS_NO_DATA;
						
				}
				else if (resp->status_code & RPC_TABLE_NOT_EXIST)
				{
					rtn_stat = CLI_TABLE_NOT_EXIST;
				}
		
				MEMFREEHEAP(new_buf);
				goto finish;

			}

			MEMFREEHEAP(new_buf);

			if (sstab_split_resp->result_length != 0)
			{
				idx_root_split = TRUE;

				conn_destroy_resp(resp);
				
				resp = sstab_split_resp;

				sstab_split_resp = NULL;

				sstab_split = FALSE;
				
				goto rg_again;	
			}
		}
	}

finish:
	if (querytype == SELECT)
	{
		/* Save the response infor into the SELECT execution context. */
		t_exec_ctx->meta_resp = (char *)resp;
		t_exec_ctx->rg_resp = (char *)rg_resp;
		t_exec_ctx->querytype = querytype;
		
		/* SELECT needs to free the memory for the ranger response. */
		t_exec_ctx->rg_cnt = 1;

		if (rtn_stat == CLI_HAS_NO_DATA)
		{
			t_exec_ctx->end_rowpos = 0;
		}
		else
		{
			t_exec_ctx->end_rowpos = 1;
		}
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

	parser_close();    

	return rtn_stat;
}


/* Extract from mt_cli_exec_crtseldel(). */
int 
mt_cli_exec_drop_tab(CONN * connection, char * cmd, MT_CLI_EXEC_CONTEX **exec_ctx)
{
	LOCALTSS(tss);
	char		send_buf[LINE_BUF_SIZE];/* The RPC buffer for sending
						** to meta server. 
						*/
	char		send_rg_buf[LINE_BUF_SIZE];/* The RPC buffer for sending
						** to ranger server. 
						*/
	char		tab_name[64];		/* Table name. */
	char		idx_name[64];		/* Index name. */
	int		send_buf_size;		/* Buffer size. */
	RPCRESP		*resp;			/* Ptr to the response from the
						** meta server.
						*/			
	RPCRESP		*rg_resp;		/* Ptr to the response from the
						** ranger server. 
						*/
	RPCRESP		*remove_tab_resp;	/* Ptr to the response from the
						** meta server by the REMOVETAB.
						*/
	int		querytype;		/* Query type. */
	int		rtn_stat;		/* The return stat for the client. */	
	int		remove_tab_hit;		/* Flag if this query is REMOVETAB. */
	int		retry_cnt;		/* The # of retry to connect meta if 
						** ranger server fail to response.
						*/
	int		meta_retry;		/* The # of retry to connect meta if 
						** meta server fail to response.
						*/
	MT_CLI_EXEC_CONTEX	*t_exec_ctx;


	/* Initialization. */
	rtn_stat = CLI_SUCCESS;
	remove_tab_resp = NULL;
	remove_tab_hit = FALSE;
	retry_cnt = 0;
	meta_retry = 0;
	resp = NULL;
	rg_resp = NULL;

	/* Aquire the query type. */
	querytype = ((TREE *)(tss->tcmd_parser))->sym.command.querytype;

	if (querytype == DROPTAB)
	{
		MEMSET(tab_name, 64);
		MEMCPY(tab_name, ((TREE *)(tss->tcmd_parser))->sym.command.tabname,
		((TREE *)(tss->tcmd_parser))->sym.command.tabname_len);
	}
	else
	{
		Assert(querytype == DROPINDEX);
		
		/* Index name*/
		MEMSET(idx_name, 64);
		MEMCPY(idx_name, ((TREE *)(tss->tcmd_parser))->sym.command.tabname,
		((TREE *)(tss->tcmd_parser))->sym.command.tabname_len);

		/* Table name locate at the sub-command ON. */
		MEMSET(tab_name, 64);
		MEMCPY(tab_name, (((TREE *)(tss->tcmd_parser))->right)->sym.command.tabname,
		(((TREE *)(tss->tcmd_parser))->right)->sym.command.tabname_len);
	}
	
	*exec_ctx = (MT_CLI_EXEC_CONTEX *)MEMALLOCHEAP(sizeof(MT_CLI_EXEC_CONTEX));
	MEMSET(*exec_ctx, sizeof(MT_CLI_EXEC_CONTEX));

	t_exec_ctx = *exec_ctx;
retry:
	send_buf_size = strlen(cmd);

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
			rtn_stat = CLI_RPC_FAIL;
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

		if (resp->status_code & RPC_TAB_HAS_NO_DATA)
		{
			rtn_stat = CLI_HAS_NO_DATA;
				
		}
		else if (resp->status_code & RPC_TABLE_NOT_EXIST)
		{
			rtn_stat = CLI_TABLE_NOT_EXIST;
		}

		
		goto finish;

	}

	/* 
	** Here we have got the right response and the meta data from the meta 
	** server and then fire the query in the ranger server.
	*/

	/* Ptr to the meta data information from the meta server. */
	INSMETA		*resp_ins;
	
	resp_ins = (INSMETA *)resp->result;
	
	/* Ptr to the context of ranger connection. */
	RG_CONN * rg_connection;
	int i;
	int k = -1;
	//printf("rg server: %s/%d\n", resp_ins->i_hdr.rg_info.rg_addr, resp_ins->i_hdr.rg_info.rg_port);

	/* Find the right ranger from the ranger list. */
	for(i = 0; i < connection->rg_list_len; i++)
	{
		if(   (resp_ins->i_hdr.rg_info.rg_port == 
			connection->rg_list[i].rg_server_port)
		   && (!strcmp(resp_ins->i_hdr.rg_info.rg_addr, 
			connection->rg_list[i].rg_server_ip))
		   && (connection->rg_list[i].status == ESTABLISHED))
		{
			rg_connection = &(connection->rg_list[i]);
			break;
		}

		
		if (connection->rg_list[i].status == CLOSED)
		{
			k = i;
		}
	}
	
	if(i == connection->rg_list_len)
	{
		/* 
		** The ranger to be connected is not exist in the ranger 
		** list, we need to create the new connection to the 
		** ranger server.
		*/
		if (k != -1)
		{
			/* Re-use the context of zomb connection. */
			Assert(connection->rg_list[k].status == CLOSED);

			rg_connection = &(connection->rg_list[k]);

			MEMSET(rg_connection, sizeof(RG_CONN));
		}
		else
		{
			rg_connection = &(connection->rg_list[i]);
		}
		
		//rg_connection = (rg_conn *)MEMALLOCHEAP(sizeof(rg_conn));
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

//			MEMFREEHEAP(rg_connection);
			rtn_stat = CLI_FAIL;
			goto finish;

		}
		
		rg_connection->status = ESTABLISHED;

		/* Add the new connection into the connection context. */
		//connection->rg_list[connection->rg_list_len] = rg_connection;

		if (k == -1)
		{
			connection->rg_list_len++;
		}
	}

	/* 
	** Serilize the require data into the require buffer and send it
	** to ranger server.
	*/

	if (querytype == DROPTAB)
	{
		/* Put the DROP's magic into the buffer. */
		MEMCPY(resp->result, RPC_DROP_TABLE_MAGIC, RPC_MAGIC_MAX_LEN);
	}
	else
	{
		Assert(querytype == DROPINDEX);
		
		/* Put the DROP's magic into the buffer. */
		MEMCPY(resp->result, RPC_DROPIDX_MAGIC, RPC_MAGIC_MAX_LEN);	
	}
	
	MEMSET(send_rg_buf, LINE_BUF_SIZE);
	MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);

	
	MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, resp->result, 
					RPC_MAGIC_MAX_LEN);
	MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + RPC_MAGIC_MAX_LEN,
					cmd, send_buf_size);

	/* Send the query requirment to the ranger server. */
	tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
        	(RPC_MAGIC_MAX_LEN + send_buf_size + RPC_MAGIC_MAX_LEN));
	
	

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
			rtn_stat = CLI_RPC_FAIL;
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
			rtn_stat = CLI_TABLE_NOT_EXIST;
		}
		goto finish;

	}

	/* 
	** Continue to do the operation and send the requirement to meta 
	** server and remove the meta  infor. 
	*/
	Assert(rg_resp->result_length);

	char	*new_buf;
	int	new_size;

	if (querytype == DROPTAB)
	{
		char *cli_remove_tab = "remove table ";				    

		/* The command likes "remove table tab_name". */
		new_size = TABLE_NAME_MAX_LEN + STRLEN(cli_remove_tab);
		new_buf = MEMALLOCHEAP(new_size);				    

		MEMSET(new_buf, new_size);

		sprintf(new_buf, "remove table %s", tab_name);
	}
	else
	{
		Assert(querytype == DROPINDEX);

		char *cli_remove_index = "remove index ";				    

		/* The command likes "remove index idx_name on tab_name". */
		new_size = TABLE_NAME_MAX_LEN + STRLEN(cli_remove_index);
		new_buf = MEMALLOCHEAP(new_size);				    

		MEMSET(new_buf, new_size);

		sprintf(new_buf, "remove index %s on %s", idx_name, tab_name);
	}
	
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
			rtn_stat = CLI_TABLE_NOT_EXIST;
		}
		MEMFREEHEAP(new_buf);
		goto finish;

	}

	MEMFREEHEAP(new_buf);

			
	

finish:
	
	/* Destroy the response infor in the other cases. */
	conn_destroy_resp(resp);
	conn_destroy_resp(rg_resp);	

	
	if (remove_tab_hit)
	{
		Assert((querytype == DROPTAB) || (querytype == DROPINDEX));
		conn_destroy_resp(remove_tab_resp);
	}
	
	parser_close();    

	return rtn_stat;
}

/*
** This routine processes the command of SELECTRANGE and SELECTWHERE 
** and CREATE INDEX.
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
mt_cli_exec_selrang(CONN * connection, char * cmd, MT_CLI_EXEC_CONTEX **exec_ctx, int querytype)
{
	char		*ip;		/* Ranger address. */
	int		port;		/* Ranger port. */
	SVR_IDX_FILE	*rglist;	/* Ptr to the list of ranger. */
	int		rtn_state;	/* Return state. */
	char		*meta_resp;
	int		rg_cnt;
	int		i;
	RANGE_PROF	*rg_prof;
	MT_CLI_EXEC_CONTEX	*t_exec_ctx;
	


	/* Get the meta data information from the meta server. */
	rtn_state = mt_cli_rgsel_meta(connection, cmd, &meta_resp);

	rg_cnt = 0;

	if (rtn_state == CLI_FAIL)
	{
		goto exit;
	}

	Assert(((RPCRESP *)(meta_resp))->status_code & RPC_SUCCESS);
	
	/* Get the ranger profiler. */
	if (querytype == CRTINDEX)
	{
		rglist =  (SVR_IDX_FILE *)(((RPCRESP *)(meta_resp))->result + 
				sizeof(IDXMETA));
	}
	else
	{
		rglist =  (SVR_IDX_FILE *)(((RPCRESP *)(meta_resp))->result + 
				sizeof(SELWHERE));		
	}

	rg_prof = (RANGE_PROF *)(rglist->data);
	
	for(i = 0; i < rglist->nextrno; i++)
	{
		if(rg_prof[i].rg_stat & RANGER_IS_ONLINE)
		{
			rg_cnt++;
		}
	}
		
	*exec_ctx = (MT_CLI_EXEC_CONTEX *)MEMALLOCHEAP(rg_cnt * 
						sizeof(MT_CLI_EXEC_CONTEX));
	MEMSET(*exec_ctx, rg_cnt * sizeof(MT_CLI_EXEC_CONTEX));

	t_exec_ctx = *exec_ctx;

	int	j;

	for(i = 0, j = 0; i < rglist->nextrno; i++)
	{
		
		if((j < rg_cnt) && (rg_prof[i].rg_stat & RANGER_IS_ONLINE))
		{
			ip = rg_prof[i].rg_addr;
			port = rg_prof[i].rg_port;

			t_exec_ctx->meta_resp = meta_resp;
			t_exec_ctx->querytype = querytype;
			t_exec_ctx->rg_cnt = rg_cnt;
			t_exec_ctx->status = CLICTX_IS_OPEN;

			/*
			** Once one ranger server hit the connection issue,
			** it will abort this session.
			*/
			if (!mt_cli_rgsel_ranger(connection, cmd,
					t_exec_ctx, ip, port))
			{
				rtn_state = CLI_FAIL;
				goto exit;
			}

			j++;
			t_exec_ctx++;
		}

		if (j == rg_cnt)
		{
			break;
		}
	}

exit:		
	return rtn_state;
}


static int
mt_cli_rgsel_ranger(CONN * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx, 
				char *ip, int port)
{
	RG_CONN		*rg_connection;	/* The context of connection to the ranger. */
	int		bigdataport;	/* Big data port. */
	int		rtn_state;	/* Return state. */
	int 		sockfd = -1;	/* Socket id. */
	int		conn_cnt = 0;	/* The # of connection. */
	

	rtn_state = TRUE;
	
	/* Switch the query to the range server. */
	rg_connection = mt_cli_rgsel__ranger(connection, cmd, exec_ctx, ip, port);

	if (   (rg_connection == NULL) 
	    || (   (exec_ctx->querytype != SELECTCOUNT) 
	        && (exec_ctx->querytype != SELECTSUM)
	        && (exec_ctx->querytype != DELETEWHERE)
	        && (exec_ctx->querytype != CRTINDEX)
	        && (exec_ctx->querytype != UPDATE)
		&& (!mt_cli_rgsel_is_bigdata(rg_connection, &bigdataport))))
	{
		rtn_state = FALSE;
		goto exit;
	}

	/* The result from SELECTCOUNT doesn't need to be sent by the bigdata port. */
	if (   (exec_ctx->querytype == SELECTCOUNT)
	    || (exec_ctx->querytype == SELECTSUM)
	    || (exec_ctx->querytype == DELETEWHERE)
	    || (exec_ctx->querytype == CRTINDEX)
	    || (exec_ctx->querytype == UPDATE))
	{
		rtn_state = TRUE;
		exec_ctx->rg_conn = rg_connection;
		goto exit;
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
		goto exit;
	}

	exec_ctx->socketid = sockfd;
	exec_ctx->status |= CLICTX_DATA_BUF_NO_DATA;

exit:		
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
mt_cli_rgsel_meta(CONN * connection, char * cmd, char **meta_resp)
{
	LOCALTSS(tss);
	char		send_buf[LINE_BUF_SIZE];/* Buffer for the data sending. */
	int		send_buf_size;		/* Buffer size. */
	RPCRESP		*resp;			/* Ptr to the response information. */
	int		querytype;		/* Query type. */
	int		rtn_stat;		/* Return state. */
	int		meta_retry;		/* The # of retrying to meta. */


	/* initialization. */
	rtn_stat = CLI_SUCCESS;
	meta_retry = 0;

	querytype = ((TREE *)(tss->tcmd_parser))->sym.command.querytype;

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

	
	if (resp != NULL)
	{
		if (resp->status_code & RPC_RETRY)
		{
			traceprint("\n Waiting for the retry the meta server\n");
			
			if(meta_retry)
			{
				rtn_stat = CLI_RPC_FAIL;
				goto finish;
			}

			sleep(5);
			
			meta_retry++;

			conn_destroy_resp(resp);
			resp = NULL;

			goto retry;
		}
		else if(!(resp->status_code & RPC_SUCCESS))
		{
			if (resp->status_code & ( RPC_TAB_HAS_NO_DATA 
						| RPC_TABLE_NOT_EXIST))
			{
				traceprint("Table has no data OR this table is not exist.\n");
			}
			else
			{
				traceprint("\n ERROR in response \n");
			}

			rtn_stat = CLI_FAIL;
			goto finish;
		}
	}
	
	if (resp == NULL)
	{
		traceprint("\n ERROR in response \n");
		rtn_stat = CLI_FAIL;
		goto finish;

	}
	
finish:

	/* Save the meta data infor into the execution context. */
	*meta_resp = (char *)resp;
	parser_close();

	return rtn_stat;
}




/*
** Send the result of create index to the meta server and receive its response.
**
** Parameters:
**	connection	- (IN) the context of net connection.
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
mt_cli_result_meta(CONN * connection, MT_CLI_EXEC_CONTEX *exec_ctx, int result)
{
	char		send_buf[LINE_BUF_SIZE];/* Buffer for the data sending. */
	RPCRESP		*resp;			/* Ptr to the response information. */
	int		rtn_stat;		/* Return state. */
	int		meta_retry;		/* The # of retrying to meta. */
	int		idx;
	IDXMETA		*idxmeta;


	/* initialization. */
	rtn_stat = CLI_SUCCESS;
	meta_retry = 0;
	idx = 0;

retry:

	MEMSET(send_buf, LINE_BUF_SIZE);

	switch (exec_ctx->querytype)
	{
	    case CRTINDEX:		
		idxmeta = (IDXMETA *)(((RPCRESP *)(exec_ctx->meta_resp))->result);
		
		PUT_TO_BUFFER(send_buf, idx, RPC_REQUEST_MAGIC, 
						RPC_MAGIC_MAX_LEN);
		PUT_TO_BUFFER(send_buf, idx, RPC_CRTIDX_DONE_MAGIC, 
						RPC_MAGIC_MAX_LEN);
		PUT_TO_BUFFER(send_buf, idx, &result, sizeof(int));
		PUT_TO_BUFFER(send_buf, idx, idxmeta, sizeof(IDXMETA));
		
	    	break;
		
	    default:
	    	break;
	}
	
	/* Send the requirment to the meta server. */
	tcp_put_data(connection->connection_fd, send_buf, idx);

	/* Get the meta data information from the meta server. */
	resp = conn_recv_resp(connection->connection_fd);

	
	if (resp != NULL)
	{
		if (resp->status_code & RPC_RETRY)
		{
			traceprint("\n Waiting for the retry the meta server\n");
			
			if(meta_retry)
			{
				rtn_stat = CLI_RPC_FAIL;
				goto finish;
			}

			sleep(5);
			
			meta_retry++;

			conn_destroy_resp(resp);
			resp = NULL;

			goto retry;
		}
		else if(!(resp->status_code & RPC_SUCCESS))
		{
			if (resp->status_code & ( RPC_TAB_HAS_NO_DATA 
						| RPC_TABLE_NOT_EXIST))
			{
				traceprint("Table has no data OR this table is not exist.\n");
			}
			else
			{
				traceprint("\n ERROR in response \n");
			}

			rtn_stat = CLI_FAIL;
			goto finish;
		}
	}
	
	if (resp == NULL)
	{
		traceprint("\n ERROR in response \n");
		rtn_stat = CLI_FAIL;
		goto finish;

	}
	
finish:

	return rtn_stat;
}


/*
** Send the requirment to the range server and receive the result of query
** in the case of SELECTRANGE and SELECTWHERE or CREATE INDEX.
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
static RG_CONN *
mt_cli_rgsel__ranger(CONN * connection, char * cmd, MT_CLI_EXEC_CONTEX *exec_ctx, 
			char *ip, int port)
{
	char	*send_rg_buf;			/* Buffer for the data sending
						** to the range server.
						*/
	int	send_rg_buflen;
	int	rtn_stat;			/* return state. */
	RG_CONN	*rg_connection;			/* Ptr to the context of 
						** connection to the range 
						** server.
						*/
	TABLEHDR *tab_hdr;
	int 	i;
	int	k;


	rg_connection = NULL;
	tab_hdr = NULL;
	k = -1;
	
	for(i = 0; i < connection->rg_list_len; i++)
	{
		if(   (port == connection->rg_list[i].rg_server_port)
		   && (!strcmp(ip, connection->rg_list[i].rg_server_ip))
		   && (connection->rg_list[i].status == ESTABLISHED))
		{
			rg_connection = &(connection->rg_list[i]);
			break;
		}

		if (connection->rg_list[i].status == CLOSED)
		{
			k = i;
		}
	}


	/*
	** While the input ranger server is not exist in the list of ranger ,
	** it will create the new connection to the ranger server and add
	** it into the list of range server.
	*/
	if(i == connection->rg_list_len)
	{
		if (k != -1)
		{
			/* Re-use the context of zomb connection. */
			Assert(connection->rg_list[k].status == CLOSED);

			rg_connection = &(connection->rg_list[k]);

			MEMSET(rg_connection, sizeof(RG_CONN));
		}
		else
		{
			rg_connection = &(connection->rg_list[i]);
		}
		
		rg_connection->rg_server_port = port;
		strcpy(rg_connection->rg_server_ip, ip);

		if((rg_connection->connection_fd = conn_open(rg_connection->rg_server_ip,
				rg_connection->rg_server_port)) < 0)
		{
			perror("error in create connection with rg server: ");

//			MEMFREEHEAP(rg_connection);
			rtn_stat = FALSE;

			return NULL;

		}
		
		rg_connection->status = ESTABLISHED;

//		connection->rg_list[connection->rg_list_len] = rg_connection;
		if (k == -1)
		{
			connection->rg_list_len++;
		}
	}

	int cmd_size = strlen(cmd);

#if 0
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
				cmd_size);

		/* Send the requirment to the range server. */
		tcp_put_data(rg_connection->connection_fd, send_rg_buf, 
			(sizeof(SELRANGE) + cmd_size + RPC_MAGIC_MAX_LEN));
	}
	else if (exec_ctx->querytype == SELECTWHERE)
	{
#endif
	MEMSET(((RPCRESP *)(exec_ctx->meta_resp))->result, RPC_MAGIC_MAX_LEN);

	
	if (exec_ctx->querytype == CRTINDEX)
	{
		MEMCPY(((RPCRESP *)(exec_ctx->meta_resp))->result, 
				RPC_CRT_INDEX_MAGIC, RPC_MAGIC_MAX_LEN);

		tab_hdr = (TABLEHDR *)(((RPCRESP *)(exec_ctx->meta_resp))->result +
					sizeof(IDXMETA) + sizeof(SVR_IDX_FILE));

		
		send_rg_buflen = RPC_MAGIC_MAX_LEN + sizeof(IDXMETA) + sizeof(TABLEHDR)
				+ (tab_hdr->tab_col) * sizeof(COLINFO) + cmd_size;
		
		send_rg_buf = malloc(send_rg_buflen);
		MEMSET(send_rg_buf, send_rg_buflen);

//		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);

		int	buf_idx = 0;

		PUT_TO_BUFFER(send_rg_buf, buf_idx, RPC_REQUEST_MAGIC, 
				RPC_MAGIC_MAX_LEN);

		PUT_TO_BUFFER(send_rg_buf, buf_idx, 
				((RPCRESP *)(exec_ctx->meta_resp))->result, 
				sizeof(IDXMETA));
		
//		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, 
//				((RPCRESP *)(exec_ctx->meta_resp))->result, 
//				sizeof(IDXMETA));

		PUT_TO_BUFFER(send_rg_buf, buf_idx, (char *)tab_hdr,
				sizeof(TABLEHDR)+ (tab_hdr->tab_col) * sizeof(COLINFO));

//		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(IDXMETA), (char *)tab_hdr,
//				sizeof(TABLEHDR)+ (tab_hdr->tab_col) * sizeof(COLINFO));

		PUT_TO_BUFFER(send_rg_buf, buf_idx, cmd, cmd_size);

//		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(IDXMETA) +
//				sizeof(TABLEHDR) + (tab_hdr->tab_col) * sizeof(COLINFO), 
//				cmd, cmd_size);
	}
	else
	{
		/* SELECTWHERE/SELECTRANGE/COUNT/SUM. */
		MEMCPY(((RPCRESP *)(exec_ctx->meta_resp))->result, 
				RPC_SELECTWHERE_MAGIC, RPC_MAGIC_MAX_LEN);


		tab_hdr = (TABLEHDR *)(((RPCRESP *)(exec_ctx->meta_resp))->result +
					sizeof(SELWHERE) + sizeof(SVR_IDX_FILE));

		
		send_rg_buflen = RPC_MAGIC_MAX_LEN + sizeof(SELWHERE) + sizeof(TABLEHDR)
				+ (tab_hdr->tab_col) * sizeof(COLINFO) + cmd_size;
		
		send_rg_buf = malloc(send_rg_buflen);
		MEMSET(send_rg_buf, send_rg_buflen);
		MEMCPY(send_rg_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);


		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN, 
				((RPCRESP *)(exec_ctx->meta_resp))->result, 
				sizeof(SELWHERE));

		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(SELWHERE), (char *)tab_hdr,
				sizeof(TABLEHDR)+ (tab_hdr->tab_col) * sizeof(COLINFO));
		
		MEMCPY(send_rg_buf + RPC_MAGIC_MAX_LEN + sizeof(SELWHERE) +
				sizeof(TABLEHDR) + (tab_hdr->tab_col) * sizeof(COLINFO), 
				cmd, cmd_size);
	}

	/* Send the requirment to the range server. */
	tcp_put_data(rg_connection->connection_fd, send_rg_buf, send_rg_buflen);
#if 0
	}
#endif

	free(send_rg_buf);

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
mt_cli_rgsel_is_bigdata(RG_CONN * rg_connection, int *bigdataport)
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
	int		sockfd;
	

	if (   (exec_ctx->querytype == SELECTCOUNT)
	    || (exec_ctx->querytype == SELECTSUM)
	    || (exec_ctx->querytype == CRTINDEX)
	    || (exec_ctx->querytype == DELETEWHERE)
	    || (exec_ctx->querytype == UPDATE))
	{
		sockfd = exec_ctx->rg_conn->connection_fd;
	}
	else
	{
		sockfd = exec_ctx->socketid;
	}

retry:	
	rg_resp = conn_recv_resp_abt(sockfd);

	switch (rg_resp->status_code)
	{
	    case RPC_UNAVAIL:
	
		traceprint("\n need to re-get rg meta \n");
		
		if (   (exec_ctx->querytype == SELECTCOUNT)
		    || (exec_ctx->querytype == SELECTSUM)
		    || (exec_ctx->querytype == DELETEWHERE)
		    || (exec_ctx->querytype == CRTINDEX)
		    || (exec_ctx->querytype == UPDATE))
		{
			exec_ctx->rg_conn->status = CLOSED;
		}
		else
		{
			exec_ctx->status |= CLICTX_SOCKET_CLOSED;
		}
		conn_close(sockfd, NULL, rg_resp);
		
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
			*rlen = ((RPCRESP *)(exec_ctx->rg_resp))->result_length;
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
#if 0
		else
		{
			mt_cli_write_range(exec_ctx->socketid);
			conn_destroy_resp((RPCRESP *)(exec_ctx->rg_resp));
				
			exec_ctx->rg_resp = NULL;
			exec_ctx->status &= ~CLICTX_DATA_BUF_HAS_DATA;
			exec_ctx->status |= CLICTX_DATA_BUF_NO_DATA;
		}
#endif
		break;

	    default:
	    	break;

	}

	return rp;
}



int
mt_cli_exec_builtin(MT_CLI_EXEC_CONTEX *exec_ctx)
{
	int	rtn_stat;


	rtn_stat = TRUE;

	switch (exec_ctx->querytype)
	{
	    case SELECTCOUNT:

		/* Fetch data from the range server. */
		if (!mt_cli_read_range(exec_ctx))
		{
			exec_ctx->status |= CLICTX_RANGER_IS_UNCONNECT;
			rtn_stat = FALSE;
		}

		exec_ctx->rowcnt = *(int *)(((RPCRESP *)(exec_ctx->rg_resp))->result);
		
		break;
		
	    case SELECTSUM:

		/* Fetch data from the range server. */
		if (!mt_cli_read_range(exec_ctx))
		{
			exec_ctx->status |= CLICTX_RANGER_IS_UNCONNECT;
			rtn_stat = FALSE;
		}

		exec_ctx->sum_colval = *(int *)(((RPCRESP *)(exec_ctx->rg_resp))->result);
		
		break;
		
	    case DELETEWHERE:

		if (!mt_cli_read_range(exec_ctx))
		{
			exec_ctx->status |= CLICTX_RANGER_IS_UNCONNECT;
			rtn_stat = FALSE;
		}
		
		break;

	    case UPDATE:

		if (!mt_cli_read_range(exec_ctx))
		{
			exec_ctx->status |= CLICTX_RANGER_IS_UNCONNECT;
			rtn_stat = FALSE;
		}
		
		break;
		
	    case CRTINDEX:
	    	if (!mt_cli_read_range(exec_ctx))
		{
			exec_ctx->status |= CLICTX_RANGER_IS_UNCONNECT;
			rtn_stat = FALSE;
		}
		
	    	break;

	    default:
	    	break;

	}

	return rtn_stat;
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

	if (   (exec_ctx->querytype == SELECTWHERE) 
	    || (exec_ctx->querytype == SELECTRANGE))
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
		return NULL;
	}
	
	meta_buf += sizeof(TABLEHDR);

	COLINFO *col_info = (COLINFO *)meta_buf;
	
	*collen = (col_info + col_idx)->col_len;
	char *ret_rp =  row_locate_col(rowbuf, (col_info + col_idx)->col_offset,
				tab_hdr->tab_row_minlen, collen);

	return ret_rp;
}

int
mt_cli_coltype_fixed(MT_CLI_EXEC_CONTEX *exec_ctx, int col_idx)
{
	char		*meta_buf;	/* Ptr to the meta data information. */


	/* Meta data information. */
	meta_buf = ((RPCRESP *)(exec_ctx->meta_resp))->result;

	if (   (exec_ctx->querytype == SELECTWHERE)
	    || (exec_ctx->querytype == SELECTRANGE))
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

	return TYPE_IS_FIXED((col_info + col_idx)->col_type);
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
mt_cli_open_execute(CONN *connection, char *cmd, MT_CLI_EXEC_CONTEX **exec_ctx)
{
	int 	querytype;	/* Query type. */
	int	s_idx;		/* The char index of command string. */
	int	rtn_stat;	/* Return state. */

	Assert (*exec_ctx == NULL);
		
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
		
	    case DROPTAB:
	    	rtn_stat = par_dropremovrebalanmcc_tab(cmd + s_idx, DROPTAB);
	    	break;
		
	    case SELECTWHERE:
	    	rtn_stat = par_selwherecnt_tab(cmd + s_idx, SELECTWHERE);
		break;

	    case DELETEWHERE:
	    	rtn_stat = par_selwherecnt_tab(cmd + s_idx, DELETEWHERE);
		break;

	    case UPDATE:
	    	tss->topid |= TSS_OP_UPDATE;
	    	rtn_stat = par_selwherecnt_tab(cmd + s_idx, UPDATE);
		break;

	    case SELECTCOUNT:
	    	rtn_stat = par_selwherecnt_tab(cmd + s_idx, SELECTCOUNT);
		break;

	    case SELECTSUM:
	    	rtn_stat = par_selwherecnt_tab(cmd + s_idx, SELECTSUM);
	    	break;
		
	    case CRTINDEX:
	    	rtn_stat = par_crt_idx_tab(cmd + s_idx, CRTINDEX);
		break;

	    case DROPINDEX:
	    	rtn_stat = par_dropremov_idx_tab(cmd + s_idx, DROPINDEX);
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
		return CLI_FAIL;
	}

	switch (querytype)
	{
	    case TABCREAT:
	    case INSERT:
	    case DELETE:
	    case SELECT:
		rtn_stat = mt_cli_exec_crtseldel(connection, cmd, exec_ctx);
		break;

	    case DROPINDEX:
	    case DROPTAB:
	    	rtn_stat = mt_cli_exec_drop_tab(connection, cmd, exec_ctx);
    		break;
		
	    case SELECTRANGE:
	    case SELECTWHERE:
	    case SELECTCOUNT:
	    case SELECTSUM:
	    case CRTINDEX:
	    case DELETEWHERE:
	    case UPDATE:
	    	rtn_stat = mt_cli_exec_selrang(connection, cmd, exec_ctx, querytype);
	    	
		break;

	    default:
	    	rtn_stat = CLI_FAIL;
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
	MT_CLI_EXEC_CONTEX	*t_exec_ctx;
	int	i;


	t_exec_ctx = exec_ctx;

	/*
	** Release the metadata infor that save the same copy on the EXEC_CTX,
	** so it's ok to release only one copy.
	*/
	if (t_exec_ctx->meta_resp)
	{
		conn_destroy_resp((RPCRESP *)(t_exec_ctx->meta_resp));
	}
	
	for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
	{
		
		t_exec_ctx->meta_resp = NULL;
		
		if(t_exec_ctx->rg_resp)
		{
			conn_destroy_resp((RPCRESP *)(t_exec_ctx->rg_resp));

			t_exec_ctx->rg_resp = NULL;
		}

		if (   (t_exec_ctx->querytype == SELECTRANGE) 
		    || (t_exec_ctx->querytype == SELECTWHERE))
		{
			if (!(   (t_exec_ctx->status & CLICTX_BAD_SOCKET)
			      || (t_exec_ctx->status & CLICTX_SOCKET_CLOSED)))
			{
				mt_cli_close_range(t_exec_ctx->socketid);
				mt_cli_write_range(t_exec_ctx->socketid);
			}
		}
		
		t_exec_ctx->status  = 0;

	}

	MEMFREEHEAP(exec_ctx);
		
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
		printf("DROP TABLE......drop table table_name\n");

		return TRUE;
	}

	return FALSE;
}

int
mt_mapred_get_splits(CONN *connection, MT_SPLIT ** splits, int * split_count, char * table_name)
{
	//LOCALTSS(tss);
	char		send_buf[LINE_BUF_SIZE];/* Buffer for the data sending. */
	int 	send_buf_size;		/* Buffer size. */
	RPCRESP 	*resp;			/* Ptr to the response information. */
	int 	rtn_stat;		/* Return state. */
	
	
	/* initialization. */
	rtn_stat = TRUE;
	
	send_buf_size = 0;
	MEMSET(send_buf, LINE_BUF_SIZE);
	
	PUT_TO_BUFFER(send_buf, send_buf_size, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_buf, send_buf_size, RPC_MAPRED_GET_SPLITS, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_buf, send_buf_size, table_name, strlen(table_name)+1);
	
	/* Send the requirment to the meta server. */
	tcp_put_data(connection->connection_fd, send_buf, send_buf_size);
	
	/* Get the splits info from the meta server. */
	resp = conn_recv_resp(connection->connection_fd);
	
	if ((resp == NULL) || (!(resp->status_code & RPC_SUCCESS)))
	{
		traceprint("\n ERROR in response: in get splits from meta \n");
		rtn_stat = FALSE;
		return rtn_stat;
	
	}
	
	if(resp)
	{
		*splits= (MT_SPLIT*)resp->result;
		*split_count = resp->result_length/sizeof(MT_SPLIT);

		int i;

		for(i = 0; i < *split_count; i++)
		{
		
			MEMCPY(((*splits)+i)->meta_ip, connection->meta_server_ip, sizeof(connection->meta_server_ip));
			((*splits)+i)->meta_port = connection->meta_server_port;
		}
		
		MEMFREEHEAP(resp);

	}
	
	return rtn_stat;
}



int mt_mapred_free_splits(MT_SPLIT * splits)
{
	MEMFREEHEAP(splits);
	return TRUE;
}

int mt_mapred_get_reader_meta(MT_READER * reader)
{
	char		send_buf[LINE_BUF_SIZE];/* Buffer for the data sending. */
	int 	send_buf_size;		/* Buffer size. */
	RPCRESP 	*resp;			/* Ptr to the response information. */
	int 	rtn_stat;		/* Return state. */
		
	/* initialization. */
	rtn_stat = TRUE;
		
	send_buf_size = 0;
	MEMSET(send_buf, LINE_BUF_SIZE);
		
	PUT_TO_BUFFER(send_buf, send_buf_size, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_buf, send_buf_size, RPC_MAPRED_GET_META, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_buf, send_buf_size, reader->table_name, strlen(reader->table_name)+1);
		
	/* Send the requirment to the meta server. */
	int fd;
	if((fd = conn_open(reader->meta_ip, reader->meta_port)) < 0)
	{
		perror("error in create connection with meta server: ");
	
		rtn_stat = FALSE;

		reader->status |= READER_BAD_SOCKET;
	
		return rtn_stat;
	
	}
	tcp_put_data(fd, send_buf, send_buf_size);
		
	/* Get the splits info from the meta server. */
	resp = conn_recv_resp(fd);
		
	if ((resp == NULL) || (!(resp->status_code & RPC_SUCCESS)))
	{
		traceprint("\n ERROR in response: in get meta from meta \n");
		rtn_stat = FALSE;
		return rtn_stat;
		
	}
		
	if(resp)
	{
		char * resp_buf = resp->result;
		
		reader->table_header= resp_buf;	

		resp_buf += sizeof(TABLEHDR);

		reader->col_info = resp_buf;

		MEMFREEHEAP(resp);
	}

	conn_close(fd, NULL, NULL);
		
	return rtn_stat;

}

int mt_mapred_create_reader(MT_READER ** mtreader, MT_SPLIT * split)
{
	MT_READER * reader = (MT_READER *)MEMALLOCHEAP(sizeof(MT_READER));
	MEMSET(reader, sizeof(MT_READER));
	*mtreader = reader;
	
	char	send_rg_buf[LINE_BUF_SIZE]; 
	int rtn_stat;			/* return state. */
	RG_CONN *rg_connection = &(reader->connection); 		

	rg_connection->rg_server_port = split->range_port;
	strcpy(rg_connection->rg_server_ip, split->range_ip);
	
	if((rg_connection->connection_fd = conn_open(rg_connection->rg_server_ip,
		rg_connection->rg_server_port)) < 0)
	{
		perror("error in create connection with rg server: ");
	
		rtn_stat = FALSE;

		reader->status |= READER_BAD_SOCKET;
	
		return rtn_stat;
	
	}
			
	rg_connection->status = ESTABLISHED;

	MEMCPY(reader->table_name, split->table_name, strlen(split->table_name));
	MEMCPY(reader->tablet_name, split->tablet_name, strlen(split->tablet_name));
	
	int send_buf_size = 0;
	MEMSET(send_rg_buf, LINE_BUF_SIZE);
	
	PUT_TO_BUFFER(send_rg_buf, send_buf_size, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_rg_buf, send_buf_size, RPC_MAPRED_GET_DATAPORT, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_rg_buf, send_buf_size, reader->table_name, strlen(reader->table_name)+1);
	PUT_TO_BUFFER(send_rg_buf, send_buf_size, reader->tablet_name, strlen(reader->tablet_name)+1);	

	/* Send the requirment to the range server. */
	tcp_put_data(rg_connection->connection_fd, send_rg_buf, send_buf_size);

	RPCRESP		*rg_resp;
	int data_port;
	
	rg_resp = conn_recv_resp_abt(rg_connection->connection_fd);

	switch (rg_resp->status_code)
	{
	    case RPC_BIGDATA_CONN:
	    	/* Successfully. */
	    	data_port = *(int *)rg_resp->result;
	    	rtn_stat = TRUE;
		break;
		
	    default:

			traceprint("\nfailed to create reader from rg\n");
			conn_close(rg_connection->connection_fd, NULL, rg_resp);
			reader->status |= READER_PORT_INVALID;
			rtn_stat = FALSE;
			return rtn_stat;
	
	}

	conn_destroy_resp(rg_resp);
	rg_resp = NULL;

	RG_CONN *data_connection = &(reader->data_connection); 		

	data_connection->rg_server_port = data_port;
	strcpy(data_connection->rg_server_ip, split->range_ip);
	
	while((data_connection->connection_fd = conn_open(data_connection->rg_server_ip,
		data_connection->rg_server_port)) < 0)
	{
		perror("error in create connection with rg server data port: ");
	
		sleep(1);	
	}
			
	data_connection->status = ESTABLISHED;

	reader->status |= READER_IS_OPEN;
	MEMCPY(reader->meta_ip, split->meta_ip, sizeof(split->meta_ip));
	reader->meta_port = split->meta_port;
	reader->block_cache = NULL;

	rtn_stat = mt_mapred_get_reader_meta(reader);

	return rtn_stat;
	
}

char*
mt_mapred_get_cache_nextvalue(MT_READER * reader, int * rp_len)
{
	MT_BLOCK_CACHE *cache = reader->block_cache;
	assert(cache);
	assert(cache->cache_index < cache->max_row_count);

	int * offset = ROW_OFFSET_PTR(cache->data_cache);

	offset -= cache->cache_index;

	char * rp = cache->data_cache + *offset;
	*rp_len = ROW_GET_LENGTH(rp, cache->row_min_len);

	cache->cache_index++;

	if(cache->cache_index == cache->max_row_count)
	{
		reader->status |= READER_CACHE_NO_DATA;
	}

	return rp;
}

int
mt_mapred_get_rg_nextvalue(MT_READER * reader)
{
	if(reader->block_cache)
	{
		MEMFREEHEAP(reader->block_cache);
		reader->block_cache = NULL;
	}

	char	send_rg_buf[LINE_BUF_SIZE]; 
	RG_CONN *rg_connection = &(reader->data_connection);		
		
	assert(rg_connection->status == ESTABLISHED);
	
	int send_buf_size = 0;
	MEMSET(send_rg_buf, LINE_BUF_SIZE);
			
	//PUT_TO_BUFFER(send_rg_buf, send_buf_size, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	PUT_TO_BUFFER(send_rg_buf, send_buf_size, RPC_MAPRED_GET_NEXT_VALUE, RPC_MAGIC_MAX_LEN);
	//PUT_TO_BUFFER(send_rg_buf, send_buf_size, reader->table_name, strlen(reader->table_name)+1);
	//PUT_TO_BUFFER(send_rg_buf, send_buf_size, reader->tablet_name, strlen(reader->tablet_name)+1);	
		
	/* Send the requirment to the range server. */
	tcp_put_data(rg_connection->connection_fd, send_rg_buf, send_buf_size);
		
	RPCRESP 	*rg_resp;
			
	rg_resp = conn_recv_resp_abt(rg_connection->connection_fd);
	
	switch (rg_resp->status_code)
	{
		case RPC_UNAVAIL: //to fix me, need to re-get split for this tablet from meta server
		
		traceprint("\n need to re-get split info \n");
			
		rg_connection->status = CLOSED;
		conn_close(rg_connection->connection_fd, NULL, rg_resp);
		conn_close(reader->connection.connection_fd, NULL, NULL);
		rg_resp = NULL;
			
		sleep(HEARTBEAT_INTERVAL + 1);
	
		reader->status |= READER_RG_INVALID;
	
		return FALSE;
	
		case RPC_NO_VALUE:
		traceprint("\n get rpc: RPC_NO_VALUE \n");
		reader->status |= READER_RG_NO_DATA;
		return FALSE;
			
		default:
		if (!(rg_resp->status_code & RPC_SUCCESS))
		{
			traceprint("\n ERROR in rg_server response \n");

			reader->status |= READER_READ_ERROR;
	
			return FALSE;
		}
	}
			

	assert(rg_resp->result != NULL);
		
	reader->block_cache = (MT_BLOCK_CACHE *)rg_resp->result;
	reader->block_cache->cache_index = 0;
		
	reader->status &= ~READER_CACHE_NO_DATA;

	MEMFREEHEAP(rg_resp);
	
	return TRUE;

}

char *
mt_mapred_get_nextvalue(MT_READER * reader, int * rp_len)
{
	if(reader->status & READER_RG_NO_DATA)
	{		
		traceprint("\n current split in range server come to eof\n");
		return NULL;
	}
	if((!reader->block_cache) || (reader->status & READER_CACHE_NO_DATA))
	{
		traceprint("\n current cache in reader come to eof\n");
		int ret = mt_mapred_get_rg_nextvalue(reader);

		if(!ret)
		{
			if(reader->status & READER_RG_NO_DATA)
			{
				traceprint("\n current split in range server come to eof\n");
				return NULL;
			}
			else
			{
				traceprint("\n error when get new value from rg, status: %d\n", reader->status);
				assert(0);
			}
		}
	}

	assert(!(reader->status & READER_CACHE_NO_DATA));

	return mt_mapred_get_cache_nextvalue(reader, rp_len);
}

char * mt_mapred_get_currentvalue(MT_READER * reader, char * row, int col_idx, int * value_len)
{
	TABLEHDR *table_hdr = (TABLEHDR *)reader->table_header;
	COLINFO * col_info = (COLINFO *)reader->col_info;
	/* Validation. */
	if ((col_idx < 0) || ((col_idx + 1) > table_hdr->tab_col))
	{
		traceprint("Can't find the column.\n");
		return NULL;
	}
		
	*value_len = (col_info + col_idx)->col_len;
	char *ret_rp =  row_locate_col(row, (col_info + col_idx)->col_offset,
				table_hdr->tab_row_minlen, value_len);

	return ret_rp;
}

int mt_mapred_free_reader(MT_READER * reader)
{
#if 0
	char	send_rg_buf[LINE_BUF_SIZE]; 
	rg_conn *rg_connection = &(reader->data_connection);		
			
	assert(rg_connection->status == ESTABLISHED);
		
	int send_buf_size = 0;
	MEMSET(send_rg_buf, LINE_BUF_SIZE);
				
	PUT_TO_BUFFER(send_rg_buf, send_buf_size, RPC_MAPRED_EXIT, RPC_MAGIC_MAX_LEN);
			
	// Send the requirment to the range server. 
	tcp_put_data(rg_connection->connection_fd, send_rg_buf, send_buf_size);
			
	RPCRESP 	*rg_resp;
				
	rg_resp = conn_recv_resp_abt(rg_connection->connection_fd);

	if (!(rg_resp->status_code & RPC_SUCCESS))
	{
		traceprint("\n ERROR in rg_server response \n");

		Assert(0);
	}
#endif
	conn_close(reader->connection.connection_fd, NULL, NULL);
	conn_close(reader->data_connection.connection_fd, NULL, NULL);

	if(reader->table_header)
		MEMFREEHEAP(reader->table_header);

	if(reader->block_cache)
		MEMFREEHEAP(reader->block_cache);

	MEMFREEHEAP(reader);

	return TRUE;
}

#define TYPE_INT        0x00
#define TYPE_VARCHAR    0x01
char * mt_mapred_reorg_value(MT_READER * reader, char * row, int row_len, int * new_row_len)
{
        char * new_row = (char *)malloc(row_len * 2);
        memset(new_row, 0, row_len*2);
        char * index = new_row;

        int i;

        TABLEHDR *table_hdr = (TABLEHDR *)reader->table_header;
        COLINFO * col_info = (COLINFO *)reader->col_info;

        //set column num
        *((int *)index) = table_hdr->tab_col;
        index += 4;
        //printf("total %d cols\n", table_hdr->tab_col);

        //set each colume
        for(i = 1; i < table_hdr->tab_col; i ++)
        {
                int value_len = (col_info + i)->col_len;
                char *value =  row_locate_col(row, (col_info + i)->col_offset,
                                table_hdr->tab_row_minlen, &value_len);

                int value_type = 0;

                if((col_info + i)->col_offset > 0)
                {
                        value_type |= TYPE_INT;
                }
                else
                {
                        value_type |= TYPE_VARCHAR;
                }

                value_type |= (value_len << 2);
                //printf("reorg debug%d: %s, %d, %d\n", i, value_len, value_type);

                *((int *)index) = value_type;
                index += 4;
                memcpy(index, value, value_len);
                index += value_len;
        }

        *new_row_len = index - new_row;
        //printf("new reorg row len: %d\n", *new_row_len);
        return new_row;
}

