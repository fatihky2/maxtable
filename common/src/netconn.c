/*
** netconn.c 2010-06-28 xueyingfei
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
#include "utils.h"
#include "netconn.h"
#include "tss.h"
#include "parser.h"
#include "memcom.h"
#include "strings.h"
#include <pthread.h>
#include "thread.h"
#include "hkgc.h"

extern	TSS	*Tss;

char *
conn_insert_resp_parser(char *result, char *rg_ip, int rg_port)
{
	int	addr_idx;
	char	port_addr[16];

	/* Parse the range IP. */
	addr_idx = 0;
	result = trim(result, ' ');
	
	while(result[addr_idx++] != ':');

	MEMCPY(rg_ip, result, (addr_idx - 1));

	/* Parse the range port. */
	result = &(result[addr_idx]);
	
	result = trim(result, ' ');

	addr_idx = 0;
	while(result[addr_idx] != ':')
	{
		port_addr[addr_idx] = result[addr_idx];
		addr_idx++;
	}

	port_addr[addr_idx] = '\0';
	rg_port = atoi(port_addr);

	/* Parse the sstable file name. */
	result = &(result[addr_idx++]);
	result = trim(result, ' ');

	return result;
}

RPCRESP *
conn_build_resp(char *resp_bp)
{    
	int     	data_index;
	RPCRESP		*resp;

	resp = MEMALLOCHEAP(sizeof(RPCRESP));
	MEMSET(resp, sizeof(RPCRESP));
	
	if (resp_bp == NULL)
	{
		strcpy(resp->magic, RPC_RESPONSE_MAGIC);
		resp->status_code = RPC_FAIL;
		resp->result_length = 0;
		resp->result = NULL;

		return resp;
	}

	data_index = 0;

	if (!conn_chk_respmagic(resp_bp))
	{
		return NULL;
	}
	
	/* Parse the buffer. */
	GET_FROM_BUFFER(resp_bp, data_index, resp->magic, sizeof(resp->magic));

	resp->status_code = ((RPCRESP *)resp_bp)->status_code;
	resp->result_length = ((RPCRESP *)resp_bp)->result_length;

	data_index += 2 * sizeof(int);

	if (resp->result_length)
	{
		resp->result = MEMALLOCHEAP(resp->result_length);
		GET_FROM_BUFFER(resp_bp, data_index, resp->result, 
				resp->result_length);
	}
	
	return resp;
}

int
conn_get_resp_size(RPCRESP *resp)
{
	int size;

	size =0;
	size += sizeof(resp->magic);
	size += sizeof(resp->status_code);
	size += sizeof(resp->result_length);
	size += resp->result_length;

	return size;
}

char *
conn_build_resp_byte(int status, int result_len, char *result)
{
	int 	resp_buf_idx;
	int	resp_size;
	char	*des_buf;
	
	resp_buf_idx = 0;	

	/* Need to discount the size of pointer of result. */
	resp_size = sizeof(RPCRESP) - sizeof(char *) + result_len;

	des_buf = MEMALLOCHEAP(resp_size);	

	PUT_TO_BUFFER(des_buf, resp_buf_idx, RPC_RESPONSE_MAGIC, 
					RPC_MAGIC_MAX_LEN);
	((RPCRESP *)des_buf)->status_code = status;
	((RPCRESP *)des_buf)->result_length = result_len;

	resp_buf_idx += 2 * sizeof(int);

	if (result_len)
	{
		PUT_TO_BUFFER(des_buf, resp_buf_idx, result, result_len);
	}

	return des_buf;
}

void
conn_destroy_resp_byte(char* resp)
{
	assert(resp);

	MEMFREEHEAP(resp);
}

void
conn_destroy_resp(RPCRESP* resp)
{
	assert(resp);

	if (resp->result_length)
	{
		MEMFREEHEAP(resp->result);
	}
	
	MEMFREEHEAP(resp);
}

RPCREQ *
conn_build_req(char *data, int data_len)
{
	int     data_index;
	RPCREQ  *req;


	if (!conn_chk_reqmagic(data))
	{
		return NULL;
	}

	req = MEMALLOCHEAP(sizeof(RPCREQ));
	MEMSET(req, sizeof(RPCREQ));

	data_index = 0;
	GET_FROM_BUFFER(data, data_index, req->magic, sizeof(req->magic));
		
	req->data_size = data_len - RPC_MAGIC_MAX_LEN + 1;

	req->data = MEMALLOCHEAP(req->data_size);
	MEMSET(req->data, req->data_size);

	GET_FROM_BUFFER(data, data_index, req->data, req->data_size);

	return req;
}

int
conn_req_byte_size(RPCREQ  *req)
{
	int size;

	size = 0;
	size += sizeof(req->magic);

	size += sizeof(int);

	size += req->data_size;

	return size;    
}

void
conn_req_byte_buf(char *rpc_buf, RPCREQ  *req)
{
	int rpc_buf_idx;

	rpc_buf_idx = 0;

	PUT_TO_BUFFER(rpc_buf, rpc_buf_idx, req->magic, sizeof(req->magic));
	PUT_TO_BUFFER(rpc_buf, rpc_buf_idx, &(req->data_size), sizeof(int));
	PUT_TO_BUFFER(rpc_buf, rpc_buf_idx, req->data, req->data_size);
}

void
conn_destroy_req(RPCREQ *req)
{
	if (req)
	{
		if(req->data_size)
		{
			MEMFREEHEAP(req->data);
		}
		
		MEMFREEHEAP(req);
	}
}

/** client part **/

/** Currently, mainly used in the server(Callee) side for dispatching the data faster **/
static void 
setTcpNoDelay(int sockfd)
{
	int yes = 1;
	if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1)
	{
		;
	}
}

/** Currently, mainly used in the client(Caller) side for ending the call if the target is not online **/
static void 
setTcpKeepAlive(int sockfd)
{
	int yes = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) 
	{
		;
	}
}

static void 
setTcpReuse(int sockfd)
{
	int yes = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) 
	{
		;
	}
}


/* need to free the result */
int 
conn_open(char* ip_address, int port)
{
	struct sockaddr_in servaddr;
	int sockfd;
	int ret;

	sockfd = socket(AF_INET, SOCK_STREAM, 0);

	if (sockfd < 0) 
	{
		goto cleanup;     
	}
	bzero(&servaddr, sizeof(servaddr));

	servaddr.sin_family = AF_INET;
	inet_pton(AF_INET, ip_address, &servaddr.sin_addr);
	servaddr.sin_port = htons(port);

	setTcpKeepAlive(sockfd);

	ret = connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr));

	if (ret < 0)
	{
		printf("Connection is failed \n");
	}

cleanup:    
	return sockfd;
}

int
conn_send_req(RPCREQ* req, int sockfd)
{
	int ret;  
	char    *rpc_buf;
	int rpc_buf_size;

	rpc_buf_size = conn_req_byte_size(req);

	rpc_buf = MEMALLOCHEAP(rpc_buf_size);

	conn_req_byte_buf(rpc_buf, req);

	//process rpc request
	ret = write(sockfd, rpc_buf, rpc_buf_size);

	return ret;
    
}

RPCRESP * 
conn_recv_resp(int sockfd)
{
	char 		*buf;
	int 		n;
	RPCRESP		*resp;

	buf = MEMALLOCHEAP(CONN_BUF_SIZE);

	n = read(sockfd, buf, CONN_BUF_SIZE);

	//If n == 0, measn the remote node have encounter some bad problems
	if(n == 0)
	{    	
		resp = conn_build_resp(NULL);

		printf("Remote Server is not connectable!\n");
	}
	else
	{
		resp = conn_build_resp(buf);
	}
    
	MEMFREEHEAP(buf);
	return resp;
}

void
conn_close(int sockfd, RPCREQ* req, RPCRESP* resp)
{
	conn_destroy_req(req);
	conn_destroy_resp(resp);
	close(sockfd);
}


int
conn_chk_reqmagic(char *str)
{
	if (!strncasecmp(RPC_REQUEST_MAGIC, str, STRLEN(RPC_REQUEST_MAGIC)))
	{
		return RPC_REQ_NORMAL_OP;
	}
	else if (!strncasecmp(RPC_DROP_TABLE_MAGIC, str, STRLEN(RPC_DROP_TABLE_MAGIC)))
	{
		return RPC_REQ_DROP_OP;
	}
		

	return 0;
}

int
conn_chk_respmagic(char *str)
{
	if (!strncasecmp(RPC_RESPONSE_MAGIC, str, STRLEN(RPC_RESPONSE_MAGIC)))
	{
		return TRUE;
	}

	return FALSE;
}


/** server part **/
void 
start_daemon(int listenfd, char * (*handler_request)(char *req_buf))
{
	LOCALTSS(tss);
	int ret;
	struct sockaddr_in cliaddr;
	int connfd, n;
	char buf[CONN_BUF_SIZE];
	int resp_size;
	RPCREQ* req;
	char* resp;


	while(TRUE)
	{
		socklen_t cliaddr_len = sizeof(cliaddr);
		connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &cliaddr_len);
		if (connfd < 0) 
		{
			if (connfd == EINTR) 
			{
				continue;
			}
			break;
		}
		MEMSET(buf, CONN_BUF_SIZE);

_read_again:
		n = read(connfd, buf, CONN_BUF_SIZE - 1);
		if (n > 0) 
		{
			buf[n] = '\0'; /* terminate the string */
		} 
		else if (n == 0) 
		{
			close(connfd);
		} 
		else if (errno == EINTR) 
		{
			goto _read_again;
		} 
		else 
		{
			close(connfd);
			ret = errno;
			break;
		}

		req = conn_build_req(buf, n);
		  
		if(n)
		{
			resp = handler_request(req->data);

			if (resp == NULL)
			{
				resp = conn_build_resp_byte(RPC_PARSER_ERR, 0, NULL);
			}
		}
		else
		{	resp = conn_build_resp_byte(RPC_PARSER_ERR, 0, NULL);
		}

		resp_size = conn_get_resp_size((RPCRESP *)resp);
  
		write(connfd, resp, resp_size);
		close(connfd);

		conn_destroy_req(req);
		conn_destroy_resp_byte(resp);
		tss_init(tss);
	}
}

void startup(int servPort, int opid, char * (*handler_request)(char *req_buf))
{
	/*struct sockaddr_in servaddr;
	int listenfd = socket(AF_INET, SOCK_STREAM, 0);

	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	servaddr.sin_port = htons(servPort);

	setTcpReuse(listenfd);
	setTcpNoDelay(listenfd);

	if(bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) != 0)
	{
		exit(-1);
	}

	listen(listenfd, 20);
	*/

	pthread_t pthread_id;
	pthread_t pthread_id1;
	
	tss_setup(opid);

	msg_recv_args * args = MEMALLOCHEAP(sizeof(msg_recv_args));
	args->port = servPort;

	pthread_create(&pthread_id, NULL, msg_recv, (void *)args);

	pthread_create(&pthread_id1, NULL, hkgc_boot, NULL);

	//start_daemon(listenfd, handler_request);
	msg_process(handler_request);
}
