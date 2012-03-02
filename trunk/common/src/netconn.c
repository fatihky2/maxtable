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
#include <sys/time.h>

#include "master/metaserver.h"
#include "utils.h"
#include "netconn.h"
#include "tss.h"
#include "parser.h"
#include "memcom.h"
#include "strings.h"
#include <pthread.h>
#include "buffer.h"
#include "block.h"
#include "thread.h"
#include "hkgc.h"
#include "m_socket.h"


extern	TSS	*Tss;

char *
conn_insert_resp_parser(char *result, char *rg_ip, int rg_port)
{
	int	addr_idx;
	char	port_addr[16];

	
	addr_idx = 0;
	result = trim(result, ' ');
	
	while(result[addr_idx++] != ':');

	MEMCPY(rg_ip, result, (addr_idx - 1));

	
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
	
	
	GET_FROM_BUFFER(resp_bp, data_index, resp->magic, sizeof(resp->magic));

	resp->status_code = ((RPCRESP *)resp_bp)->status_code;
	resp->result_length = ((RPCRESP *)resp_bp)->result_length;

	data_index += 2 * sizeof(int);

	if (resp->result_length)
	{
		
	//	(resp->result_length)++;
		
		resp->result = MEMALLOCHEAP(resp->result_length);
		MEMSET(resp->result, resp->result_length);
		GET_FROM_BUFFER(resp_bp, data_index, resp->result, 
				(resp->result_length));
	}
	
	return resp;
}

RPCRESP *
conn_build_resp_meta(char *resp_bp, char * recv_buf)
{    
	int     	data_index;
	RPCRESP		*resp;

	resp = (RPCRESP *)recv_buf; 
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
	
	
	GET_FROM_BUFFER(resp_bp, data_index, resp->magic, sizeof(resp->magic));

	resp->status_code = ((RPCRESP *)resp_bp)->status_code;
	resp->result_length = ((RPCRESP *)resp_bp)->result_length;

	data_index += 2 * sizeof(int);

	if (resp->result_length)
	{
		Assert((resp->result_length) < (HB_DATA_SIZE - sizeof(RPCRESP)));
		
	//	(resp->result_length)++;
		
		resp->result = recv_buf + sizeof(RPCRESP);
		MEMSET(resp->result, resp->result_length);
		GET_FROM_BUFFER(resp_bp, data_index, resp->result, 
				(resp->result_length));
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
	Assert(resp);

	MEMFREEHEAP(resp);
}

void
conn_destroy_resp(RPCRESP* resp)
{
	if (resp)
	{

		if (resp->result_length)
		{
			MEMFREEHEAP(resp->result);
		}
		
		MEMFREEHEAP(resp);
	}
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

	
	GET_FROM_BUFFER(data, data_index, req->data, (req->data_size));

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




/*
static void 
setTcpKeepAlive(int sockfd)
{
	int yes = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) 
	{
		;
	}
}
*/


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

	//setTcpKeepAlive(sockfd);

	ret = connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr));

	if (ret < 0)
	{
		traceprint("Connection is failed \n");
		return ret;
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

	buf = malloc(CONN_BUF_SIZE);

//	MEMSET(buf, CONN_BUF_SIZE);

//	n = read(sockfd, buf, CONN_BUF_SIZE);

	n = tcp_get_data(sockfd, buf, CONN_BUF_SIZE);

	//If n == 0, measn the remote node have encounter some bad problems
	if(n == 0)
	{    	
		resp = conn_build_resp(NULL);

		traceprint("Remote Server is not connectable!\n");
	}
	else
	{
		resp = conn_build_resp(buf);
	}
    
	free(buf);
	return resp;
}

RPCRESP * 
conn_recv_resp_abt(int sockfd)
{
	char 		*buf;
	int 		n;
	RPCRESP		*resp;

	buf = malloc(CONN_BUF_SIZE);

//	MEMSET(buf, CONN_BUF_SIZE);
	
	struct timeval tv;
	tv.tv_sec = RECVIO_TIMEOUT;
	tv.tv_usec = 0;
	setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

//	n = read(sockfd, buf, CONN_BUF_SIZE);

	n = tcp_get_data(sockfd, buf, CONN_BUF_SIZE);
	if(n > 0)
	{
		resp = conn_build_resp(buf);
	}
	else
	{
		if(n == 0)
		{
			traceprint("Rg server is closed after client send request, before client receive response!\n");
		}
		else if(errno == ECONNRESET)
		{
			traceprint("Rg server is closed before client send request!\n");
		}
		else if((errno == ETIMEDOUT)||(errno == EHOSTUNREACH)||(errno == ENETUNREACH))
		{
			traceprint("Rg server is breakdown before client send request!\n");
		}
		else if(errno == EWOULDBLOCK)
		{
			traceprint("Rg server is breakdown after client send request, before client receive response!\n");
		}
		else
		{
			traceprint("Client receive response error for unknown reason (ErrNum = %d)!\n", n);
			perror("Error in rg server response");
		}
		resp = conn_build_resp(NULL);
		resp->status_code = RPC_UNAVAIL;
	}
	

    	assert(resp);
	free(buf);
	return resp;
}


RPCRESP * 
conn_recv_resp_meta(int sockfd, char *recv_buf)
{
	char 		*buf;
	int 		n;
	RPCRESP		*resp;

	buf = malloc(CONN_BUF_SIZE);

//	MEMSET(buf, CONN_BUF_SIZE);
	
	struct timeval tv;
	tv.tv_sec = RECVIO_TIMEOUT;
	tv.tv_usec = 0;
	setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

//	n = read(sockfd, buf, CONN_BUF_SIZE);

	n = tcp_get_data(sockfd, buf, CONN_BUF_SIZE);

	if(n > 0)
	{
		resp = conn_build_resp_meta(buf, recv_buf);
	}
	else
	{
		if(n == 0)
		{
			traceprint("Rg server is closed after client send request, before client receive response!\n");
		}
		else if(errno == ECONNRESET)
		{
			traceprint("Rg server is closed before client send request!\n");
		}
		else if((errno == ETIMEDOUT)||(errno == EHOSTUNREACH)||(errno == ENETUNREACH))
		{
			traceprint("Rg server is breakdown before client send request!\n");
		}
		else if(errno == EWOULDBLOCK)
		{
			traceprint("Rg server is breakdown after client send request, before client receive response!\n");
		}
		else
		{
			traceprint("Client receive response error for unknown reason!\n");
			perror("Error in rg server response");
		}
		resp = conn_build_resp_meta(NULL, recv_buf);
		resp->status_code = RPC_UNAVAIL;
	}
	
    
	free(buf);
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
	else if (!strncasecmp(RPC_RBD_MAGIC, str, STRLEN(RPC_RBD_MAGIC)))
	{
		return RPC_REQ_REBALANCE_OP;
	}
	else if (!strncasecmp(RPC_SELECTRANGE_MAGIC, str, STRLEN(RPC_RBD_MAGIC)))
	{
		return RPC_REQ_SELECTRANGE_OP;
	}
	else if (!strncasecmp(RPC_MASTER2RG_NOTIFY, str, STRLEN(RPC_MASTER2RG_NOTIFY)))
	{
		return RPC_REQ_M2RNOTIFY_OP;
	}
	else if (!strncasecmp(RPC_MASTER2RG_HEARTBEAT, str, STRLEN(RPC_MASTER2RG_HEARTBEAT)))
	{
		return RPC_REQ_M2RHEARTBEAT_OP;
	}
	else if (!strncasecmp(RPC_SELECTWHERE_MAGIC, str, STRLEN(RPC_SELECTWHERE_MAGIC)))
	{
		return RPC_REQ_SELECWHERE_OP;
	}
	else if (!strncasecmp(RPC_RECOVERY, str, STRLEN(RPC_RECOVERY)))
	{
		return RPC_REQ_RECOVERY_RG_OP;
	}
	else if (!strncasecmp(RPC_MAPRED_GET_DATAPORT, str, STRLEN(RPC_MAPRED_GET_DATAPORT)))
	{
		return RPC_REQ_MAPRED_GET_DATAPORT_OP;
	}
	else if (!strncasecmp(RPC_MAPRED_GET_NEXT_VALUE, str, STRLEN(RPC_MAPRED_GET_NEXT_VALUE)))
	{
		return RPC_REQ_MAPRED_GET_NEXT_VALUE_OP;
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
//		n = read(connfd, buf, CONN_BUF_SIZE - 1);

		n = tcp_get_data(connfd, buf, CONN_BUF_SIZE - 1);

		if (n > 0) 
		{
			buf[n] = '\0'; 
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
  
		tcp_put_data(connfd, resp, resp_size);
		close(connfd);

		conn_destroy_req(req);
		conn_destroy_resp_byte(resp);
		tss_init(tss);
	}
}

int conn_socket_open(int servPort)
{
	struct sockaddr_in servaddr;
	int listenfd = socket(AF_INET, SOCK_STREAM, 0);
	

	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	servaddr.sin_port = htons(servPort);

	if(bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) != 0)
	{
		return 0;
	}

	listen(listenfd, LISTENQ);

	return listenfd;
}


int 
conn_socket_accept(int sockfd)
{	
	int		connfd;

	struct sockaddr_in cliaddr;
	socklen_t cliaddr_len = sizeof(cliaddr);
	
	//struct timeval tv;
	//tv.tv_sec = RECVIO_TIMEOUT;
	//tv.tv_usec = 0;
	//setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));	

	connfd = accept(sockfd, (struct sockaddr *)&cliaddr, &cliaddr_len);

	if(connfd > 0)
	{
		return connfd;
	}
	else
	{	
		if(errno == ECONNRESET)
		{
			traceprint("Rg server is closed before client send request!\n");
		}
		else if((errno == ETIMEDOUT)||(errno == EHOSTUNREACH)||(errno == ENETUNREACH))
		{
			traceprint("Rg server is breakdown before client send request!\n");
		}
		else if(errno == EWOULDBLOCK)
		{
			traceprint("Rg server is breakdown after client send request, before client receive response!\n");
		}
		else
		{
			traceprint("Client receive response error for unknown reason!\n");
			perror("Error in rg server response");
		}

		connfd = -1;
		
	}
	
    
	return connfd;
}


/* Currently, this method just be used to read the fixed (8 bytes) data from the client (mt_cli_write_range). */
int 
conn_socket_read(int sockfd, char *buf, int size)
{
	int	n;

	struct timeval tv;
	tv.tv_sec = RECVIO_TIMEOUT;
	tv.tv_usec = 0;
	setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

	n = read(sockfd, buf, size);

	if(n > 0)
	{
		buf[n] = '\0';
	}
	else
	{
		if(n == 0)
		{
			traceprint("Rg server is closed after client send request, before client receive response!\n");
		}
		else if(errno == ECONNRESET)
		{
			traceprint("Rg server is closed before client send request!\n");
		}
		else if((errno == ETIMEDOUT)||(errno == EHOSTUNREACH)||(errno == ENETUNREACH))
		{
			traceprint("Rg server is breakdown before client send request!\n");
		}
		else if(errno == EWOULDBLOCK)
		{
			traceprint("Rg server is breakdown after client send request, before client receive response!\n");
		}
		else
		{
			traceprint("Client receive response error for unknown reason!\n");
			perror("Error in rg server response");
		}

	}

	return n;
}

void conn_socket_close(int sockfd)
{
	close(sockfd);
}


void startup(int servPort, int opid, char * (*handler_request)(char *req_buf, int fd))
{
	

	pthread_t pthread_id;
	pthread_t pthread_id2;
	int	*tmpid;


	msg_recv_args * args = MEMALLOCHEAP(sizeof(msg_recv_args));
	args->port = servPort;

	pthread_create(&pthread_id, NULL, msg_recv, (void *)args);

	if (opid == TSS_OP_RANGESERVER)
	{
		tmpid = &opid;
		pthread_create(&pthread_id2, NULL, hkgc_boot, (void *)tmpid);
	}
	
	//start_daemon(listenfd, handler_request);
	msg_process(handler_request);
}
