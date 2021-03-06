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

#ifndef NETCONN_H_
#define NETCONN_H_

#include "global.h"


#define RPC_DATA_LOCATION	8
#define RPC_MAGIC_MAX_LEN	12
#define RPC_REQUEST_MAGIC "rpcrqst"
#define RPC_RESPONSE_MAGIC "rpcresp"

/* Following is the special magic number. */
#define	RPC_DROP_TABLE_MAGIC	"droptab"
#define RPC_RG2MASTER_REPORT	"rg_rpt"
#define RPC_RBD_MAGIC		"rebalan"
#define RPC_SELECTRANGE_MAGIC	"sel_rg"
#define RPC_MASTER2RG_HEARTBEAT	"hb_rqst"
#define RPC_FAILOVER 		"rgfailo"
#define RPC_MASTER2RG_NOTIFY	"rsync"
#define RPC_SELECTWHERE_MAGIC	"sel_wh"
#define	RPC_RECOVERY		"rg_reco"
#define RPC_MAPRED_GET_SPLITS	"rgsplit"
#define RPC_MAPRED_GET_META	"rgmeta"
#define RPC_MAPRED_GET_DATAPORT "dataport"
#define RPC_MAPRED_GET_NEXT_VALUE "nextval"
#define RPC_MAPRED_EXIT		"rg_exit"
#define	RPC_CRT_INDEX_MAGIC	"crt_idx"
#define	RPC_IDXROOT_SPLIT_MAGIC	"idxrspl"
#define	RPC_CRTIDX_DONE_MAGIC	"idxcrtd"
#define	RPC_DROPIDX_MAGIC	"dropidx"
#define	RPC_MCCSSTAB_MAGIC	"mccssta"





#define RECVIO_TIMEOUT			30
#define HEARTBEAT_INTERVAL		30

/*
** The Buffer Pool for holding RPCRESP Data
**	Note: It must be greater than the BLOCKSIZE.
*/
#define CONN_BUF_SIZE (1025 * 1024)

typedef struct rpcreq
{
	char    magic[RPC_MAGIC_MAX_LEN];
	int     data_size;
	char    *data;
}RPCREQ;

typedef struct rpcresp
{
	char	magic[RPC_MAGIC_MAX_LEN];
	int	status_code; 
	int	result_length;	/* used in serializing and deserializing */
	char	*result;
}RPCRESP;


#define RPC_SUCCESS		0x0001	
#define RPC_CONN_FAIL		0x0002	
#define RPC_FAIL		0x0004 	
#define RPC_PARSER_ERR		0x0008
#define RPC_RETRY		0x0010	
#define RPC_UNAVAIL		0x0020
#define RPC_BIGDATA_CONN	0x0040
#define	RPC_TABLE_NOT_EXIST	0x0080
#define	RPC_SKIP_SEND		0x0100	/* Skip this rpc sending, such as the
					** select where/range. 
					*/
#define	RPC_TAB_HAS_NO_DATA	0x0200	/* Flag this is an empty table. */
#define RPC_NO_VALUE		0x0400

/*
** Copy data from the transmission buffer to the location
** specified by the pointer.
*/  
#define GET_FROM_BUFFER( buf, idx, ptr, size )		\
{							\
	MEMCPY ( ptr, buf+idx, size );                  \
	idx += size;                                    \
}

#define PUT_TO_BUFFER( buf, idx, ptr, size )		\
{							\
	MEMCPY (buf+idx, ptr, size);                    \
	idx += size;                                    \
}


#define	RPC_REQ_NORMAL_OP			0x0001
#define RPC_REQ_DROPTAB_OP			0x0002
#define RPC_REQ_REBALANCE_OP			0x0004
#define RPC_REQ_SELECTRANGE_OP			0x0008
#define RPC_REQ_M2RNOTIFY_OP			0x0010
#define RPC_REQ_M2RHEARTBEAT_OP			0x0020
#define	RPC_REQ_SELECWHERE_OP			0x0040
#define	RPC_REQ_RECOVERY_RG_OP			0x0080
#define RPC_REQ_MAPRED_GET_DATAPORT_OP 		0x0100
#define RPC_REQ_MAPRED_GET_NEXT_VALUE_OP	0x0200
#define	RPC_REQ_CRT_IDX_OP			0x0400
#define	RPC_IDXROOT_SPLIT_OP			0x0800
#define	RPC_REQ_DROPIDX_OP			0x1000
#define	RPC_REQ_MCCSSTAB_OP			0x2000


char *
conn_insert_resp_parser(char *result, char *rg_ip, int rg_port);

RPCRESP*
conn_build_resp(char *resp_bp);

int
conn_get_resp_size(RPCRESP *resp);

char *
conn_build_resp_byte(int status, int result_len, char *result);

void
conn_destroy_resp(RPCRESP* resp);

RPCREQ *
conn_build_req(char *data, int data_len);

int
conn_req_byte_size(RPCREQ  *req);

void
conn_req_byte_buf(char *rpc_buf, RPCREQ  *req);

void
conn_destroy_req(RPCREQ *req);

int 
conn_open(char* ip_address, int port);

int
conn_send_req(RPCREQ* req, int sockfd);

RPCRESP * 
conn_recv_resp(int sockfd);

RPCRESP * 
conn_recv_resp_abt(int sockfd);

RPCRESP * 
conn_recv_resp_meta(int sockfd, char *recv_buf);


void
conn_close(int sockfd, RPCREQ* req, RPCRESP* resp);

int
conn_chk_reqmagic(char *str);

int
conn_chk_respmagic(char *str);

void
conn_destroy_resp_byte(char* resp);

int 
conn_socket_open(int servPort);

int 
conn_socket_accept(int sockfd);

int 
conn_socket_read(int sockfd, char *buf, int size);

void 
conn_socket_close(int sockfd);


void 
start_daemon(int listenfd, char * (*handler_request)(char *req_buf));

void 
startup(int servPort, int opid, char * (*handler_request)(char *req_buf, int fd));

#endif
