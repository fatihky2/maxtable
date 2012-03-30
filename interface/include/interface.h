/*
** interface.h 2011-07-05 xueyingfei
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

#ifndef	__INTERFACE_H
#define __INTERFACE_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "spinlock.h"


#define MAX_RG_NUM 256

#define ESTABLISHED 1
#define CLOSED 2

#define SUC_RET "client operation finished successfully"

/* The connection context of ranger server. */
typedef struct rg_conn
{
	char	rg_server_ip[32];	/* Ranger server address. */
	int	rg_server_port;		/* Port. */
	int	status;			/* Status for the ranger server. */
	int	connection_fd;		/* Socket id. */
	int	pad;
}RG_CONN;

/* The connection context of master server. */
typedef struct conn
{
	char	meta_server_ip[32];	/* Meta server address. */
	int	meta_server_port;	/* Port. */
	int	connection_fd;		/* Socket id. */
	int	status;			/* Status for the meta server. */
	RG_CONN rg_list[MAX_RG_NUM];	/* Rangers connected by the client. */
	int	rg_list_len;		/* The # of rangers connected by the 
					** client.
					*/
	
}CONN;

typedef struct mt_cli_context
{
	LOCKATTR mutexattr;
	SPINLOCK mutex;
}MT_CLI_CONTEXT;


/* Following definition is for the return value of mt_cli_exec_crtseldel(). */
#define	CLI_FAIL		-1	/* The operation is failed. */
#define	CLI_RPC_FAIL		-2	/* Hit the RPC issue, such as error or 
					** no response.
					*/
#define	CLI_SUCCESS		0	/* Success. */
#define	CLI_TABLE_NOT_EXIST	1	/* Table is not exist, this flag can 
					** warnning user to create the table.
					*/
#define	CLI_HAS_NO_DATA		2	/* Table has no data. */

/* The contex of user query execution. */
typedef struct mt_cli_exec_contex
{
	int	status;		/* Status for the query execution. */
	int	rg_cnt;		/* The # of online ranger. */
	int	querytype;	/* Query type. */
	int	first_rowpos;	/* For the SELECT query. */
	int	end_rowpos;	/* For the SELECT query. */
	int	cur_rowpos;	/* For the SELECT query. */
	int	rowcnt;		/* For the SELECTCOUNT. */
	int	sum_colval;	/* For the SELECTSUM. */
	int	rowminlen;	/* The min-length of row. */
	int	socketid;	/* connection for the bigport. */
	char	*rg_resp;	/* The response of ranger that contains the data. */
	char	*meta_resp;	/* The response of meta that contains the meta data. */
	RG_CONN	*rg_conn;	/* For the SELECTCOUNT. */
}MT_CLI_EXEC_CONTEX;

#define	CLICTX_DATA_BUF_HAS_DATA	0x0001		/* has data */
#define	CLICTX_DATA_BUF_NO_DATA		0x0002		/* no data and need to read if we want the next */
#define	CLICTX_IS_OPEN			0x0004		/* client context has been openned. */
#define	CLICTX_BAD_SOCKET		0x0008		/* fail to get the socket from bigport. */
#define	CLICTX_RANGER_IS_UNCONNECT	0x0010		/* fail to connect to the ranger server. */
#define	CLICTX_SOCKET_CLOSED		0x0020		/* socket (bigdata port) has been closed. */


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
extern void
mt_cli_crt_context();

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
extern void
mt_cli_destroy_context();


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
extern int
mt_cli_open_connection(char * meta_ip, int meta_port, CONN ** connection);

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
extern void
mt_cli_close_connection(CONN * connection);


/*
** Open the context to execute the command user specified.
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
extern int 
mt_cli_open_execute(CONN *connection, char *cmd, MT_CLI_EXEC_CONTEX **exec_ctx);


/*
** Close the execution context.
**
** Parameters:
**	exec_ctx		- (IN) the execution context.
** 
** Returns:
**	None.
** 
** Side Effects
**	None
** 
*/
extern void 
mt_cli_close_execute(MT_CLI_EXEC_CONTEX *exec_ctx);


/*
** Get the row from the execution context.
**
** Parameters:
**	exec_ctx		- (IN) the execution context.
**	rlen		- (OUT) the length of row.
** 
** Returns:
**	The pointer of the row value.
** 
** Side Effects
**	It may change the content in the execution context. 
** 
*/
extern char *
mt_cli_get_nextrow(MT_CLI_EXEC_CONTEX *exec_ctx, int *rlen);


/*
** Get the column user specified from the returned row.
**
** Parameters:
**	exec_ctx		- (IN) the execution context.
**	row_buf		- (IN) the row pointer.
**	col_idx		- (IN) the column index user specified.		
**	collen		- (OUT) the length of column.
** 
** Returns:
**	The pointer of the column value.
** 
** Side Effects
**	It may change the content in the execution context. 
** 
*/
extern char *
mt_cli_get_colvalue(MT_CLI_EXEC_CONTEX *exec_ctx, char *rowbuf, int col_idx, int *collen);

extern int
mt_cli_exec_builtin(MT_CLI_EXEC_CONTEX *exec_ctx);


typedef struct _mt_split
{
	char	table_name[128];
	char	tablet_name[128];
	char	range_ip[32];
	int	range_port;

	char	meta_ip[32];
	int	meta_port;
}MT_SPLIT;

typedef struct _mt_block_cache
{
	char	data_cache[BLOCKSIZE];
	char	current_sstable_name[128];
	int	current_block_index;
	int	cache_index;
	int	max_row_count;
	int	row_min_len;
}MT_BLOCK_CACHE;


typedef struct _mt_reader
{
	RG_CONN	connection;
	RG_CONN data_connection;
	char	table_name[128];
	char	tablet_name[128];

	MT_BLOCK_CACHE * block_cache;
	int	status;

	char	meta_ip[32];
	int	meta_port;

	char 	*table_header;
	char 	*col_info;
}MT_READER;

#define	READER_CACHE_NO_DATA		0x0001		/* reader cache has no data */
#define	READER_IS_OPEN			0x0002		
#define	READER_BAD_SOCKET		0x0004		
#define	READER_RG_INVALID	0x0008	
#define	READER_PORT_INVALID	0x0010
#define	READER_RG_NO_DATA	0x0020
#define	READER_READ_ERROR	0x0040




extern int
mt_mapred_get_splits(CONN *connection, MT_SPLIT ** splits, int * split_count, char * table_name);

extern int 
mt_mapred_free_splits(MT_SPLIT * splits);

extern int
mt_mapred_create_reader(MT_READER * * mtreader, MT_SPLIT * split);

extern int
mt_mapred_free_reader(MT_READER * reader);

extern char *
mt_mapred_get_nextvalue(MT_READER * reader, int * rp_len);

extern char *
mt_mapred_get_currentvalue(MT_READER * reader, char * row, int col_idx, int * value_len);

extern char *
mt_mapred_reorg_value(MT_READER * reader, char * row, int row_len, int * new_row_len);

#endif

