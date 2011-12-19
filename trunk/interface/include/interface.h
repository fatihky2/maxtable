#ifndef	__INTERFACE_H
#define __INTERFACE_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "spinlock.h"


#define	BLOCKSIZE		(64 * 1024)
//#define	BLOCKSIZE		512



#define MAX_RG_NUM 256

#define ESTABLISHED 1
#define CLOSED 2

#define SUC_RET "client operation finished successfully"

typedef struct _rg_conn
{
	char rg_server_ip[24];
	int rg_server_port;
	int status;
	int connection_fd;
}rg_conn;

typedef struct _conn
{
	char meta_server_ip[24];
	int meta_server_port;
	int connection_fd;
	int status;
	rg_conn * rg_list[MAX_RG_NUM];
	int rg_list_len;
	
}conn;

typedef struct _range_query_contex
{
	int	status;
	int	first_rowpos;
	int	end_rowpos;
	int	cur_rowpos;
	int	rowminlen;
	char	data[BLOCKSIZE];
}RANGE_QUERYCTX;

typedef struct mt_cli_context
{
	LOCKATTR mutexattr;
	SPINLOCK mutex;
}MT_CLI_CONTEXT;

#define	DATA_CONT	0x0001	
#define DATA_DONE	0x0002
#define DATA_EMPTY	0x0004


/* Following definition is for the return value of mt_cli_exec_crtseldel(). */
#define	CLI_FAIL		0x0001
#define	CLI_SUCCESS		0x0002
#define	CLI_RPC_FAIL		0x0004
#define	CLI_TABLE_NOT_EXIST	0x0008

typedef struct mt_cli_exec_contex
{
	int	status;
	int	querytype;
	int	first_rowpos;
	int	end_rowpos;
	int	cur_rowpos;
	int	rowminlen;
	int	socketid;	/* connection for the bigport. */
	char	*rg_resp;
	char	*meta_resp;
}MT_CLI_EXEC_CONTEX;

#define	CLICTX_DATA_BUF_HAS_DATA	0x0001		/* has data */
#define	CLICTX_DATA_BUF_NO_DATA		0x0002		/* no data and need to read if we want the next */
#define	CLICTX_IS_OPEN			0x0004		/* client context has been openned. */
#define	CLICTX_BAD_SOCKET		0x0008		/* fail to get the socket from bigport. */
#define	CLICTX_RANGER_IS_UNCONNECT	0x0010		/* fail to connect to the ranger server. */	


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
mt_cli_open_connection(char * meta_ip, int meta_port, conn ** connection);

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
mt_cli_close_connection(conn * connection);


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
mt_cli_open_execute(conn *connection, char *cmd, MT_CLI_EXEC_CONTEX *exec_ctx);


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


#endif

