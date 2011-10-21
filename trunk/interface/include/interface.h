#ifndef	__INTERFACE_H
#define __INTERFACE_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define	BLOCKSIZE		(64 * 1024)


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
	char	data[BLOCKSIZE];
}range_query_contex;


#define	DATA_CONT	0x0001	
#define DATA_DONE	0x0002

/*
create one connection between cli and svr, return the connection
*/
extern int  cli_connection(char * meta_ip, int meta_port, conn ** connection);

/*
close one connection between cli and svr
*/
extern void cli_exit(conn * connection);

/*
commit one request
*/
extern int cli_commit(conn * connection, char * cmd, char * response, int * length);


extern char *
cli_open_range(conn * connection, char * cmd);

extern rg_conn *
cli_rgsel_send(conn * connection, char * cmd, char *selrg);

extern range_query_contex *
cli_rgsel_recv(rg_conn * rg_connection);

extern void
cli_close_range(char *selrg);


#endif

