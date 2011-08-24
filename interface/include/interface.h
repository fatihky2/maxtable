#ifndef	__INTERFACE_H
#define __INTERFACE_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

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


#endif

