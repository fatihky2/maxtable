**1. Intoduction to the client API
```

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
extern int
mt_cli_result_meta(CONN * connection, MT_CLI_EXEC_CONTEX *exec_ctx, int result);


```**


**2. Sample code.**

```

int main(int argc, char *argv[])
{
	CONN 	*connection;
	char	resp[256], cmd[256];
	int	rtn_stat;
	int	i;
	

	if (argc != 2)
	{
		printf("Testing create table: ./sample create\n");
		printf("Testing insert data:  ./sample insert\n");
		printf("Testing delete data:  ./sample delete\n");
                printf("Testing deletewhere:  ./sample deletewhere\n");
                printf("Testing update data:  ./sample update\n");
		printf("Testing select data:  ./sample select\n");
		printf("Testing selectrange:  ./sample selectrange\n");
		printf("Testing selectwhere:  ./sample selectwhere\n");
		printf("Testing selectcount:  ./sample selectcount\n");
		printf("Testing selectsum:    ./sample selectsum\n");
		printf("Testing crtindex:     ./sample crtindex\n");
		printf("Testing dropindex:    ./sample dropindex\n");
		printf("Testing drop table:   ./sample drop\n");

		return 0;
	}

	mt_cli_crt_context();

	MT_CLI_EXEC_CONTEX *exec_ctx = NULL;
	
	if(mt_cli_open_connection("127.0.0.1", 1959, &connection))
	{
		gettimeofday(&tpStart, NULL);
		
		if (match(argv[1], "create"))
		{
			
			/* Create Table */
			memset(resp, 0, 256);
			memset(cmd , 0, 256);

			sprintf(cmd, "create table maxtab(id1 varchar, id2 varchar,id3 int, id4 varchar,id5 varchar,id6 varchar,id7 varchar,id8 varchar,id9 varchar)");
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitcrt;
			}

			printf("Client 1: %s\n", cmd);
				
exitcrt:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);			
			}
		}

		if (match(argv[1], "insert"))
		{
			
				
			/* Insert 10000 data rows into table */
			for(i = 1; i < 5000; i++)
			{
				char	*c = "cccccc";
				char	*d = "dddddd";
				char	*e = "eeeeee";
				char 	*f = "ffffff";
				char	*g = "gggggg";
				char	*h = "hhhhhh";
 
				sprintf(cmd, "insert into maxtab(aaaa%d, bbbb%d, %d, %s%d, %s%d, %s%d, %s%d, %s%d, %s%d)", i,i,i,c,i,d, i,e, i,f,i,g,i,h,i);

				rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

				if (rtn_stat != CLI_SUCCESS)
				{
					printf ("Error! \n");

	                                goto exitins;
				}
				
				printf("Client 1: %s\n", cmd);
exitins:
				if (exec_ctx)
				{
					mt_cli_close_execute(exec_ctx);
				}
			}

			
	
		}

		if (match(argv[1], "selectwhere"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectwhere maxtab where id2(bbbb1, bbbb9999999)");

			
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitselwh;
			}

			printf("Client 1: %s\n", cmd);

			int	rlen;
			char	*rp = NULL;
			MT_CLI_EXEC_CONTEX	*t_exec_ctx;

			t_exec_ctx = exec_ctx;

			int	test = 0;
			
			for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
			{
				test++;
				
				do{
					rp = mt_cli_get_nextrow(t_exec_ctx, &rlen);
					
					if (rp)
					{
						int	collen = 0;				
						char	*col;
						
						col = mt_cli_get_colvalue(t_exec_ctx, rp, 6, &collen);

						printf ("col: %s\n", col);
					}			
				} while(rp);

			}
exitselwh:			
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);
			}
		}

		if (match(argv[1], "selectcount"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectcount maxtab where id2(bbbb3, bbbb8)");

			
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitselcnt;
			}

			printf("Client 1: %s\n", cmd);

			MT_CLI_EXEC_CONTEX	*t_exec_ctx;

			t_exec_ctx = exec_ctx;

			int	total_rowcnt = 0;
			
			for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
			{
				mt_cli_exec_builtin(t_exec_ctx);

				total_rowcnt += t_exec_ctx->rowcnt;
			}

			printf(" The total row # is %d\n", total_rowcnt);

exitselcnt:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);
			}
		}

		if (match(argv[1], "selectsum"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectsum (id3) maxtab where id2(bbbb38, bbbb47)");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitselsum;
			}

			printf("Client 1: %s\n", cmd);

			MT_CLI_EXEC_CONTEX	*t_exec_ctx;

			t_exec_ctx = exec_ctx;

			int	sum_colval = 0;
			
			for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
			{
				mt_cli_exec_builtin(t_exec_ctx);

				sum_colval += t_exec_ctx->sum_colval;
			}

			printf(" The total value is %d\n", sum_colval);
exitselsum:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);
			}
		}

		
		if (match(argv[1], "crtindex"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "create index idx1 on maxtab (id2)");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitcrtidx;
			}

			printf("Client 1: %s\n", cmd);

			MT_CLI_EXEC_CONTEX	*t_exec_ctx;

			t_exec_ctx = exec_ctx;

			int	result = TRUE;
			
			for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
			{
				if (!mt_cli_exec_builtin(t_exec_ctx))
				{
					result = FALSE;
				}
			}

			mt_cli_result_meta(connection, exec_ctx, result);
			
exitcrtidx:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);
			}
		}

		if (match(argv[1], "dropindex"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "drop index idx1 on maxtab");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitdropindx;
			}

			printf("Client 1: %s\n", cmd);
			
exitdropindx:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);
			}
		}
		
		if (match(argv[1], "select"))
		{
			/* Select datas from table */
			for(i = 200; i < 5000; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "select maxtab (aaaa%d)", i);

				rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

				if (rtn_stat != CLI_SUCCESS)
				{
					printf ("Error! \n");

	                                goto exitsel;
				}
				
				//printf("Client 1: %s\n", cmd);

				int	rlen;
				char	*rp = mt_cli_get_nextrow(exec_ctx, &rlen);
				printf("row length: %d\n", rlen);
				printf("original rp: %s\n", rp);

				if (rp)
				{
					int	collen = 0;
					char	*col;
					col = mt_cli_get_colvalue(exec_ctx, rp, 0, &collen);
					printf("%d, col 1: %s\n", collen, col);
					col = mt_cli_get_colvalue(exec_ctx, rp, 1, &collen);
                                        printf("%d, col 2: %s\n", collen, col);
					collen = 0;
					col = mt_cli_get_colvalue(exec_ctx, rp, 2, &collen);
                                        printf("%d, col 3: %s, %d\n", collen, col, *((int*)col));
				}

exitsel:
				if (exec_ctx)
				{
					mt_cli_close_execute(exec_ctx);
				}
			
			}
		}

		if (match(argv[1], "selectrange"))
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectrange maxtab (*, *)");
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitselrg;
			}

			printf("Client 1: %s\n", cmd);

			int	rlen;
			char	*rp = NULL;
			MT_CLI_EXEC_CONTEX	*t_exec_ctx;

			t_exec_ctx = exec_ctx;

			int	test = 0;
			
			for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
			{
				test++;
				
				do{
					rp = mt_cli_get_nextrow(t_exec_ctx, &rlen);
					
					if (rp)
					{
						int	collen = 0;				
						char	*col;
						
						col = mt_cli_get_colvalue(t_exec_ctx, rp, 6, &collen);

						printf ("col: %s\n", col);
					}			
				} while(rp);

			}
exitselrg:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);
			}
		}

	
		if (match(argv[1], "delete"))
		{

			/* Delete data in the table */
			for(i = 6010; i < 7000; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "delete maxtab (aaaa%d)", i);
				rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

				if (rtn_stat != CLI_SUCCESS)
				{
					printf ("Error! \n");
					goto exitdel;
				}
exitdel:
				if (exec_ctx)
				{
					mt_cli_close_execute(exec_ctx);
				}
			
			}

		}

                if (match(argv[1], "deletewhere"))
                {
                        memset(resp, 0, 256);
                        memset(cmd, 0, 256);

                        sprintf(cmd, "deletewhere maxtab where id1(aaaa20, aaaa30)");

                        rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

                        if (rtn_stat != CLI_SUCCESS)
                        {
                                printf ("Error! \n");
                                goto exitdelwhere;
                        }

                        printf("Client 1: %s\n", cmd);

                        MT_CLI_EXEC_CONTEX      *t_exec_ctx;

                        t_exec_ctx = exec_ctx;

                        for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
                        {
                                mt_cli_exec_builtin(t_exec_ctx);
                        }


exitdelwhere:
                        if (exec_ctx)
                        {
                                mt_cli_close_execute(exec_ctx);
                        }
                }

                if (match(argv[1], "update"))
                {
                        memset(resp, 0, 256);
                        memset(cmd, 0, 256);

                        sprintf(cmd, "update set id2(aaaa60) maxtab where id2(bbbb20, bbbb20)");

                        rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

                        if (rtn_stat != CLI_SUCCESS)
                        {
                                printf ("Error! \n");
                                goto exitupdate;
                        }

                        printf("Client 1: %s\n", cmd);

                        MT_CLI_EXEC_CONTEX      *t_exec_ctx;

                        t_exec_ctx = exec_ctx;

                        for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
                        {
                                mt_cli_exec_builtin(t_exec_ctx);
                        }


exitupdate:
                        if (exec_ctx)
                        {
                                mt_cli_close_execute(exec_ctx);
                        }
                }


		if (match(argv[1], "drop"))
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "drop table maxtab");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitdrop;
			}

exitdrop:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);
			}
			
		}

		gettimeofday(&tpEnd, NULL);
		timecost = 0.0f;
		timecost = tpEnd.tv_sec - tpStart.tv_sec + (float)(tpEnd.tv_usec-tpStart.tv_usec)/1000000;
		printf("time cost: %f\n", timecost);
		
		mt_cli_close_connection(connection);
	}

	mt_cli_destroy_context();
	return 0;
}

```