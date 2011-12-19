#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "interface.h"


#ifdef MAXTABLE_SAMPLE_TEST


static int
match(char* dest, char *src)
{
	return !strcasecmp(dest, src);
}


/* Following definition is for the stat. */
#define	SELECT_RANGE_OP		0x0001
#define	SELECT_WHERE_OP		0x0002

int main(int argc, char *argv[])
{
	conn 	*connection;
	char	resp[256], cmd[256];
	int	rtn_stat;
	int	i;
	

	if (argc != 2)
	{
		printf("Testing create table: ./sample create\n");
		printf("Testing insert table: ./sample insert\n");
		printf("Testing delete:       ./sample delete\n");
		printf("Testing select:       ./sample select\n");
		printf("Testing selectrange:  ./sample selectrange\n");
		printf("Testing selectwhere:  ./sample selectwhere\n");
		printf("Testing drop:         ./sample drop\n");

		return 0;
	}

	mt_cli_crt_context();

	MT_CLI_EXEC_CONTEX t_exec_ctx;
	MT_CLI_EXEC_CONTEX *exec_ctx = &t_exec_ctx;

	if(mt_cli_open_connection("172.16.10.11", 1959, &connection))
	{
		if (match(argv[1], "create"))
		{
			
			/* Create Table */
			memset(resp, 0, 256);
			memset(cmd , 0, 256);

			sprintf(cmd, "create table gu(id1 varchar, id2 varchar,id3 int, id4 varchar,id5 varchar,id6 varchar,id7 varchar,id8 varchar,id9 varchar)");
			mt_cli_open_execute(connection, cmd, exec_ctx);

			mt_cli_close_execute(exec_ctx);			
		}

		if (match(argv[1], "insert"))
		{
			/* Insert 10000 data rows into table */
			for(i = 1; i < 10000; i++)
			{			
				char	*c = "cccccccccccccccccccccccccccccccccccccccccccccccccccccc";
				char	*d = "dddddddddddddddddddddddddddddddddddddddddddddddddddddd";
				char	*e = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

				char 	*f = "ffffffffffffffffffffffffffffffffffffffffffffffffffffff";
				char	*g = "gggggggggggggggggggggggggggggggggggggggggggggggggggggg";

				char	*h = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh";

				sprintf(cmd, "insert into gu(aaaa%d, bbbb%d, %d, %s%d, %s%d, %s%d, %s%d, %s%d, %s%d)", i,i,i,c,i,d, i,e, i,f,i,g,i,h,i);

				//sprintf(cmd, "insert into gu(aaaa%d, bbbb%d)", i,i);
				rtn_stat = mt_cli_open_execute(connection, cmd, exec_ctx);

				if (!(rtn_stat & CLI_SUCCESS))
				{
					printf ("Error! \n");

	                                continue;
				}
				
				printf("Client 1: %s\n", cmd);

				mt_cli_close_execute(exec_ctx);
				
			}
		}

		if (match(argv[1], "selectwhere"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectwhere gu where id1(aaaa7, aaaa9) and id2(bbbb6, bbbb8)");
//			sprintf(cmd, "selectwhere gu where id2(bbbb3, bbbb8)");

			
			rtn_stat = mt_cli_open_execute(connection, cmd, exec_ctx);

			printf("Client 1: %s\n", cmd);

			int	rlen;
			char	*rp = NULL;

			do{
				rp = mt_cli_get_nextrow(exec_ctx, &rlen);
				
				if (rp)
				{
					int	collen = 0;				
					char	*col;
					
					col = mt_cli_get_colvalue(exec_ctx, rp, 6, &collen);

					printf ("col: %s\n", col);
				}			
			} while(rp);
			
			mt_cli_close_execute(exec_ctx);
		}
		
		if (match(argv[1], "select"))
		{
			/* Select datas from table */
			for(i = 1; i < 10000; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "select gu(aaaa%d)", i);

				rtn_stat = mt_cli_open_execute(connection, cmd, exec_ctx);

				if (!(rtn_stat & CLI_SUCCESS))
				{
					printf ("Error! \n");

	                                continue;
				}
				
				printf("Client 1: %s\n", cmd);

				int	rlen;
				char	*rp = mt_cli_get_nextrow(exec_ctx, &rlen);

				if (rp)
				{
					int	collen = 0;
					char	*col;
					col = mt_cli_get_colvalue(exec_ctx, rp, 6, &collen);
					printf("col 6: %s\n", col);
				}
				
				mt_cli_close_execute(exec_ctx);
			
			}
		}

		if (match(argv[1], "selectrange"))
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectrange gu(aaaa7, aaaa8)");

			int	row_cnt = 0;

			mt_cli_open_execute(connection, cmd, exec_ctx);

			int	rlen;
			char	*rp = NULL;
			int	collen = 0;				
			char	*col;

			while ((rp = mt_cli_get_nextrow(exec_ctx, &rlen)))
			{					
				col = mt_cli_get_colvalue(exec_ctx, rp, 6, &collen);

				printf ("row: %d, col 6: %s\n", row_cnt, col);

				row_cnt++;
			};
			
			mt_cli_close_execute(exec_ctx);
		}

	
		if (match(argv[1], "delete"))
		{

			/* Delete data in the table */
			for(i = 1; i < 100; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "delete gu(aaaa%d)", i);
				mt_cli_open_execute(connection, cmd, exec_ctx);

				mt_cli_close_execute(exec_ctx);
			
			}

		}

		if (match(argv[1], "drop"))
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "drop gu");
			
			mt_cli_open_execute(connection, cmd, exec_ctx);

			mt_cli_close_execute(exec_ctx);
			
		}
		
		mt_cli_close_connection(connection);
	}

	mt_cli_destroy_context();
	return 0;
}

#endif

