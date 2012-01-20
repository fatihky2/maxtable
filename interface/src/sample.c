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

struct timeval tpStart;
struct timeval tpEnd;
float timecost;


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

	MT_CLI_EXEC_CONTEX *exec_ctx = NULL;
	
	if(mt_cli_open_connection("172.16.10.42", 1959, &connection))
	{
		gettimeofday(&tpStart, NULL);
		
		if (match(argv[1], "create"))
		{
			
			/* Create Table */
			memset(resp, 0, 256);
			memset(cmd , 0, 256);

			sprintf(cmd, "create table lbs(id1 varchar, id2 varchar,id3 int)");
			mt_cli_open_execute(connection, cmd, &exec_ctx);

			mt_cli_close_execute(exec_ctx);			
		}

		if (match(argv[1], "insert"))
		{
			
				
			/* Insert 10000 data rows into table */
			for(i = 1; i < 10000; i++)
			{
#if 0
				char	*c = "cccccccccccccccccccccccccccccccccccccccccccccccccccccc";
				char	*d = "dddddddddddddddddddddddddddddddddddddddddddddddddddddd";
				char	*e = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

				char 	*f = "ffffffffffffffffffffffffffffffffffffffffffffffffffffff";
				char	*g = "gggggggggggggggggggggggggggggggggggggggggggggggggggggg";

				char	*h = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh";
#endif
				
				sprintf(cmd, "insert into lbs(aaaa%d, bbbb%d, %d)", i,i,i);

				//sprintf(cmd, "insert into gu(aaaa%d, bbbb%d)", i,i);
				rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

				if (!(rtn_stat & CLI_SUCCESS))
				{
					printf ("Error! \n");

	                                continue;
				}
				
				//printf("Client 1: %s\n", cmd);

				mt_cli_close_execute(exec_ctx);
				
			}

			
	
		}

		if (match(argv[1], "selectwhere"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectwhere gu where id1(aaaa38, aaaa47) and id2(bbbb35, bbbb46)");
//			sprintf(cmd, "selectwhere gu where id2(bbbb3, bbbb8)");

			
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

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
			
			mt_cli_close_execute(exec_ctx);
		}
		
		if (match(argv[1], "select"))
		{
			/* Select datas from table */
			for(i = 1; i < 2; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "select gu(aaaa)");

				rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

				if (!(rtn_stat & CLI_SUCCESS))
				{
					printf ("Error! \n");

	                                continue;
				}
				
				//printf("Client 1: %s\n", cmd);

				int	rlen;
				char	*rp = mt_cli_get_nextrow(exec_ctx, &rlen);
				printf("row length: %d\n", rlen);
				printf("original rp: %s\n", rp);
#if 1
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
#endif				
				mt_cli_close_execute(exec_ctx);
			
			}
		}

		if (match(argv[1], "selectrange"))
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectrange gu(aaaa38, aaaa46)");

			int	row_cnt = 0;

			mt_cli_open_execute(connection, cmd, &exec_ctx);

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
				mt_cli_open_execute(connection, cmd, &exec_ctx);

				mt_cli_close_execute(exec_ctx);
			
			}

		}

		if (match(argv[1], "drop"))
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "drop gu");
			
			mt_cli_open_execute(connection, cmd, &exec_ctx);

			mt_cli_close_execute(exec_ctx);
			
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

#endif

