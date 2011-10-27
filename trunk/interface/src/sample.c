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


int main(int argc, char *argv[])
{
	conn 	*connection;
	char	resp[256], cmd[256];
	int	i, len;
		

	if (argc != 2)
	{
		printf("Testing create table: ./sample create\n");
		printf("Testing insert table: ./sample insert\n");
		printf("Testing delete:       ./sample delete\n");
		printf("Testing select:       ./sample select\n");
		printf("Testing select:       ./sample selectrange\n");
		printf("Testing drop:         ./sample drop\n");

		return 0;
	}

	if(cli_connection("127.0.0.1", 1959, &connection))
	{
		if (match(argv[1], "create"))
		{
			/* Create Table */
			memset(resp, 0, 256);
			memset(cmd , 0, 256);
			sprintf(cmd, "create table gu(id1 varchar, id2 int, id3 varchar)");
			cli_commit(connection, cmd, resp, &len);
			printf("ret: %s\n", resp);
		}

		if (match(argv[1], "insert"))
		{
			/* Insert 10000 data rows into table */
			for(i = 1; i < 2000; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "insert into gu(gggg%d, %d, bbbb%d)", i, i, i);
				if (!cli_commit(connection, cmd, resp, &len))
				{
					printf ("Error! \n");

	                                continue;
				}
				printf("Client 1: %s, ret(%d): %s\n", cmd, len, resp);
			}
		}

		if (match(argv[1], "select"))
		{
			/* Select datas from table */
			for(i = 1; i < 2000; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "select gu(gggg%d)", i);
				if (!cli_commit(connection, cmd, resp, &len))
				{
					printf ("Error! \n");

	                                continue;
				}
				resp[len] = '\0';
				printf("cmd: %s, ret:  len = %d, %s\n", cmd, len, resp);
				//printf("cmd: %s, col_num: %d, ret(%d): %s, %d, %s\n", cmd, *((int *)(resp + len -4)), len, resp + *((int *)(resp + len -8)), *((int *)(resp + *((int *)(resp + len -12)))), resp + *((int *)(resp + len -16)));
			}
		}

		if (match(argv[1], "selectrange"))
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectrange gu(gggg10, gggg5)");
//			sprintf(cmd, "selectrange gu(*, gggg5)");
//			sprintf(cmd, "selectrange gu(gggg10, *)");
//			sprintf(cmd, "selectrange gu(*, *)");

			int sockfd = cli_open_range(connection, cmd);

			range_query_contex rgsel_cont;

			char *test_rp;

retry:

			cli_read_range(sockfd, &rgsel_cont);

			if (!(rgsel_cont.status & DATA_EMPTY))
			{
				do
				{
					test_rp = cli_get_nextrow(&rgsel_cont);
				} while(test_rp != NULL);

				if (rgsel_cont.status & DATA_CONT)
				{
					cli_write_range(sockfd);					
					goto retry;
				}
			}
			
			cli_close_range(sockfd);
			
			//cli_commit(connection, cmd, resp, &len);
			printf("Client 1: %s, ret(%d): %s\n", cmd, len, resp);
		}

	
		if (match(argv[1], "delete"))
		{

			/* Delete data in the table */
			for(i = 1; i < 100; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "delete gu(gggg%d)", i);
				if (!cli_commit(connection, cmd, resp, &len))
				{
					printf ("Error! \n");

	                                continue;
				}
				resp[len] = '\0';
			//	printf("cmd: %s, col_num: %d, ret(%d): %s, %d, %s\n", cmd, *((int *)(resp + len -4)), len, resp + *((int *)(resp + len -8)), *((int *)(resp + *((int *)(resp + len -12)))), resp + *((int *)(resp + len -16)));
				printf("cmd: %s, %s\n", cmd, resp);
			}

			for(i = 1; i < 1000; i ++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "select gu(gggg%d)", i);
				if (!cli_commit(connection, cmd, resp, &len))
				{
					printf ("Error! \n");

					continue;
				}
				resp[len] = '\0';
				printf("cmd: %s, col_num: %d, ret(%d): %s, %d, %s\n", cmd, *((int *)(resp + len -4)), len, resp + *((int *)(resp + len -8)), *((int *)(resp + *((int *)(resp + len -12)))), resp + *((int *)(resp + len -16)));
			}
		}

		if (match(argv[1], "drop"))
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "drop gu");
			if (!cli_commit(connection, cmd, resp, &len))
			{
				printf ("Error! \n");

				return 0;
			}
			resp[len] = '\0';
		
			printf("cmd: %s, %s\n", cmd, resp);
		}
		
		cli_exit(connection);
	}
	
	return 0;
}

#endif

