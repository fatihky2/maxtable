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
	int	i, len;
	char	key[32];
	int	keylen;
	char	val[32];
	int	vallen;
	int	cmd_len;
	

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
#ifdef MT_KEY_VALUE
			sprintf(cmd, "create table gu(id1 varchar, id2 varchar)");
#else	
			sprintf(cmd, "create table gu(id1 varchar, id2 varchar,id3 int, id4 varchar,id5 varchar,id6 varchar,id7 varchar,id8 varchar,id9 varchar)");
#endif
			cli_execute(connection, cmd, resp, &len);
			printf("ret: %s\n", resp);
		}

		if (match(argv[1], "insert"))
		{
			/* Insert 10000 data rows into table */
			for(i = 1; i < 10000; i++)
			{
#ifdef MT_KEY_VALUE
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				memset(key, 0, 32);
				memset(val, 0, 32);

				sprintf(key, "gggg%d", i);

				
				keylen = strlen(key);


				sprintf(cmd, "insert into gu(");

				cmd_len= strlen(cmd);
				*(int *)(&cmd[cmd_len]) = keylen;
				
				sprintf(cmd + cmd_len + sizeof(int), "%s,", key);


				sprintf(val, "bbbb%d", i);
				vallen = strlen(val);

				

				*(int *)(&cmd[cmd_len + sizeof(int) + keylen + 1]) = vallen;

				
				sprintf(cmd + cmd_len + 2 * sizeof(int) + keylen + 1, "%s)", val);
#else				
				
				char	*c = "cccccccccccccccccccccccccccccccccccccccccccccccccccccc";
				char	*d = "dddddddddddddddddddddddddddddddddddddddddddddddddddddd";
				char	*e = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

				char 	*f = "ffffffffffffffffffffffffffffffffffffffffffffffffffffff";
				char	*g = "gggggggggggggggggggggggggggggggggggggggggggggggggggggg";

				char	*h = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh";

				sprintf(cmd, "insert into gu(aaaa%d, bbbb%d, %d, %s%d, %s%d, %s%d, %s%d, %s%d, %s%d)", i,i,i,c,i,d, i,e, i,f,i,g,i,h,i);
#endif
				//sprintf(cmd, "insert into gu(aaaa%d, bbbb%d)", i,i);
				if (!cli_execute(connection, cmd, resp, &len))
				{
					printf ("Error! \n");

	                                continue;
				}
				printf("Client 1: %s, ret(%d): %s\n", cmd, len, resp);
			}
		}

		if (match(argv[1], "selectwhere"))
		{
	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "selectwhere gu where id1(aaaa7, aaaa9) and id2(bbbb6, bbbb8)");
//			sprintf(cmd, "selectwhere gu where id2(bbbb3, bbbb8)");
			int sockfd = cli_open_range(connection, cmd, SELECT_WHERE_OP);

			RANGE_QUERYCTX rgsel_cont;

			char *test_rp;

retry_where:

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
					goto retry_where;
				}
			}
			
			cli_close_range(sockfd);
			//cli_execute(connection, cmd, resp, &len);
			printf("Client 1: %s, ret(%d): %s\n", cmd, len, resp);
			//printf("cmd: %s, col_num: %d, ret(%d): %s, %d, %s\n", cmd, *((int *)(resp + len -4)), len, resp + *((int *)(resp + len -8)), *((int *)(resp + *((int *)(resp + len -12)))), resp + *((int *)(resp + len -16)));

		}
		
		if (match(argv[1], "select"))
		{
			/* Select datas from table */
			for(i = 1; i < 10000; i++)
			{
/*
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				memset(key, 0, 32);
				memset(val, 0, 32);

				sprintf(key, "gggg%d", i);

				
				keylen = strlen(key);


				sprintf(cmd, "select gu(");

				cmd_len= strlen(cmd);
				*(int *)(&cmd[cmd_len]) = keylen;
				
				sprintf(cmd + cmd_len + sizeof(int), "%s),", key);

*/				
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "select gu(gggg%d)", i);
				if (!cli_execute(connection, cmd, resp, &len))
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

			int sockfd = cli_open_range(connection, cmd, SELECT_RANGE_OP);

			RANGE_QUERYCTX rgsel_cont;

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
			
			//cli_execute(connection, cmd, resp, &len);
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
				if (!cli_execute(connection, cmd, resp, &len))
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
				if (!cli_execute(connection, cmd, resp, &len))
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
			if (!cli_execute(connection, cmd, resp, &len))
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

