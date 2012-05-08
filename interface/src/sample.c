#include "global.h"
#include "rpcfmt.h"
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
	CONN 	*connection;
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
	
	if(mt_cli_open_connection("127.0.0.1", 1959, &connection))
	{
		gettimeofday(&tpStart, NULL);
		
		if (match(argv[1], "create"))
		{
			
			/* Create Table */
			memset(resp, 0, 256);
			memset(cmd , 0, 256);

			sprintf(cmd, "create table maxtab(id1 varchar, id2 varchar,id3 int, id4 varchar,id5 varchar,id6 varchar,id7 varchar,id8 varchar,id9 varchar)");
		//	sprintf(cmd, "create table maxtab(id1 varchar, id2 varchar,id3 int)");
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
			for(i = 50101; i < 70100; i++)
			{
#if 0
				char	*c = "cccccccccccccccccccccccccccccccccccccccccccccccccccccc";
				char	*d = "dddddddddddddddddddddddddddddddddddddddddddddddddddddd";
				char	*e = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

				char 	*f = "ffffffffffffffffffffffffffffffffffffffffffffffffffffff";
				char	*g = "gggggggggggggggggggggggggggggggggggggggggggggggggggggg";

				char	*h = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh";
#endif

				char	*c = "cccccc";
				char	*d = "dddddd";
				char	*e = "eeeeee";
				char 	*f = "ffffff";
				char	*g = "gggggg";
				char	*h = "hhhhhh";
 
//				sprintf(cmd, "insert into maxtab(aaaa%d, bbbb20, %d, %s%d, %s%d, %s%d, %s%d, %s%d, %s%d)", i,i,c,i,d, i,e, i,f,i,g,i,h,i);
				sprintf(cmd, "insert into maxtab(aaaa%d, bbbb%d, %d, %s%d, %s%d, %s%d, %s%d, %s%d, %s%d)", i,i,i,c,i,d, i,e, i,f,i,g,i,h,i);
//				 sprintf(cmd, "insert into maxtab(aaaa1653, bbbb%d, %d, %s%d, %s%d, %s%d, %d, %d, %d)", i,i,c,i,d, i,e, i,i,i,i);
				
//				sprintf(cmd, "insert into maxtab(aaaa%d, bbbb%d, %d)", i,i,i);

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
			//sprintf(cmd, "selectwhere maxtab where id1(aaaa1, *) and id2(bbbb35, bbbb46)");
			//sprintf(cmd, "selectwhere maxtab where id2(bbbb39, bbbb39) and id4(cccccc39, cccccc39)");
//			sprintf(cmd, "selectwhere maxtab where id2(bbbb1, bbbb9999999)");
			//sprintf(cmd, "selectwhere maxtab where id1(aaaa1, aaaa999999)");
//			sprintf(cmd, "selectwhere maxtab where id1(aaaa20, aaaa30)");
//			sprintf(cmd, "selectwhere maxtab where id2(bbbb20, bbbb30)");
//			sprintf(cmd, "selectwhere maxtab where id2(bbbb20, bbbb20)");
			sprintf(cmd, "selectwhere maxtab where id3(cccccc20, cccccc20)");

			
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
//			sprintf(cmd, "selectcount maxtab where id1(aaaa38, aaaa47) and id2(bbbb35, bbbb46)");
//			sprintf(cmd, "selectcount maxtab where id1(aaaa3, aaaa8)");	
//			sprintf(cmd, "selectcount maxtab where id2(bbbb551, bbbb551)");

//			sprintf(cmd, "selectcount maxtab where id2(bbbb20, bbbb20)");
			sprintf(cmd, "selectcount maxtab where id2(aaaa60, aaaa60)");
//			sprintf(cmd, "selectcount maxtab where id4(cccccc20, cccccc20)");



			
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
		//	sprintf(cmd, "selectsum (id3) maxtab where id1(aaaa38, aaaa47)");
			sprintf(cmd, "selectsum (id3) maxtab where id2(bbbb2, bbbb202)");

//			sprintf(cmd, "selectsum (id3) maxtab where id2(aaaa20, aaaa20)");
			
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

		if (match(argv[1], "deletewhere"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
//			sprintf(cmd, "selectcount maxtab where id1(aaaa38, aaaa47) and id2(bbbb35, bbbb46)");
//			sprintf(cmd, "selectcount maxtab where id1(aaaa3, aaaa8)");	
//			sprintf(cmd, "deletewhere maxtab where id2(bbbb551, bbbb551)");
			sprintf(cmd, "deletewhere maxtab where id1(aaaa20, aaaa30)");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitdelwhere;
			}

			printf("Client 1: %s\n", cmd);

			MT_CLI_EXEC_CONTEX	*t_exec_ctx;

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
//			sprintf(cmd, "selectcount maxtab where id1(aaaa38, aaaa47) and id2(bbbb35, bbbb46)");
//			sprintf(cmd, "selectcount maxtab where id1(aaaa3, aaaa8)");	
//			sprintf(cmd, "deletewhere maxtab where id2(bbbb551, bbbb551)");
			sprintf(cmd, "update set id2(aaaa60) maxtab where id2(bbbb20, bbbb20)");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitupdate;
			}

			printf("Client 1: %s\n", cmd);

			MT_CLI_EXEC_CONTEX	*t_exec_ctx;

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
			for(i = 1; i < 10000; i++)
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
			//sprintf(cmd, "selectrange maxtab (aaaa45, aaaa46)");
			//sprintf(cmd, "selectrange maxtab (*, aaaa46)");
			//sprintf(cmd, "selectrange maxtab (aaaa45, *)");
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

#endif

