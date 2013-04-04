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
char *s[50];

/* Following definition is for the stat. */
#define	SELECT_RANGE_OP		0x0001
#define	SELECT_WHERE_OP		0x0002

int main(int argc, char *argv[])
{
	CONN 	*connection;
	char	resp[256], cmd[4096];
	int	rtn_stat;
	int	i;
	

	if (argc < 2)
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

	char    c[256]; 
	char    d[256];
	char    e[256];
	char    f[256];
	char    g[256];
	char    h[256];

	for (i = 0; i < 55; i++)
	{
		c[i] = 'c';
	}

	c[55] = '\0';

	for (i = 0; i < 55; i++)
	{
		d[i] = 'd';
	}

	d[55] = '\0';

	for (i = 0; i < 55; i++)
	{
		e[i] = 'e';
	}

	e[55] = '\0';

	for (i = 0; i < 55; i++)
	{
		f[i] = 'f';
	}

	f[55] = '\0';

	for (i = 0; i < 55; i++)
	{
		g[i] = 'g';
	}

	g[55] = '\0';

	for (i = 0; i < 55; i++)
	{
		h[i] = 'h';
	}

	h[55] = '\0';

	mt_cli_crt_context();

	MT_CLI_EXEC_CONTEX *exec_ctx = NULL;
	
	if(mt_cli_open_connection("127.0.0.1", 1959, &connection))
	{
		gettimeofday(&tpStart, NULL);
		
		if (match(argv[1], "create"))
		{
			
			/* Create Table */
			memset(resp, 0, 256);
			memset(cmd , 0, 4096);

//			sprintf(cmd, "create table maxtab(id1 varchar, id2 varchar,id3 int, id4 varchar,id5 varchar,id6 varchar,id7 varchar,id8 varchar,id9 varchar)");
			
//			sprintf(cmd, "create table maxtab(id1 varchar, id2 varchar, id3 varchar, id4 varchar, id5 varchar, id6 varchar)");
			sprintf(cmd, "create table maxtab(key varchar, shop_id varchar,shop_pic text)");
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

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

				exec_ctx = NULL;
			}
		}

		if (match(argv[1], "insert"))
		{
			memset(cmd , 0, 4096);

			/* Insert 10000 data rows into table */
			for(i = 1; i < 10000; i++)
//			for(i = 1000; i < 3000; i++)
			{
//				sprintf(cmd, "insert into maxtab(aaaa%d, bbbb20, %d, %s%d, %s%d, %s%d, %s%d, %s%d, %s%d)", i,i,c,i,d, i,e, i,f,i,g,i,h,i);
				sprintf(cmd, "insert into maxtab(%d, bbbb%d, 1, %s%d, %s%d, %s%d, %s%d, %s%d, %s%d)", i,i,c,i,d, i,e, i,f,i,g,i,h,i);
//				sprintf(cmd, "insert into maxtab(aaaa1653, bbbb%d, %d, %s%d, %s%d, %s%d, %d, %d, %d)", i,i,c,i,d, i,e, i,i,i,i);
			

				rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

				if (rtn_stat != CLI_SUCCESS)
				{
					printf ("Error! \n");

	                                goto exitins;
				}

				if ((i % 10000) == 0)
				{
					gettimeofday(&tpEnd, NULL);
					timecost = 0.0f;
					timecost = tpEnd.tv_sec - tpStart.tv_sec + (float)(tpEnd.tv_usec-tpStart.tv_usec)/1000000;
					printf("insert %d data rows. time cost: %f\n", i, timecost);

				}				
exitins:
				if (exec_ctx)
				{
					mt_cli_close_execute(exec_ctx);

					exec_ctx = NULL;
				}
			}

			
	
		}
		
		if (match(argv[1], "inserttext"))
		{
			/* Insert 10000 data rows into table */
			for(i = 1; i < 1000; i++)
//			for(i = 1001; i < 1100; i++)
//			for(i = 1101; i < 71100; i++)
			{
#if 0
				char	*blob = "Edward P. Gibson, Esq., CISSP, FBCS, is a Director in PricewaterhouseCoopers' Forensic Services Group. He specializes in gathering intelligence to detect, mitigate, and prevent corporate IT and security risks such as economic espionage, IT / bank fraud, and cyber attacks.  Prior to PwC, 2005-2009, Gibson was the Chief cyber Security Advisor for Microsoft Ltd in the United Kingdom (UK) and praised for his  unique ability to make security issues relevant and personal. From 1985-2005 Gibson was a career FBI Special Agent.  He specialized in investigating complex international white-collar crimes, and was cited for his work in various espionage investigations involving FBI & CIA traitors.\0";	
				int	blob_len = strlen(blob);
#endif
				char *blob = "http://i3.s1.dpfile.com/pc/281097f66763c0661f92df78eb78fa8b(240c180)/thumb.jpg";
				int     blob_len = strlen(blob);

#if 0
				char filename[100];
				memset(filename, 0, 100);
				sprintf(filename, "./u1.gif");
				FILE *fd = fopen(filename, "rb");             
				fseek(fd, 0, SEEK_END);                            
                                                   
				int blob_len = ftell(fd);                          
                                                   
				rewind(fd);                                        
                                                   
				char *blob = (char *)malloc(blob_len * sizeof(char));
                                                   
				fread(blob, sizeof(char), blob_len, fd);            		
#endif
				int	blob_start = 0;
				char	blob_id[8];

				char	cmd_header[64];
				int	cmd_hdlen;

				memset(cmd_header, 0, 64);		

				memcpy(blob_id, &blob_start, 4);
				memcpy(&(blob_id[4]), &blob_len, 4);

				sprintf(cmd_header, "insert into maxtab(0100_0_%d, 0000%d, ", i,i);
			
				cmd_hdlen = strlen(cmd_header);
	
				memcpy(cmd, cmd_header, cmd_hdlen);
				memcpy(cmd + cmd_hdlen, "[", 1);
				memcpy(cmd + cmd_hdlen+1, blob_id, 8);
				memcpy(cmd + cmd_hdlen + 9, "]", 1);
				
				memcpy(cmd + cmd_hdlen + 10, ")\0", 1);
				
				memcpy(cmd + cmd_hdlen + 11, blob, blob_len);
				

				assert((cmd_hdlen + 11 + blob_len) < (20 *1024));				

				rtn_stat = mt_cli_open_execute(connection, cmd, (cmd_hdlen + 11 + blob_len), &exec_ctx);

				if (rtn_stat != CLI_SUCCESS)
				{
					printf ("Error! \n");

	                                goto exitinstxt;
				}
				
				printf("Client 1: %s\n", cmd);
exitinstxt:
				if (exec_ctx)
				{
					mt_cli_close_execute(exec_ctx);

					exec_ctx = NULL;
				}
			}

			
	
		}

		if (match(argv[1], "selectwhere"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			//sprintf(cmd, "selectwhere maxtab where id1(aaaa1, *) and id2(bbbb35, bbbb46)");
			//sprintf(cmd, "selectwhere maxtab where id2(bbbb39, bbbb39) and id4(cccccc39, cccccc39)");
			sprintf(cmd, "selectwhere maxtab where key(0, 9)");
			//sprintf(cmd, "selectwhere maxtab where id1(aaaa1, aaaa999999)");
//			sprintf(cmd, "selectwhere maxtab where id1(aaaa20, aaaa30)");
//			sprintf(cmd, "selectwhere maxtab where id2(bbbb2000, bbbb2000)");
//			sprintf(cmd, "selectwhere maxtab where id2(bbbb3744, bbbb3750)");
//			sprintf(cmd, "selectwhere maxtab where id3(cccccc20, cccccc20)");

			
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);


			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitselwh;
			}

#if 0
			printf("Client 1: %s\n", cmd);
#endif
			int	rlen;
			char	*rp = NULL;
			MT_CLI_EXEC_CONTEX	*t_exec_ctx;

			t_exec_ctx = exec_ctx;

			int	tot_row = 0;
			
			for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
			{
				do{
					rp = mt_cli_get_nextrow(t_exec_ctx, &rlen);
					
					if (rp)
					{
#if 1 
						int	collen = 0;				
						char	*col;
						
						col = mt_cli_get_colvalue(t_exec_ctx, rp, 0, &collen);

						printf ("col: %s\n", col);
#endif
						if (((++tot_row) % 10000) == 0)
						{
							gettimeofday(&tpEnd, NULL);
							timecost = 0.0f;
							timecost = tpEnd.tv_sec - tpStart.tv_sec + (float)(tpEnd.tv_usec-tpStart.tv_usec)/1000000;
							printf("select where %d data rows, time cost: %f\n", tot_row, timecost);
						}

			

					}			
				} while(rp);

			}
exitselwh:			
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);

				exec_ctx = NULL;
			}
		}

		
		if (match(argv[1], "selectlike"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			//sprintf(cmd, "selectwhere maxtab where id1(aaaa1, *) and id2(bbbb35, bbbb46)");
			//sprintf(cmd, "selectwhere maxtab where id2(bbbb39, bbbb39) and id4(cccccc39, cccccc39)");
			sprintf(cmd, "selectwhere maxtab where id3 like (bank fraud)");
			//sprintf(cmd, "selectwhere maxtab where id1(aaaa1, aaaa999999)");
//			sprintf(cmd, "selectwhere maxtab where id1(aaaa20, aaaa30)");
//			sprintf(cmd, "selectwhere maxtab where id2(bbbb2000, bbbb2000)");
//			sprintf(cmd, "selectwhere maxtab where id2(bbbb3744, bbbb3750)");
//			sprintf(cmd, "selectwhere maxtab where id3(cccccc20, cccccc20)");


			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitsellike;
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
						
						col = mt_cli_get_colvalue(t_exec_ctx, rp, 0, &collen);

						printf ("col: %s\n", col);
					}			
				} while(rp);

			}
exitsellike:			
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);

				exec_ctx = NULL;
			}
		}

		if (match(argv[1], "selectcount"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
//			sprintf(cmd, "selectcount maxtab where id1(aaaa38, aaaa47) and id2(bbbb35, bbbb46)");
//			sprintf(cmd, "selectcount maxtab where id1(aaaa3, aaaa8)");	
//			sprintf(cmd, "selectcount maxtab where id2(bbbb551, bbbb551)");

			sprintf(cmd, "selectcount maxtab where id2(bbbb3744, bbbb3750)");
//			sprintf(cmd, "selectcount maxtab where id2(aaaa60, aaaa60)");
//			sprintf(cmd, "selectcount maxtab where id4(cccccc20, cccccc20)");



			
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

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

				exec_ctx = NULL;
			}
		}

		if (match(argv[1], "checksstab"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);

			sprintf(cmd, "mcc checksstab %s", argv[2]);
			
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitchksstab;
			}

			printf("Client 1: %s\n", cmd);

			MT_CLI_EXEC_CONTEX	*t_exec_ctx;

			t_exec_ctx = exec_ctx;

			for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
			{
				mt_cli_exec_builtin(t_exec_ctx);
			}

exitchksstab:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);

				exec_ctx = NULL;
			}
		}

		if (match(argv[1], "selectsum"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
		//	sprintf(cmd, "selectsum (id3) maxtab where id1(aaaa38, aaaa47)");
			sprintf(cmd, "selectsum (id3) maxtab where id2(bbbb2, bbbb202)");

//			sprintf(cmd, "selectsum (id3) maxtab where id2(aaaa20, aaaa20)");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

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

				exec_ctx = NULL;
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
			
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

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

				exec_ctx = NULL;
			}
		}

		
		if (match(argv[1], "update"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
//			sprintf(cmd, "selectcount maxtab where id1(aaaa38, aaaa47) and id2(bbbb35, bbbb46)");
//			sprintf(cmd, "selectcount maxtab where id1(aaaa3, aaaa8)");	
//			sprintf(cmd, "deletewhere maxtab where id2(bbbb551, bbbb551)");
			sprintf(cmd, "update set id2(aaaa60) maxtab where id2(bbbb2000, bbbb2000)");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

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

				exec_ctx = NULL;
			}
		}

		
		if (match(argv[1], "crtindex"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "create index idx1 on maxtab (id2)");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

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

				exec_ctx = NULL;
			}
		}

		if (match(argv[1], "dropindex"))
		{	
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "drop index idx1 on maxtab");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

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

				exec_ctx = NULL;
			}
		}
		
		if (match(argv[1], "select"))
		{
			/* Select datas from table */
			for(i = 1; i < 2; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "select maxtab (0000_01_r850g106_2060380_1)");

				rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

				if (rtn_stat != CLI_SUCCESS)
				{
					printf ("Error! \n");

	                                goto exitsel;
				}
				
				//printf("Client 1: %s\n", cmd);

				if ((i % 10000) == 0)
				{
					gettimeofday(&tpEnd, NULL);
					timecost = 0.0f;
					timecost = tpEnd.tv_sec - tpStart.tv_sec + (float)(tpEnd.tv_usec-tpStart.tv_usec)/1000000;
					printf("select %d data rows. time cost: %f\n", i, timecost);
				}
#if 1 
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
					col = mt_cli_get_colvalue(exec_ctx, rp, 2, &collen);
                                        printf("%d, col 2: %s\n", collen, col);
				}
#endif				

exitsel:
				if (exec_ctx)
				{
					mt_cli_close_execute(exec_ctx);

					exec_ctx = NULL;
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
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

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

			int	tot_row = 0;
			
			for (i = 0; i < exec_ctx->rg_cnt; i++, t_exec_ctx++)
			{
				
				do{
					rp = mt_cli_get_nextrow(t_exec_ctx, &rlen);
					
					if (rp)
					{
						tot_row++;
#if 1
						int	collen = 0;				
						char	*col;
						
						col = mt_cli_get_colvalue(t_exec_ctx, rp, 0, &collen);

						printf ("col: %s\n", col);
#endif
					}			
				} while(rp);

			}
exitselrg:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);

				exec_ctx = NULL;
			}
			printf("total row : %d \n", tot_row);
		}

	
		if (match(argv[1], "delete"))
		{

			/* Delete data in the table */
			for(i = 70010; i < 70100; i++)
			{
				memset(resp, 0, 256);
				memset(cmd, 0, 256);
				sprintf(cmd, "delete maxtab (aaaa%d)", i);
				rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

				if (rtn_stat != CLI_SUCCESS)
				{
					printf ("Error! \n");
					goto exitdel;
				}
exitdel:
				if (exec_ctx)
				{
					mt_cli_close_execute(exec_ctx);

					exec_ctx = NULL;
				}
			
			}

		}

		if (match(argv[1], "drop"))
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "drop table maxtab");
			
			rtn_stat = mt_cli_open_execute(connection, cmd, strlen(cmd), &exec_ctx);

			if (rtn_stat != CLI_SUCCESS)
			{
				printf ("Error! \n");
				goto exitdrop;
			}

exitdrop:
			if (exec_ctx)
			{
				mt_cli_close_execute(exec_ctx);

				exec_ctx = NULL;
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

