#include "global.h"
#include "rpcfmt.h"
#include "interface.h"

struct timeval tpStart;
struct timeval tpEnd;
float timecost;


#ifdef MAXTABLE_BENCH_TEST
int main()
{
	CONN * connection;


	if(mt_cli_open_connection("127.0.0.1", 1959, &connection))
	{
		printf("hello\n");
		char resp[256], cmd[256];
		int i, len;
if(1)
{
		memset(resp, 0, 256);
		sprintf(cmd, "create table gu(id1 varchar, id2 int, id3 varchar)");
//		mt_cli_exec_crtseldel(connection, cmd, resp, &len);
		printf("ret: %s\n", resp);

		gettimeofday(&tpStart, NULL);


		for(i = 1; i < 100000; i ++)
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "insert into gu(gggg%d, %d, bbbb%d)", i, i, i);

//			mt_cli_exec_crtseldel(connection, cmd, resp, &len);
		}
		
		gettimeofday(&tpEnd, NULL);
		timecost = 0.0f;
		timecost = tpEnd.tv_sec - tpStart.tv_sec + (float)(tpEnd.tv_usec-tpStart.tv_usec)/1000000;
		printf("Inserted rows = %d\n", i);
		printf("time cost: %f\n", timecost);


}//if(0)


		gettimeofday(&tpStart, NULL);

		for(i = 1; i < 20000; i ++)
		{
			memset(resp, 0, 256);
			memset(cmd, 0, 256);
			sprintf(cmd, "select gu(gggg%d)", i);

//			mt_cli_exec_crtseldel(connection, cmd, resp, &len);
		}
	
		gettimeofday(&tpEnd, NULL);
		timecost = 0.0f;
		timecost = tpEnd.tv_sec - tpStart.tv_sec + (float)(tpEnd.tv_usec-tpStart.tv_usec)/1000000;
		
		printf ("selected rows = %d \n", i);
		printf("time cost: %f\n", timecost);
	
		mt_cli_close_connection(connection);
	}
	
	return 0;
}
#endif

