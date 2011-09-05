#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include "interface.h"

struct timeval tpStart;
struct timeval tpEnd;
float timecost;

int main()
{
    conn * connection;
	

    if(cli_connection("127.0.0.1", 1959, &connection))
    {
        char resp[256], cmd[256];
        int i, len;
        
        /*memset(resp, 0, 256);
        sprintf(cmd, "create table gu(id1 varchar, id2 int, id3 varchar)");
        cli_commit(connection, cmd, resp, &len);
        printf("ret: %s\n", resp);

        FILE * fp = fopen("timecost.txt", "wb");

        for(i = 0; i < 50000; i ++)
        {
            memset(resp, 0, 256);
            memset(cmd, 0, 256);
            sprintf(cmd, "insert into gu(gggg%d, %d, bbbb%d)", i, i, i);
            gettimeofday(&tpStart, NULL);
            cli_commit(connection, cmd, resp, &len);
            gettimeofday(&tpEnd, NULL);
            timecost = 0.0f;
            timecost = tpEnd.tv_sec - tpStart.tv_sec + (float)(tpEnd.tv_usec-tpStart.tv_usec)/1000000;
            printf("time cost: %f\n", timecost);
            printf("cmd: %s, ret(%d): %s\n", cmd, len, resp);

            if(timecost>(float)0.1)
            {
                fprintf(fp, "time cost: %f\n", timecost);
                fprintf(fp, "cmd: %s, ret(%d): %s\n", cmd, len, resp);
            }
        }

        fclose(fp);

	exit(1);*/
        for(i = 30000; i < 50000; i ++)
        {
            memset(resp, 0, 256);
            memset(cmd, 0, 256);
            sprintf(cmd, "select gu(gggg%d)", i);
            gettimeofday(&tpStart, NULL);
            cli_commit(connection, cmd, resp, &len);
            gettimeofday(&tpEnd, NULL);
            timecost = 0.0f;
            timecost = tpEnd.tv_sec - tpStart.tv_sec + (float)(tpEnd.tv_usec-tpStart.tv_usec)/1000000;
            printf("time cost: %f\n", timecost);
            resp[len] = '\0';
            printf("cmd: %s, col_num: %d, ret(%d): %s, %d, %s\n", cmd, *((int *)(resp + len -4)), len, resp + *((int *)(resp + len -8)), *((int *)(resp + *((int *)(resp + len -12)))), resp + *((int *)(resp + len -16)));
        }

        cli_exit(connection);
    }
    return 0;
}

