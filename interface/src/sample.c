#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "interface.h"

int main()
{
    conn * connection;

    if(cli_connection("127.0.0.1", 1959, &connection))
    {
        char resp[256], cmd[256];
        int i, len;
        
        memset(resp, 0, 256);
        sprintf(cmd, "create table gu(id1 varchar, id2 int)");
        cli_commit(connection, cmd, resp, &len);
        printf("ret: %s\n", resp);

        for(i = 10000; i < 20000; i ++)
        {
            memset(resp, 0, 256);
            memset(cmd, 0, 256);
            sprintf(cmd, "insert into gu(gggg%d, %d)", i, i);
            cli_commit(connection, cmd, resp, &len);
            printf("cmd: %s, ret(%d): %s\n", cmd, len, resp);
        }
        for(i = 100; i < 200; i ++)
        {
            memset(resp, 0, 256);
            memset(cmd, 0, 256);
            sprintf(cmd, "select gu(gggg%d)", i*100);
            cli_commit(connection, cmd, resp, &len);
            resp[len] = '\0';
            printf("cmd: %s, col_num: %d, ret(%d): %s, %d\n", cmd, *((int *)(resp + len -4)), len, resp + *((int *)(resp + len -8)), *((int *)(resp + *((int *)(resp + len -12)))));
        }

	cli_exit(connection);
    }
    return 0;
}
