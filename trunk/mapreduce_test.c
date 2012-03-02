#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "interface.h"
#include "master/metaserver.h"





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
	

	if (argc == 1)
	{
		printf("Testing get splits: ./mapred_test getsplits\n");
		
		return 0;
	}

	mt_cli_crt_context();

	MT_CLI_EXEC_CONTEX *exec_ctx = NULL;
	
	if(mt_cli_open_connection("127.0.0.1", 1959, &connection))
	{
		gettimeofday(&tpStart, NULL);
		
		if (match(argv[1], "getsplits"))
		{
			MT_SPLIT * splits = NULL;
			char *table_name = "test";
			int split_count = 0;

			mt_mapred_get_splits(connection, &splits, &split_count, table_name);

			printf("total %d splits got\n", split_count);

			int i;
			for(i = 0; i < split_count; i++)
			{
				MT_SPLIT * current = splits + i;
				printf("%s, %d, %s, %d\n", current->range_ip, current->range_port, current->tablet_name, current->meta_port);
			}

			mt_mapred_free_splits(splits);
			
		}

		if (match(argv[1], "inputtest"))
		{
			MT_SPLIT * splits = NULL;
			char *table_name = "test";
			int split_count = 0;

			mt_mapred_get_splits(connection, &splits, &split_count, table_name);

			printf("total %d splits got\n", split_count);

			int i;
			for(i = 0; i < split_count; i++)
			{
				MT_SPLIT * current = splits + i;
				printf("%s, %d, %s\n", current->range_ip, current->range_port, current->tablet_name);
			}

			char * split_idx = argv[2];
                        
			int split_number;
			sscanf(split_idx, "%d", &split_number);
                        printf("split_number: %d\n", split_number);

			MT_SPLIT *split = splits + split_number;
			MT_READER * mtreader = NULL;

			mt_mapred_create_reader(&mtreader, split);
			printf("%d, %d, %d\n", mtreader->data_connection.rg_server_port, ((TABLEHDR *)(mtreader->table_header))->tab_row_minlen,
				((TABLEHDR *)(mtreader->table_header))->tab_col);

			int rp_len;
			char * rp;

			while(rp = mt_mapred_get_nextvalue(mtreader, &rp_len))
			{
				printf("get row %d: %s with size %d\n", mtreader->block_cache->cache_index, rp, rp_len);
				int col_idx = 1;
				int value_len;
				char * value = mt_mapred_get_currentvalue(mtreader, rp, col_idx, &value_len);
				printf("get value: %s with size %d\n", value, value_len);
			}

			mt_mapred_free_reader(mtreader);

			mt_mapred_free_splits(splits);
			
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



