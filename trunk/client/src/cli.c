/*
** Copyright (C) 2011 Xue Yingfei
**
** This file is part of MaxTable.
**
** Maxtable is free software: you can redistribute it and/or modify
** it under the terms of the GNU General Public License as published by
** the Free Software Foundation, either version 3 of the License, or
** (at your option) any later version.
**
** Maxtable is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with Maxtable. If not, see <http://www.gnu.org/licenses/>.
*/

#include "global.h"
#include "utils.h"
#include "master/metaserver.h"
#include "rpcfmt.h"
#include "parser.h"
#include "ranger/rangeserver.h"
#include "netconn.h"
#include "conf.h"
#include "token.h"
#include "tss.h"
#include "memcom.h"
#include "strings.h"
#include "trace.h"
#include "m_socket.h"


extern	TSS	*Tss;

#define CLI_CONF_PATH_MAX_LEN   64

/** cli related constants **/
#define CLI_CMD_CRT_TABLE   "create table"
#define CLI_CMD_QUIT "quit"

/* MaxTable */
#define CLI_DEFAULT_PREFIX "IMQL:"


typedef struct cli_infor
{
	/* default place is conf/cli.conf */
	char	cli_conf_path[CLI_CONF_PATH_MAX_LEN]; 
	char	cli_meta_ip[RANGE_ADDR_MAX_LEN];
	int     cli_meta_port;
	char    cli_ranger_ip[RANGE_ADDR_MAX_LEN];
	int     cli_ranger_port;
	int     cli_status;
	//List *	cli_tab_infor;
} CLI_INFOR;

/* cli_status */
#define CLI_CONN_MASTER         0x0001
#define CLI_CONN_REGION         0x0002
#define CLI_CONN_OVER           0x0004
#define CLI_CONN_MASTER_AGAIN	0x0008

#define CLI_IS_CONN2MASTER(cli) (cli->cli_status & CLI_CONN_MASTER)
#define CLI_IS_CONN2REGION(cli) (cli->cli_status & CLI_CONN_REGION)
#define CLI_IS_CONN_OVER(cli)   (cli->cli_status & CLI_CONN_OVER)
#define CLI_IS_CONN2MASTER_AGAIN(cli)	(cli->cli_status & CLI_CONN_MASTER_AGAIN)

CLI_INFOR * Cli_infor = NULL;

void
mt_cli_infor_init(char* conf_path)
{
	char	metaport[32];


	MEMSET(metaport, 32);
	
	Cli_infor = MEMALLOCHEAP(sizeof(CLI_INFOR));

	/* Get thet path of configure. */
	MEMCPY(Cli_infor->cli_conf_path, conf_path, STRLEN(conf_path));

	/* Get the IP and Port of metaserver. */
	conf_get_value_by_key(Cli_infor->cli_meta_ip, conf_path, CONF_META_IP);
	conf_get_value_by_key(metaport, conf_path, CONF_META_PORT);
	Cli_infor->cli_meta_port = m_atoi(metaport, STRLEN(metaport));

	/* Init the status of client. */
	Cli_infor->cli_status = CLI_CONN_MASTER;


	/* Load Metadata infor form Master */
}

static char *
cli_cmd_normalize(char *cmd)
{
	cmd = trim(cmd, LINE_SEPARATOR);
	cmd = trim(cmd, ' ');
	return cmd;
}

int
mt_cli_prt_help(char *cmd)
{
	if (!strncasecmp("help", cmd, 4))
	{
		printf("CREATE TABLE....create table table_name (col1_name col1_type, col2_name col2_type)\n");
		printf("INSERT DATA.....insert into table_name (col1_value, col2_value)\n");
		printf("SELECT DATA.....select table_name (col1_value)\n");
		printf("SELECT RANGE....selectrange table_name (col1_value1, col1_value2)\n");
		printf("DELETE DATA.....delete table_name (col1_value)\n");
		printf("DROP TABLE......drop table_name\n");

		return TRUE;
	}

	return FALSE;
}
int
cli_deamon()
{
	LOCALTSS(tss);
	int		ret = 0;
	char 		*cli_str;
	char 		*ip;
	int		port;
	RPCRESP		*resp;
	RPCREQ 		*req;
	int 		sockfd;
	int 		querytype;
	int		meta_only;
	int 		meta_again;
	char		*send_rg_bp = NULL;
	int		send_rg_bp_idx;
	int		send_buf_size;
	char 		buf[LINE_BUF_SIZE];
	char		send_buf[LINE_BUF_SIZE];
	INSMETA		*resp_ins;
	SELRANGE	*resp_selrg;
	char		tab_name[64];

	printf("Please type \"help\" if you need some further information.\n");
	
	while (1)
	{
		req = NULL;
		resp = NULL;
		meta_only = TRUE;
		send_buf_size = 0;
		
		printf(CLI_DEFAULT_PREFIX);
		
		cli_str = fgets(buf, LINE_BUF_SIZE, stdin);

		if (!cli_str)
		{
		    ret = ferror(stdin);
		    fprintf(stderr, "get cmd error %d\n", ret);
		    break; /* continue or break */
		}

		cli_str = cli_cmd_normalize(cli_str);

		send_buf_size = STRLEN(cli_str);
		if (send_buf_size == 0)
		{
		    continue;
		}

		if (!strncasecmp("quit", cli_str, 4))
		{
			break;
		}
		
		if (mt_cli_prt_help(cli_str))
		{
			continue;
		}
		
		if (!parser_open(cli_str))
		{
			parser_close();
			printf("PARSER ERR: Please input the command again by the 'help' signed.\n");
			continue;
		}

		querytype = ((TREE *)(tss->tcmd_parser))->sym.command.querytype;

		if (querytype != MCCRANGER)
		{
			MEMSET(tab_name, 64);
			MEMCPY(tab_name, 
				((TREE *)(tss->tcmd_parser))->sym.command.tabname,
				((TREE *)(tss->tcmd_parser))->sym.command.tabname_len);
		}
		
		/* Each command must send the request to metadata server first. */
		Cli_infor->cli_status = CLI_CONN_MASTER;

conn_again:
		if (CLI_IS_CONN2MASTER(Cli_infor) || CLI_IS_CONN2MASTER_AGAIN(Cli_infor))
		{
		    ip		= Cli_infor->cli_meta_ip;
		    port	= Cli_infor->cli_meta_port;
		}
		else if(CLI_IS_CONN2REGION(Cli_infor))
		{
		    ip		= Cli_infor->cli_ranger_ip;
		    port	= Cli_infor->cli_ranger_port;
		}
		   
		sockfd = conn_open(ip, port);

		MEMSET(send_buf, LINE_BUF_SIZE);
		MEMCPY(send_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
		/* Set the information header with the MAGIC. */
		MEMCPY((send_buf + RPC_MAGIC_MAX_LEN), cli_str, send_buf_size);

		//if ((Cli_infor->cli_status == CLI_CONN_REGION) && (meta_only == FALSE) && send_rg_bp)
		if (send_rg_bp)
		{
			MEMFREEHEAP(send_rg_bp);
			send_rg_bp = NULL;
		}

		tcp_put_data(sockfd, send_buf, (send_buf_size + RPC_MAGIC_MAX_LEN));

		resp = conn_recv_resp(sockfd);

		if ((resp == NULL) || (resp->status_code != RPC_SUCCESS))
		{
			printf("\n ERROR \n");
			goto finish;
		}

		switch(querytype)
		{
		    case ADDSERVER:

			meta_again = FALSE;			
			meta_only = TRUE;
			break;
				
		    case TABCREAT:
			meta_again = FALSE;
			meta_only = TRUE;
			break;
			
		    case INSERT:
		
			if (CLI_IS_CONN2MASTER(Cli_infor))
			{
				resp_ins = (INSMETA *)resp->result;

				MEMCPY(Cli_infor->cli_ranger_ip, 
				       resp_ins->i_hdr.rg_info.rg_addr, 
				       RANGE_ADDR_MAX_LEN);

				Cli_infor->cli_ranger_port = 
					resp_ins->i_hdr.rg_info.rg_port;

				/* Override the UNION part for this reques. */
				MEMCPY(resp->result, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);

				send_buf_size = resp->result_length + STRLEN(cli_str);
				send_rg_bp = MEMALLOCHEAP(send_buf_size);
				MEMSET(send_rg_bp, send_buf_size);

				send_rg_bp_idx = 0;
				PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
					      resp->result, resp->result_length);
				PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
					      cli_str, STRLEN(cli_str));

				cli_str = send_rg_bp;
				meta_again = FALSE;

				meta_only = FALSE;
			}
			else if (!meta_again)
			{
				meta_only = TRUE;

				if (resp->result_length)
				{
					/*Split case. */

					
					char *cli_add_sstab = "addsstab into ";				
					int	sstab_id;

					send_buf_size = resp->result_length + 64 + STRLEN(cli_add_sstab);
					send_rg_bp = MEMALLOCHEAP(send_buf_size);				

					MEMSET(send_rg_bp, send_buf_size);

					char newsstabname[SSTABLE_NAME_MAX_LEN];

					MEMSET(newsstabname, SSTABLE_NAME_MAX_LEN);

					MEMCPY(newsstabname, resp->result, SSTABLE_NAME_MAX_LEN);

					sstab_id = *(int *)(resp->result + SSTABLE_NAME_MAX_LEN);

					int split_ts = *(int *)(resp->result + SSTABLE_NAME_MAX_LEN + sizeof(int));

					int split_sstabid = *(int *)(resp->result + SSTABLE_NAME_MAX_LEN + sizeof(int) + sizeof(int));

					sprintf(send_rg_bp, "addsstab into %s (%s, %d, %d, %d, %s)", tab_name, newsstabname,
						sstab_id, split_ts, split_sstabid, resp->result + SSTABLE_NAME_MAX_LEN + sizeof(int) + sizeof(int) + sizeof(int));

					cli_str = send_rg_bp;

					meta_again = TRUE;
				}
			}

			
			break;
			
		    case CRTINDEX:
			break;
			
		    case SELECTRANGE:
			meta_again = FALSE;
			if (CLI_IS_CONN2MASTER(Cli_infor))
			{
				resp_selrg = (SELRANGE *)resp->result;

				resp_ins = &(resp_selrg->left_range);

				MEMCPY(Cli_infor->cli_ranger_ip, 
				       resp_ins->i_hdr.rg_info.rg_addr, 
				       RANGE_ADDR_MAX_LEN);

				Cli_infor->cli_ranger_port = 
					resp_ins->i_hdr.rg_info.rg_port;

				/* Override the UNION part for this reques. */
				MEMCPY(resp->result, RPC_SELECTRANGE_MAGIC, RPC_MAGIC_MAX_LEN);

				send_buf_size = resp->result_length + STRLEN(cli_str);
				send_rg_bp = MEMALLOCHEAP(send_buf_size);				

				send_rg_bp_idx = 0;
				PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
					      resp->result, resp->result_length);
				PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
					      cli_str, STRLEN(cli_str));

				cli_str = send_rg_bp;

				meta_only = FALSE;
			}
			else
			{
				traceprint("Result : %s\n",resp->result);
				meta_only = TRUE;
			}
			break;
			
		    case SELECT:
		    case DELETE:
			if (CLI_IS_CONN2MASTER(Cli_infor))
			{
				resp_ins = (INSMETA *)resp->result;

				MEMCPY(Cli_infor->cli_ranger_ip, 
				       resp_ins->i_hdr.rg_info.rg_addr, 
				       RANGE_ADDR_MAX_LEN);

				Cli_infor->cli_ranger_port = 
					resp_ins->i_hdr.rg_info.rg_port;

				/* Override the UNION part for this reques. */
				MEMCPY(resp->result, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);

				send_buf_size = resp->result_length + STRLEN(cli_str);
				send_rg_bp = MEMALLOCHEAP(send_buf_size);				

				send_rg_bp_idx = 0;
				PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
					      resp->result, resp->result_length);
				PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
					      cli_str, STRLEN(cli_str));

				cli_str = send_rg_bp;

				meta_only = FALSE;
			}
			else
			{
				traceprint("Result : %s\n",resp->result);
				meta_only = TRUE;
			}
			break;
			
		    case DROPTAB:
			if (CLI_IS_CONN2MASTER(Cli_infor))
			{
				/*
				** Drop table case:
				**	1st: Set the DELETE flag on the table header in the metadata server.
				**	2nd:Delete the whole file dir corresponding to the table in the ranger server.
				**	3th: Delete the whole file dir corresponding to the table in the metadata server.
				*/

				RANGE_PROF * ranger_list;

				ranger_list = (RANGE_PROF *)resp->result;

				MEMCPY(Cli_infor->cli_ranger_ip, ranger_list->rg_addr, 
				   RANGE_ADDR_MAX_LEN);

				Cli_infor->cli_ranger_port = ranger_list->rg_port;

				/* 
				** Re-construct the request for the ranger server. The information include
				** 	1. DROP magic.
				** 	2. drop command
				*/
				send_buf_size = RPC_MAGIC_MAX_LEN + STRLEN(cli_str);
				send_rg_bp = MEMALLOCHEAP(send_buf_size);				    

				send_rg_bp_idx = 0;
				PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
					  RPC_DROP_TABLE_MAGIC, RPC_MAGIC_MAX_LEN);

				PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, cli_str, STRLEN(cli_str));

				cli_str = send_rg_bp;

				meta_again = FALSE;
				meta_only = FALSE;
			}
			else if (!meta_again)
			{
				meta_only = TRUE;

				char *cli_remove_tab = "remove table ";				    

				send_buf_size = TABLE_NAME_MAX_LEN + STRLEN(cli_remove_tab);
				send_rg_bp = MEMALLOCHEAP(send_buf_size);				    

				MEMSET(send_rg_bp, send_buf_size);

				sprintf(send_rg_bp, "remove table %s", tab_name);

				cli_str = send_rg_bp;

				meta_again = TRUE;

			}
			else
			{
				meta_again = FALSE;
			}

		    	break;

		    case MCCTABLE:
			meta_again = FALSE;
			meta_only = TRUE;
		    	break;
			
		    case MCCRANGER:
		    	meta_again = FALSE;
			meta_only = TRUE;
		    	break;
			
		    case REBALANCE:
		    	meta_again = FALSE;
			meta_only = TRUE;
		    	break;
			
		    case SHARDING:
		    	meta_again = FALSE;
			meta_only = TRUE;
		    	break;
		
		    default:
			break;
		}


		if (!meta_only)
		{		
			/* 
			** Connection step 1: connect to metadaserver.
			** Connection step 2: connect to region server.
			*/
			Cli_infor->cli_status = CLI_CONN_REGION;

			conn_close(sockfd, req, resp);

			goto conn_again;

		}

		if (meta_again)
		{
			Cli_infor->cli_status = CLI_CONN_MASTER_AGAIN;

			conn_close(sockfd, req, resp);

			goto conn_again;
			
		}

finish:		
		conn_close(sockfd, req, resp);
		parser_close();
		tss_init(tss);
	}
	
	return ret;
}


#ifdef MAXTABLE_UNIT_TEST


static int
cli_test_main(char *cmd)
{
	LOCALTSS(tss);
	int		ret = 0;
	char 		*cli_str;
	char 		*ip;
	int		port;
	RPCRESP		*resp;
	RPCREQ 		*req;
	int 		sockfd;
	int 		querytype;
	int		meta_only;
	int 		meta_again;
	char		*send_rg_bp = NULL;
	int		send_rg_bp_idx;
	int		send_buf_size;
	char		send_buf[LINE_BUF_SIZE];
	INSMETA		*resp_ins;
	char		tab_name[64];
	SELRANGE	*resp_selrg;

	

	req = NULL;
	resp = NULL;
	meta_only = TRUE;
	send_buf_size = 0;
	
	//printf(CLI_DEFAULT_PREFIX);
	
	cli_str = cmd;

	if (!cli_str)
	{
	    ret = ferror(stdin);
	    fprintf(stderr, "get cmd error %d\n", ret);
	    return FALSE; 
	}

	cli_str = cli_cmd_normalize(cli_str);

	send_buf_size = STRLEN(cli_str);
	if (send_buf_size == 0)
	{
	    return FALSE;
	}

	if (mt_cli_prt_help(cli_str))
	{
		return TRUE;
	}
	
	if (!parser_open(cli_str))
	{
		parser_close();
		printf("PARSER ERR: Please input the command again by the 'help' signed.\n");
		return FALSE;
	}

	querytype = ((TREE *)(tss->tcmd_parser))->sym.command.querytype;
	MEMSET(tab_name, 64);
	MEMCPY(tab_name, ((TREE *)(tss->tcmd_parser))->sym.command.tabname,
		((TREE *)(tss->tcmd_parser))->sym.command.tabname_len);

	/* Each command must send the request to metadata server first. */
	Cli_infor->cli_status = CLI_CONN_MASTER;

conn_again:
	if (CLI_IS_CONN2MASTER(Cli_infor) || CLI_IS_CONN2MASTER_AGAIN(Cli_infor))
	{
	    ip		= Cli_infor->cli_meta_ip;
	    port	= Cli_infor->cli_meta_port;
	}
	else if(CLI_IS_CONN2REGION(Cli_infor))
	{
	    ip		= Cli_infor->cli_ranger_ip;
	    port	= Cli_infor->cli_ranger_port;
	}
	   
	sockfd = conn_open(ip, port);

	MEMSET(send_buf, LINE_BUF_SIZE);
	MEMCPY(send_buf, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);
	/* Set the information header with the MAGIC. */
	MEMCPY((send_buf + RPC_MAGIC_MAX_LEN), cli_str, send_buf_size);

	//if ((Cli_infor->cli_status == CLI_CONN_REGION) && (meta_only == FALSE) && send_rg_bp)
	if (send_rg_bp)
	{
		MEMFREEHEAP(send_rg_bp);
		send_rg_bp = NULL;
	}

	tcp_put_data(sockfd, send_buf, (send_buf_size + RPC_MAGIC_MAX_LEN));

	resp = conn_recv_resp(sockfd);

	if ((resp == NULL) || (resp->status_code != RPC_SUCCESS))
	{
		printf("\n ERROR \n");
		goto finish;
	}

	meta_again = FALSE;

	switch(querytype)
	{
	    case ADDSERVER:
		meta_only = TRUE;
		break;
			
	    case TABCREAT:
		meta_only = TRUE;
		break;
		
	    case INSERT:
		
		if (CLI_IS_CONN2MASTER(Cli_infor))
		{
			resp_ins = (INSMETA *)resp->result;

			MEMCPY(Cli_infor->cli_ranger_ip, 
			       resp_ins->i_hdr.rg_info.rg_addr, 
			       RANGE_ADDR_MAX_LEN);

			Cli_infor->cli_ranger_port = 
				resp_ins->i_hdr.rg_info.rg_port;

			/* Override the UNION part for this reques. */
			MEMCPY(resp->result, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);

			send_buf_size = resp->result_length + STRLEN(cli_str);
			send_rg_bp = MEMALLOCHEAP(send_buf_size);
			MEMSET(send_rg_bp, send_buf_size);

			send_rg_bp_idx = 0;
			PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
				      resp->result, resp->result_length);
			PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
				      cli_str, STRLEN(cli_str));

			cli_str = send_rg_bp;

			meta_only = FALSE;
		}
		else if (!meta_again)
		{
			meta_only = TRUE;

			if (resp->result_length)
			{
				/*Split case. */

				
				char *cli_add_sstab = "addsstab into ";				
				int	sstab_id;
				
				send_buf_size = resp->result_length + 128 + STRLEN(cli_add_sstab);
				send_rg_bp = MEMALLOCHEAP(send_buf_size);				

				MEMSET(send_rg_bp, send_buf_size);

				char newsstabname[SSTABLE_NAME_MAX_LEN];

				MEMSET(newsstabname, SSTABLE_NAME_MAX_LEN);

				MEMCPY(newsstabname, resp->result, SSTABLE_NAME_MAX_LEN);
				
				sstab_id = *(int *)(resp->result + SSTABLE_NAME_MAX_LEN);

				int split_ts = *(int *)(resp->result + SSTABLE_NAME_MAX_LEN + sizeof(int));

				int split_sstabid = *(int *)(resp->result + SSTABLE_NAME_MAX_LEN + sizeof(int) + sizeof(int));
				
				sprintf(send_rg_bp, "addsstab into %s (%s, %d, %d, %d, %s)", tab_name, newsstabname,
						sstab_id, split_ts, split_sstabid, resp->result + SSTABLE_NAME_MAX_LEN + sizeof(int) + sizeof(int) + sizeof(int));
				
				cli_str = send_rg_bp;

				
				meta_again = TRUE;
			}
		}

		
		break;
		
	    case CRTINDEX:
		break;

	    case SELECTRANGE:
		if (CLI_IS_CONN2MASTER(Cli_infor))
		{
			resp_selrg = (SELRANGE *)resp->result;

			resp_ins = &(resp_selrg->left_range);

			MEMCPY(Cli_infor->cli_ranger_ip, 
			       resp_ins->i_hdr.rg_info.rg_addr, 
			       RANGE_ADDR_MAX_LEN);

			Cli_infor->cli_ranger_port = 
				resp_ins->i_hdr.rg_info.rg_port;

			/* Override the UNION part for this reques. */
			MEMCPY(resp->result, RPC_SELECTRANGE_MAGIC, RPC_MAGIC_MAX_LEN);

			send_buf_size = resp->result_length + STRLEN(cli_str);
			send_rg_bp = MEMALLOCHEAP(send_buf_size);				

			send_rg_bp_idx = 0;
			PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
				      resp->result, resp->result_length);
			PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
				      cli_str, STRLEN(cli_str));

			cli_str = send_rg_bp;

			meta_only = FALSE;
		}
		else
		{
			traceprint("Result : %s\n",resp->result);
			meta_only = TRUE;
		}
		break;
	    
	    case SELECT:
	    case DELETE:
		if (CLI_IS_CONN2MASTER(Cli_infor))
		{
			resp_ins = (INSMETA *)resp->result;

			MEMCPY(Cli_infor->cli_ranger_ip, 
			       resp_ins->i_hdr.rg_info.rg_addr, 
			       RANGE_ADDR_MAX_LEN);

			Cli_infor->cli_ranger_port = 
				resp_ins->i_hdr.rg_info.rg_port;

			/* Override the UNION part for this reques. */
			MEMCPY(resp->result, RPC_REQUEST_MAGIC, RPC_MAGIC_MAX_LEN);

			send_buf_size = resp->result_length + STRLEN(cli_str);
			send_rg_bp = MEMALLOCHEAP(send_buf_size);				

			send_rg_bp_idx = 0;
			PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
				      resp->result, resp->result_length);
			PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
				      cli_str, STRLEN(cli_str));

			cli_str = send_rg_bp;

			meta_only = FALSE;
		}
		else
		{
			traceprint("Result : %s\n",resp->result);
			meta_only = TRUE;
		}
		break;
		
	    case DROPTAB:
    		if (CLI_IS_CONN2MASTER(Cli_infor))
		{
			/*
			** Drop table case:
			**	1st: Set the DELETE flag on the table header in the metadata server.
			**	2nd:Delete the whole file dir corresponding to the table in the ranger server.
			**	3th: Delete the whole file dir corresponding to the table in the metadata server.
			*/

			RANGE_PROF * ranger_list;

			ranger_list = (RANGE_PROF *)resp->result;
			
			MEMCPY(Cli_infor->cli_ranger_ip, ranger_list->rg_addr, 
			       RANGE_ADDR_MAX_LEN);

			Cli_infor->cli_ranger_port = ranger_list->rg_port;

			/* 
			** Re-construct the request for the ranger server.  The information include
			** 1. DROP magic.
			** 2. drop command
			*/
			send_buf_size = RPC_MAGIC_MAX_LEN + STRLEN(cli_str);
			send_rg_bp = MEMALLOCHEAP(send_buf_size);				

			send_rg_bp_idx = 0;
			PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, 
				      RPC_DROP_TABLE_MAGIC, RPC_MAGIC_MAX_LEN);
			
			PUT_TO_BUFFER(send_rg_bp, send_rg_bp_idx, cli_str, STRLEN(cli_str));

			cli_str = send_rg_bp;
			
			meta_only = FALSE;
		}
		else if (!meta_again)
		{
			meta_only = TRUE;
			
			char *cli_remove_tab = "remove table ";				

			send_buf_size = TABLE_NAME_MAX_LEN + STRLEN(cli_remove_tab);
			send_rg_bp = MEMALLOCHEAP(send_buf_size);				

			MEMSET(send_rg_bp, send_buf_size);

			sprintf(send_rg_bp, "remove table %s", tab_name);

			cli_str = send_rg_bp;

			meta_again = TRUE;
		
		}
	    	break;

	    case MCCTABLE:
		meta_again = FALSE;
		meta_only = TRUE;
		break;

	    case MCCRANGER:
	    	break;
		
	    case REBALANCE:
	    	meta_only = TRUE;
	    	break;

	    case SHARDING:
	    	meta_again = FALSE;
		meta_only = TRUE;
	    	break;
		
	    default:
		break;
	}


	if (!meta_only)
	{		
		/* 
		** Connection step 1: connect to metadaserver.
		** Connection step 2: connect to region server.
		*/
		Cli_infor->cli_status = CLI_CONN_REGION;

		conn_close(sockfd, req, resp);

		goto conn_again;

	}

	if (meta_again)
	{
		Cli_infor->cli_status = CLI_CONN_MASTER_AGAIN;

		conn_close(sockfd, req, resp);

		goto conn_again;
		
	}


finish:		
	conn_close(sockfd, req, resp);
	parser_close();
	tss_init(tss);

	return ret;
}



int main(int argc, char **argv)
{
	char		*crtab;
	char		*instab;
	char		*c1;
	char		*c2;
	char		*seltab;
	char		*cli_conf_path;	
	int		i;


	mem_init_alloc_regions();

	Trace = 0;
	
	cli_conf_path = CLI_DEFAULT_CONF_PATH;
	conf_get_path(argc, argv, &cli_conf_path);

	mt_cli_infor_init(cli_conf_path);

	tss_setup(TSS_OP_CLIENT);
	
	c1 = (char *)MEMALLOCHEAP(32);	
	c2 = (char *)MEMALLOCHEAP(32);

	instab = (char *)MEMALLOCHEAP(128);	
	seltab = (char *)MEMALLOCHEAP(128);	
	
	/* 1st step: create table. */
	
	crtab = "create table yxue (filename varchar, servername varchar)";

	printf("CRATING TABLE yxue\n");
	printf("create table yxue (filename varchar, servername varchar)\n");
	cli_test_main(crtab);


	Trace = 0;


	printf("Begain to INSERT data into the table yxue\n");

	for(i = 5; i < 10; i++)
	{
		MEMSET(c1, 32);
		sprintf(c1, "%s%d", "gggg", i);
		MEMSET(instab, 128);
		sprintf(instab,"insert into yxue (%s, bbbb%d)", c1, i);

		if((i > 99) && ((i % 100) == 0))
		{
			printf("inserting %d rows into yxue\n", i);
		}
		cli_test_main(instab);
	}

	
	/* 2nd step: insert into table. */
	for(i = 0; i < 1000; i++)
	{
		MEMSET(c1, 32);
		sprintf(c1, "%s%d", "gggg", i);
		MEMSET(instab, 128);
		sprintf(instab,"insert into yxue (%s, bbbb%d)", c1, i);

		if((i > 99) && ((i % 100) == 0))
		{
			printf("inserting %d rows into yxue\n", i);
		}
		cli_test_main(instab);
	}
	Trace = 0;
/*
	printf("Begain to DELETE data from the table yxue\n");
        for(i = 1550; i < 1555; i++)
        {
                MEMSET(c1, 32);
                sprintf(c1, "%s%d", "gggg", i);
                MEMSET(seltab, 128);
                sprintf(seltab,"delete yxue (%s)", c1);

                cli_test_main(seltab);
        }

*/
	printf("Begain to SELECT data from the table yxue\n");
	/* 3rd step: select data from table. */

	for(i = 1; i < 1000; i++)
	{
		MEMSET(c1, 32);
		sprintf(c1, "%s%d", "gggg", i);
		MEMSET(seltab, 128);
		sprintf(seltab,"select yxue (%s)", c1);

		cli_test_main(seltab);
	}

/*	
	for(i = 5535; i < 5560; i++)
	{
		MEMSET(c1, 32);
		sprintf(c1, "%s%d", "gggg", i);
		MEMSET(seltab, 128);
		sprintf(seltab,"select yxue (%s)", c1);

		cli_test_main(seltab);
	}
*/
	return -1;
}

#else
int main(int argc, char **argv)
{
	int ret;
	char *cli_conf_path;	


	mem_init_alloc_regions();

	cli_conf_path = CLI_DEFAULT_CONF_PATH;
	conf_get_path(argc, argv, &cli_conf_path);

	mt_cli_infor_init(cli_conf_path);

	tss_setup(TSS_OP_CLIENT);
		
	ret = cli_deamon();

	tss_release();
	
	return ret;
}
#endif
