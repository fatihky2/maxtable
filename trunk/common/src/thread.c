#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>

#include "global.h"
#include "utils.h"
#include "master/metaserver.h"
#include "ranger/rangeserver.h"
#include "netconn.h"
#include "conf.h"
#include "token.h"
#include "tss.h"
#include "parser.h"
#include "memcom.h"
#include "memobj.h"
#include "strings.h"
#include "trace.h"
#include "buffer.h"
#include "block.h"
#include "rebalancer.h"
#include "thread.h"
#include "m_socket.h"


extern	TSS	*Tss;
extern	KERNEL *Kernel;

struct epoll_event events[20];

MSG_DATA * msg_list_head = NULL;
MSG_DATA * msg_list_tail = NULL;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

int msg_list_len = 0;

int epfd;


int set_nonblock(int fd)
{
	return fcntl(fd, F_SETFL, fcntl( fd, F_GETFL)|O_NONBLOCK);
}

#if 0

void msg_write(int fd, char * buf, int size)
{
	int written = 0;
	int ret;
	while ((size - written) > 0)
	{
		ret = write(fd, buf + written, size - written);
                
		if(ret >= 0)
                    written += ret;
        }
}

#endif

void * msg_recv(void *args)

{
    	msg_recv_args * input = (msg_recv_args *)args;
	int i, n;
	int listenfd, nfds, connfd, sockfd;
	socklen_t clilen;
	char cliip[24];
	char buf[MSG_SIZE];

	struct sockaddr_in servaddr, cliaddr;

	MSG_DATA *new_msg;
	

	//socket
	listenfd = socket(AF_INET, SOCK_STREAM, 0);

	//setnonblock
	if(set_nonblock(listenfd)==-1)
	{
		perror("error in set_nonblock");
		return NULL;
	}

	MEMSET(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_port = htons(input->port);
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY);

	//bind
	bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr));

	//listen
	listen(listenfd, LISTENQ);

	//printf("Listening %d\n", SERV_PORT);


	epfd = epoll_create(EPOLL_SIZE);
	if (epfd == -1) {
		perror("epoll_create");
	}

	struct epoll_event ev0;
	ev0.data.fd = listenfd;
	ev0.events=  EPOLLIN;
	if(epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev0) == -1){
		perror("epoll_ctl");
	}

	while(TRUE)
	{
		//printf("waiting...\n");

		nfds = epoll_wait(epfd, events, 20, -1);
		if(nfds == -1)
		{
			;
			//perror("epoll_wait");
		}


		for(i=0; i< nfds; i++)	
		{
			//New connection
			if(events[i].data.fd == listenfd)
			{
				clilen = sizeof(cliaddr);
				if((connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &clilen))==-1)
				{
					perror("error in accept new connection");
					continue;
				}
				if(set_nonblock(connfd)==-1)
				{
					perror("error in set_nonblock");
					continue;
				}
				inet_ntop(AF_INET, &cliaddr.sin_addr, cliip, sizeof(cliip));
				printf("New connection %s %d\n", cliip, ntohs(cliaddr.sin_port));

				struct epoll_event ev;
				ev.data.fd = connfd;
				ev.events = EPOLLIN;
				epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &ev);
			}

			else if(events[i].events & EPOLLIN)
			{
				if((sockfd = events[i].data.fd)<0)	
				  continue;
				MEMSET(buf, MSG_SIZE);


				n = tcp_get_data(sockfd, buf, MSG_SIZE);

				if (n < RPC_MAGIC_MAX_LEN)
				{					
					switch (n)
					{
					    case MT_READDISCONNECT:
					    	close(sockfd);
						break;
						
					    case MT_READQUIT:
					    	close(sockfd);
						printf("client close connect!\n");
						break;
						
					    case MT_READERROR:
					    	perror("error in read");
						break;
						
					    default:
					    	Assert(0);
					    	break;
					}

					traceprint("TODO: Tcp/ip information definition (%d)\n", n);

					goto finish;
				}

				Assert(n > RPC_MAGIC_MAX_LEN);

#if 0						
				if((n = read(sockfd, buf, MSG_SIZE))==-1)
				{
					if(errno==ECONNRESET)
					  close(sockfd);
					else
					  perror("error in read");
				}
				else if( n==0 )
				{
					close(sockfd);
					printf("client close connect!\n");
				}
				else
				{
					printf("read %d->[%s]\n", n, buf);
#endif

				if (!strncasecmp(RPC_RBD_MAGIC, buf, STRLEN(RPC_RBD_MAGIC)))
				{
					int block_left = sizeof(REBALANCE_DATA);
					char * buffer = malloc(block_left);
					MEMCPY(buffer, buf, n);
					block_left -= n;
					
					n = tcp_get_data(sockfd, buffer + n, block_left);

					if (n != block_left)
					{					
						switch (n)
						{
						    case MT_READDISCONNECT:
						    	close(sockfd);
							break;
							
						    case MT_READQUIT:
						    	close(sockfd);
							printf("client close connect!\n");
							break;
							
						    case MT_READERROR:
						    	perror("error in read");
							break;
							
						    default:
						    	Assert(0);
						    	break;
						}
					}

					new_msg = msg_mem_alloc();
					MEMCPY(new_msg->data, buf, STRLEN(RPC_RBD_MAGIC));
					new_msg->fd = sockfd;
					new_msg->n_size = sizeof(REBALANCE_DATA);
					new_msg->block_buffer = buffer;
					new_msg->next = NULL;
				}
				else if (n > MSG_SIZE)
				{
					/* TODO : for the big data transferring. */
					Assert(0);
				}
				else						
				{
					new_msg = msg_mem_alloc();
					MEMCPY(new_msg->data, buf, n);
					new_msg->fd = sockfd;
					new_msg->n_size = n;
					new_msg->block_buffer = NULL;
					new_msg->next = NULL;
				}
		
				pthread_mutex_lock(&mutex);
				if (msg_list_head == NULL)
				{
					msg_list_head = new_msg;
					msg_list_tail = new_msg;
				} 
				else
				{
					msg_list_tail->next = new_msg;
					msg_list_tail = new_msg;
				}
				msg_list_len++;
		
				pthread_cond_signal(&cond);
				pthread_mutex_unlock(&mutex);
#if 0
				}
#endif

finish:
				if(msg_list_len > MAX_MSG_LIST)
					fprintf(stderr, "big error, msg list length exceeds the max len!\n");
			}

			else if(events[i].events & EPOLLOUT)
			{
				MSG_DATA * resp_msg = (MSG_DATA *)events[i].data.ptr;

				events[i].data.ptr = NULL;
				
				sockfd = resp_msg->fd;
				//msg_write(sockfd, resp_msg->data, resp_msg->n_size);
				tcp_put_data(sockfd, resp_msg->data, resp_msg->n_size);
				printf("write %d->[%s] -- socketfd = %d \n", resp_msg->n_size, resp_msg->data, sockfd);

				if(resp_msg->block_buffer)
				{
					free(resp_msg->block_buffer);
				}
				
				msg_mem_free(resp_msg);

				
				struct epoll_event ev;
				ev.data.fd = sockfd;
				ev.events = EPOLLIN;
				epoll_ctl(epfd, EPOLL_CTL_MOD, sockfd, &ev);				
			}
		}
	}

}

void msg_process(char * (*handler_request)(char *req_buf, int fd))
{
	LOCALTSS(tss);
	int resp_size;
	RPCREQ* req;
	char* resp;

	int fd;
	MSG_DATA *req_msg;
	MSG_DATA *resp_msg;


	req_msg = NULL;
	resp_msg = NULL;
	
	while(TRUE)
	{
		
		pthread_mutex_lock(&mutex);

		while (msg_list_head == NULL)
			pthread_cond_wait(&cond, &mutex);

	
		req_msg = msg_list_head;
		msg_list_head = msg_list_head->next;
		msg_list_len--;
		pthread_mutex_unlock(&mutex);

		if(!strncasecmp(RPC_RBD_MAGIC, req_msg->data, STRLEN(RPC_RBD_MAGIC)))
		{
			req = conn_build_req(req_msg->block_buffer, req_msg->n_size);
		}
		else
		{
			req = conn_build_req(req_msg->data, req_msg->n_size);
		}
		
		fd = req_msg->fd;

		if(req_msg->n_size)
		{
			resp = handler_request(req->data, fd);

			if (resp == NULL)
			{
				if (tss->tstat & TSS_PARSER_ERR)
				{
					resp = conn_build_resp_byte(RPC_PARSER_ERR, 0, NULL);
				}
				else
				{
					resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);					
				}
			}
		}
		else
		{	resp = conn_build_resp_byte(RPC_FAIL, 0, NULL);
		}

		/* If it's the select where/range session, skip the success operation. */
		if((fd < 0) || (((RPCRESP *)resp)->status_code & RPC_SKIP_SEND))
		{
			//local msg, such as recovery task msg, so no need to response
			goto finish;
		}

		resp_size = conn_get_resp_size((RPCRESP *)resp);

		resp_msg = NULL;
		resp_msg = (MSG_DATA *)msg_mem_alloc();
		resp_msg->n_size = resp_size;
		resp_msg->block_buffer = NULL;
		Assert(resp_size < MSG_SIZE);
		MEMCPY(resp_msg->data, resp, resp_size);
		resp_msg->fd = fd;	
		
		struct epoll_event ev;
		ev.data.ptr = resp_msg;
	
		ev.events = EPOLLOUT | EPOLLET;

		epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);

finish:

		if (req_msg->block_buffer != NULL)
		{
			free(req_msg->block_buffer);
		}
		
		msg_mem_free(req_msg);
  
		conn_destroy_req(req);
		conn_destroy_resp_byte(resp);		
		
		tss_init(tss);
	}
}


MSG_DATA *
msg_mem_alloc(void)
{
	register MSG_DATA	*msg_data;
	MSG_DATA_OBJ		*msg_data_obj;

	P_SPINLOCK(MSG_OBJ_SPIN);
	
	msg_data_obj = (MSG_DATA_OBJ *) mp_obj_alloc(Kernel->ke_msgdata_objpool);

	V_SPINLOCK(MSG_OBJ_SPIN);
	
	if (msg_data_obj == NULL)
	{
		Assert(NULL);
		return NULL;
	}

	msg_data = &(msg_data_obj->to_msg_datap);

	MEMSET(msg_data, sizeof(MSG_DATA));

	msg_data->msg_datap = msg_data_obj;

	return msg_data;
}


void
msg_mem_free(MSG_DATA *msg_data)
{
	int ret;

	P_SPINLOCK(MSG_OBJ_SPIN);
	
	ret = mp_obj_free(Kernel->ke_msgdata_objpool, (void *)msg_data->msg_datap);

	V_SPINLOCK(MSG_OBJ_SPIN);
	
	Assert(ret == MEMPOOL_SUCCESS);

	return;
}


static void
prLINK(LINK *link)
{
	traceprint("\n LinkAddr = 0x%p \n", link);
	traceprint("\t prev=0x%p \t next=0x%p \n", link->prev, link->next);
}


void
msg_mem_prt()
{
	void		*item;		
	LINK		*tmplink;
	MEMOBJECT 	*fp = Kernel->ke_msgdata_objpool;

	
	item = NULL;
	tmplink = &fp->f_free;

	while (!(tmplink->next == &fp->f_free))
	{

		item = tmplink->next;
		
		tmplink = tmplink->next;

		prLINK(tmplink);
	}

	return;
}



void
msg_mem_test()
{
	MSG_DATA	*msg_data[100];
	int		i;


	msg_mem_prt();

	printf("\n ==========================================\n");
	for (i= 0; i< 20; i++)
	{
		msg_data[i] = msg_mem_alloc();
	}
	
	msg_mem_prt();
}
