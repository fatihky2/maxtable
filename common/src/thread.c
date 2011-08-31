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
#include "region/rangeserver.h"
#include "netconn.h"
#include "conf.h"
#include "token.h"
#include "tss.h"
#include "parser.h"
#include "memcom.h"
#include "strings.h"
#include "trace.h"


#include "thread.h"

extern	TSS	*Tss;



struct epoll_event ev, events[20];

msg_data * msg_list_head = NULL;
msg_data * msg_list_tail = NULL;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

int msg_list_len = 0;

int epfd;

int set_nonblock(int fd)
{
	return fcntl(fd, F_SETFL, fcntl( fd, F_GETFL)|O_NONBLOCK);
}


void * msg_recv(void *args)

{
    msg_recv_args * input = (msg_recv_args *)args;
	int i, n;
	int listenfd, nfds, connfd, sockfd;
	socklen_t clilen;
	char cliip[24];
	char buf[MAXLINE];

	struct sockaddr_in servaddr, cliaddr;

	msg_data *new_msg;
	

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
	ev.data.fd = listenfd;
	ev.events=  EPOLLIN;
	if(epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev) == -1){
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

				ev.data.fd = connfd;
				ev.events = EPOLLIN;
				epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &ev);
			}

			else if(events[i].events & EPOLLIN)
			{
				if((sockfd = events[i].data.fd)<0)	
				  continue;
				MEMSET(buf, sizeof(buf));
				if((n = read(sockfd, buf, sizeof(buf)))==-1)
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

					
					new_msg = malloc(sizeof(msg_data));
					MEMSET(new_msg->data, MAXLINE);
					MEMCPY(new_msg->data, buf, n);
					new_msg->fd = sockfd;
					new_msg->n_size = n;
					new_msg->next = NULL;
			
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
					msg_list_len ++;
			
					pthread_cond_signal(&cond);
					pthread_mutex_unlock(&mutex);
				}

				if(msg_list_len > MAX_MSG_LIST)
					fprintf(stderr, "big error, msg list length exceeds the max len!\n");

			}

			else if(events[i].events & EPOLLOUT)
			{
				msg_data * resp_msg = (msg_data *)events[i].data.ptr;						
				sockfd = resp_msg->fd;
				write(sockfd, resp_msg->data, resp_msg->n_size);
				printf("write %d->[%s]\n", resp_msg->n_size, resp_msg->data);

	
				ev.data.fd = sockfd;
				ev.events = EPOLLIN;
				epoll_ctl(epfd, EPOLL_CTL_MOD, sockfd, &ev);

				free(resp_msg);
			}
		}
	}

}

void msg_process(char * (*handler_request)(char *req_buf))
{
	LOCALTSS(tss);
	int resp_size;
	RPCREQ* req;
	char* resp;

	int fd;
	msg_data *req_msg;
	msg_data *resp_msg;


	while(TRUE)
	{
		
		pthread_mutex_lock(&mutex);

		while (msg_list_head == NULL)
			pthread_cond_wait(&cond, &mutex);

	
		req_msg = msg_list_head;
		msg_list_head = msg_list_head->next;
		msg_list_len --;
		pthread_mutex_unlock(&mutex);

		req = conn_build_req(req_msg->data, req_msg->n_size);
		fd = req_msg->fd;
		  
		if(req_msg->n_size)
		{
			resp = handler_request(req->data);

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

		resp_size = conn_get_resp_size((RPCRESP *)resp);

		resp_msg = malloc(sizeof(msg_data));
		resp_msg->n_size = resp_size;
		MEMSET(resp_msg->data, MAXLINE);
		MEMCPY(resp_msg->data, resp, resp_size);
		resp_msg->fd = fd;

	
		ev.data.ptr = resp_msg;
	
		ev.events = EPOLLOUT;

		epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
  
		conn_destroy_req(req);
		conn_destroy_resp_byte(resp);
		
		free(req_msg);
		
		tss_init(tss);
	}
}


