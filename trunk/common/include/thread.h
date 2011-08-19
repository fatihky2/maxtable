#ifndef	__THREAD_H
#define __THREAD_H

#define MAXLINE 1024

#define EPOLL_SIZE 256

#define LISTENQ 20

#define MAX_MSG_LIST 4096


typedef struct _msg_recv_args
{
    int port;
}msg_recv_args;

typedef struct _msg_data
{
	int fd;
	char data[MAXLINE];
	int n_size;
	struct _msg_data * next;
}msg_data;



void * msg_recv(void *args);

void msg_process(char * (*handler_request)(char *req_buf));



#endif
