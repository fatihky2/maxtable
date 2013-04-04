#ifndef	__THREAD_H
#define __THREAD_H

/* Must be greater than the block size. */
#define MSG_SIZE ((1024 + 1) * 1024)

#define EPOLL_SIZE 1024

#define LISTENQ 20

#define MAX_MSG_LIST 1024

#define MSG_MAX_SIZE	(2 * SSTABLE_SIZE)

typedef struct _msg_recv_args
{
    int port;
}msg_recv_args;

typedef struct msg_data
{
	struct msg_data_obj	*msg_datap;
	int			fd;
	char			data[MSG_SIZE];
	int			n_size;
	struct msg_data		*next;
	char 			*block_buffer;
}MSG_DATA;

typedef struct msg_data_obj
{
	LINK		to_link;	
	MSG_DATA	to_msg_datap;	
} MSG_DATA_OBJ;

void * msg_recv(void *args);

void msg_process(char * (*handler_request)(char *req_buf, int fd));

MSG_DATA *
msg_mem_alloc(void);

void
msg_mem_free(MSG_DATA *msg_data);

void
msg_mem_test();


#endif
