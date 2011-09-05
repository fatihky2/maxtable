/*
** hkgc.c 2011-03-17 xueyingfei
**
** Copyright flying/xueyingfei.
**
** This file is part of MaxTable.
**
** Licensed under the Apache License, Version 2.0
** (the "License"); you may not use this file except in compliance with
** the License. You may obtain a copy of the License at
**
** http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
** implied. See the License for the specific language governing
** permissions and limitations under the License.
*/
#include <pthread.h>
#include <sys/time.h>

#include "global.h"
#include "buffer.h"
#include "hkgc.h"
#include "atomic_status.h"
#include "memcom.h"

io_data * io_list_head = NULL;
io_data * io_list_tail = NULL;

pthread_mutex_t io_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t bufkeep_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t io_list_mutex = PTHREAD_MUTEX_INITIALIZER;

int io_list_len = 0;

#define	HKGC_WORK_INTERVAL	600

struct timeval io_tpStart;
struct timeval io_tpEnd;
float io_timecost;

void put_io_list(BUF * buffer)
{
	io_data * new_io;
	new_io = (io_data *)malloc(sizeof(io_data));
	new_io->hk_dirty_buf = buffer;
	new_io->next = NULL;
				
	pthread_mutex_lock(&io_list_mutex);
	if (io_list_head == NULL)
	{
		io_list_head = new_io;
		io_list_tail = new_io;
	} 
	else
	{
		io_list_tail->next = new_io;
		io_list_tail = new_io;
	}
	//change_value_add(&io_list_len, 1);
				
	pthread_mutex_unlock(&io_list_mutex);

}


void write_io_data(BUF * buffer)
{
	change_status(buffer, NONKEPT, KEPT);
	
	bufawrite(buffer);
	
	bufunkeep(buffer);
	
	change_status(buffer, KEPT, NONKEPT);
	
}

void get_io_list()
{
	int i;
	io_data *iodata;
	io_data *head;
	io_data *tail;

	if(io_list_head == NULL)
	{
		return;
	}

	pthread_mutex_lock(&io_list_mutex);

	head = io_list_head;
	tail = io_list_tail;
	io_list_head = io_list_tail->next;
	
	pthread_mutex_unlock(&io_list_mutex);

	iodata = head;
	while(head != tail)
	{
		//gettimeofday(&io_tpStart, NULL);
		
		write_io_data(iodata->hk_dirty_buf);

		/*gettimeofday(&io_tpEnd, NULL);
            	io_timecost = 0.0f;
            	io_timecost = io_tpEnd.tv_sec - io_tpStart.tv_sec + (float)(io_tpEnd.tv_usec-io_tpStart.tv_usec)/1000000;
            	printf("@@@@@@write io time cost: %f\n", io_timecost);*/
		
		head = head->next;
		free(iodata);
		iodata = head;
	}

	write_io_data(tail->hk_dirty_buf);
	free(tail);
}


void * hkgc_boot(void *args)
{
	while(TRUE)
	{
		//sleep(HKGC_WORK_INTERVAL);
		sleep(1);
		get_io_list();
	}

}


