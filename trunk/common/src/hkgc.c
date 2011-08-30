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

void put_io_list(BUF * buffer)
{
	io_data * new_io;
	new_io = (io_data *)MEMALLOCHEAP(sizeof(io_data));
	new_io->hk_dirty_buf = buffer;
	new_io->next = NULL;
				
	//pthread_mutex_lock(&io_list_mutex);
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
	change_value_add(io_list_len, 1);
				
	//pthread_mutex_unlock(&io_list_mutex);

}


void write_io_data(BUF * buffer)
{
	change_status(buffer, NONKEPT, KEPT);
	
	bufawrite(buffer);
	
	buffer->bstat &= ~BUF_DIRTY;

	bufunkeep(buffer);
	
	change_status(buffer, KEPT, NONKEPT);
	
}

void get_io_list()
{
	int length = io_list_len;
	int i;
	io_data *iodata;
	
	for(i=0; i<length; i++)
	{
		while (io_list_head == NULL)
			usleep(1000);
	
		iodata = io_list_head;
		io_list_head = io_list_head->next;
		
		write_io_data(iodata->hk_dirty_buf);
	}

	change_value_sub(io_list_len, length);
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


