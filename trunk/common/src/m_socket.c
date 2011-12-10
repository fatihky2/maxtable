/*
** m_socket.c 2011-09-04 xueyingfei
**
** Copyright Transoft Corp.
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
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include "list.h"
#include "thread.h"
#include "utils.h"
#include "netconn.h"
#include "m_socket.h"

int
m_recvdata(int socket, char *recvbp, int count)
{
	int	rlen;		

	
	if (count == 0)
	{
		return(0);
	}

restart:
	//rlen = recv(socket, recvbp, count, 0);
	rlen = read(socket, recvbp, count);

	switch (rlen)
	{
	    case -1:
		
		if (errno == ECONNRESET)
		{
			return MT_READDISCONNECT;
		}

		if (errno == EAGAIN)
		{
			goto restart;
		}

		return MT_READERROR;

	   case 0:
		
		return MT_READQUIT;

	   default:

		
		return (rlen);
	}
}

int
m_senddata(int socket, char *sendbp, int count)
{
	int	slen;		


	
retry:
//	if ((slen = send(socket, sendbp, count, 0)) < 0)
	if ((slen = write(socket, sendbp, count)) < 0)
	{
		switch (errno)
		{
		    case ENOTCONN:
		    case EWOULDBLOCK:
			
			return MT_SENDBLOCKED;

		    case EPIPE:

			return MT_SENDDISCONNECT;


		    case EINTR:
			
			goto retry;

		    default:
			
			return MT_SENDERROR;
		}
	}

	
	if (slen > count)
	{
		return MT_SENDERROR;
	}

	
	return slen;
}

int
tcp_get_data(int socket, char *recvbp, int count)
{
	int	n;
	int	got;
	int	need;


	Assert(count < (MSG_SIZE + 1));

retry:
	n = m_recvdata(socket, recvbp, count);
//	n = read(socket, recvbp, count);

	if (n < 0)
	{
		//printf("errno = %d\n", errno);
		return n;			
	}

	if (n < RPC_MAGIC_MAX_LEN)
	{
		/* TODO: further research. */
		traceprint("TCP: suspect read data (len = %d).\n", n);
		goto retry;
	}

	need = *(int *)(recvbp + RPC_DATA_LOCATION);

	if (n > need)
	{
		Assert(0);
	}
	
	need -= n;

	got = n;
	
	if (need == 0)
	{
		return got;
	}

	recvbp += n;
	n = 0;
	while(need)
	{
		n = m_recvdata(socket, recvbp, need);

		if (n < 0)
		{
			//printf("errno = %d\n", errno);
			return n;			
		}
										
		recvbp += n;
		need -= n;
	}

	got += n;

	return got;
}

int
tcp_put_data(int socket, char *sendbp, int count)
{
	int	n;
	int	put;

	
	put = 0;
	Assert(count > RPC_MAGIC_MAX_LEN);

	*(int *)(sendbp + RPC_DATA_LOCATION) = count;

	while (count)
	{
		n = m_senddata(socket, sendbp, count);
//		n = write(socket, sendbp, count);
                
		if(n >= 0)
		{
			sendbp += n;
                 	count -= n;
			put += n;
		}
        }

	return put;
	//return m_senddata(socket, sendbp, count);
}
