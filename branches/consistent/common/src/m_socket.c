/*
** session.c 2011-09-04 xueyingfei
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
	rlen = recv(socket, recvbp, count, 0);

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
	if ((slen = send(socket, sendbp, count, 0)) < 0)
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

