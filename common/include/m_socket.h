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

#ifndef M_SOCKET_H_
#define M_SOCKET_H_

/* Error returns from m_recvdata	*/
#define	MT_READERROR		(-1)	/* System i/o error */
#define	MT_READQUIT		(-2)	/* Socket has quit */
#define	MT_READATTN		(-3)	/* Socket has set attention */
#define	MT_READDISCONNECT	(-4)	/* Socket has disconnected */
#define	MT_READBLOCKED		(-5)	/* Read is blocked */

/* Error returns from m_senddata	*/
#define	MT_SENDERROR		(-1)	/* System i/o error */
#define	MT_SENDBLOCKED		(-2)	/* Output is blocked */
#define	MT_SENDDISCONNECT	(-3)	/* Socket has disconnected */

int
m_recvdata(int socket, char *recvbp, int count);

int
m_senddata(int socket, char *sendbp, int count);

int
tcp_get_data(int socket, char *recvbp, int count);

int
tcp_put_data(int socket, char *sendbp, int count);

void
tcp_get_err_output(int rtn_num);

#endif
