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

#endif
