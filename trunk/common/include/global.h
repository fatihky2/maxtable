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

#ifndef GLOBAL_H_
#define GLOBAL_H_

/**  common header files   **/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <signal.h>
#include <sys/vfs.h>
#include <fcntl.h>
#include <sys/wait.h>

/* version part */
#define VERSION "0.5Dev"

#define TRUE	1
#define FALSE	0

#define INDEFINITE	-1 

/** Global Constants Part **/
#define CLI_DEFAULT_CONF_PATH "config/cli.conf"

#define META_DEFAULT_PORT 1001
#define META_DEFAULT_CONF_PATH "config/master.conf"

#define RANGE_DEFAULT_PORT 1949
#define RANGE_DEFAULT_CONF_PATH "config/ranger.conf"

#define LINE_BUF_SIZE 4096

/** Currently only works at Linux **/
#define LINE_SEPARATOR '\n'

#define CONF_SEPARATOR '='

#define SEND_HELLO "send_hello"
#define CMD_CRT_TABLE   "create_table"

#define RANGER_KEY "ranger"

#define MASTER_KEY "master"

#endif /* GLOBAL_H_ */
