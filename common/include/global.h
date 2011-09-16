/*
** global.h 2010-06-05 xueyingfei
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

#define LINE_BUF_SIZE 1000 //Means the max size of one line

/** Currently only works at Linux **/
#define LINE_SEPARATOR '\n'

#define CONF_SEPARATOR '='

#define SEND_HELLO "send_hello"
#define CMD_CRT_TABLE   "create_table"

#define RANGER_KEY "ranger"

#define MASTER_KEY "master"

#endif /* GLOBAL_H_ */
