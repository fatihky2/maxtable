/*
** file_op.h 2010-08-19 xueyingfei
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


#ifndef FILE_OP_H_
#define FILE_OP_H_

extern char	Kfsserver[32];
extern int	Kfsport;

#ifdef MT_KFS_BACKEND
//#include "mt2kfs.h"
//#include "dlfcn.h"
extern int
kfs_open(char *fname, int flag, char *serverHost, int port);

extern int
kfs_create(char *fname, char *serverHost, int port);

extern int
kfs_read(int fd,char *buf, int len, char *serverHost, int port);

extern int
kfs_mkdir(char *tab_dir, char *serverHost, int port);

extern int
kfs_write(int fd, char *buf, int buf_len, char *serverHost, int port);

extern int
kfs_close(int fd, char *serverHost, int port);

extern int
kfs_seek(int fd, int offset, int flag, char *serverHost, int port);

extern int
kfs_exist(char *tab_dir, char *serverHost, int port);

extern int
kfs_readdir(char * tab_dir, char* mt_entries, char *serverHost, int port);

extern int
kfs_append(int fd, char *buf, int buf_len, char *serverHost, int port);



#define	OPEN(fd, tab_dir, flag)							\
	do{									\
		fd = kfs_open((char *)(tab_dir), (flag | O_CREAT), Kfsserver, Kfsport);		\
										\
		if (fd < 0)							\
		{								\
			fprintf(stderr, "open file failed for %s\n", strerror(errno));\
		}								\
										\
	}while(0)

#define	MKDIR(status, tab_dir, flag)						\
	do{									\
		status = kfs_mkdir((char *)(tab_dir), Kfsserver, Kfsport); 	\
										\
		if (status < 0)							\
		{								\
			fprintf(stderr, "mkdir failed for %s\n", strerror(errno));\
		}								\
	}while(0)

#define	READ(fd, buf, len)	kfs_read((fd), (char *)(buf), (len), Kfsserver, Kfsport)
	
#define	WRITE(fd, buf, buf_len)	kfs_write((fd), (char *)(buf), (buf_len), Kfsserver, Kfsport)

#define	CLOSE(fd)		kfs_close(fd, Kfsserver, Kfsport)

#define LSEEK(fd, offset flag)	kfs_seek(fd, offset, flag, Kfsserver, Kfsport)

#define	STAT(dir, state)	stat((dir), (state))

#define EXIST(dir)		kfs_exist((char *)(dir), Kfsserver, Kfsport)

#define READDIR(dir, mt_entries)	kfs_readdir((char *)dir, mt_entries, Kfsserver, Kfsport)

#define APPEND(fd, buf, buf_len)	kfs_append(fd, buf, buf_len, Kfsserver, Kfsport)

#else

#define	OPEN(fd, tab_dir, flag)							\
	do{									\
		fd = open((tab_dir), (flag | O_CREAT), 0666);			\
										\
		if (fd < 0)							\
		{								\
			fprintf(stderr, "open file failed for %s\n", strerror(errno));\
		}								\
										\
	}while(0)

#define	MKDIR(status, tab_dir, flag)						\
	do{									\
		status = mkdir((tab_dir), (flag)); 				\
										\
		if (status < 0)							\
		{								\
			fprintf(stderr, "mkdir failed for %s\n", strerror(errno));\
		}								\
	}while(0)

#define	READ(fd, buf, len)	read((fd), (buf), (len))
	
#define	WRITE(fd, buf, buf_len)	write((fd), (buf), (buf_len))

#define	CLOSE(fd)		close(fd)

#define LSEEK(fd, offset, flag)	lseek(fd, offset, flag)

#define	STAT(dir, state)	stat((dir), (state))

#define APPEND(fd, buf, buf_len, status)					\
	do{									\
		int offset = LSEEK(fd, 0, SEEK_END);				\
										\
		if ((offset + buf_len) > SSTABLE_SIZE)				\
		{								\
			status = 0;						\
		}								\
		else								\
		{								\
			*(int *)(buf + buf_len - sizeof(int) - sizeof(int)) = offset;\
			*(int *)(buf + buf_len - sizeof(int)) = offset + buf_len;\
			status = WRITE(fd, buf, buf_len);			\
		}								\
	}while(0)

#endif


int
file_read(char *file_path, char *buf, int file_size);

void 
file_crt_or_rewrite(char *file_name, char* content);

int
file_exist(char* file_path);

int
file_get_size(char *file_path);

#endif 
