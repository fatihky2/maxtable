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
kfs_rmdir(char * tab_dir,char * serverHost,int port);

extern int
kfs_remove(char * tab_file,char * serverHost,int port);

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

extern int
kfs_copy(char * filename_src,char * filename_dest,char * serverHost,int port);


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

#define	RMDIR(status, tab_dir)						\
	do{									\
		status = kfs_rmdir((char *)(tab_dir), Kfsserver, Kfsport); 	\
										\
		if (status < 0)							\
		{								\
			fprintf(stderr, "rmdir failed for %s\n", strerror(errno));\
		}								\
	}while(0)		

#define	RMFILE(status, tab_dir)						\
			do{									\
				status = kfs_remove((char *)(tab_dir), Kfsserver, Kfsport);	\
												\
				if (status < 0) 						\
				{								\
					fprintf(stderr, "rmfile failed for %s\n", strerror(errno));\
				}								\
			}while(0)		

#define	READ(fd, buf, len)	kfs_read((fd), (char *)(buf), (len), Kfsserver, Kfsport)
	
#define	WRITE(fd, buf, buf_len)	kfs_write((fd), (char *)(buf), (buf_len), Kfsserver, Kfsport)

#define	CLOSE(fd)		kfs_close(fd, Kfsserver, Kfsport)

#define LSEEK(fd, offset, flag)	kfs_seek(fd, offset, flag, Kfsserver, Kfsport)

#define	STAT(dir, state)	kfs_exist((char *)(dir), Kfsserver, Kfsport)

#define EXIST(dir)		kfs_exist((char *)(dir), Kfsserver, Kfsport)

#define READDIR(dir, mt_entries)	kfs_readdir((char *)dir, mt_entries, Kfsserver, Kfsport)

#define APPEND(fd, buf, buf_len)	kfs_append(fd, buf, buf_len, Kfsserver, Kfsport)

#define COPYFILE(filename_src, filename_dest)					\
				kfs_copy(filename_src, filename_dest, Kfsserver, Kfsport)


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

#define APPEND_d(fd, buf, buf_len, status)					\
	do{									\
		int offset = LSEEK(fd, 0, SEEK_END);				\
										\
		if ((offset + buf_len) > SSTABLE_SIZE)				\
		{								\
			status = 0;						\
		}								\
		else								\
		{								\
			status = WRITE(fd, buf, buf_len);			\
		}								\
	}while(0)

#define APPEND(fd, buf, buf_len, status)					\
	do{									\
		status = WRITE(fd, buf, buf_len);				\
	}while(0)


#endif

#define	OPEN_LOG(fd, tab_dir, flag)							\
		do{									\
			fd = open((tab_dir), (flag | O_CREAT), 0666);			\
											\
			if (fd < 0)							\
			{								\
				fprintf(stderr, "open file failed for %s\n", strerror(errno));\
			}								\
											\
		}while(0)
		
#define	MKDIR_LOG(status, tab_dir, flag)						\
		do{									\
			status = mkdir((tab_dir), (flag));				\
											\
			if (status < 0) 						\
			{								\
				fprintf(stderr, "mkdir failed for %s\n", strerror(errno));\
			}								\
		}while(0)
		
#define	READ_LOG(fd, buf, len)		read((fd), (buf), (len))
			
#define	WRITE_LOG(fd, buf, buf_len)	write((fd), (buf), (buf_len))
		
#define	CLOSE_LOG(fd)			close(fd)
		
#define LSEEK_LOG(fd, offset, flag)	lseek(fd, offset, flag)
		
#define	STAT_LOG(dir, state)		stat((dir), (state))
		
#define APPEND_LOG(fd, buf, buf_len, status)						\
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

int
file_read(char *file_path, char *buf, int file_size);

void 
file_crt_or_rewrite(char *file_name, char* content);

int
file_exist(char* file_path);

int
file_get_size(char *file_path);

#endif 
