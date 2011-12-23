#ifndef KFSFACER_H_
#define KFSFACER_H_

#ifdef __cplusplus
extern "C" {
#endif
int
kfs_open(char *fname, int flag, char *serverHost, int port);

int
kfs_create(char *fname, char *serverHost, int port);

int
kfs_read(int fd,char *buf, int len, char *serverHost, int port);

int
kfs_mkdir(char *tab_dir, char *serverHost, int port);

int
kfs_rmdir(char * tab_dir,char * serverHost,int port);

int
kfs_remove(char * tab_file,char * serverHost,int port);

int
kfs_write(int fd, char *buf, int buf_len, char *serverHost, int port);

int
kfs_close(int fd, char *serverHost, int port);

int
kfs_seek(int fd, int offset, int flag, char *serverHost, int port);

int
kfs_exist(char *tab_dir, char *serverHost, int port);

int
kfs_readdir(char * tab_dir, char *mt_entries, char *serverHost, int port);

int
kfs_append(int fd, char *buf, int buf_len, char *serverHost, int port);

int
kfs_copy(char * filename_src,char * filename_dest,char * serverHost,int port);


#ifdef __cplusplus
}
#endif

#endif
