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
kfs_write(int fd, char *buf, int buf_len, char *serverHost, int port);

int
kfs_close(int fd, char *serverHost, int port);

int
kfs_seek(int fd, int offset, char *serverHost, int port);

int
kfs_exist(char *tab_dir, char *serverHost, int port);

int
kfs_readdir(char * tab_dir,MT_ENTRIES * mt_entries);



#ifdef __cplusplus
}
#endif

#endif
