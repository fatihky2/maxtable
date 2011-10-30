#include <iostream>
#include <fstream>
#include <cerrno>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <fcntl.h>
}

#include "libkfsClient/KfsClient.h"
#include "../include/kfsfacer.h"

using std::cout;
using std::endl;
using namespace KFS;



//string serverHost = "127.0.0.1";
//int port = 20000;


int 
kfs_open(char *fname, int flag, char *serverHost, int port)
{
	int	fd;
	KfsClientPtr kfsClient;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	fd = kfsClient->Open(fname, flag);

	return fd;
}

int
kfs_create(char *fname, char *serverHost, int port)
{
        int     fd;
        KfsClientPtr kfsClient;

        kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

        if (!kfsClient)
        {
                cout << "kfs client failed to initialize...exiting" << endl;
                exit(-1);
        }

        fd = kfsClient->Create(fname);

        return fd;
}


int
kfs_read(int fd,char *buf, int len, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	nread;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	nread = kfsClient->Read(fd, buf, (size_t)len);

	return nread;
	
}

int
kfs_mkdir(char *tab_dir, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	stat;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	stat = kfsClient->Mkdir(tab_dir);

	return stat;
}

int
kfs_write(int fd, char *buf, int buf_len, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	nwrite;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	nwrite = kfsClient->Write(fd, buf, buf_len);
	
	if (nwrite != buf_len) 
	{
		cout << "Was able to write only: " << nwrite << " instead of " << buf_len << endl;
	}

	// flush out the changes
	kfsClient->Sync(fd);
    
	return nwrite;
}

int
kfs_close(int fd, char *serverHost, int port)
{
	KfsClientPtr kfsClient;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}
	
	// Close the file-handle
	kfsClient->Close(fd);

	return 1;
} 

int
kfs_seek(int fd, int offset, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	stat;


	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	stat = kfsClient->Seek(fd, offset, SEEK_SET);
	
	return stat;
}


int
kfs_exist(char *tab_dir, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	stat;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	stat = kfsClient->Exists(tab_dir);

	return stat;

}

/*
int
main(int argc, char **argv)
{
        char    filebuf[1024];
        char    filebuf1[1024];
        int     flag;
        int     fd;
        char    *content;
        int     nwrite;
        int     nread;


        //mkdir((char *)"/table");

        flag = O_CREAT | O_WRONLY | O_TRUNC;
        content = (char *)"/table/test9";

        //fd = create(content);

        fd = kfs_open(content, flag);
        if (fd < 0)
        {
                return -1;
        }

        filebuf[0] = 'r';
        filebuf[1] = 'g';
        filebuf[2] = 'l';
        filebuf[3] = 'i';
        filebuf[4] = 's';
        filebuf[5] = 't';

        nwrite = kfs_write(fd, filebuf, 1024);

//      nread = kfs_read(fd, filebuf1, 1024);

        if (nwrite != 1024)
        {
                ;
        }

        kfs_close(fd);

        return 1;
}
*/
