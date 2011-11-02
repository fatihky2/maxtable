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

using std::vector;
using std::string;
using std::cout;
using std::endl;
using namespace KFS;


//char *serverHost = "127.0.0.1";
//int port = 20000;

#define TABLE_MAX_NUM		32
#define TABLE_NAME_MAX_LEN	128
typedef struct mt_entries
{
	int	ent_num;
	char	tabname[TABLE_MAX_NUM][TABLE_NAME_MAX_LEN];
	
}MT_ENTRIES;

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

int
kfs_readdir(char *tab_dir, MT_ENTRIES *mt_entries)
{
	KfsClientPtr kfsClient;
	int	stat;
	vector<string> entries;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	stat = kfsClient->Readdir(tab_dir, entries);
    
    	if ((stat < 0) || (entries.size() == 0))
        {
        	return FALSE;
    	}

	mt_entries->ent_num = entries.size();
	
	int i;
	for (i = 0; i < entries.size(); i++)
	{
		memcpy(mt_entries->tabname[i], entries[i].c_str(),entries[i].size);		
	}
	

	return TRUE;
}

/*

int
main(int argc, char **argv)
{
        char    filebuf[6400];
        int     flag;
        int     fd;
        char    *content;
        int     nwrite;
        int     nread;


//        kfs_mkdir((char *)"/table", serverHost, port);

        flag = O_CREAT | O_WRONLY | O_TRUNC;
//	flag = O_WRONLY;
        content = (char *)"/table/test107";

        //fd = create(content);

        fd = kfs_open(content, flag, serverHost, port);
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

        nwrite = kfs_write(fd, filebuf, 6400, serverHost, port);

//      nread = kfs_read(fd, filebuf1, 1024);

        if (nwrite != 6400)
        {
                ;
        }

        kfs_close(fd, serverHost, port);

        return 1;
}

*/
