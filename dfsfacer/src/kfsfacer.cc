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

#define TABLE_READDIR_MAX_NUM		32
#define TABLE_NAME_READDIR_MAX_LEN	128
typedef struct mt_entries
{
	int	ent_num;
	char	tabname[TABLE_READDIR_MAX_NUM][TABLE_NAME_READDIR_MAX_LEN];
	
}MT_ENTRIES;

#define	KFS_LOG_FILE_SIZE	(4 * 1024 * 1024)

#define	BLOCKSIZE		(64 * 1024)		
//#define BLOCKSIZE		(512)


#define	BLK_CNT_IN_SSTABLE	64
#define	SSTABLE_SIZE		(BLK_CNT_IN_SSTABLE * BLOCKSIZE)


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
kfs_rmdir(char *tab_dir, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	stat;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	stat = kfsClient->Rmdir(tab_dir);

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
kfs_seek(int fd, int offset, int flag, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	stat;


	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	stat = kfsClient->Seek(fd, offset, flag);
	
	return stat;
}


int
kfs_exist(char *tab_dir, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	stat;


	stat = -1;
	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	if (kfsClient->Exists(tab_dir))
	{
		/* 0 stands for the file existing, it will match the stat(). */
		stat = 0;
	}

	return stat;

}

int
kfs_readdir(char *tab_dir, char *ent, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	stat;
	vector<string> entries;
	MT_ENTRIES	*mt_entries;


	mt_entries = (MT_ENTRIES *)ent;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	stat = kfsClient->Readdir(tab_dir, entries);
    
    	if ((stat < 0) || (entries.size() == 0))
        {
        	return 0;
    	}

	mt_entries->ent_num = entries.size();
	
	vector<string>::size_type i;
	for (i = 0; i < entries.size(); i++)
	{
		memcpy(mt_entries->tabname[i], entries[i].c_str(),strlen(entries[i].c_str()));		
	}
	

	return 1;
}


int
kfs_append(int fd, char *buf, int buf_len, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	nwrite;
	int	offset;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	offset = kfsClient->Tell(fd);

	if ((offset + buf_len) > KFS_LOG_FILE_SIZE)
	{
		return 0;
	}

	/* Fill the LOGREC. */
	*(int *)(buf + buf_len - sizeof(int) - sizeof(int)) = offset;
	*(int *)(buf + buf_len - sizeof(int)) = offset + buf_len;

	nwrite = kfsClient->Write(fd, buf, buf_len);

	if (nwrite != buf_len) 
	{
		cout << "Was able to write only: " << nwrite << " instead of " << buf_len << endl;
	}

	// flush out the changes
	kfsClient->Sync(fd);

	return 1;
}


int
kfs_copy(char *filename_src, char *filename_dest, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	nread;
	int	srcfd;
	int	destfd;
	int	nwrite;
	int	len = SSTABLE_SIZE;	/* This routine is for the sstable split backup. */
	char 	buf[len];

	
	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}	

	srcfd = kfsClient->Open(filename_src, O_RDONLY);
	if (srcfd < 0) 
	{
		cout << "kfs: unable to open: " << filename_src << endl;
		exit(-1);
	}
	nread = kfsClient->Read(srcfd, buf, (size_t)len);

	 if (nread != len) 
	 {
            cout << "kfs: read error " << filename_src << endl;

	    kfsClient->Close(srcfd);
            exit(-1);
        }
	

	destfd = kfsClient->Create(filename_dest);

	if (destfd < 0) 
	{
		cout << "kfs: unable to create: " << filename_dest << endl;
		exit(-1);
	}
	
	nwrite = kfsClient->Write(destfd, buf, len);

	if (nwrite != len) 
	{
		cout << "kfs: write error " << filename_dest << endl;
	}
	
	kfsClient->Close(destfd);
	kfsClient->Close(srcfd);
	
	return 0;
	
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
