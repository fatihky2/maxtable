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
#include "rpcfmt.h"
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

#define TABLE_READDIR_MAX_NUM		1024
#define TABLE_NAME_READDIR_MAX_LEN	128
typedef struct mt_entries
{
	int	ent_num;
	char	tabname[TABLE_READDIR_MAX_NUM][TABLE_NAME_READDIR_MAX_LEN];
	
}MT_ENTRIES;

#define	KFS_LOG_FILE_SIZE	(4 * 1024 * 1024)


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
kfs_remove(char *tab_file, char *serverHost, int port)
{
	KfsClientPtr kfsClient;
	int	stat;

	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

	stat = kfsClient->Remove(tab_file);

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

	if (mt_entries->ent_num > TABLE_READDIR_MAX_NUM)
	{
		cout << "File number extend the container." << endl;
		exit(-1);
	}
	
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


	kfsClient = getKfsClientFactory()->GetClient(serverHost, port);

	if (!kfsClient) 
	{
		cout << "kfs client failed to initialize...exiting" << endl;
		exit(-1);
	}

//	nwrite = kfsClient->Write(fd, buf, buf_len);
//	nwrite = kfsClient->AtomicRecordAppend(fd, buf, buf_len);
	
	nwrite = kfsClient->RecordAppend(fd, buf, buf_len);
	
	if (nwrite != buf_len) 
	{
		cout << "Was able to write only: " << nwrite << " instead of " << buf_len << endl;
	}

	// flush out the changes
	kfsClient->Sync(fd);

	return nwrite;
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

#ifdef MEMMGR_KFS_TEST

struct timeval tpStart;
struct timeval tpEnd;
float timecost;

int
main(int argc, char **argv)
{
        char    filebuf[64000];
        int     flag;
        int     fd;
        char    *content;
	char	*rglog;
        int     nwrite;
        int     nread;
	char	*serverIp = "172.16.10.42\0";
	int	serverPort = 20000;

	int wcount = 1000;
	int offset = 0;

//        kfs_mkdir((char *)"/table", serverHost, port);

	flag = O_CREAT | O_APPEND | O_RDWR;
//	flag = O_CREAT | O_RDWR;
//       	flag = O_APPEND | O_RDWR;
//	flag = O_WRONLY;
//	flag = O_RDWR;
        content = (char *)"/table";
	rglog = (char *)"rglog";
	

        //fd = create(content);

 //       fd = kfs_create(content, serverIp, serverPort);

	
	gettimeofday(&tpStart, NULL);
//#if 0
//retry:
	fd = kfs_open(content, flag,serverIp, serverPort);
        if (fd < 0)
        {
                return -1;
        }

	
   
	
	while(wcount)
	{	
		//kfs_exist(rglog, serverIp, serverPort);
		
		nwrite = kfs_append(fd, filebuf, 500, serverIp, serverPort);

		//kfs_exist(rglog, serverIp, serverPort);

//		offset = kfs_seek(fd, 0, SEEK_END,serverIp, serverPort);

		
//		cout << "offset = " << offset <<endl;

//		nwrite = kfs_write(fd,  filebuf, 1000, serverIp, serverPort);
		//      nread = kfs_read(fd, filebuf1, 1024);

		if (nwrite != 500)
		{
		        cout << "kfs: write error " << endl;
		}

	
		

		wcount--;
//		kfs_close(fd, serverIp, serverPort);

//		goto retry;

		
	}

	
	
        kfs_close(fd, serverIp, serverPort);






		
	gettimeofday(&tpEnd, NULL);
	timecost = 0.0f;
	timecost = tpEnd.tv_sec - tpStart.tv_sec + (float)(tpEnd.tv_usec-tpStart.tv_usec)/1000000;
	printf("Inserted rows = %d\n", wcount);
	printf("time cost: %f\n", timecost);


//#endif
#if 0
	flag = O_RDWR;

	fd = kfs_open(content, flag,serverIp, serverPort);

	int nbytes = 0;
	char	readbuf[30000];

	memset(readbuf, 0, 30000);

//	nwrite = kfs_append(fd, filebuf, 6400, serverIp, serverPort);
//	nbytes = kfs_read(fd, readbuf, 30000,serverIp, serverPort);
	
	offset = kfs_seek(fd, 0, SEEK_END,serverIp, serverPort);
	 kfs_close(fd, serverIp, serverPort);

#endif	

        return 1;
}

#endif

