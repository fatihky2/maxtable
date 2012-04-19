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

#ifndef LOG_H_
#define LOG_H_



#ifdef MAXTABLE_BENCH_TEST

#define LOG_FILE_DIR   "./rglog"
#define	BACKUP_DIR	"./rgbackup"

#else

#define LOG_FILE_DIR   "/mnt/rglog"
#define	BACKUP_DIR	"/mnt/rgbackup"

#endif


#define	LOG_TOTALNUM	8


#define	LOG_INVALID		0
#define	LOG_BEGIN		1	
#define LOG_END			2
#define	LOG_DATA_SSTAB_SPLIT	3
#define LOG_DATA_INSERT		4
#define LOG_DATA_DELETE		5
#define CHECKPOINT_BEGIN	6
#define CHECKPOINT_COMMIT	7
#define	LOG_SKIP		8
#define	LOG_UPDRID		9
#define	LOG_BLK_SPLIT		10	
#define	LOG_INDEX_SSTAB_SPLIT	11
#define	LOG_INDEX_INSERT	12
#define	LOG_INDEX_DELETE	13


#define	LOG_MAGIC_LEN		8
#define	MT_LOG			"MT_LOG"
#define	INSDEL_LOG_MAGIC	"INSDEL"
#define	CHKPOINT_BEGLOG_MAGIC	"CHKPTBE"
#define	CHKPOINT_COMMLOG_MAGIC	"CHKPTEN"
#define	LOG_BEGIN_MAGIC		"LOGBEG"
#define	LOG_END_MAGIC		"LOGEND"
#define	SSTAB_SPLIT_MAGIC	"SSTABSP"
#define	UPDRID_MAGIC		"UPDRID"
#define	BLOCK_SPLIT_MAGIC	"BLKSPLI"
#define	INDEX_INSDEL_MAGIC	"IDXINDE"


typedef struct loghdr
{
	char		logmagic[LOG_MAGIC_LEN];
	int		opid;		
	int		status;
	int		loglen;
	int		pad;
	char		log_test_magic[8];
}LOGHDR;


typedef struct loginsdel
{
	LOGHDR		loghdr;

	unsigned int	oldts;		
	unsigned int	newts;
	int		minrowlen;
	int		tabid;
	int		sstabid;
	int		blockid;
	int		rnum;
	int		status;
	char		sstabname[SSTABLE_NAME_MAX_LEN];

}LOGINSDEL;


#define	LOGINSDEL_RID_UPD	0x0001


typedef struct logchkpt
{
	LOGHDR		loghdr;
}LOGCHKPT;


typedef struct logbegin
{
	LOGHDR		loghdr;
	int		begincase;
	int		pad;
}LOGBEGIN;


#define	LOGBEG_INDEX_CASE	0x0001	


typedef struct logend
{
	LOGHDR		loghdr;
}LOGEND;


typedef struct logsplit
{
	LOGHDR		loghdr;

	char		oldsstabname[SSTABLE_NAME_MAX_LEN];
	char		newsstabname[SSTABLE_NAME_MAX_LEN];
}LOGSPLIT;


typedef struct logupdrid
{
	LOGHDR		loghdr;

	unsigned int	oldts;		
	unsigned int	newts;
	int		minrowlen;
	int		sstabid;
	int		blockid;
	int		rnum;
	int		idx_id;
	int		pad;
	char		sstabname[SSTABLE_NAME_MAX_LEN];
	RID		oldrid;
	RID		newrid;

}LOGUPDRID;


typedef union logrec
{
	LOGINSDEL	loginsdel;
	LOGCHKPT	logchkpt;
	LOGBEGIN	logbeg;
	LOGEND		logend;
	LOGSPLIT	logsplit;
	LOGUPDRID	logupdrid;
	
} LOGREC;




#define	CHECKPOINT_BIT_BEGIN	0x0001
#define CHECKPOINT_BIT_END	0x0002



typedef struct logfile
{
	char	magic[16];
	int	logtotnum;
	int	logtype;
	LOGREC	logrec[LOG_TOTALNUM];
}LOGFILE;


#define	SPLIT_LOG	1


void
log_build(LOGREC *logrec, int logopid, unsigned int oldts, unsigned int newts,
		char *oldsstab, char *newsstab, int minrowlen, int tabid,
		int sstabid, int blockid, int rnum, char *oldrid, char *newrid);


int
log_get_rgbackup(char *rgbackup, char *rgip, int rgport);

int
log_put(LOGREC *logrec, char *rp, int rlen);

int
log_get_latest_rglogfile(char *rginsdellogfile, char *rg_ip, int port);

int
log_recovery(char *insdellogfile, char *rg_ip, int rg_port);

int
log_recov_rg(char *rgip, int rgport);



#endif

