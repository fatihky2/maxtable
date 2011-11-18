/*
** log.h 2011-10-27 xueyingfei
**
** Copyright Transoft Corp.
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
#define	LOG_DO_SPLIT		3
#define LOG_INSERT		4
#define LOG_DELETE		5
#define CHECKPOINT_BEGIN	6
#define CHECKPOINT_COMMIT	7

typedef struct logrec
{
	int		opid;
	int		status;
	unsigned int	oldts;
	unsigned int	newts;
	int		minrowlen;
	int		tabid;
	int		sstabid;
	char		tablename[TABLET_NAME_MAX_LEN];
	char		oldsstabname[SSTABLE_NAME_MAX_LEN];
	char		newsstabname[SSTABLE_NAME_MAX_LEN];
	int		rowend_off;
	int		cur_log_off;
	int		next_log_off;
}LOGREC;


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
log_build(LOGREC *logrec, int logopid, unsigned int oldts, unsigned int newts, char *oldsstab, 
		char *newsstab, int minrowlen, int tabid, int sstabid);

int
log_insert_sstab_split(char	 *logfile_dir, LOGREC *logrec, int logtype);

int
log_delete(LOGFILE *logfilebuf, int logtype);

int
log_undo_sstab_split(char *logfile_dir, char *backup_dir, int logtype);

int
log_get_sstab_split_logfile(char *rglogfile, char *rgip, int rgport);

int
log_get_rgbackup(char *rgbackup, char *rgip, int rgport);

int
log_insert_insdel(char	 *logfile_dir, LOGREC *logrec, char *rp, int rlen);

int
log_get_latest_rginsedelfile(char *rginsdellogfile, char *rg_ip, int port);

int
log_redo_insdel(char *insdellogfile);


#endif

