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

/* Following definition id for the opid. */
#define	LOG_INVALID		0
#define	LOG_BEGIN		1
#define LOG_END			2
#define	LOG_DO_SPLIT		3

typedef struct logrec
{
	int		opid;
	unsigned int	ts;
	char		tablename[TABLET_NAME_MAX_LEN];
	char		oldsstabname[SSTABLE_NAME_MAX_LEN];
	char		newsstabname[SSTABLE_NAME_MAX_LEN];
}LOGREC;

/* Following definition is for the logtype. */
#define	SPLIT_LOG	1

typedef struct logfile
{
	char	magic[16];
	int	logtotnum;
	int	logtype;
	LOGREC	logrec[LOG_TOTALNUM];
}LOGFILE;


void
log_build(LOGREC *logrec, int logopid, unsigned int ts, char *oldsstab, char *newsstab);

int
log_insert(char	 *logfile_dir, LOGREC *logrec, int logtype);

int
log_delete(LOGFILE *logfilebuf, int logtype);

int
log_undo(char *logfile_dir, char *backup_dir, int logtype);

int
log_get_rglogfile(char *rglogfile, char *rgip, int rgport);

int
log_get_rgbackup(char *rgbackup, char *rgip, int rgport);


#endif

