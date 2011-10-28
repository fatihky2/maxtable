/*
** log.c 2011-10-27 xueyingfei
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

#include "global.h"
#include "strings.h"
#include "master/metaserver.h"
#include "ranger/rangeserver.h"
#include "memcom.h"
#include "file_op.h"
#include "utils.h"
#include "tss.h"
#include "log.h"


extern TSS	*Tss;

static int
log_check_insert(LOGFILE *logfile, int logopid, int logtype)
{
	int	lastopid;
	int	status;


	status = FALSE;


	if ((logopid == LOG_BEGIN) && (logfile->logtotnum == 0))
    	{
    		return TRUE;
    	}

	Assert((logfile->logtotnum > 0) && (logfile->logtotnum < (LOG_TOTALNUM + 1)));

	if (logtype != logfile->logtype)
	{
		return TRUE;
	}
	
	lastopid = logfile->logrec[logfile->logtotnum - 1].opid;

	switch (lastopid)
	{
	    case LOG_BEGIN:
	    	/* TODO: we need to expand it for the other log opid, like insert, delete.*/
	    	if (logopid == LOG_DO_SPLIT)
	    	{
	    		status = TRUE;
	    	}
	    	break;
	    case LOG_END:
	    	if (logopid == LOG_BEGIN)
	    	{
	    		status = log_delete(logfile, logfile->logtype);
	    	}
	    	break;
	    case LOG_DO_SPLIT:
	    	if (logopid == LOG_END)
	    	{
	    		status = TRUE;
	    	}
	    	break;
	    default:
	    			
	    	break;
	}

	return status;
}

static int
log_check_delete(LOGFILE *logfile, int logtype, char *backup)
{
	int	status;
	LOGREC	*logrec;


	status = FALSE;
	
	if (logtype != logfile->logtype)
	{
		return status;
	}

	logrec = logfile->logrec;

	switch (logtype)
	{
	    case SPLIT_LOG:
	    	if (logfile->logtotnum != 3)
	    	{
	    		break;
	    	}

		if (   (logrec[0].opid == LOG_BEGIN) && (logrec[1].opid == LOG_DO_SPLIT)
		    && (logrec[2].opid == LOG_END))
		{
			char	cmd_str[TABLE_NAME_MAX_LEN];
			
			MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
			char	tmpsstab[TABLE_NAME_MAX_LEN];

			MEMSET(tmpsstab, TABLE_NAME_MAX_LEN);
			int i = strmnstr(logrec[1].oldsstabname, "/", STRLEN(logrec[1].oldsstabname));
	
			MEMCPY(tmpsstab, logrec[1].oldsstabname + i, STRLEN(logrec[1].oldsstabname + i));
	
				
			sprintf(cmd_str, "rm -rf %s/%s",  backup, tmpsstab);
			
			if (!system(cmd_str))
			{
				status = TRUE;
			}
		}

		break;
		
	    	/* TODO: we need to expand it for the other log opid, like insert, delete.*/

	    default:
	    			
	    	break;
	}

	return status;
}

void
log_build(LOGREC *logrec, int logopid, unsigned int ts, char *oldsstab, char *newsstab)
{
	MEMSET(logrec, sizeof(LOGREC));
	
	logrec->opid = logopid;
	logrec->ts = ts;

	if (oldsstab)
	{
		MEMCPY(logrec->oldsstabname, oldsstab,SSTABLE_NAME_MAX_LEN);
	}

	if (newsstab)
	{
		MEMCPY(logrec->newsstabname, newsstab,SSTABLE_NAME_MAX_LEN);
	}
}

int
log_insert(char	 *logfile_dir, LOGREC *logrec, int logtype)
{
	LOCALTSS(tss);
	int		fd;
	int		status;
	LOGFILE		*logfilebuf;

	
	logfilebuf = (LOGFILE *)MEMALLOCHEAP(sizeof(LOGFILE));
	
	OPEN(fd, logfile_dir, (O_RDWR));

	if (fd < 0)
	{
		goto exit;
	}

	READ(fd,logfilebuf, sizeof(LOGFILE));

	status = log_check_insert(logfilebuf , logrec->opid, logtype);

	if (status == FALSE)
	{
		goto exit;
	}

	if (logrec->opid == LOG_DO_SPLIT)
	{
		/* Copy the file into the backup file. */
		char	cmd_str[TABLE_NAME_MAX_LEN];
		
		MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
			
		sprintf(cmd_str, "cp %s %s", logrec->oldsstabname, tss->rgbackpfile);
		
		if (system(cmd_str))
		{
			status = FALSE;
			goto exit;
		}
	}
	
	MEMCPY(&(logfilebuf->logrec[logfilebuf->logtotnum]), logrec, sizeof(LOGREC));
	
	(logfilebuf->logtotnum)++;
	logfilebuf->logtype = logtype;

	LSEEK(fd, 0, SEEK_SET);
	WRITE(fd, logfilebuf, sizeof(LOGFILE));

exit:

	CLOSE(fd);	

	MEMFREEHEAP(logfilebuf);

	return status;
}

int
log_delete(LOGFILE *logfilebuf, int logtype)
{
	LOCALTSS(tss);
	int		status;


	status = FALSE;
	
	status = log_check_delete(logfilebuf, logtype, tss->rgbackpfile);

	if (status == FALSE)
	{
		goto exit;
	}

	logfilebuf->logtotnum = 0;
	logfilebuf->logtype = 0;

exit:

	return status;
}

int
log_undo(char *logfile_dir, char *backup_dir, int logtype)
{
	int		fd;
	int		status;
	LOGFILE		*logfilebuf;

	
	logfilebuf = (LOGFILE *)MEMALLOCHEAP(sizeof(LOGFILE));
	status = FALSE;
	
	OPEN(fd, logfile_dir, (O_RDWR));

	if (fd < 0)
	{
		goto exit;
	}

	READ(fd,logfilebuf, sizeof(LOGFILE));

	if (logfilebuf->logtotnum == 0)
	{
		goto exit;
	}
	
	if (logfilebuf->logrec[logfilebuf->logtotnum - 1].opid == LOG_END)
	{
		status = log_check_delete(logfilebuf, logtype, backup_dir);

		if (status == FALSE)
		{
			goto exit;
		}
	}
	else
	{
		if (logfilebuf->logtype == SPLIT_LOG)
		{
			if (logfilebuf->logtotnum == 2)
			{
				Assert(logfilebuf->logrec[1].opid == LOG_DO_SPLIT);
			
			
				/* Copy the file into the backup file. */
				char	cmd_str[64];
				
				MEMSET(cmd_str, 64);

				LOGREC	*logrec = logfilebuf->logrec;


				char	tmpsstab[TABLE_NAME_MAX_LEN];

				MEMSET(tmpsstab, TABLE_NAME_MAX_LEN);
				int i = strmnstr(logrec[1].oldsstabname, "/", STRLEN(logrec[1].oldsstabname));
		
				MEMCPY(tmpsstab, logrec[1].oldsstabname + i, STRLEN(logrec[1].oldsstabname + i));


				char	srcfile[TABLE_NAME_MAX_LEN];

				MEMSET(srcfile, TABLE_NAME_MAX_LEN);
				sprintf(srcfile, "%s/%s", backup_dir, tmpsstab);				
				

				char	tab_dir[TABLE_NAME_MAX_LEN];
				
				MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
				MEMCPY(tab_dir, logrec[1].oldsstabname, i);			
										
				if (STAT(tab_dir, &st) != 0)
				{
					if (status < 0)
					{
						goto exit;
					}
				}
			
				sprintf(cmd_str, "cp %s %s", srcfile, tab_dir);
				
				if (system(cmd_str))
				{
					status = FALSE;
					goto exit;
				}

				sprintf(cmd_str, "rm %s", srcfile);
				
				if (system(cmd_str))
				{
					status = FALSE;
					goto exit;
				}
				

			}
		}
	}
	
	logfilebuf->logtotnum = 0;
	logfilebuf->logtype = 0;

	LSEEK(fd, 0, SEEK_SET);
	WRITE(fd, logfilebuf, sizeof(LOGFILE));

exit:

	CLOSE(fd);	

	MEMFREEHEAP(logfilebuf);

	return status;
}


int
log_get_rglogfile(char *rglogfile, char *rgip, int rgport)
{
	MEMSET(rglogfile, 256);
	MEMCPY(rglogfile, LOG_FILE_DIR, STRLEN(LOG_FILE_DIR));

	char	rgname[64];
	
	MEMSET(rgname, 64);
	sprintf(rgname, "%s%d", rgip, rgport);

	str1_to_str2(rglogfile, '/', rgname);

	if (STAT(rglogfile, &st) != 0)
	{
		traceprint("Log file %s is not exist.\n", rglogfile);
		return FALSE;
	}

	return TRUE;
}

int
log_get_rgbackup(char *rgbackup, char *rgip, int rgport)
{
	MEMSET(rgbackup, 256);
	MEMCPY(rgbackup, BACKUP_DIR, STRLEN(BACKUP_DIR));

	
	char	rgname[64];
		
	MEMSET(rgname, 64);
	sprintf(rgname, "%s%d", rgip, rgport);

	str1_to_str2(rgbackup, '/', rgname);

	if (STAT(rgbackup, &st) != 0)
	{
		traceprint("Backup file %s is not exist.\n", rgbackup);
		return FALSE;
	}

	return TRUE;
}
