/*
** Copyright (C) 2012 Xue Yingfei
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
	

#include "global.h"
#include "strings.h"
#include "master/metaserver.h"
#include "tabinfo.h"
#include "rpcfmt.h"
#include "parser.h"
#include "ranger/rangeserver.h"
#include "memcom.h"
#include "buffer.h"
#include "block.h"
#include "file_op.h"
#include "utils.h"
#include "tss.h"
#include "log.h"
#include "rginfo.h"
#include "rollback.h"


extern TSS		*Tss;
extern KERNEL		*Kernel;
extern RANGEINFO 	*Range_infor;
extern	RG_LOGINFO	*Rg_loginfo;



int
rollback_rg()
{

	LOCALTSS(tss);
	int	log_end;
	int	log_beg;
	int	fd;
	int	offset;
	char	*logfilebuf;
	LOGREC	*logrec;
	char	logfile[TABLE_NAME_MAX_LEN];
	

	if (tss->tstat & TSS_BEGIN_LOGGING)
	{
		traceprint("No log need to rollback.\n");
		
		return TRUE;
	}

	P_SPINLOCK(BUF_SPIN);
	
	logfilebuf = (char *)malloc(LOG_FILE_SIZE);

	CLOSE(Rg_loginfo->logfd);

	log_get_latest_rglogfile(logfile, Range_infor->rg_ip, Range_infor->port);

	OPEN(Rg_loginfo->logfd, logfile, (O_RDWR));
	
	if (tss->tstat & TSS_LOGGING_SCOPE)
	{
		log_beg = 0;
		fd = Rg_loginfo->logfd;

		offset = READ(fd, logfilebuf, LOG_FILE_SIZE);

		offset = log_get_last_logoffset((LOGREC *)logfilebuf);

		Assert(offset < LOG_FILE_SIZE);

		
		log_end = offset;

		while (log_end > log_beg)
		{			
			logrec = (LOGREC *)(logfilebuf + log_end - sizeof(LOGREC));

			log_undo((LOGHDR *)logrec, Range_infor->rg_ip, Range_infor->port);
					
			/*
			** Insert and Delete log's length will be greater than the 
			** sizeof(LOGREC) because it includes the row length. 
			*/
			log_end -= ((LOGHDR *)logrec)->loglen;		
		}

		
		char	prelogfile[TABLE_NAME_MAX_LEN];
		
		int idxpos = str1nstr(logfile, "/log\0", STRLEN(logfile));
	
		int logfilenum = m_atoi(logfile + idxpos, 
					STRLEN(logfile) - idxpos);

		/* It can only back one log file. */
		if (logfilenum > 0)
		{				
			logfilenum--;

			MEMSET(prelogfile, STRLEN(prelogfile));
			MEMCPY(prelogfile, logfile, idxpos);
			sprintf(prelogfile + idxpos, "%d", logfilenum);

			OPEN(fd, prelogfile, (O_RDWR));
		}
		else
		{
			Assert(0);
		}

		
	}
	else
	{	
		fd = Rg_loginfo->logfd;
	}
	
	log_beg = tss->tlogbeg_off;
	
	offset = READ(fd, logfilebuf, LOG_FILE_SIZE);

	offset = log_get_last_logoffset((LOGREC *)logfilebuf);

	Assert(offset < LOG_FILE_SIZE);

	
	log_end = offset;

	while (log_end > log_beg)
	{
//		log_end -= sizeof(LOGREC);
		
		logrec = (LOGREC *)(logfilebuf + log_end - sizeof(LOGREC));

		log_undo((LOGHDR *)logrec, Range_infor->rg_ip, Range_infor->port);
				
		/*
		** Insert and Delete log's length will be greater than the 
		** sizeof(LOGREC) because it includes the row length. 
		*/
		log_end -= ((LOGHDR *)logrec)->loglen;
	}

	if (tss->tstat & TSS_LOGGING_SCOPE)
	{
		CLOSE(fd);
	}

	free(logfilebuf);

	V_SPINLOCK(BUF_SPIN);
	
	return TRUE;

}

