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
#include "row.h"
#include "type.h"
#include "timestamp.h"
#include "session.h"
#include "rginfo.h"
#include "index.h"
#include "tablet.h"
#include "redo.h"
#include "undo.h"
#include "hkgc.h"


extern TSS	*Tss;
extern KERNEL	*Kernel;

RG_LOGINFO	*Rg_loginfo = NULL;


#define	LOG_FILE_SIZE		(16 * SSTABLE_SIZE)


#define	LOG_BUF_RESERV_MAX	2


typedef struct log_buf
{	
	int		fd;
	int		freeoff;
	char		*buf;
}LOG_BUF;

typedef struct log_recov
{	
	int		buf_idxmax;
	int		buf_idxcur;
	LOG_BUF		log_buf[LOG_BUF_RESERV_MAX];
	int		undo_end_buf;
	int		redo_start_buf;
	char		*undo_end_off;
	char		*redo_start_off;
}LOG_RECOV;


static int
log__get_last_logoffset(LOGREC *logrec);

static void
log__find_undoend(char *log_off, int log_beg_hit, LOG_RECOV *log_recov);


static char *
log__recov_redo(char *logfilebuf, int freeoff, char *log_redo_start);

static void
log__recov_undo(char *logfilebuf, int freeoff, char *log_undo_end, 
		char *rg_ip, int rg_port);


void
log_build(LOGREC *logrec, int logopid, unsigned int oldts, unsigned int newts, 
	char *oldsstab, char *newsstab, int minrowlen, int tabid, int sstabid,
	int blockid, int rnum, char *oldrid, char *newrid)
{
	MEMSET(logrec, sizeof(LOGREC));
	MEMCPY(((LOGHDR *)logrec)->logmagic, MT_LOG, LOG_MAGIC_LEN);

	
	((LOGHDR *)logrec)->opid = logopid;
	((LOGHDR *)logrec)->loglen = 0;
	((LOGHDR *)logrec)->status = 0;
	
	switch (logopid)
	{
	    case LOG_BEGIN:
		MEMCPY(((LOGHDR *)logrec)->log_test_magic, LOG_BEGIN_MAGIC, 
						LOG_MAGIC_LEN);
	    	break;
		
	    case LOG_END:

		MEMCPY(((LOGHDR *)logrec)->log_test_magic, LOG_END_MAGIC, 
						LOG_MAGIC_LEN);
	    	break;
		
	    case LOG_DATA_SSTAB_SPLIT:
	    case LOG_INDEX_SSTAB_SPLIT:
	    	MEMCPY(((LOGHDR *)logrec)->log_test_magic, SSTAB_SPLIT_MAGIC, 
						LOG_MAGIC_LEN);

		MEMCPY(logrec->logsplit.oldsstabname, oldsstab,SSTABLE_NAME_MAX_LEN);
	
		MEMCPY(logrec->logsplit.newsstabname, newsstab,SSTABLE_NAME_MAX_LEN);

		break;
	    case LOG_BLK_SPLIT:
	    	

		MEMCPY(((LOGHDR *)logrec)->log_test_magic, BLOCK_SPLIT_MAGIC,
						LOG_MAGIC_LEN);

		MEMCPY(logrec->logsplit.oldsstabname, oldsstab,SSTABLE_NAME_MAX_LEN);
	
		MEMCPY(logrec->logsplit.newsstabname, newsstab,SSTABLE_NAME_MAX_LEN);
	    	break;
		
	    case LOG_DATA_INSERT:
	    case LOG_DATA_DELETE:
	   
		MEMCPY(((LOGHDR *)logrec)->log_test_magic, INSDEL_LOG_MAGIC, 
						LOG_MAGIC_LEN);
		
		logrec->loginsdel.oldts = oldts;
		logrec->loginsdel.newts = newts;
		logrec->loginsdel.minrowlen = minrowlen;
		logrec->loginsdel.tabid = tabid;
		logrec->loginsdel.sstabid = sstabid;
		logrec->loginsdel.blockid = blockid;
		logrec->loginsdel.rnum = rnum;

		MEMCPY(logrec->loginsdel.sstabname, oldsstab,SSTABLE_NAME_MAX_LEN);
	    	break;

	     case LOG_INDEX_INSERT:
	     case LOG_INDEX_DELETE:
	     	MEMCPY(((LOGHDR *)logrec)->log_test_magic, INDEX_INSDEL_MAGIC, 
						LOG_MAGIC_LEN);
		
		logrec->loginsdel.oldts = oldts;
		logrec->loginsdel.newts = newts;
		logrec->loginsdel.minrowlen = minrowlen;
		logrec->loginsdel.tabid = tabid;
		logrec->loginsdel.sstabid = sstabid;
		logrec->loginsdel.blockid = blockid;
		logrec->loginsdel.rnum = rnum;

		MEMCPY(logrec->loginsdel.sstabname, oldsstab,SSTABLE_NAME_MAX_LEN);
	    	break;
		
	    case LOG_UPDRID:
		MEMCPY(((LOGHDR *)logrec)->log_test_magic, UPDRID_MAGIC, 
				LOG_MAGIC_LEN);
	    	logrec->logupdrid.oldts = oldts;
		logrec->logupdrid.newts = newts;
		logrec->logupdrid.minrowlen = minrowlen;
		logrec->logupdrid.idx_id = tabid;
		logrec->logupdrid.sstabid = sstabid;
		logrec->logupdrid.blockid = blockid;
		logrec->logupdrid.rnum = rnum;

		MEMCPY(logrec->logupdrid.sstabname, oldsstab,SSTABLE_NAME_MAX_LEN);
		MEMCPY((char *)&(logrec->logupdrid.oldrid), oldrid, sizeof(RID));
		MEMCPY((char *)&(logrec->logupdrid.newrid), newrid, sizeof(RID));
	    	break;
		
	    case CHECKPOINT_BEGIN:

		MEMCPY(((LOGHDR *)logrec)->log_test_magic, CHKPOINT_BEGLOG_MAGIC, 
						LOG_MAGIC_LEN);
		
	    	break;
		
	    case CHECKPOINT_COMMIT:

		MEMCPY(((LOGHDR *)logrec)->log_test_magic, CHKPOINT_COMMLOG_MAGIC, 
						LOG_MAGIC_LEN);
		
	    	break;
		
	    case LOG_SKIP:
	    	break;
		
	    default:
	    	break;
	}
	

	
}


int
log_put(LOGREC *logrec, char *rp, int rlen)
{

	LOCALTSS(tss);
	int		status;
	char		*logbuf;
	int		logbuflen;
	int		idx;


	status = 0;
	idx = 0;
	logbuflen = sizeof(LOGREC);

	if (rp)
	{
		Assert(   (((LOGHDR *)logrec)->opid == LOG_DATA_INSERT) 
		       || (((LOGHDR *)logrec)->opid == LOG_DATA_DELETE)
		       || (((LOGHDR *)logrec)->opid == LOG_INDEX_INSERT)
		       || (((LOGHDR *)logrec)->opid == LOG_INDEX_DELETE)
		      );

		Assert(rlen > 0);

		logbuflen += sizeof(int) + rlen;
	}
		
//	logbuf = (char *)MEMALLOCHEAP(logbuflen);
	BUF_GET_LOGBUF(logbuf);
	
	if (rp)
	{
		*(int *)logbuf = rlen;
		idx += sizeof(int);
			
		PUT_TO_BUFFER(logbuf, idx, rp, rlen);
	}
	
//	MEMCPY(logrec->logmagic, MT_LOG, LOG_MAGIC_LEN);

	((LOGHDR *)logrec)->loglen = logbuflen;

	PUT_TO_BUFFER(logbuf, idx, logrec, sizeof(LOGREC));

	Rg_loginfo->logoffset += logbuflen;

	if (Rg_loginfo->logoffset > LOG_FILE_SIZE)
	{
		CLOSE(Rg_loginfo->logfd);
		
		int idxpos = str1nstr(Rg_loginfo->logdir, tss->rglogfile, 
					STRLEN(Rg_loginfo->logdir));

		int logfilenum = m_atoi(Rg_loginfo->logdir+ idxpos, 
					STRLEN(Rg_loginfo->logdir) - idxpos);
		
		logfilenum++;

		MEMSET(Rg_loginfo->logdir, STRLEN(Rg_loginfo->logdir));
		sprintf(Rg_loginfo->logdir, "%s%d", tss->rglogfile, logfilenum);

		OPEN(Rg_loginfo->logfd, Rg_loginfo->logdir, 
				(O_CREAT | O_APPEND | O_RDWR |O_TRUNC));

		Rg_loginfo->logoffset = 0;
	}

	if (Rg_loginfo->logoffset > LOG_FILE_SIZE)
	{
		traceprint("The size of logfile (%d) is greater than the sstable size", Rg_loginfo->logoffset);
		goto exit;
	}

	if (   (((LOGHDR *)logrec)->opid == LOG_DATA_SSTAB_SPLIT)
	    || (((LOGHDR *)logrec)->opid == LOG_INDEX_SSTAB_SPLIT)
	    || (((LOGHDR *)logrec)->opid == LOG_BLK_SPLIT))
	{
		
		char		cmd_str[TABLE_NAME_MAX_LEN];
		LOGSPLIT	*logsplit;

		logsplit = (LOGSPLIT *)logrec;
		
		MEMSET(cmd_str, TABLE_NAME_MAX_LEN);
					
#ifdef MT_KFS_BACKEND
		int i = strmnstr(logrec->oldsstabname, "/", 
				STRLEN(logsplit->oldsstabname));
		sprintf(cmd_str, "%s/%s", tss->rgbackpfile, 
				logsplit->oldsstabname + i);

		if (COPYFILE(logsplit->oldsstabname,cmd_str) != 0)
#else			
		sprintf(cmd_str, "cp %s %s", logsplit->oldsstabname, 
				tss->rgbackpfile);
		
		if (system(cmd_str))
#endif
		{
			status = FALSE;
			goto exit;
		}
	}
	
#ifdef MT_KFS_BACKEND

	status = APPEND(Rg_loginfo->logfd, logbuf, logbuflen);

#else

	APPEND(Rg_loginfo->logfd, logbuf, logbuflen, status);
#endif

exit:
	BUF_RELEASE_LOGBUF(logbuf);

	return status;
}


int
log_undo_split(LOGREC	*logrec, char *rgip, int rgport)
{

	char	backup[TABLE_NAME_MAX_LEN];
	int	status;
	
	char	cmd_str[64];
	char	tmpsstab[TABLE_NAME_MAX_LEN];


	status = TRUE;
	
	MEMSET(cmd_str, 64);	

	MEMSET(tmpsstab, TABLE_NAME_MAX_LEN);

	log_get_rgbackup(backup, rgip, rgport);
	
	int i = strmnstr(logrec->logsplit.oldsstabname, "/", 
			STRLEN(logrec->logsplit.oldsstabname));

	MEMCPY(tmpsstab, logrec->logsplit.oldsstabname + i, 
			STRLEN(logrec->logsplit.oldsstabname + i));

	
	char	new_sstab[TABLE_NAME_MAX_LEN];

	MEMSET(new_sstab, TABLE_NAME_MAX_LEN);
	MEMCPY(new_sstab, logrec->logsplit.newsstabname,
			STRLEN(logrec->logsplit.newsstabname));

	char	srcfile[TABLE_NAME_MAX_LEN];

	MEMSET(srcfile, TABLE_NAME_MAX_LEN);
	sprintf(srcfile, "%s/%s", backup, tmpsstab);				


	char	tab_dir[TABLE_NAME_MAX_LEN];

	MEMSET(tab_dir, TABLE_NAME_MAX_LEN);
	MEMCPY(tab_dir, logrec->logsplit.oldsstabname, i);			
							
	if (STAT(tab_dir, &st) != 0)
	{
		status = FALSE;
		goto exit;
	}

#ifdef MT_KFS_BACKEND
	i = strmnstr(srcfile, "/", STRLEN(srcfile));
	sprintf(cmd_str, "%s", tab_dir);
	sprintf(cmd_str, "%s", srcfile + i);

	if (COPYFILE(srcfile, cmd_str) != 0)
	{
		status = FALSE;
		goto exit;
	}

	int	rtn_stat;

	RMFILE(rtn_stat,srcfile);

	if (rtn_stat < 0)


#else			
	sprintf(cmd_str, "cp %s %s", srcfile, tab_dir);

	if (system(cmd_str))
	{
		status = FALSE;
		goto exit;
	}

	sprintf(cmd_str, "rm %s", srcfile);

	if (system(cmd_str))
#endif
	{
		status = FALSE;
		goto exit;
	}

	if (((LOGHDR *)logrec)->opid == LOG_DATA_SSTAB_SPLIT)
	{
		
		char	rgstate[TABLE_NAME_MAX_LEN];

		ri_get_rgstate(rgstate, rgip, rgport);

		ri_rgstat_deldata(rgstate, new_sstab);
	}

exit:			
	return status;
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

int
log_get_latest_rglogfile(char *rginsdellogfile, char *rg_ip, int port)
{
	int	slen1;
	int	slen2;
	char	logdir[256];
	char	rgname[64];
	int	status;


	MEMSET(rginsdellogfile, 256);
	MEMSET(logdir, 256);

	
	MEMCPY(logdir, LOG_FILE_DIR, STRLEN(LOG_FILE_DIR));

	MEMSET(rgname, 64);
	sprintf(rgname, "%s%d", rg_ip, port);

	str1_to_str2(logdir, '/', rgname);
	
#ifdef MT_KFS_BACKEND
	
	MT_ENTRIES	mt_entries;

	MEMSET(&mt_entries, sizeof(MT_ENTRIES));

	if (!READDIR(logdir, (char *)&mt_entries))
	{
		traceprint("Read dir %s hit error.\n", logdir);
		return FALSE;
	}

	int i;

	for (i = 0; i < mt_entries.ent_num; i++)
	{
		if(strcmp(mt_entries.tabname[i],".")==0 || strcmp(mt_entries.tabname[i],"..")==0)
		{
			continue;
		}

		slen1 = STRLEN(mt_entries.tabname[i]);
		slen2 = STRLEN(rginsdellogfile);

		if (slen1 > slen2)
		{
			MEMCPY(rginsdellogfile, mt_entries.tabname[i], slen1);
		}
		else if (slen1 == slen2)
		{
			if (strcmp(mt_entries.tabname[i], rginsdellogfile) > 0)
			{
				MEMCPY(rginsdellogfile, mt_entries.tabname[i], slen1);
			}
		}	

		
		status = (slen2 == 0)? FALSE : TRUE;
	} 
	
#else
	DIR *pDir ;
	struct dirent *ent ;

	pDir=opendir(logdir);
	while((ent=readdir(pDir))!=NULL)
	{		
		if(strcmp(ent->d_name,".")==0 || strcmp(ent->d_name,"..")==0)
		{
			continue;
		}
		
		slen1 = STRLEN(ent->d_name);
		slen2 = STRLEN(rginsdellogfile);

		if (slen1 > slen2)
		{
			MEMCPY(rginsdellogfile, ent->d_name, slen1);
		}
		else if (slen1 == slen2)
		{
			if (strcmp(ent->d_name, rginsdellogfile) > 0)
			{
				MEMCPY(rginsdellogfile, ent->d_name, slen1);
			}
		}	

		
                status = (slen2 == 0)? FALSE : TRUE;
	}
#endif

	str1_to_str2(logdir, '/', rginsdellogfile);
	MEMCPY(rginsdellogfile, logdir, STRLEN(logdir));

	if (status == FALSE)
	{
		return FALSE;
	}
	
	return TRUE;
}


int
log_recovery(char *insdellogfile, char *rg_ip, int rg_port)
{
	int		status;
	char	 	*logfilebuf;
	int		offset;
	LOGREC		*logrec;
	int		recov_done;
	int		buf_spin;
	int		tmp;
	int		log_scope_start;
	LOG_RECOV	log_recov;
	char 		*log_redo_start;
	int		freeoff;
	char		*logfilename;
	char		prelogfile[TABLE_NAME_MAX_LEN];
	int		hit_empty_file;
	

	recov_done	= FALSE;
	buf_spin	= FALSE;

	log_scope_start = FALSE;
	
	status		= FALSE;
	

	logfilename	= insdellogfile;

	MEMSET(&log_recov, sizeof(LOG_RECOV));

	
pre_logfile:
	hit_empty_file	= FALSE;
	
	log_recov.log_buf[log_recov.buf_idxcur].buf= (char *)malloc(LOG_FILE_SIZE);

	logfilebuf = log_recov.log_buf[log_recov.buf_idxcur].buf;
	
	log_recov.buf_idxmax++;
	
	MEMSET(logfilebuf, LOG_FILE_SIZE);
	
	OPEN(log_recov.log_buf[log_recov.buf_idxcur].fd, logfilename, (O_RDWR));

	if (log_recov.log_buf[log_recov.buf_idxcur].fd < 0)
	{
		traceprint("logfile (%s) can not open.\n", logfilename);
		goto exit;
	}
	
	offset = READ(log_recov.log_buf[log_recov.buf_idxcur].fd, logfilebuf,
								LOG_FILE_SIZE);

	offset = log__get_last_logoffset((LOGREC *)logfilebuf);

	Assert(offset < LOG_FILE_SIZE);

	log_recov.log_buf[log_recov.buf_idxcur].freeoff = offset;
	
	tmp = offset;

	
	if (tmp == 0)
	{
		hit_empty_file = TRUE;
	}
	
	
	while(tmp > 0)
	{
		
		logrec = (LOGREC *)(logfilebuf + tmp - sizeof(LOGREC));

		if (((LOGHDR *)logrec)->opid == CHECKPOINT_COMMIT)
		{
			Assert(log_scope_start == FALSE);

			log_scope_start = TRUE;

			tmp -= ((LOGHDR *)logrec)->loglen;
			continue;
		}

		if (   (((LOGHDR *)logrec)->opid == CHECKPOINT_BEGIN) 
		    && (log_scope_start))
		{
			status = TRUE;
			break;
		}

		
		tmp -= ((LOGHDR *)logrec)->loglen;

	}

	

	if (!status)
	{
		if (log_recov.buf_idxmax == LOG_BUF_RESERV_MAX)
		{
			traceprint("We should expand the size of log file.\n");
			Assert(0);
		}

		if ((hit_empty_file) && (log_recov.buf_idxcur == 0))
		{
			traceprint("No log will be recovery.\n");
			status = TRUE;
			goto exit;
		}
		
		int idxpos = str1nstr(logfilename, "/log\0", STRLEN(logfilename));
	
		int logfilenum = m_atoi(logfilename + idxpos, 
					STRLEN(logfilename) - idxpos);

		
		if (logfilenum > 0)
		{				
			logfilenum--;

			MEMSET(prelogfile, STRLEN(prelogfile));
			MEMCPY(prelogfile, logfilename, idxpos);
			sprintf(prelogfile + idxpos, "%d", logfilenum);

			logfilename = prelogfile;

			log_recov.buf_idxcur++;
		
			goto pre_logfile;
		}
		else
		{
			
			Assert (logfilenum == 0);

			status = TRUE;
		}
	}

	log_recov.redo_start_buf = log_recov.buf_idxcur;
	log_recov.redo_start_off = (char *)logrec;

	int		log_beg_hit = FALSE;	

	
	P_SPINLOCK(BUF_SPIN);
	buf_spin = TRUE;

	

	log_recov.buf_idxcur = log_recov.redo_start_buf;
	
	while((log_recov.buf_idxcur + 1) > 0)
	{
		freeoff = 
			log_recov.log_buf[log_recov.buf_idxcur].freeoff;
			
		logfilebuf = 
			log_recov.log_buf[log_recov.buf_idxcur].buf;

		if (log_recov.buf_idxcur == log_recov.redo_start_buf)
		{
			log_redo_start = log_recov.redo_start_off;
		}
		else if (log_recov.buf_idxcur < log_recov.redo_start_buf)
		{
			log_redo_start = 
			 log_recov.log_buf[log_recov.buf_idxcur].buf;
		}
		else
		{
			Assert(0);
		}
	
		logrec = (LOGREC *)log__recov_redo(logfilebuf, freeoff,
							log_redo_start);

		if ((char *)logrec < (logfilebuf + freeoff))
		{
			log_beg_hit = TRUE;
			break;
		}

		if(log_recov.buf_idxcur == 0)
		{
			break;
		}

		log_recov.buf_idxcur--;
	}


	
	
	int	undo_need;
	char	*log_undo_end;	


	if (log_beg_hit)
	{
		Assert(((LOGHDR *)logrec)->opid == LOG_BEGIN);
		
		if (logrec->logbeg.begincase == LOGBEG_INDEX_CASE)
		{
			
			log__find_undoend((char *)logrec, TRUE,	&log_recov);
		}
		else
		{
			
			log_recov.undo_end_off = (char *)logrec;
			log_recov.undo_end_buf = log_recov.buf_idxcur;
		}

		undo_need = TRUE;
	}
	else
	{
		
		log_recov.buf_idxcur = 0;
		
		logfilebuf = log_recov.log_buf[log_recov.buf_idxcur].buf;
		freeoff = log_recov.log_buf[log_recov.buf_idxcur].freeoff;
		
		log__find_undoend((logfilebuf + freeoff - sizeof(LOGREC)),
					FALSE, &log_recov);

		undo_need = (log_recov.undo_end_off == (logfilebuf + freeoff))
				? FALSE :TRUE;
	}

		
	
	if (undo_need)
	{		
		
		log_recov.buf_idxcur = 0;

		while(log_recov.buf_idxcur < (log_recov.undo_end_buf + 1))
		{			
			freeoff = 
				log_recov.log_buf[log_recov.buf_idxcur].freeoff;
			
			logfilebuf = 
				log_recov.log_buf[log_recov.buf_idxcur].buf;

			if (log_recov.buf_idxcur == log_recov.undo_end_buf)
			{
				log_undo_end = log_recov.undo_end_off;
			}
			else if (log_recov.buf_idxcur < log_recov.undo_end_buf)
			{
				log_undo_end = 
				 log_recov.log_buf[log_recov.buf_idxcur].buf;
			}
			else
			{
				Assert(0);
			}

			log__recov_undo(logfilebuf, freeoff, log_undo_end, 
					rg_ip, rg_port);

			log_recov.buf_idxcur++;
		}
	}	

	recov_done = TRUE;

exit:

	
	log_recov.buf_idxcur = 0;
	
	while (log_recov.buf_idxcur < log_recov.buf_idxmax)
	{
		CLOSE(log_recov.log_buf[log_recov.buf_idxcur].fd);	

		free(log_recov.log_buf[log_recov.buf_idxcur].buf);

		log_recov.buf_idxcur++;
	}
	
	if (recov_done)
	{
		LOGREC		log_recov_done;

		hkgc_wash_sstab(TRUE);
		
		log_build(&log_recov_done, CHECKPOINT_BEGIN, 0, 0, NULL, NULL,
					0, 0, 0, 0, 0, NULL, NULL);
		
		log_put(&log_recov_done, NULL, 0);

		log_build(&log_recov_done, CHECKPOINT_COMMIT, 0, 0, NULL, NULL,
					0, 0, 0, 0, 0, NULL, NULL);
		
		log_put(&log_recov_done, NULL, 0);
	}

	if (buf_spin)
	{
		V_SPINLOCK(BUF_SPIN);
	}
	
	return status;

}


static void
log__recov_undo(char *logfilebuf, int freeoff, char *log_undo_end, char *rg_ip,
			int rg_port)
{
	int		tmp;
	char		*rp;
	int		rlen;
	LOGREC		*logrec;


	tmp = freeoff;

	Assert(   ((log_undo_end - logfilebuf) > 0)
	       && ((log_undo_end - logfilebuf) < tmp));
	
	while((logfilebuf + tmp) > log_undo_end)
	{			
		logrec = (LOGREC *)(logfilebuf + tmp - sizeof(LOGREC));

		Assert(strncasecmp(((LOGHDR *)logrec)->logmagic, MT_LOG,
						STRLEN(MT_LOG)) == 0);

		switch (((LOGHDR *)logrec)->opid)
		{
		    case LOG_DATA_SSTAB_SPLIT:

			
			undo_split(logrec, rg_ip, rg_port);
			break;

		    case LOG_BLK_SPLIT:

			
			undo_split(logrec, rg_ip, rg_port);
			break;
			
		    case LOG_INDEX_SSTAB_SPLIT:

			
			undo_split(logrec, rg_ip, rg_port);

		    	break;
			
		    case LOG_UPDRID:

			
			undo_updrid(logrec);

			
		    	break;

		    case LOG_INDEX_INSERT:
		    case LOG_INDEX_DELETE:

			rp = (char *)logrec + sizeof(LOGREC) -
					((LOGHDR *)logrec)->loglen;
			rlen = *(int *)rp;
			rp += sizeof(int);

			undo_index_insdel(logrec, rp);
			
		    	break;
			
		    case LOG_DATA_INSERT:
		    case LOG_DATA_DELETE:

		 	rp = (char *)logrec + sizeof(LOGREC) -
					((LOGHDR *)logrec)->loglen;
			rlen = *(int *)rp;
			rp += sizeof(int);

		    	undo_data_insdel(logrec, rp);

			break;
			
		    case LOG_BEGIN:
		    case LOG_END:
		    	break;
			
		    case CHECKPOINT_BEGIN:
		    case LOG_SKIP:
		    	break;
			
		    default:
		    	traceprint("Hit error log type(%d).\n", ((LOGHDR *)logrec)->opid);
			Assert(0);
		    	break;
		}


		tmp -= ((LOGHDR *)logrec)->loglen;
		
	}

	return;
}

static char *
log__recov_redo(char *logfilebuf, int freeoff, char *log_redo_start)
{
	int		jmp_undo = FALSE;
	LOGREC		*logrec;
	char		*rp;
	int		rlen;
	char		*log_redo_end;


	logrec = (LOGREC *)log_redo_start;
	log_redo_end = logfilebuf + freeoff;

	
	while ((char *)logrec < log_redo_end)
	{	
		if (strncasecmp(((LOGHDR *)logrec)->logmagic, MT_LOG, 
						STRLEN(MT_LOG)) != 0)
		{
			rlen = *(int *)logrec;
			rp = (char *)logrec + sizeof(int);
			logrec = (LOGREC *)(rp + rlen);
		}

		switch (((LOGHDR *)logrec)->opid)
		{
		    case CHECKPOINT_BEGIN:
		    case CHECKPOINT_COMMIT:
		    	
		    	Assert(((LOGHDR *)logrec)->loglen == sizeof(LOGREC));
			logrec = (LOGREC *)((char *)logrec + sizeof(LOGREC));
		    	break;

		    case LOG_SKIP:
		    	logrec = (LOGREC *)((char *)logrec + sizeof(LOGREC));
		    	break;

		    case LOG_DATA_INSERT:
		    case LOG_DATA_DELETE:

			rp = (char *)logrec + sizeof(LOGREC) -
					((LOGHDR *)logrec)->loglen;
			rlen = *(int *)rp;
			rp += sizeof(int);

			Assert(strncasecmp(((LOGHDR *)logrec)->logmagic, MT_LOG,
						STRLEN(MT_LOG)) == 0);

			redo_data_insdel(logrec, rp);

			logrec = (LOGREC *)((char *)logrec + sizeof(LOGREC));
		    	break;
			
		    case LOG_INDEX_INSERT:
		    case LOG_INDEX_DELETE:

		    	rp = (char *)logrec + sizeof(LOGREC) -
					((LOGHDR *)logrec)->loglen;
			rlen = *(int *)rp;
			rp += sizeof(int);

			Assert(strncasecmp(((LOGHDR *)logrec)->logmagic, MT_LOG, 
						STRLEN(MT_LOG)) == 0);

			redo_index_insdel(logrec, rp);

			logrec = (LOGREC *)((char *)logrec + sizeof(LOGREC));
		    	break;

		    case LOG_BEGIN:
		    	jmp_undo = TRUE;
		    	break;

		    default:
		    	
		    	traceprint("The log recovery hit error.\n");
			Assert(0);
		    	break;
		}

		if (jmp_undo) 
		{
			break;
		}
	}


	return (char *)logrec;
}




static void
log__find_undoend(char *log_off, int log_beg_hit, LOG_RECOV *log_recov)
{
	char		*undoend;
	LOGREC		*logrec;


	undoend		= log_off;
	logrec		= (LOGREC *)log_off;

	if (log_beg_hit)
	{
		
		Assert(   (((LOGHDR *)logrec)->opid == LOG_BEGIN) 
		       && (logrec->logbeg.begincase == LOGBEG_INDEX_CASE));

		while(undoend > log_recov->log_buf[log_recov->buf_idxcur].buf)
		{
			undoend -= ((LOGHDR *)logrec)->loglen;

			
			logrec = (LOGREC *)undoend;

			if (   (((LOGHDR *)logrec)->opid == LOG_DATA_INSERT)
			    || (((LOGHDR *)logrec)->opid == LOG_DATA_DELETE))
			{
				
				break;
			}
			else
			{
				Assert(   (((LOGHDR *)logrec)->opid 
				        	== LOG_INDEX_INSERT)
				       || (((LOGHDR *)logrec)->opid 
				        	== LOG_INDEX_DELETE));				
			}

			if (  (undoend - ((LOGHDR *)logrec)->loglen)
			    < log_recov->log_buf[log_recov->buf_idxcur].buf)
			{
				
				log_recov->buf_idxcur++;
				
				Assert(log_recov->buf_idxcur < log_recov->buf_idxmax);

				undoend = log_recov->log_buf[log_recov->buf_idxcur].buf 
				    + log_recov->log_buf[log_recov->buf_idxcur].freeoff
				    - sizeof(LOGREC);

				logrec = (LOGREC *)undoend;
			}
		}
	}
	else
	{
		Assert(log_recov->buf_idxcur == 0);
		
		

		switch (((LOGHDR *)logrec)->opid)
		{
		    case LOG_BEGIN:
		    case LOG_END:
		    case CHECKPOINT_BEGIN:
		    case CHECKPOINT_COMMIT:
		    case LOG_SKIP:

			Assert(((LOGHDR *)logrec)->loglen == sizeof(LOGREC));
			
		    	
		    	logrec = (LOGREC *)(undoend + ((LOGHDR *)logrec)->loglen);
			
		    	break;

		    case LOG_DATA_INSERT:
		    	
			if (((LOGHDR *)logrec)->status & LOG_NOT_UNDO)
			{
				
				while(undoend > log_recov->log_buf[log_recov->buf_idxcur].buf)
				{
					undoend -= ((LOGHDR *)logrec)->loglen;
					
					logrec = (LOGREC *)undoend;

					if (((LOGHDR *)logrec)->opid == LOG_DATA_DELETE)
					{
						
						Assert(((LOGHDR *)logrec)->status & LOG_NOT_REDO);
						
						break;
					}
					else
					{
						Assert(   ((LOGHDR *)logrec)->opid 
						        == LOG_INDEX_DELETE);
						
					}

					if (  (undoend - ((LOGHDR *)logrec)->loglen) 
					    < log_recov->log_buf[log_recov->buf_idxcur].buf)
					{
						log_recov->buf_idxcur++;
						
						Assert(log_recov->buf_idxcur < log_recov->buf_idxmax);

						undoend = log_recov->log_buf[log_recov->buf_idxcur].buf 
						    + log_recov->log_buf[log_recov->buf_idxcur].freeoff
						    - sizeof(LOGREC);

						logrec = (LOGREC *)undoend;
					}
				}
			}
			
		    	break;

		    case LOG_DATA_DELETE:
		    	break;
			
		    case LOG_INDEX_INSERT:

			
			while(undoend > log_recov->log_buf[log_recov->buf_idxcur].buf)
			{
				undoend -= ((LOGHDR *)logrec)->loglen;
				
				logrec = (LOGREC *)undoend;

				if (((LOGHDR *)logrec)->opid == 
							LOG_DATA_INSERT)
				{
					
					break;
				}
				else
				{
					Assert(((LOGHDR *)logrec)->opid 
				        		== LOG_INDEX_INSERT);					
				}

				if (  (undoend - ((LOGHDR *)logrec)->loglen)
				    < log_recov->log_buf[log_recov->buf_idxcur].buf)
				{
					log_recov->buf_idxcur++;
					
					Assert(log_recov->buf_idxcur < log_recov->buf_idxmax);

					undoend = log_recov->log_buf[log_recov->buf_idxcur].buf 
					    + log_recov->log_buf[log_recov->buf_idxcur].freeoff
					    - sizeof(LOGREC);

					logrec = (LOGREC *)undoend;
				}
			}

			break;
			
		    case LOG_INDEX_DELETE:
		    	
			while(undoend > log_recov->log_buf[log_recov->buf_idxcur].buf)
			{
				undoend -= ((LOGHDR *)logrec)->loglen;
				
				logrec = (LOGREC *)undoend;

				if (((LOGHDR *)logrec)->opid == LOG_DATA_DELETE)
				{
					
					break;
				}
				else
				{
					Assert(   ((LOGHDR *)logrec)->opid 
					        == LOG_INDEX_DELETE);
					
				}

				if (  (undoend - ((LOGHDR *)logrec)->loglen) 
				    < log_recov->log_buf[log_recov->buf_idxcur].buf)
				{
					log_recov->buf_idxcur++;
					
					Assert(log_recov->buf_idxcur < log_recov->buf_idxmax);

					undoend = log_recov->log_buf[log_recov->buf_idxcur].buf 
					    + log_recov->log_buf[log_recov->buf_idxcur].freeoff
					    - sizeof(LOGREC);

					logrec = (LOGREC *)undoend;
				}
			}
		    	break;
			
		    default:
		    	traceprint("Log opid (%d) error!\n", ((LOGHDR *)logrec)->opid);
		    	break;
		}
	}

	log_recov->undo_end_buf = log_recov->buf_idxcur;
	log_recov->undo_end_off = (char *)logrec;

	return;
}

int
log_recov_rg(char *rgip, int rgport)
{
	char	logfile[TABLE_NAME_MAX_LEN];

//	log_get_sstab_split_logfile(logfile, rgip, rgport);
//	log_get_rgbackup(backup, rgip, rgport);

//	log_undo_sstab_split(logfile, backup, SPLIT_LOG, rgip, rgport);

	if (!log_get_latest_rglogfile(logfile, rgip, rgport))
	{
		return TRUE;
	}

	log_recovery(logfile, rgip, rgport);

	return TRUE;
}

static int
log__get_last_logoffset(LOGREC *logrec)
{
	int	rlen;
	char	*rp;
	int	logoffset;
	char	*tmp;


	logoffset = 0;
	tmp = (char *)logrec + LOG_FILE_SIZE;
	
	while (logoffset < LOG_FILE_SIZE)
	{	
		if (strncasecmp(((LOGHDR *)logrec)->logmagic, MT_LOG, 
						STRLEN(MT_LOG)) != 0)
		{
			
			rlen = *(int *)logrec;
			
			rp = (char *)logrec + sizeof(int);
			logrec = (LOGREC *)(rp + rlen);

			if ((char *)logrec > tmp)
			{
				
				break;
			}
		}
		
		if (   (((LOGHDR *)logrec)->opid == CHECKPOINT_BEGIN) 
		    || (((LOGHDR *)logrec)->opid == CHECKPOINT_COMMIT)
		   )
		{
			Assert(((LOGHDR *)logrec)->loglen == sizeof(LOGREC));

			logoffset += ((LOGHDR *)logrec)->loglen;

			//traceprint("logoffset -- %d, logrec->opid  -- %d\n", logoffset, logrec->opid );
			
			logrec = (LOGREC *)((char *)logrec + sizeof(LOGREC));
			
			continue;
		}
		else if(((LOGHDR *)logrec)->opid == LOG_SKIP)
		{
			logoffset += ((LOGHDR *)logrec)->loglen;

			//traceprint("logoffset -- %d, logrec->opid  -- %d\n", logoffset, logrec->opid );
			
			logrec = (LOGREC *)((char *)logrec + sizeof(LOGREC));
			continue;
		}

		
		if(strncasecmp(((LOGHDR *)logrec)->logmagic, MT_LOG, 
						STRLEN(MT_LOG)) == 0)
		{
			logoffset += ((LOGHDR *)logrec)->loglen;
			//traceprint("logoffset -- %d, logrec->opid  -- %d\n", logoffset, logrec->opid );
		}
		else
		{
			break;
		}
		

		logrec = (LOGREC *)((char *)logrec + sizeof(LOGREC));
	}

	return logoffset;
}

