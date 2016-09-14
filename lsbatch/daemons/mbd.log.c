/*
 * Copyright (C) 2015 - 2016 David Bigagli
 * Copyright (C) 2007 Platform Computing Inc
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA  02110-1301, USA
 *
 */

#include "mbd.h"

#define NL_SETN         10

#define SAFE_JID_GAP 100
#define NULL_RES     "NRR"
#define NULL_HOST    "NULL_HOST"
#define SKIPSPACE(sp)      while (isspace(*(sp))) (sp)++;

time_t eventTime;

extern bool_t          logMapFileEnable;

extern int              sigNameToValue_(char *sigString);
extern char            *getLsbSigSymbol(int);
extern char            *getSigSymbol(int);
extern void             chanFreeStashedBuf(struct Buffer *);
extern int              chanAllocBuf_(struct Buffer **, int);
static int              replay_event(char *, int);
static struct jData *   checkJobInCore(LS_LONG_INT);

static int              replay_newjob(char *, int);
static int              replay_startjob(char *, int, int);
static int              replay_executejob(char *, int);
static int              replay_startjobaccept(char *, int);
static int              replay_newstat(char *, int);
static int              replay_switchjob(char *, int);
static int              replay_movejob(char *, int);
static int              replay_qc(char *, int);
static int              replay_hostcontrol(char *, int);
static int              replay_mbdDie(char *, int);
static int              replay_unfulfill(char *, int);
static int              replay_loadIndex(char *, int);
static int              replay_chkpnt(char *, int);
static int              replay_mig(char *, int);
static int              replay_mbdStart(char *, int);
static int              replay_modifyjob(char *, int);
static int              replay_modifyjob2(char *, int);

static struct jData    *replay_jobdata(char *, int, char *);
static int              replay_signaljob(char *, int);
static int              replay_jobsigact(char *, int);
static int              replay_jobrequeue(char *, int);
static int              replay_cleanjob(char *, int);
static bool_t           replay_jobforce(char *, int);
static int              replay_logSwitch(char *, int);
static int              replay_jobattrset(char *, int );
static int              replay_jobmsg(char *, int);
static int              replay_newjgrp(char *, int);
static int              replay_deljgrp(char *, int);
static int              replay_modjgrp(char *, int);
static int              replay_jobpreempting(char *, int);
static int              replay_jobpreempted(char *, int);
static int replay_logSwitch(char *, int);
extern bool_t memberOfVacateList(struct lsQueueEntry *, struct lsQueue *);

static void             logFinishedjob(struct jData *);
static void             log_loadIndex(void);
static void             ckSchedHost (void);
static int              checkJobStarter(char *, char *);

static FILE            *log_fp;
static FILE            *joblog_fp;
static int              openEventFile(const char *);
static int              openEventFile2(const char *);
static int              putEventRec(const char *);
static int              putEventRecTime(const char *, time_t);
static int              putEventRec1(const char *);
static int              log_jobdata(struct jData *, const char *, int);
static int              createEvent0File(void);
static int              renameElogFiles(void);
static int              createAcct0File(void);

void                    log_timeExpired(int, time_t);
static int canSwitch(struct eventRec *, struct jData *);
static char *instrJobStarter1(char *, int, char *, char *, char *);
static int streamEvent(struct eventRec *);
static int countStream(char *);
static int do_switch_child_end(void);

int                     nextJobId_t = 1;
time_t                  dieTime;
static char             elogFname[MAXFILENAMELEN];
static char             jlogFname[MAXFILENAMELEN];
static char             elogFname2[MAXFILENAMELEN];

/* Global for all events.
 */
static struct eventRec *logPtr;

static int              logLoadIndex = TRUE;


extern int sigNameToValue_( char *);

extern float version;

struct hostCtrlEvent {
    time_t                time;
    struct hostCtrlLog    *hostCtrlLog;
};

static hTab    hostCtrlEvents;

static void    initHostCtrlTable(void);
static void    destroyHostCtrlTable(void);
static void    destroyHostCtrlEvent(void *);
static void    saveHostCtrlEvent(struct hostCtrlLog *, const time_t);
static void    writeHostCtrlEvent(void);
static void    log_hostStatusAtSwitch(const struct hostCtrlEvent *);

struct queueCtrlEvent {
    time_t                 time;
    struct queueCtrlLog    *queueCtrlLog;
};

static hTab    closedQueueEvents;
static hTab    inactiveQueueEvents;

static void    initQueueCtrlTable(void);
static void    destroyQueueCtrlTable(void);
static void    destroyQueueCtrlEvent(void *);
static void    saveQueueCtrlEvent(struct queueCtrlLog *, const time_t);
static void    queueTableUpdate(hTab *, struct queueCtrlLog *);
static void    writeQueueCtrlEvent(void);
static void    log_queueStatusAtSwitch(const struct queueCtrlEvent *);

extern time_t lsb_getAcctFileTime(char *);

static char    *getSignalSymbol(const struct signalReq *);
static int     replay_arrayrequeue(struct jData *,
                                   const struct signalLog *);
static int renameAcctLogFiles(int);
static void jobMsgInit(void);
static int checkDirAccess(const char *);

/* Global data structure with information
 * about child switching the lsb.events
 */
struct switch_child *swchild;
static int do_switch(void);
static int make_switch_pidfile(void);
static int rm_switch_pidfile(void);
static void wait_for_switch_child(void);
static int merge_switch_file(void);

/* Functions related to job messages
 */
static FILE *open_job_msg_file(const char *);
static int put_job_msg(FILE *, const char *);

/* Functions related to preemption
 */
static void log_job_preempting(struct jData *);
static void log_job_preempted(struct jData *);

/* init_log()
 */
int
init_log(void)
{
    char first = TRUE;
    int ConfigError = 0;
    int lineNum;
    int list;
    struct jData *jp;
    char dirbuf[MAXPATHLEN];
    char infoDir[MAXPATHLEN];
    struct stat sbuf;

    mSchedStage = M_STAGE_REPLAY;

    sprintf(elogFname, "%s/logdir/lsb.events",
            daemonParams[LSB_SHAREDIR].paramValue);

    sprintf(jlogFname, "%s/logdir/lsb.acct",
            daemonParams[LSB_SHAREDIR].paramValue);

    sprintf(elogFname2, "%s/logdir/lsb.events.parent",
            daemonParams[LSB_SHAREDIR].paramValue);

    sprintf(dirbuf, "%s/logdir", daemonParams[LSB_SHAREDIR].paramValue);

    if (stat(dirbuf, &sbuf) < 0) {

        ls_syslog(LOG_ERR, "\
%s: stat() on %s failed: %m", __func__, dirbuf);
        if (!lsb_CheckMode) {
            mbdDie(MASTER_FATAL);
        }
        ConfigError = -1;
    }

    if (sbuf.st_uid != managerId) {
        ls_syslog(LOG_ERR, "\
%s: Log directory %s not owned by LSF administrator: %s/%d (directory owner ID is %d)",
                  __func__,
                  dirbuf,
                  lsbManager,
                  managerId,
                  (int)sbuf.st_uid);
        if (debug == 0) {
            mbdDie(MASTER_FATAL);
        }

        ConfigError = -1;
    }

    if (sbuf.st_mode & 02) {

        ls_syslog(LOG_ERR, "\
%s: Mode <%03o> not allowed for job log directory <%s>; the permission bits for others should be 05",
                  __func__,
                  (int)sbuf.st_mode,
                  dirbuf);
        if (debug == 0) {
            mbdDie(MASTER_FATAL);
        }

        ConfigError = -1;
    }

    getElogLock();

    sprintf(infoDir, "%s/logdir/info",
            daemonParams[LSB_SHAREDIR].paramValue);

    if (mkdir(infoDir, 0700) == -1 && errno != EEXIST) {
        ls_syslog(LOG_ERR, "%s: mkdir() %s failed: %m", __func__, infoDir);
        mbdDie(MASTER_FATAL);
    }

    /* Create the stream directory.
     */
    sprintf(infoDir, "%s/logdir/stream",
            daemonParams[LSB_SHAREDIR].paramValue);

    if (mkdir(infoDir, 0700) == -1 && errno != EEXIST) {
        ls_syslog(LOG_ERR, "%s: mkdir() %s failed: %m", __func__, infoDir);
        mbdDie(MASTER_FATAL);
    }

    wait_for_switch_child();

    if ((joblog_fp = fopen(jlogFname, "a")) == NULL) {
        ls_syslog(LOG_ERR, "\
%s: failed to fopen() %s %m", __func__, jlogFname);
    }

    /* Open for reading and appending (writing at end of file).
     * The file is  created  if it  does  not exist.  The initial
     * file position for reading is at the beginning of the file,
     * but output is always appended to the end of the file.
     */
    log_fp = fopen(elogFname, "a+");
    if (log_fp == NULL) {
        ls_syslog(LOG_ERR, "\
%s: cannot fopen() the %s file; %m", __func__, elogFname);
        mbdDie(MASTER_FATAL);
    }

    /* Loop thru the whole lsb.events replaying all EVENTs in it.
     * First get the record, then replay it.
     * The lsb.event file record is a static global pointer allocated
     * inside the library.
     * If  the lsb_geteventrec() returns an error different from
     * EOF try the next record.
     */
    lsberrno = LSBE_NO_ERROR;
    lineNum = 0;
    while (lsberrno != LSBE_EOF) {

        /* Use single purpose reading getc2() routine.
         */
        if ((logPtr = lsb_geteventrecord(log_fp, &lineNum)) == NULL) {

            if (lsberrno != LSBE_EOF) {
                ls_syslog(LOG_ERR, "\
%s: Reading event file <%s> at line <%d>: %s",
                          __func__,
                          elogFname,
                          lineNum,
                          lsb_sysmsg());
                first = FALSE;
                if (lsberrno == LSBE_NO_MEM) {
                    mbdDie(MASTER_MEM);
                }

            } else {
                /* keep log_fp always open
                 */
                reset_getc2();
            }
            continue;
        }

        eventTime = logPtr->eventTime;
        if (! replay_event(elogFname, lineNum) && first) {
            ls_syslog(LOG_ERR, "\
%s: File %s at line %d: First replay_event() failed; line ignored",
                      __func__,
                      elogFname,
                      lineNum);
            first = FALSE;
        }

    } /* while (lsberrno != LSBE_EOF) */

    /* keep log_fp always open
     */
    reset_getc2();

    if (qsort_jobs) {
        sort_job_list(PJL);
    }

    for (list = SJL; list <= PJL; list++) {

        for (jp = jDataList[list]->back;
             jp != jDataList[list];
             jp = jp->back) {
            int svJStatus = jp->jStatus;
            int i;
            int num;

            num = jp->shared->jobBill.maxNumProcessors;

            jp->jStatus = JOB_STAT_PEND;

            updQaccount(jp, num, num, 0, 0, 0, 0);
            updUserData(jp, num, num, 0, 0, 0, 0);
            updLimitSlotData(jp, num, num, 0, 0, 0, 0);
            updLimitJobData(jp, 1, 1, 0, 0, 0, 0);

            jp->jStatus = svJStatus;
            if (jp->jStatus & JOB_STAT_PEND)
                continue;

            updCounters(jp, JOB_STAT_PEND, !LOG_IT);

            if ((jp->shared->jobBill.options & SUB_EXCLUSIVE)
                && IS_START (jp->jStatus)) {
                for (i = 0; i < jp->numHostPtr; i ++)
                    jp->hPtr[i]->hStatus |= HOST_STAT_EXCLUSIVE;
            }
        }
    }

    if (logclass & LC_JGRP)
        printTreeStruct(treeFile);

    if (nextJobId == 1) {
        nextJobId = nextJobId_t + SAFE_JID_GAP;
        if (nextJobId >= maxJobId)
            nextJobId = 1;
    }


    /* Don't switch here, let the
     * child mbatchd to do it when necessary.
     */
    if (logLoadIndex)
        log_loadIndex();

    ckSchedHost();

    mSchedStage = 0;

    checkAcctLog();

    jobMsgInit();

    return ConfigError;
}

static int
replay_event(char *filename, int lineNum)
{
    switch (logPtr->type) {
        case EVENT_JOB_NEW:
            return (replay_newjob(filename, lineNum));
        case EVENT_PRE_EXEC_START:
            return (replay_startjob(filename, lineNum, TRUE));
        case EVENT_JOB_START:
            return (replay_startjob(filename, lineNum, FALSE));
        case EVENT_JOB_STATUS:
            return (replay_newstat(filename, lineNum));
        case EVENT_JOB_SWITCH:
            return (replay_switchjob(filename, lineNum));
        case EVENT_JOB_MOVE:
            return (replay_movejob(filename, lineNum));
        case EVENT_QUEUE_CTRL:
            return (replay_qc(filename, lineNum));
        case EVENT_HOST_CTRL:
            return (replay_hostcontrol(filename, lineNum));
        case EVENT_MBD_UNFULFILL:
            return (replay_unfulfill(filename, lineNum));
        case EVENT_MBD_START:
            return (replay_mbdStart(filename, lineNum));
        case EVENT_MBD_DIE:
            return (replay_mbdDie(filename, lineNum));
        case EVENT_LOAD_INDEX:
            return (replay_loadIndex(filename, lineNum));
        case EVENT_CHKPNT:
            return (replay_chkpnt(filename, lineNum));
        case EVENT_MIG:
            return (replay_mig(filename, lineNum));
        case EVENT_JOB_MODIFY:
            return (replay_modifyjob(filename, lineNum));
        case EVENT_JOB_MODIFY2:
            return (replay_modifyjob2(filename, lineNum));
        case EVENT_JOB_ATTR_SET:
            return (replay_jobattrset(filename, lineNum));
        case EVENT_JOB_SIGNAL:
            return (replay_signaljob(filename, lineNum));
        case EVENT_JOB_EXECUTE:
            return (replay_executejob(filename, lineNum));
        case EVENT_JOB_START_ACCEPT:
            return (replay_startjobaccept(filename, lineNum));
        case EVENT_JOB_SIGACT:
            return (replay_jobsigact(filename, lineNum));
        case EVENT_JOB_REQUEUE:
            return (replay_jobrequeue(filename, lineNum));
        case EVENT_JOB_CLEAN:
            return (replay_cleanjob(filename, lineNum));
        case EVENT_JOB_FORCE:
            return (replay_jobforce(filename, lineNum));
        case EVENT_LOG_SWITCH:
            return (replay_logSwitch(filename, lineNum));
        case EVENT_JOB_MSG:
            return replay_jobmsg(filename, lineNum);
        case EVENT_NEW_JGRP:
            return replay_newjgrp(filename, lineNum);
        case EVENT_DEL_JGRP:
            return replay_deljgrp(filename, lineNum);
        case EVENT_MOD_JGRP:
            return replay_modjgrp(filename, lineNum);
        case EVENT_JOB_PREEMPTING:
            return replay_jobpreempting(filename, lineNum);
        case EVENT_JOB_PREEMPTED:
            return replay_jobpreempted(filename, lineNum);
        default:
            ls_syslog(LOG_ERR, "\
%s: File %s at line %d: Invalid event_type %c",
                      __func__, filename, lineNum, logPtr->type);
            return FALSE;
    }
}

static int
replay_newjob(char *filename, int lineNum)
{
    static char      fname[] = "replay_newjob";
    struct jData     *job;
    struct idxList   *idxList;
    int              error;
    int              maxJLimit = 0;

    job = replay_jobdata(filename, lineNum, fname);

    if ((job->shared->jobBill.options & SUB_RESTART) ||
        (idxList = parseJobArrayIndex(job->shared->jobBill.jobName,
                                      &error,
                                      &maxJLimit)) == NULL) {
        handleNewJob(job, JOB_REPLAY, logPtr->eventTime);
    } else {
        handleNewJobArray(job, idxList, maxJLimit);
        freeIdxList(idxList);
    }

    nextJobId_t = (int)job->jobId + 1;

    if (nextJobId_t >= maxJobId)
        nextJobId_t = 1;

    return TRUE;
}

static int
replay_switchjob(char *filename, int lineNum)
{
    static char             fname[] = "replay_switchjob";
    struct jobSwitchReq     switchReq;
    struct qData           *qfp, *qtp;
    struct jData           *jp;

    switchReq.jobId = LSB_JOBID(logPtr->eventLog.jobSwitchLog.jobId,
                                logPtr->eventLog.jobSwitchLog.idx);
    strcpy(switchReq.queue, logPtr->eventLog.jobSwitchLog.queue);


    if ((jp = getJobData(switchReq.jobId)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6709,
                                         "%s: File %s at line %d: Job <%s> not found in job list "), /* catgets 6709 */
                  fname, filename, lineNum, lsb_jobid2str(switchReq.jobId));
        return false;
    }
    qfp = jp->qPtr;


    qtp = getQueueData(switchReq.queue);
    if (qtp == NULL) {
        ls_syslog(LOG_DEBUG, "replay_switchjob: File %s at line %d: Destination queue <%s> not found, switching job <%s> to queue <%s>", filename, lineNum, switchReq.queue, lsb_jobid2str(switchReq.jobId), LOST_AND_FOUND);
        qtp = getQueueData(LOST_AND_FOUND);
        if (qtp == NULL)
            qtp = lostFoundQueue();
    }
    jobInQueueEnd(jp, qtp);

    if ( qfp != qtp ) {
        if ((jp->shared->jobBill.options & SUB_CHKPNT_DIR )
            && !(jp->shared->jobBill.options2 & SUB2_QUEUE_CHKPNT)) {


        } else {


            if ( qfp->qAttrib & Q_ATTRIB_CHKPNT ) {
                jp->shared->jobBill.options  &= ~ SUB_CHKPNT_DIR;
                jp->shared->jobBill.options  &= ~ SUB_CHKPNT_PERIOD;
                jp->shared->jobBill.options2 &= ~ SUB2_QUEUE_CHKPNT;
            }

            FREEUP(jp->shared->jobBill.chkpntDir);

            if ( qtp->qAttrib & Q_ATTRIB_CHKPNT ) {

                char dir[MAXPATHLEN];
                sprintf(dir, "%s/%s", qtp->chkpntDir, lsb_jobid2str(jp->jobId));
                jp->shared->jobBill.chkpntDir = safeSave(dir);
                jp->shared->jobBill.chkpntPeriod = qtp->chkpntPeriod;
                jp->shared->jobBill.options  |= SUB_CHKPNT_DIR;
                jp->shared->jobBill.options  |= SUB_CHKPNT_PERIOD;
                jp->shared->jobBill.options2 |= SUB2_QUEUE_CHKPNT;
            }
            else
                jp->shared->jobBill.chkpntDir = safeSave("");

        }
    }

    if (qfp != qtp) {
        if ((jp->shared->jobBill.options & SUB_RERUNNABLE )
            && !(jp->shared->jobBill.options2 & SUB2_QUEUE_RERUNNABLE)) {

        } else {

            jp->shared->jobBill.options  &= ~ SUB_RERUNNABLE;
            jp->shared->jobBill.options2 &= ~ SUB2_QUEUE_RERUNNABLE;


            if ( qtp->qAttrib & Q_ATTRIB_RERUNNABLE ) {
                jp->shared->jobBill.options  |= SUB_RERUNNABLE;
                jp->shared->jobBill.options2 |= SUB2_QUEUE_RERUNNABLE;
            }
        }
    }

    return true;

}

static int
replay_cleanjob(char *filename, int lineNum)
{
    LS_LONG_INT  jobId;

    jobId = LSB_JOBID(logPtr->eventLog.jobCleanLog.jobId,
                      logPtr->eventLog.jobCleanLog.idx);
    removeJob(jobId);
    return true;
}

static int
replay_movejob(char *filename, int lineNum)
{
    static char             fname[] = "replay_movejob";
    struct jobMoveReq       jobMoveReq;
    int                     reply;
    struct lsfAuth          auth;

    (void)memset((void *)&auth, '\0', sizeof (struct lsfAuth));

    auth.uid = logPtr->eventLog.jobMoveLog.userId;
    strcpy(auth.lsfUserName, logPtr->eventLog.jobMoveLog.userName);

    jobMoveReq.jobId = LSB_JOBID(logPtr->eventLog.jobMoveLog.jobId,
                                 logPtr->eventLog.jobMoveLog.idx);
    jobMoveReq.position = logPtr->eventLog.jobMoveLog.position;
    jobMoveReq.opCode = logPtr->eventLog.jobMoveLog.base;

    reply = moveJobArray(&jobMoveReq, FALSE, &auth);
    switch (reply) {
        case LSBE_NO_ERROR:
            return true;
        case LSBE_NO_JOB:
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6711,
                                             "%s: File %s at line %d: Cannot move job <%s>: job not found"), fname, filename, lineNum, lsb_jobid2str(jobMoveReq.jobId)); /* catgets 6711 */
            return false;
        case LSBE_PERMISSION:
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6712,
                                             "%s: File %s at line %d: Cannot move job <%s>: not operated by owner or manager"), fname, filename, lineNum, lsb_jobid2str(jobMoveReq.jobId)); /* catgets 6712 */
            return false;
        case LSBE_JOB_STARTED:
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6713,
                                             "%s: File %s at line %d: Cannot move job <%s>: job already started"), fname, filename, lineNum, lsb_jobid2str(jobMoveReq.jobId)); /* catgets 6713 */
            return false;
        default:
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6714,
                                             "%s: File %s at line %d: Wrong return code <%d> from movejob() for job <%s>"), fname, filename, lineNum, reply, lsb_jobid2str(jobMoveReq.jobId)); /* catgets 6714 */
            return false;
    }


}

static int
replay_startjob(char *filename, int lineNum, int preExecStart)
{
    struct jData job;
    struct jData *jp;
    struct jShared shared;
    int i;

    job.jobId = LSB_JOBID(logPtr->eventLog.jobStartLog.jobId,
                          logPtr->eventLog.jobStartLog.idx);
    job.jStatus = logPtr->eventLog.jobStartLog.jStatus;
    job.startTime = logPtr->eventTime;
    job.jobPid = logPtr->eventLog.jobStartLog.jobPid;
    job.jobPGid = logPtr->eventLog.jobStartLog.jobPGid;
    job.queuePreCmd = logPtr->eventLog.jobStartLog.queuePreCmd;
    job.queuePostCmd = logPtr->eventLog.jobStartLog.queuePostCmd;
    job.numHostPtr = logPtr->eventLog.jobStartLog.numExHosts;

    job.shared = &shared;

    if ((jp = getJobData(job.jobId)) == NULL) {
        ls_syslog(LOG_ERR, "\
%s: Job %s not found in job list",
                  __func__, lsb_jobid2str(job.jobId));
        return FALSE;
    }

    if (job.numHostPtr > 0) {
        job.hPtr = my_calloc(job.numHostPtr,
                             sizeof(struct hData *), __func__);
    } else {
        ls_syslog(LOG_ERR, "\
%s: File %s at line %d: No execution hosts for job %s ??",
                  __func__,
                  filename,
                  lineNum,
                  lsb_jobid2str(job.jobId));
        return FALSE;
    }

    for (i = 0; i < job.numHostPtr; i++) {

        job.hPtr[i] = getHostData(logPtr->eventLog.jobStartLog.execHosts[i]);
        if (job.hPtr[i] == NULL) {
            ls_syslog(LOG_WARNING, "\
%s: Execution host %s not found; saving job %s to host %s",
                      __func__,
                      logPtr->eventLog.jobStartLog.execHosts[i],
                      lsb_jobid2str(job.jobId),
                      LOST_AND_FOUND);
            job.hPtr[i] = getHostData(LOST_AND_FOUND);
            assert(job.hPtr[i]);
        }
    }

    jp->startTime = job.startTime;
    jp->jobPid = job.jobPid;
    jp->jobPGid = job.jobPGid;
    jp->oldReason = 0;
    jp->newReason = 0;
    jp->subreasons = 0;
    jp->numHostPtr = job.numHostPtr;
    if (jp->hPtr)
        FREEUP(jp->hPtr);
    jp->hPtr = job.hPtr;
    jp->jFlags = logPtr->eventLog.jobStartLog.jFlags;

    if (!preExecStart
        && (jp->shared->jobBill.options & SUB_PRE_EXEC)
        && (jp->jStatus & JOB_STAT_PRE_EXEC)) {
        jp->jStatus &= ~JOB_STAT_PRE_EXEC;
        return true;
    }

    if (job.queuePreCmd && job.queuePreCmd[0] != '\0')
        jp->queuePreCmd = safeSave(job.queuePreCmd);
    if (job.queuePostCmd && job.queuePostCmd[0] != '\0')
        jp->queuePostCmd = safeSave(job.queuePostCmd);

    jStatusChange(jp, job.jStatus, logPtr->eventTime, "replay_startjob");
    if  (!preExecStart){
        updHostLeftRusageMem(jp, -1);
    }

    if (jp->shared->jobBill.options & SUB_EXCLUSIVE)
        for (i = 0; i < jp->numHostPtr; i ++)
            jp->hPtr[i]->hStatus |= HOST_STAT_EXCLUSIVE;

    if (preExecStart)
        jp->jStatus |= JOB_STAT_PRE_EXEC;

    return TRUE;
}

static int
replay_executejob(char *filename, int lineNum)
{
    struct jobExecuteLog   *jobExecuteLog;
    struct jData           *jp;
    LS_LONG_INT            jobId;

    jobExecuteLog = &logPtr->eventLog.jobExecuteLog;

    jobId = LSB_JOBID(jobExecuteLog->jobId, jobExecuteLog->idx);

    if ((jp = getJobData(jobId)) == NULL) {
        ls_syslog(LOG_ERR, "\
%s: Job %s not found in job list",
                  __func__, lsb_jobid2str(jobId));
        return false;
    }

    jp->execUid = jobExecuteLog->execUid;
    jp->jobPGid = jobExecuteLog->jobPGid;


    if (jobExecuteLog->jobPid != 0)
        jp->jobPid = jobExecuteLog->jobPid;

    if (jp->execHome)
        FREEUP(jp->execHome);
    jp->execHome = safeSave(jobExecuteLog->execHome);

    if (jp->execCwd)
        FREEUP(jp->execCwd);
    jp->execCwd = safeSave(jobExecuteLog->execCwd);

    if (jp->execUsername)
        FREEUP(jp->execUsername);
    jp->execUsername = safeSave(jobExecuteLog->execUsername);

    return true;
}

static int
replay_startjobaccept(char *filename, int lineNum)
{
    struct jobStartAcceptLog *jobStartAcceptLog;
    struct jData *jp;
    LS_LONG_INT jobId;

    jobStartAcceptLog = &logPtr->eventLog.jobStartAcceptLog;
    jobId = LSB_JOBID(jobStartAcceptLog->jobId, jobStartAcceptLog->idx);

    if ((jp = getJobData(jobId)) == NULL) {
        ls_syslog(LOG_ERR, "\
%s: Job %s not found in job list",
                  __func__, lsb_jobid2str(jobId));
        return false;
    }

    jp->jobPGid = jobStartAcceptLog->jobPGid;
    jp->jobPid = jobStartAcceptLog->jobPid;
    if ((jp->jgrpNode->nodeType == JGRP_NODE_ARRAY) &&
        (ARRAY_DATA(jp->jgrpNode)->jobArray->startTime == 0)){
        ARRAY_DATA(jp->jgrpNode)->jobArray->startTime = jp->startTime;
    }
    jp->jFlags = jobStartAcceptLog->jflags;

    return true;
}


static int
replay_newstat(char *filename, int lineNum)
{
    static char             fname[] = "replay_newstat";
    struct jData           *jp, *job;
    struct jobStatusLog    *newStat;

    newStat = &logPtr->eventLog.jobStatusLog;

    if ((jp = getJobData(LSB_JOBID(newStat->jobId, newStat->idx))) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6709,
                                         "%s: File %s at line %d: Job <%d> not found in job list "),
                  fname, filename, lineNum, newStat->jobId);
        return false;
    }


    if ((IS_POST_DONE(newStat->jStatus))||(IS_POST_ERR(newStat->jStatus))) {
        jp->jStatus = newStat->jStatus;
        jp->endTime = newStat->endTime;
        if ((jp->jgrpNode->nodeType == JGRP_NODE_ARRAY) &&
            ARRAY_DATA(jp->jgrpNode)->counts[JGRP_COUNT_NJOBS] ==
            (ARRAY_DATA(jp->jgrpNode)->counts[JGRP_COUNT_NDONE] +
             ARRAY_DATA(jp->jgrpNode)->counts[JGRP_COUNT_NEXIT])) {
            ARRAY_DATA(jp->jgrpNode)->jobArray->endTime = jp->endTime;
        }
        return TRUE;
    }

    if (newStat->jStatus & JOB_STAT_UNKWN) {
        jp->jStatus |= JOB_STAT_UNKWN;
        return TRUE;
    } else
        jp->jStatus &= ~JOB_STAT_UNKWN;

    if ((IS_START(jp->jStatus) && IS_PEND(newStat->jStatus))
        || IS_FINISH(newStat->jStatus)) {

        updHostLeftRusageMem(jp, 1);
    }

    if (IS_PEND(jp->jStatus) && IS_START(newStat->jStatus)) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6720,
                                         "replay_newstat: Status of job <%s> transited from <%d> to <%d> without JOB_START event record; ignored"), /* catgets 6720 */
                  lsb_jobid2str(jp->jobId),
                  jp->jStatus,
                  newStat->jStatus);
        return true;
    }
    if (newStat->reason != EXIT_ZOMBIE_JOB) {
        jp->newReason = newStat->reason;
        jp->oldReason = newStat->reason;
    }
    jp->subreasons = newStat->subreasons;
    if ((newStat->jStatus & JOB_STAT_SSUSP)
        && !(jp->jStatus & JOB_STAT_SSUSP))
        jp->ssuspTime = logPtr->eventTime;


    if (newStat->jStatus & (JOB_STAT_EXIT)) {
        if (newStat->reason & (EXIT_ZOMBIE | EXIT_KILL_ZOMBIE)) {
            jp->jStatus |= JOB_STAT_ZOMBIE;
            inZomJobList(jp, FALSE);
        } else if (newStat->reason & EXIT_RERUN) {
            jp->newReason = EXIT_RERUN;
            jp->oldReason = EXIT_RERUN;
        }
        else if (newStat->reason & EXIT_ZOMBIE_JOB) {
            if ((job = getZombieJob(jp->jobId)) != NULL) {
                offList((struct listEntry *) job);
                freeJData(job);
                if (getZombieJob(jp->jobId) != NULL)
                    return true;
            }

            if (jp->newReason == EXIT_KILL_ZOMBIE &&
                (jp->jStatus & JOB_STAT_ZOMBIE)) {
                jp->jStatus &= ~JOB_STAT_ZOMBIE;
                jp->newReason = 0;
                jp->oldReason = 0;
            }
            if ((jp->shared->jobBill.options & SUB_RERUNNABLE)) {
                return true;
            }
        } else if ( newStat->reason & EXIT_RESTART ) {
            jp->shared->jobBill.options |= SUB_RESTART | SUB_RESTART_FORCE;
        } else
            jp->jStatus &= ~JOB_STAT_ZOMBIE;

    }

    if (IS_SUSP(newStat->jStatus) && IS_START(newStat->jStatus)) {
        if (!(jp->newReason & SUSP_MBD_LOCK)
            && shouldLockJob (jp, newStat->jStatus)) {
            jp->newReason = newStat->reason | SUSP_MBD_LOCK;
            jp->oldReason = newStat->reason | SUSP_MBD_LOCK;
        }
    }
    else if (IS_FINISH(newStat->jStatus)) {
        jp->endTime = newStat->endTime;
        jp->cpuTime = newStat->cpuTime;
        jp->exitStatus = newStat->exitStatus;
    }

    jStatusChange(jp, newStat->jStatus, logPtr->eventTime, "replay_newstat");


    if (IS_PEND(newStat->jStatus) && newStat->ru && !IS_FINISH(jp->jStatus)) {
        if (!jp->lsfRusage)
            jp->lsfRusage = (struct lsfRusage *)
                my_malloc(sizeof(struct lsfRusage), fname);
        memcpy((char *) jp->lsfRusage, (char *) &newStat->lsfRusage,
               sizeof(struct lsfRusage));
        jp->cpuTime = newStat->cpuTime;
    }



    return true;

}

static int
replay_qc(char *filename, int lineNum)
{
    static char             fname[] = "replay_qc";
    int                     opCode;
    struct qData           *qp;

    opCode = logPtr->eventLog.queueCtrlLog.opCode;
    if ((qp = getQueueData(logPtr->eventLog.queueCtrlLog.queue)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6722,
                                         "%s: File %s at line %d: Queue <%s> not found in queue list "), fname, filename, lineNum, logPtr->eventLog.queueCtrlLog.queue); /* catgets 6722 */
        return false;
    }
    if (opCode == QUEUE_OPEN)
        qp->qStatus |= QUEUE_STAT_OPEN;
    if (opCode == QUEUE_CLOSED)
        qp->qStatus &= ~QUEUE_STAT_OPEN;
    if (opCode == QUEUE_ACTIVATE)
        qp->qStatus |= QUEUE_STAT_ACTIVE;
    if (opCode == QUEUE_INACTIVATE)
        qp->qStatus &= ~QUEUE_STAT_ACTIVE;
    return true;

}

static int
replay_hostcontrol(char *filename, int lineNum)
{
    static char             fname[] = "replay_hostcontrol";
    int                     opCode;
    char                    host[MAXHOSTNAMELEN];
    struct hData           *hp;

    opCode = logPtr->eventLog.hostCtrlLog.opCode;
    strcpy(host, logPtr->eventLog.hostCtrlLog.host);

    hp = getHostData(host);
    if (hp == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6723,
                                         "%s: File %s at line %d: Host <%s> not found in host list "), fname, filename, lineNum, host); /* catgets 6723 */
        return false;
    }
    if (opCode == HOST_OPEN)
        hp->hStatus &= ~HOST_STAT_DISABLED;
    if (opCode == HOST_CLOSE)
        hp->hStatus |= HOST_STAT_DISABLED;
    strcpy(hp->message,logPtr->eventLog.hostCtrlLog.message);
    return true;

}

static int
replay_mbdStart(char *filename, int lineNum)
{
    int                     list;
    struct jData           *jpbw;

    for (list = 0; list < NJLIST; list++) {
        for (jpbw = jDataList[list]->back; jpbw != jDataList[list];
             jpbw = jpbw->back) {
            jpbw->pendEvent.notSwitched = 0;
            jpbw->pendEvent.sig = SIG_NULL;
            jpbw->pendEvent.sig1 = SIG_NULL;
            jpbw->pendEvent.sig1Flags = 0;
            jpbw->pendEvent.notModified = 0;

            eventPending = FALSE;

        }
    }
    return true;

}

static int
replay_mbdDie(char *filename, int lineNum)
{
    dieTime = logPtr->eventTime;
    return true;
}

static int
replay_unfulfill(char *filename, int lineNum)
{
    static char             fname[] = "replay_unfulfill";
    LS_LONG_INT             jobId;
    struct jData           *jp;

    jobId = LSB_JOBID(logPtr->eventLog.unfulfillLog.jobId,
                      logPtr->eventLog.unfulfillLog.idx);
    if ((jp = getJobData(jobId)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6709,
                                         "%s: File %s at line %d: Job <%s> not found in job list "),
                  fname, filename, lineNum, lsb_jobid2str(jobId));
        return false;
    }

    jp->pendEvent.notSwitched = logPtr->eventLog.unfulfillLog.notSwitched;
    jp->pendEvent.sig = logPtr->eventLog.unfulfillLog.sig;
    jp->pendEvent.sig1 = logPtr->eventLog.unfulfillLog.sig1;
    jp->pendEvent.sig1Flags = logPtr->eventLog.unfulfillLog.sig1Flags;
    jp->shared->jobBill.chkpntPeriod = logPtr->eventLog.unfulfillLog.chkPeriod;
    jp->chkpntPeriod = logPtr->eventLog.unfulfillLog.chkPeriod;
    jp->pendEvent.notModified = logPtr->eventLog.unfulfillLog.notModified;

    if (jp->pendEvent.notSwitched ||
        jp->pendEvent.sig != SIG_NULL ||
        jp->pendEvent.sig1 != SIG_NULL ||
        jp->pendEvent.notModified)
        eventPending = TRUE;

    return true;
}

static int
replay_mig(char *filename, int lineNum)
{
    static char             fname[] = "replay_mig";
    struct jData           *jp;
    struct migLog          *migLog;
    int                     i;
    LS_LONG_INT             jobId;

    migLog = &logPtr->eventLog.migLog;

    jobId = LSB_JOBID(migLog->jobId, migLog->idx);
    if ((jp = getJobData(jobId)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6709,
                                         "%s: File %s at line %d: Job <%d> not found in job list"),
                  fname, filename, lineNum, migLog->jobId);
        return false;
    }

    if (jp->shared->jobBill.numAskedHosts) {
        for (i = 0; i < jp->shared->jobBill.numAskedHosts; i++)
            FREEUP (jp->shared->jobBill.askedHosts[i]);
        FREEUP (jp->shared->jobBill.askedHosts);
        FREEUP (jp->askedPtr);
        jp->numAskedPtr = 0;
    }
    jp->shared->jobBill.options &= ~SUB_HOST;
    jp->shared->jobBill.numAskedHosts = migLog->numAskedHosts;

    if (jp->shared->jobBill.numAskedHosts > 0) {
        int  numAskedHosts = 0;
        struct askedHost *askedHosts;
        int returnErr, badReqIndx, others;

        jp->shared->jobBill.askedHosts = my_calloc(migLog->numAskedHosts,
                                                   sizeof(char *), fname);
        for (i = 0; i < jp->shared->jobBill.numAskedHosts; i++)
            jp->shared->jobBill.askedHosts[i] = safeSave(migLog->askedHosts[i]);
        jp->shared->jobBill.options |= SUB_HOST;

        returnErr = chkAskedHosts(jp->shared->jobBill.numAskedHosts,
                                  jp->shared->jobBill.askedHosts,
                                  jp->shared->jobBill.numProcessors, &numAskedHosts,
                                  &askedHosts, &badReqIndx, &others, 0);
        if (returnErr == LSBE_NO_ERROR) {
            if (numAskedHosts  > 0) {
                jp->askedPtr = my_calloc (numAskedHosts,
                                          sizeof(struct askedHost), fname);
                for (i = 0; i < numAskedHosts; i++) {
                    jp->askedPtr[i].hData = askedHosts[i].hData;
                    jp->askedPtr[i].priority = askedHosts[i].priority;
                }
            }
            jp->numAskedPtr = numAskedHosts;
            jp->askedOthPrio = others;
            FREEUP(askedHosts);
        }
    }

    jp->shared->jobBill.restartPid = jp->jobPid;
    jp->restartPid = jp->jobPid;

    return true;
}
static int
replay_jobsigact(char *filename, int lineNum)
{
    static char             fname[] = "replay_jobsigact";
    struct jData           *jp;
    int                     newActPid;
    LS_LONG_INT             jobId;

    jobId = LSB_JOBID(logPtr->eventLog.sigactLog.jobId,
                      logPtr->eventLog.sigactLog.idx);
    if ((jp = getJobData(jobId)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6709,
                                         "%s: File %s at line %d: Job <%d> not found in job list"),
                  fname, filename, lineNum,
                  logPtr->eventLog.sigactLog.jobId);
        return false;
    }
    jp->newReason = logPtr->eventLog.sigactLog.reasons;
    jp->oldReason = logPtr->eventLog.sigactLog.reasons;
    jp->shared->jobBill.chkpntPeriod = logPtr->eventLog.sigactLog.period;
    jp->chkpntPeriod = logPtr->eventLog.sigactLog.period;
    if (!IS_START(jp->jStatus) &&
        logPtr->eventLog.sigactLog.actStatus == ACT_NO)

        return true;

    newActPid = logPtr->eventLog.sigactLog.pid;

    if ((logPtr->eventLog.sigactLog.actStatus == ACT_START)) {
        jp->actPid = newActPid;
        jp->sigValue =  sigNameToValue_ (logPtr->eventLog.sigactLog.signalSymbol);
        if (logPtr->eventLog.sigactLog.flags & LSB_CHKPNT_MIG)
            jp->jStatus |= JOB_STAT_MIG;

    } else if (logPtr->eventLog.sigactLog.actStatus == ACT_DONE ||
               logPtr->eventLog.sigactLog.actStatus == ACT_FAIL) {

        jp->actPid = 0;
        jp->sigValue = SIG_NULL;
        if (logPtr->eventLog.sigactLog.flags & LSB_CHKPNT_MIG) {
            jp->jStatus |= JOB_STAT_MIG;

            if (!IS_PEND (jp->jStatus)) {

                FREEUP (jp->schedHost);
                jp->schedHost = safeSave (jp->hPtr[0]->hostType);
                FREEUP (jp->shared->jobBill.schedHostType);
                jp->shared->jobBill.schedHostType = safeSave (jp->hPtr[0]->hostType);
            }
            jp->shared->jobBill.restartPid = jp->jobPid;
            jp->restartPid = jp->jobPid;

        } else
            jp->jStatus &= ~JOB_STAT_MIG;

        if (logPtr->eventLog.sigactLog.actStatus == ACT_DONE &&
            ( (strcmp(logPtr->eventLog.sigactLog.signalSymbol, "SIG_CHKPNT") == 0)
              || (strcmp(logPtr->eventLog.sigactLog.signalSymbol, "SIG_CHKPNT_COPY") == 0)))
            jp->jStatus |= JOB_STAT_CHKPNTED_ONCE;
    } else {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6727,
                                         "%s: Unknown status <%d> of action <%s> of Job <%s>"), /* catgets 6727 */
                  fname,
                  logPtr->eventLog.sigactLog.actStatus,
                  logPtr->eventLog.sigactLog.signalSymbol,
                  lsb_jobid2str(jp->jobId));
    }
    return true;
}

static int
replay_jobrequeue(char *filename, int lineNum)
{
    static char             fname[] = "replay_jobrequeue";
    struct jData           *jp;
    LS_LONG_INT             jobId;

    jobId = LSB_JOBID(logPtr->eventLog.jobRequeueLog.jobId,
                      logPtr->eventLog.jobRequeueLog.idx);
    if ((jp = getJobData(jobId)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6728,
                                         "%s: File %s at line %d: Requeued job <%d> is not found in job list"), fname, filename, lineNum, logPtr->eventLog.jobRequeueLog.jobId); /* catgets 6728 */
        return false;
    }
    handleRequeueJob(jp, eventTime);
    jp->endTime   = 0;

    return true;

}


static int
replay_chkpnt(char *filename, int lineNum)
{
    static char             fname[] = "replay_chkpnt";
    struct jData           *jp;
    int                     newChkPid;
    LS_LONG_INT             jobId;

    jobId = LSB_JOBID(logPtr->eventLog.chkpntLog.jobId,
                      logPtr->eventLog.chkpntLog.idx);

    if ((jp = getJobData(jobId)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6709,
                                         "%s: File %s at line %d: Job <%d> not found in job list"),
                  fname, filename, lineNum,
                  logPtr->eventLog.chkpntLog.jobId);
        return false;
    }
    jp->shared->jobBill.chkpntPeriod = logPtr->eventLog.chkpntLog.period;
    jp->chkpntPeriod = logPtr->eventLog.chkpntLog.period;
    if (!IS_START(jp->jStatus))
        return true;

    newChkPid = logPtr->eventLog.chkpntLog.pid;

    if (newChkPid > 0) {
        jp->actPid = newChkPid;
        if (logPtr->eventLog.chkpntLog.flags & LSB_CHKPNT_MIG)
            jp->jStatus |= JOB_STAT_MIG;
    } else if (newChkPid < 0) {
        jp->actPid = 0;
        if (logPtr->eventLog.chkpntLog.flags & LSB_CHKPNT_MIG) {
            jp->jStatus |= JOB_STAT_MIG;
            FREEUP (jp->schedHost);
            jp->schedHost = safeSave (jp->hPtr[0]->host);
            FREEUP (jp->shared->jobBill.schedHostType);
            jp->shared->jobBill.schedHostType = safeSave (jp->hPtr[0]->hostType);

            jp->shared->jobBill.restartPid = jp->jobPid;
            jp->restartPid = jp->jobPid;

        } else
            jp->jStatus &= ~JOB_STAT_MIG;

        if (logPtr->eventLog.chkpntLog.ok)
            jp->jStatus |= JOB_STAT_CHKPNTED_ONCE;
    } else {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6730,
                                         "%s: The actPid of Job <%s> cannot be 0"), /* catgets 6730 */
                  fname,
                  lsb_jobid2str(jp->jobId));
    }

    return true;
}


static int
replay_loadIndex(char *filename, int lineNum)
{
    int                     i;

    if (logPtr->eventLog.loadIndexLog.nIdx != allLsInfo->numIndx) {
        logLoadIndex = TRUE;
        return true;
    }
    for (i = 0; i < allLsInfo->numIndx; i++) {
        if (strcmp(allLsInfo->resTable[i].name,
                   logPtr->eventLog.loadIndexLog.name[i])) {
            logLoadIndex = TRUE;
            return true;
        }
    }

    logLoadIndex = FALSE;
    return true;
}

static int
replay_logSwitch(char *filename, int lineNum)
{
    nextJobId_t = logPtr->eventLog.logSwitchLog.lastJobId;
    return true;

}

/* replay_jobmsg()
 */
static int
replay_jobmsg(char *file, int num)
{
    struct jData *jPtr;
    LS_LONG_INT jobID;
    struct jobMsgLog *log;
    struct lsbMsg *jmsg;

    log = &logPtr->eventLog.jobMsgLog;
    jobID = LSB_JOBID(log->jobId, log->idx);

    jPtr = getJobData(jobID);
    if (jPtr == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh job %s not found?", __func__, lsb_jobid2str(jobID));
        return false;
    }

    if (jPtr->numMsg == 0)
        jPtr->msgs = calloc(MAX_JOB_MSG, sizeof(char *));

    if (jPtr->numMsg > MAX_JOB_MSG - 1)
        jPtr->numMsg = 0;

    jmsg = calloc(1, sizeof(struct lsbMsg));
    jmsg->t = logPtr->eventTime;
    jmsg->msg = strdup(log->msg);

    /* Check if we rolled over
     */
    if (jPtr->msgs[jPtr->numMsg]) {
        _free_(jPtr->msgs[jPtr->numMsg]->msg);
        _free_(jPtr->msgs[jPtr->numMsg]);
    }

    jPtr->msgs[jPtr->numMsg] = jmsg;
    jPtr->numMsg++;

    return true;
}

/* replay_newjgrp()
 */
static int
replay_newjgrp(char *file, int num)
{
    struct jgrpLog *jgrp_log;
    struct job_group jgrp;
    struct lsfAuth auth;

    jgrp_log = &logPtr->eventLog.jgrpLog;

    /* Copy the path not the name, the
     * add_job_group() knows how to make
     * the group right
     */
    jgrp.group_name = strdup(jgrp_log->path);
    auth.uid = jgrp_log->uid;
    strcpy(auth.lsfUserName, jgrp_log->user);
    jgrp.max_jobs = jgrp_log->max_jobs;

    add_job_group(&jgrp, &auth);

    _free_(jgrp.group_name);

    return true;
}

/* replay_deljgrp()
 */
static int
replay_deljgrp(char *file, int num)
{
    struct jgrpLog *jgrp_log;
    struct job_group jgrp;
    struct lsfAuth auth;

    jgrp_log = &logPtr->eventLog.jgrpLog;

    /* Copy the path not the name
     */
    jgrp.group_name = strdup(jgrp_log->path);
    auth.uid = jgrp_log->uid;
    strcpy(auth.lsfUserName, jgrp_log->user);

    del_job_group(&jgrp, &auth);

    _free_(jgrp.group_name);

    return true;
}

static int
replay_modjgrp(char *file, int num)
{
    struct jgrpModLog *jgrp_mod;
    struct job_group jgrp;

    jgrp_mod = &logPtr->eventLog.jgrpMod;

    jgrp.group_name = strdup(jgrp_mod->path);
    jgrp.max_jobs = jgrp_mod->max_jobs;

    modify_job_group(&jgrp, NULL);

    _free_(jgrp.group_name);

    return true;
}

/* replay_jobpreemting()
 */
static int
replay_jobpreempting(char *file, int num)
{
    struct jobPreemptingLog *jLog;
    LS_LONG_INT jobID;
    struct jData *jPtr;
    struct hData *hPtr;
    int cc;

    jLog = &logPtr->eventLog.jobPreempting;
    jobID = LSB_JOBID(jLog->jobid, jLog->idx);

    jPtr = getJobData(jobID);
    if (jPtr == NULL) {
        ls_syslog(LOG_ERR, "\
%s: job %s not found in job list", __func__, lsb_jobid2str(jobID));
        return false;
    }

    for (cc = 0; cc < jLog->numHosts; cc++) {
        hPtr = getHostData(jLog->hosts[cc]);
        if (hPtr == NULL) {
            ls_syslog(LOG_ERR, "\
%s: cannot found host %s preempted by job %s", __func__, jLog->hosts[cc],
                      lsb_jobid2str(jPtr->jobId));
        }
        push_link(jPtr->preempted_hosts, hPtr);
    }

    return true;
}

static int
replay_jobpreempted(char *file, int num)
{
    struct jobPreemptedLog *jLog;
    LS_LONG_INT jobID;
    struct jData *jPtr;

    jLog = &logPtr->eventLog.jobPreempted;
    jobID = LSB_JOBID(jLog->jobid, jLog->idx);

    jPtr = getJobData(jobID);
    if (jPtr == NULL) {
        ls_syslog(LOG_ERR, "\
%s: job %s not found in job list",
                  __func__, lsb_jobid2str(jobID));
        return false;
    }

    jPtr->preempted_by = LSB_JOBID(jLog->preempted_by,
                                   jLog->preempted_by_idx);

    return true;
}

int
log_modifyjob(struct modifyReq * modReq, struct lsfAuth *auth)
{
    static char             fname[] = "log_modifyjob";
    struct jobModLog        *jobModLog;
    int j;

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(modReq->jobId),
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_JOB_MODIFY2;

    jobModLog = &logPtr->eventLog.jobModLog;
    jobModLog->jobIdStr = modReq->jobIdStr;
    jobModLog->delOptions = modReq->delOptions;
    jobModLog->delOptions2 = modReq->delOptions2;
    jobModLog->options    = modReq->submitReq.options;
    jobModLog->options2   = modReq->submitReq.options2;
    jobModLog->userId     = auth->uid;
    jobModLog->userName   = auth->lsfUserName;
    jobModLog->submitTime   = modReq->submitReq.submitTime;
    jobModLog->umask   = modReq->submitReq.umask;
    jobModLog->numProcessors   = modReq->submitReq.numProcessors;
    jobModLog->beginTime   = modReq->submitReq.beginTime;
    jobModLog->termTime   = modReq->submitReq.termTime;
    jobModLog->sigValue   = modReq->submitReq.sigValue;
    jobModLog->restartPid   = modReq->submitReq.restartPid;
    jobModLog->jobName   = modReq->submitReq.jobName;
    jobModLog->queue   = modReq->submitReq.queue;
    jobModLog->numAskedHosts   = modReq->submitReq.numAskedHosts;
    if (jobModLog->numAskedHosts > 0)
        jobModLog->askedHosts = modReq->submitReq.askedHosts;
    jobModLog->resReq   = modReq->submitReq.resReq;
    for (j = 0; j < LSF_RLIM_NLIMITS; j++)
        jobModLog->rLimits[j] = modReq->submitReq.rLimits[j];

    jobModLog->hostSpec   = modReq->submitReq.hostSpec;
    jobModLog->dependCond   = modReq->submitReq.dependCond;
    jobModLog->subHomeDir   = modReq->submitReq.subHomeDir;
    jobModLog->inFile   = modReq->submitReq.inFile;
    jobModLog->outFile   = modReq->submitReq.outFile;
    jobModLog->errFile   = modReq->submitReq.errFile;
    jobModLog->command   = modReq->submitReq.command;
    jobModLog->inFileSpool   = modReq->submitReq.inFileSpool;
    jobModLog->commandSpool   = modReq->submitReq.commandSpool;
    jobModLog->chkpntPeriod   = modReq->submitReq.chkpntPeriod;
    jobModLog->chkpntDir   = modReq->submitReq.chkpntDir;
    jobModLog->nxf   = modReq->submitReq.nxf;
    jobModLog->xf   = modReq->submitReq.xf;
    jobModLog->jobFile   = modReq->submitReq.jobFile;
    jobModLog->fromHost   = modReq->submitReq.fromHost;
    jobModLog->cwd   = modReq->submitReq.cwd;
    jobModLog->preExecCmd   = modReq->submitReq.preExecCmd;
    jobModLog->mailUser   = modReq->submitReq.mailUser;
    jobModLog->projectName   = modReq->submitReq.projectName;
    jobModLog->niosPort   = modReq->submitReq.niosPort;
    jobModLog->maxNumProcessors   = modReq->submitReq.maxNumProcessors;
    jobModLog->loginShell   = modReq->submitReq.loginShell;
    jobModLog->schedHostType   = modReq->submitReq.schedHostType;

    jobModLog->userPriority = modReq->submitReq.userPriority;
    jobModLog->job_description = modReq->submitReq.job_description;

    if (putEventRec(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(modReq->jobId),
                  "putEventRec");
        mbdDie(MASTER_FATAL);
    }
    return 0;

}

int
log_newjob(struct jData *job)
{
    return log_jobdata(job, __func__, EVENT_JOB_NEW);
}

static int
log_jobdata(struct jData *job, const char *fname1, int type)
{
    struct submitReq *jobBill = &(job->shared->jobBill);
    struct jobNewLog *jobNewLog;
    int j;
    float *hostFactor;

    if (openEventFile(fname1) < 0) {
        ls_syslog(LOG_ERR, "\
%s: openEventFile() failed for job %s", __func__, lsb_jobid2str(job->jobId));
        mbdDie(MASTER_FATAL);
        if (type == EVENT_JOB_NEW)
            rmLogJobInfo(job, FALSE);
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = type;

    jobNewLog = &logPtr->eventLog.jobNewLog;
    jobNewLog->jobId = LSB_ARRAY_JOBID(job->jobId);
    jobNewLog->idx = LSB_ARRAY_IDX(job->jobId);

    jobNewLog->userId = job->userId;
    jobNewLog->options = jobBill->options;
    jobNewLog->options2 = jobBill->options2;
    if (jobNewLog->options2 & SUB2_USE_DEF_PROCLIMIT) {

        jobNewLog->numProcessors = 1;
        jobNewLog->maxNumProcessors = 1;
    }
    else {
        jobNewLog->numProcessors = jobBill->numProcessors;
        jobNewLog->maxNumProcessors = jobBill->maxNumProcessors;
    }
    jobNewLog->submitTime = jobBill->submitTime;
    jobNewLog->beginTime = jobBill->beginTime;
    jobNewLog->termTime = jobBill->termTime;
    strcpy(jobNewLog->userName, job->userName);
    jobNewLog->schedHostType = job->schedHost;
    jobNewLog->loginShell = jobBill->loginShell;

    if (jobBill->options & SUB_WINDOW_SIG)
        jobNewLog->sigValue = jobBill->sigValue;
    else
        jobNewLog->sigValue = SIG_NULL;

    jobNewLog->chkpntPeriod = job->chkpntPeriod;
    jobNewLog->restartPid = job->restartPid;

    for (j = 0; j < LSF_RLIM_NLIMITS; j++)
        jobNewLog->rLimits[j] = jobBill->rLimits[j];

    strcpy(jobNewLog->hostSpec, jobBill->hostSpec);
    if (jobNewLog->hostSpec[0] == '\0') {
        hostFactor = getHostFactor(jobBill->fromHost);
        if (hostFactor == NULL) {
            ls_syslog(LOG_ERR, "\
%s: cannot get hostFactor for job %s host %s", __func__,
                      lsb_jobid2str(job->jobId),
                      jobBill->fromHost);
            jobNewLog->hostFactor = 1.0;
        }
    } else if ((hostFactor = getModelFactor(jobNewLog->hostSpec)) == NULL) {
        hostFactor = getHostFactor(jobNewLog->hostSpec);
        if (hostFactor == NULL) {
            ls_syslog(LOG_ERR, "\
%s: cannot get hostFactor for job %s host spec %s", __func__,
                      lsb_jobid2str(job->jobId),
                      jobBill->hostSpec);
            jobNewLog->hostFactor = 1.0;
        }
    }
    if (hostFactor)
        jobNewLog->hostFactor = *hostFactor;

    jobNewLog->umask = jobBill->umask;
    strcpy(jobNewLog->queue, jobBill->queue);
    jobNewLog->resReq = jobBill->resReq;

    strcpy(jobNewLog->fromHost, jobBill->fromHost);
    strcpy(jobNewLog->cwd, jobBill->cwd);
    strcpy(jobNewLog->subHomeDir, jobBill->subHomeDir);
    strcpy(jobNewLog->chkpntDir,  jobBill->chkpntDir);

    if ((jobBill->options & SUB_IN_FILE) == SUB_IN_FILE)
        strcpy(jobNewLog->inFile, jobBill->inFile);
    else
        strcpy(jobNewLog->inFile, "");

    if ((jobBill->options & SUB_OUT_FILE) == SUB_OUT_FILE)
        strcpy(jobNewLog->outFile, jobBill->outFile);
    else
        strcpy(jobNewLog->outFile, "");

    if ((jobBill->options & SUB_ERR_FILE) == SUB_ERR_FILE)
        strcpy(jobNewLog->errFile, jobBill->errFile);
    else
        strcpy(jobNewLog->errFile, "");

    if ((jobBill->options2 & SUB2_IN_FILE_SPOOL) == SUB2_IN_FILE_SPOOL) {
        strcpy(jobNewLog->inFileSpool, jobBill->inFileSpool);
        strcpy(jobNewLog->inFile, jobBill->inFile);
    } else {
        strcpy(jobNewLog->inFileSpool, "");
    }

    if ((jobBill->options2 & SUB2_JOB_CMD_SPOOL) == SUB2_JOB_CMD_SPOOL)
        strcpy(jobNewLog->commandSpool, jobBill->commandSpool);
    else
        strcpy(jobNewLog->commandSpool, "");

    if (job->jobSpoolDir != NULL)
        strcpy(jobNewLog->jobSpoolDir, job->jobSpoolDir);
    else
        strcpy(jobNewLog->jobSpoolDir, "");

    strcpy(jobNewLog->jobFile, jobBill->jobFile);

    if (jobBill->numAskedHosts > 0)
        jobNewLog->askedHosts = jobBill->askedHosts;
    jobNewLog->numAskedHosts = jobBill->numAskedHosts;

    if (jobBill->options & SUB_JOB_NAME)
        strcpy(jobNewLog->jobName, jobBill->jobName);
    else
        strcpy(jobNewLog->jobName, "");

    strcpy(jobNewLog->command, jobBill->command);

    jobNewLog->dependCond = jobBill->dependCond;
    jobNewLog->preExecCmd = jobBill->preExecCmd;
    jobNewLog->mailUser = jobBill->mailUser;

    if (strcmp(jobBill->projectName, "") == 0) {

        jobNewLog->projectName = getDefaultProject();
    } else
        jobNewLog->projectName = jobBill->projectName;


    jobNewLog->nxf = jobBill->nxf;
    jobNewLog->xf = jobBill->xf;

    jobNewLog->niosPort = jobBill->niosPort;

    jobNewLog->userPriority = jobBill->userPriority;
    jobNewLog->userGroup = jobBill->userGroup;
    jobNewLog->abs_run_limit = job->abs_run_limit;
    jobNewLog->priority = job->priority;

    if (job->shared->jobBill.options2 & SUB2_JOB_GROUP)
        jobNewLog->job_group = jobBill->job_group;
    else
        jobNewLog->job_group = "";

    if (jobBill->options2 & SUB2_JOB_DESC) {
        jobNewLog->job_description = jobBill->job_description;
    } else {
        jobNewLog->job_description = "";
    }

    if (putEventRec(fname1) < 0) {
        ls_syslog(LOG_ERR, "\
%s: putEventRec() failed for job %s", __func__, lsb_jobid2str(job->jobId));
        if (type == EVENT_JOB_NEW)
            rmLogJobInfo(job, FALSE);
        mbdDie(MASTER_FATAL);
    }
    return 0;
}



void
log_switchjob(struct jobSwitchReq * switchReq, int uid, char *userName)
{
    static char             fname[] = "log_switchjob";
    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(switchReq->jobId),
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_JOB_SWITCH;
    logPtr->eventLog.jobSwitchLog.userId = uid;
    strcpy(logPtr->eventLog.jobSwitchLog.userName, userName);
    logPtr->eventLog.jobSwitchLog.jobId = LSB_ARRAY_JOBID(switchReq->jobId);
    logPtr->eventLog.jobSwitchLog.idx = LSB_ARRAY_IDX(switchReq->jobId);
    strcpy(logPtr->eventLog.jobSwitchLog.queue, switchReq->queue);
    if (putEventRec("log_switchjob") < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(switchReq->jobId),
                  "putEventRec");
    }
}

void
log_movejob(struct jobMoveReq *moveReq, struct lsfAuth *auth)
{
    if (openEventFile(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: openEventFile() failed for job %s", __func__, lsb_jobid2str(moveReq->jobId));
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_JOB_MOVE;
    logPtr->eventLog.jobMoveLog.userId = auth->uid;
    strcpy(logPtr->eventLog.jobMoveLog.userName, auth->lsfUserName);
    logPtr->eventLog.jobMoveLog.jobId = LSB_ARRAY_JOBID(moveReq->jobId);
    logPtr->eventLog.jobMoveLog.idx = LSB_ARRAY_IDX(moveReq->jobId);
    logPtr->eventLog.jobMoveLog.position = moveReq->position;
    logPtr->eventLog.jobMoveLog.base = moveReq->opCode;

    if (putEventRec(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: putEventRec() failed for job %s", __func__, lsb_jobid2str(moveReq->jobId));
        mbdDie(MASTER_FATAL);
    }
}

/* log_startjob()
 */
void
log_startjob(struct jData * job, int preExecStart)
{
    int i;
    struct jobStartLog *jobStartLog;
    char **execHosts;

    if (openEventFile("log_startjob") < 0) {
        ls_syslog(LOG_ERR, "\
%s: openEventFile() failed for job %s", __func__, lsb_jobid2str(job->jobId));
        mbdDie(MASTER_FATAL);
    }

    if (preExecStart) {
        logPtr->type = EVENT_PRE_EXEC_START;
    } else
        logPtr->type = EVENT_JOB_START;

    jobStartLog = &logPtr->eventLog.jobStartLog;
    jobStartLog->jobId = LSB_ARRAY_JOBID(job->jobId);
    jobStartLog->idx = LSB_ARRAY_IDX(job->jobId);
    jobStartLog->jStatus = JOB_STAT_RUN;
    jobStartLog->jFlags  = job->jFlags;
    jobStartLog->numExHosts = job->numHostPtr;
    jobStartLog->jobPid = job->jobPid;
    jobStartLog->jobPGid = job->jobPGid;
    execHosts = NULL;

    if (job->numHostPtr > 0) {

        execHosts = my_calloc(job->numHostPtr, sizeof(char *), __func__);
        jobStartLog->execHosts = execHosts;

        for (i = 0; i < job->numHostPtr; i++)
            jobStartLog->execHosts[i] = job->hPtr[i]->host;
        jobStartLog->hostFactor = job->hPtr[0]->cpuFactor;
    }

    job->startTime = time(NULL);

    if (!job->queuePreCmd)
        jobStartLog->queuePreCmd = "";
    else
        jobStartLog->queuePreCmd = job->queuePreCmd;

    if (!job->queuePostCmd)
        jobStartLog->queuePostCmd = "";
    else
        jobStartLog->queuePostCmd = job->queuePostCmd;

    jobStartLog->userGroup = job->shared->jobBill.userGroup;

    if (putEventRec(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: putEventRec() failed for job %s", __func__, lsb_jobid2str(job->jobId));
        mbdDie(MASTER_FATAL);
    }
    if (execHosts)
        free(execHosts);

}

void
log_executejob(struct jData * job)
{
    static char             fname[] = "log_executejob";
    struct jobExecuteLog   *jobExecuteLog;

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(job->jobId),
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_JOB_EXECUTE;

    jobExecuteLog = &logPtr->eventLog.jobExecuteLog;
    jobExecuteLog->jobId = LSB_ARRAY_JOBID(job->jobId);
    jobExecuteLog->idx = LSB_ARRAY_IDX(job->jobId);
    jobExecuteLog->execUid = job->execUid;
    jobExecuteLog->jobPGid = job->jobPGid;
    jobExecuteLog->jobPid = job->jobPid;
    jobExecuteLog->execHome = job->execHome;
    jobExecuteLog->execCwd = job->execCwd;
    jobExecuteLog->execUsername = job->execUsername;

    if (putEventRec(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(job->jobId),
                  "putEventRec");
        mbdDie(MASTER_FATAL);
    }

}

void
log_startjobaccept(struct jData *job)
{
    struct jobStartAcceptLog *jobStartAcceptLog;

    if (openEventFile(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: openEventFile() failed for job %s",
                  __func__, lsb_jobid2str(job->jobId));
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_JOB_START_ACCEPT;

    jobStartAcceptLog = &logPtr->eventLog.jobStartAcceptLog;
    jobStartAcceptLog->jobId = LSB_ARRAY_JOBID(job->jobId);
    jobStartAcceptLog->idx = LSB_ARRAY_IDX(job->jobId);
    jobStartAcceptLog->jobPGid = job->jobPGid;
    jobStartAcceptLog->jobPid = job->jobPid;
    jobStartAcceptLog->jflags = job->jFlags;

    if ((job->jgrpNode->nodeType == JGRP_NODE_ARRAY) &&
        (ARRAY_DATA(job->jgrpNode)->jobArray->startTime == 0)) {
        ARRAY_DATA(job->jgrpNode)->jobArray->startTime = job->startTime;
    }

    if (putEventRec("log_startjobaccept") < 0) {
        ls_syslog(LOG_ERR, "\
%s: putEventRec() failed for job job %s",
                  __func__, lsb_jobid2str(job->jobId));
        mbdDie(MASTER_FATAL);
    }
}



void
log_newstatus(struct jData * job)
{
    static char             fname[] = "log_newstatus";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(job->jobId),
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    if (job->jStatus & (JOB_STAT_EXIT | JOB_STAT_DONE)) {
        now = time(0);
        job->endTime = now;
    }
    logPtr->type = EVENT_JOB_STATUS;
    logPtr->eventLog.jobStatusLog.jobId = LSB_ARRAY_JOBID(job->jobId);
    logPtr->eventLog.jobStatusLog.idx = LSB_ARRAY_IDX(job->jobId);
    logPtr->eventLog.jobStatusLog.cpuTime = job->cpuTime;
    logPtr->eventLog.jobStatusLog.endTime = job->endTime;
    logPtr->eventLog.jobStatusLog.reason = job->newReason;
    logPtr->eventLog.jobStatusLog.subreasons = job->subreasons;
    logPtr->eventLog.jobStatusLog.exitStatus = job->exitStatus;


    if ((logPtr->eventLog.jobStatusLog.ru = (job->lsfRusage != NULL)))
        logPtr->eventLog.jobStatusLog.lsfRusage = *(job->lsfRusage);

    if (job->jStatus & JOB_STAT_UNKWN)
        logPtr->eventLog.jobStatusLog.jStatus = JOB_STAT_UNKWN;
    else
        logPtr->eventLog.jobStatusLog.jStatus = job->jStatus;

    logPtr->eventLog.jobStatusLog.jStatus &= MASK_INT_JOB_STAT;



    if (putEventRec(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(job->jobId),
                  "putEventRec");
        mbdDie(MASTER_FATAL);
    }
    if (job->jStatus & JOB_STAT_DONE
        || (job->jStatus & JOB_STAT_EXIT
            && !(job->jStatus & JOB_STAT_ZOMBIE))) {
        if (((IS_POST_DONE(job->jStatus))||(IS_POST_ERR(job->jStatus))) &&
            (job->jgrpNode->nodeType == JGRP_NODE_ARRAY &&
             ARRAY_DATA(job->jgrpNode)->counts[JGRP_COUNT_NJOBS] ==
             (ARRAY_DATA(job->jgrpNode)->counts[JGRP_COUNT_NDONE] +
              ARRAY_DATA(job->jgrpNode)->counts[JGRP_COUNT_NEXIT]))) {
            ARRAY_DATA(job->jgrpNode)->jobArray->endTime = job->endTime;
        }
        logFinishedjob(job);
    }
}

void
log_mig(struct jData * jData, int uid, char *userName)
{
    static char             fname[] = "log_mig";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_MIG;
    logPtr->eventLog.migLog.jobId = LSB_ARRAY_JOBID(jData->jobId);
    logPtr->eventLog.migLog.idx = LSB_ARRAY_IDX(jData->jobId);
    logPtr->eventLog.migLog.numAskedHosts = jData->shared->jobBill.numAskedHosts;
    logPtr->eventLog.migLog.askedHosts = jData->shared->jobBill.askedHosts;
    logPtr->eventLog.migLog.userId = uid;
    strcpy(logPtr->eventLog.migLog.userName, userName);

    if (putEventRec(fname) < 0)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "putEventRec");
}

void
log_jobrequeue(struct jData * jData)
{
    static char             fname[] = "log_jobrequeue";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "openeventfile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_JOB_REQUEUE;
    logPtr->eventLog.jobRequeueLog.jobId = LSB_ARRAY_JOBID(jData->jobId);
    logPtr->eventLog.jobRequeueLog.idx = LSB_ARRAY_IDX(jData->jobId);

    if (putEventRec(fname) < 0)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "putEventRec");
}

void
log_jobclean(struct jData * jData)
{
    static char             fname[] = "log_jobclean";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_JOB_CLEAN;
    logPtr->eventLog.jobCleanLog.jobId = LSB_ARRAY_JOBID(jData->jobId);
    logPtr->eventLog.jobCleanLog.idx = LSB_ARRAY_IDX(jData->jobId);

    if (putEventRec(fname) < 0)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "putEventRec");
}


void
log_chkpnt(struct jData * jData, int ok, int flags)
{
    static char             fname[] = "log_chkpnt";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_CHKPNT;
    logPtr->eventLog.chkpntLog.jobId = LSB_ARRAY_JOBID(jData->jobId);
    logPtr->eventLog.chkpntLog.idx = LSB_ARRAY_IDX(jData->jobId);
    logPtr->eventLog.chkpntLog.period = jData->chkpntPeriod;
    logPtr->eventLog.chkpntLog.pid = jData->actPid;
    logPtr->eventLog.chkpntLog.ok = ok;
    logPtr->eventLog.chkpntLog.flags = flags;

    if (putEventRec(fname) < 0)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "putEventRec");
}


void
log_jobsigact (struct jData *jData, struct statusReq *statusReq, int sigFlags)
{
    static char  fname[] = "log_jobsigact";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_JOB_SIGACT;
    logPtr->eventLog.sigactLog.jobId = LSB_ARRAY_JOBID(jData->jobId);
    logPtr->eventLog.sigactLog.idx = LSB_ARRAY_IDX(jData->jobId);
    logPtr->eventLog.sigactLog.flags = sigFlags;
    logPtr->eventLog.sigactLog.period = jData->chkpntPeriod;
    if (statusReq == NULL) {

        logPtr->eventLog.sigactLog.jStatus = jData->jStatus;
        if (jData->newReason)
            logPtr->eventLog.sigactLog.reasons = jData->newReason;
        else
            logPtr->eventLog.sigactLog.reasons = PEND_SYS_UNABLE;
        logPtr->eventLog.sigactLog.pid = 0;
        logPtr->eventLog.sigactLog.actStatus = ACT_NO;
        logPtr->eventLog.sigactLog.flags = 0;
        logPtr->eventLog.sigactLog.signalSymbol = "SIG_CHKPNT";
    } else {
        logPtr->eventLog.sigactLog.jStatus = statusReq->newStatus;
        logPtr->eventLog.sigactLog.reasons = statusReq->reason;
        logPtr->eventLog.sigactLog.pid = statusReq->actPid;
        logPtr->eventLog.sigactLog.actStatus = statusReq->actStatus;
        logPtr->eventLog.sigactLog.flags = sigFlags;
        logPtr->eventLog.sigactLog.signalSymbol = getLsbSigSymbol(statusReq->sigValue);
    }



    if (putEventRec(fname) < 0)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jData->jobId),
                  "putEventRec");
}

void
log_queuestatus(struct qData * qp, int opCode, int userId, char *userName)
{
    static char             fname[] = "log_queuestatus";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_QUEUE_FAIL,
                  fname,
                  qp->queue,
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_QUEUE_CTRL;
    logPtr->eventLog.queueCtrlLog.opCode = opCode;
    strcpy(logPtr->eventLog.queueCtrlLog.queue, qp->queue);
    logPtr->eventLog.queueCtrlLog.userId = userId;
    strcpy(logPtr->eventLog.queueCtrlLog.userName, userName);
    if (putEventRec(fname) < 0)
        ls_syslog(LOG_ERR, I18N_QUEUE_FAIL,
                  fname,
                  qp->queue,
                  "putEventRec");
}



void
log_hoststatus(struct hData * hp, int opCode, int userId, char *userName, char *message)
{
    static char             fname[] = "log_hoststatus";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_HOST_FAIL,
                  fname,
                  hp->host,
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_HOST_CTRL;
    logPtr->eventLog.hostCtrlLog.opCode = opCode;
    strcpy(logPtr->eventLog.hostCtrlLog.host, hp->host);
    logPtr->eventLog.hostCtrlLog.userId = userId;
    strcpy(logPtr->eventLog.hostCtrlLog.userName, userName);
    strcpy(logPtr->eventLog.hostCtrlLog.message, message);
    if (putEventRec(fname) < 0)
        ls_syslog(LOG_ERR, I18N_HOST_FAIL,
                  fname,
                  hp->host,
                  "putEventRec");
}

void
log_mbdDie(int sig)
{
    static char             fname[] = "log_mbdDie";
    char                    hname[MAXHOSTNAMELEN];

    if (openEventFile(fname) < 0)
        return;
    logPtr->type = EVENT_MBD_DIE;
    if (masterHost)
        strcpy(logPtr->eventLog.mbdDieLog.master, masterHost);
    else if (gethostname(hname, MAXHOSTNAMELEN) >= 0)
        strcpy(logPtr->eventLog.mbdDieLog.master, hname);
    else
        strcpy(logPtr->eventLog.mbdDieLog.master, "unknown");

    logPtr->eventLog.mbdDieLog.numRemoveJobs = numRemoveJobs;
    logPtr->eventLog.mbdDieLog.exitCode = sig;
    putEventRec(fname);
}

void
log_unfulfill(struct jData * jp)
{
    static char fname[] = "log_unfulfill";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jp->jobId),
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_MBD_UNFULFILL;

    logPtr->eventLog.unfulfillLog.jobId = LSB_ARRAY_JOBID(jp->jobId);
    logPtr->eventLog.unfulfillLog.idx = LSB_ARRAY_IDX(jp->jobId);
    logPtr->eventLog.unfulfillLog.notSwitched = jp->pendEvent.notSwitched;
    logPtr->eventLog.unfulfillLog.sig = jp->pendEvent.sig;
    logPtr->eventLog.unfulfillLog.sig1 = jp->pendEvent.sig1;
    logPtr->eventLog.unfulfillLog.sig1Flags = jp->pendEvent.sig1Flags;
    logPtr->eventLog.unfulfillLog.chkPeriod = jp->chkpntPeriod;
    logPtr->eventLog.unfulfillLog.notModified = jp->pendEvent.notModified;

    if (putEventRec(fname) < 0)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S,
                  fname,
                  lsb_jobid2str(jp->jobId),
                  "putEventRec");
}

/* log_newjgrp()
 */
void
log_newjgrp(struct jgTreeNode *jgrp)
{

    if (openEventFile(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed in openEventFile() %m", __func__);
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_NEW_JGRP;

    strcpy(logPtr->eventLog.jgrpLog.name, jgrp->name);
    strcpy(logPtr->eventLog.jgrpLog.path, jgrp->path);
    strcpy(logPtr->eventLog.jgrpLog.user, JGRP_DATA(jgrp)->userName);

    logPtr->eventLog.jgrpLog.uid = JGRP_DATA(jgrp)->userId;
    logPtr->eventLog.jgrpLog.status = JGRP_DATA(jgrp)->status;
    logPtr->eventLog.jgrpLog.submit_time = JGRP_DATA(jgrp)->submit_time;
    logPtr->eventLog.jgrpLog.max_jobs = JGRP_DATA(jgrp)->max_jobs;

    if (putEventRec(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed in putEventRec() %m", __func__);
    }
}

/* log_deljgrp()
 */
void
log_deljgrp(struct jgTreeNode *jgrp)
{

    if (openEventFile(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed in openEventFile() %m", __func__);
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_DEL_JGRP;

    memset(&logPtr->eventLog.jgrpLog, 0, sizeof(struct jgrpLog));

    /* Path is enough, if mbatchd is logging this events
     * it means this job group can be removed.
     */
    strcpy(logPtr->eventLog.jgrpLog.path, jgrp->path);

    if (putEventRec(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed in putEventRec() %m", __func__);
    }
}

/* log_modjgrp()
 */
void
log_modjgrp(struct jgTreeNode *jgrp)
{
    if (openEventFile(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed in openEventFile() %m", __func__);
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_MOD_JGRP;

    strcpy(logPtr->eventLog.jgrpMod.path, jgrp->path);
    logPtr->eventLog.jgrpMod.max_jobs = JGRP_DATA(jgrp)->max_jobs;

    if (putEventRec(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed in putEventRec() %m", __func__);
    }

}

static void
log_loadIndex(void)
{
    static char fname[] = "log_loadIndex";
    int i;
    static char **names;

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    if (!names)
        if (!(names = malloc(allLsInfo->numIndx*sizeof(char *)))) {
            ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "malloc");
            mbdDie(MASTER_FATAL);
        }
    logPtr->type = EVENT_LOAD_INDEX;
    logPtr->eventLog.loadIndexLog.nIdx = allLsInfo->numIndx;
    logPtr->eventLog.loadIndexLog.name = names;

    for (i = 0; i < allLsInfo->numIndx; i++)
        logPtr->eventLog.loadIndexLog.name[i]
            = allLsInfo->resTable[i].name;

    if (putEventRec(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "putEventRec");
        mbdDie(MASTER_FATAL);
    }
}
void
log_jobForce(struct jData* job, int uid, char *userName)
{
    static char                 fname[] = "log_JobForce()";
    int                         i;
    struct jobForceRequestLog   jobForceRequestLog;

    memset((struct jobForceRequestLog *)&jobForceRequestLog, 0,
           sizeof(struct jobForceRequestLog));


    jobForceRequestLog.jobId        =  LSB_ARRAY_JOBID(job->jobId);
    jobForceRequestLog.idx          =  LSB_ARRAY_IDX(job->jobId);
    jobForceRequestLog.userId       =  uid;
    strcpy(jobForceRequestLog.userName, userName);
    jobForceRequestLog.numExecHosts =  job->numHostPtr;
    jobForceRequestLog.execHosts    = (char **)my_calloc(job->numHostPtr,
                                                         sizeof(char *),
                                                         fname);
    for (i = 0; i < job->numHostPtr; i++) {
        jobForceRequestLog.execHosts[i] = job->hPtr[i]->host;
    }

    jobForceRequestLog.options |= (job->jFlags & JFLAG_URGENT_NOSTOP) ?
        RUNJOB_OPT_NOSTOP : RUNJOB_OPT_NORMAL;


    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "openEventFile");
        mbdDie(MASTER_FATAL);
    }


    logPtr->type = EVENT_JOB_FORCE;


    memcpy(&(logPtr->eventLog.jobForceRequestLog), &jobForceRequestLog,
           sizeof(struct jobForceRequestLog));


    if (putEventRec(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "putEventRec");
        mbdDie(MASTER_FATAL);
    }

    FREEUP(jobForceRequestLog.execHosts);

}

/* log_job_preemption()
 *
 * Log the hosts of the job that is preempting others,
 * log the preemptor jobid for all jobs that are being
 * preempted.
 */
void
log_job_preemption(struct jData *jPtr, link_t *rl)
{
    struct jData *jPtr2;
    linkiter_t iter;

    log_job_preempting(jPtr);

    traverse_init(rl, &iter);
    while ((jPtr2 = traverse_link(&iter)))
        log_job_preempted(jPtr2);
}

/* log_job_preempting()
 */
static void
log_job_preempting(struct jData *jPtr)
{
    struct jobPreemptingLog *jLog;
    struct hData *hPtr;
    linkiter_t iter;
    int cc;

    if (openEventFile(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: openEventFile() failed for job %s",
                  __func__, lsb_jobid2str(jPtr->jobId));
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_JOB_PREEMPTING;

    jLog = &logPtr->eventLog.jobPreempting;
    jLog->jobid = LSB_ARRAY_JOBID(jPtr->jobId);
    jLog->idx = LSB_ARRAY_IDX(jPtr->jobId);

    jLog->numHosts = LINK_NUM_ENTRIES(jPtr->preempted_hosts);
    jLog->hosts = calloc(jLog->numHosts, sizeof(char *));

    cc = 0;
    traverse_init(jPtr->preempted_hosts, &iter);
    while ((hPtr = traverse_link(&iter))) {
        jLog->hosts[cc] = strdup(hPtr->host);
        ++cc;
    }

    if (putEventRec(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: putEventRec() failed for job job %s",
                  __func__, lsb_jobid2str(jPtr->jobId));
        mbdDie(MASTER_FATAL);
    }
}

/* log_job_preempted()
 */
static void
log_job_preempted(struct jData *jPtr)
{
    struct jobPreemptedLog *jLog;

    if (openEventFile(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: openEventFile() failed for job %s",
                  __func__, lsb_jobid2str(jPtr->jobId));
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_JOB_PREEMPTED;

    jLog = &logPtr->eventLog.jobPreempted;
    jLog->jobid = LSB_ARRAY_JOBID(jPtr->jobId);
    jLog->idx = LSB_ARRAY_IDX(jPtr->jobId);
    jLog->preempted_by = LSB_ARRAY_JOBID(jPtr->preempted_by);
    jLog->preempted_by_idx = LSB_ARRAY_IDX(jPtr->preempted_by);

    if (putEventRec(__func__) < 0) {
        ls_syslog(LOG_ERR, "\
%s: putEventRec() failed for job job %s",
                  __func__, lsb_jobid2str(jPtr->jobId));
        mbdDie(MASTER_FATAL);
    }
}

/* openEventFile()
 *
 * Open the lsb.events file the path is
 * kept in the elogFname global variable.
 */
static int
openEventFile(const char *fname)
{
    long pos;
    sigset_t newmask, oldmask;

    /* init_log() has not been called yet.
     */
    if (elogFname[0] == 0)
        return -1;

    /* is switch child running?
     */
    if (swchild->pid > 0
        && swchild->child_gone == false) {
        return openEventFile2(elogFname2);
    }

    sigemptyset(&newmask);
    sigaddset(&newmask, SIGCHLD);
    sigprocmask(SIG_BLOCK, &newmask, &oldmask);

    if (swchild->pid > 0
        && swchild->child_gone == true) {
        do_switch_child_end();
    }

    /* Test if the file is already open.
     */
    if (log_fp == NULL) {
        if ((log_fp = fopen(elogFname, "a+")) == NULL) {
            sigprocmask(SIG_SETMASK, &oldmask, NULL);
            ls_syslog(LOG_ERR, "\
%s: fopen() %s failed: %m", __func__, elogFname);
            return -1;
        }
    }

    sigprocmask(SIG_SETMASK, &oldmask, NULL);

    fseek(log_fp, 0L, SEEK_END);

    if ((pos = ftell(log_fp)) == 0) {
        fprintf(log_fp,"#80\n");
    }

    chmod(elogFname, 0644);
    logPtr = my_calloc(1, sizeof(struct eventRec), __func__);

    /* Use the same version as the main protocol.
     */
    sprintf(logPtr->version, "%d", OPENLAVA_XDR_VERSION);

    return 0;
}

/* openEventFile2()
 *
 * Open the event file having the
 * name specified in input. This function
 * is used when mbatchd child is running.
 */
static int
openEventFile2(const char *fname)
{
    sigset_t newmask;
    sigset_t oldmask;

    sigemptyset(&newmask);
    sigaddset(&newmask, SIGCHLD);
    sigprocmask(SIG_BLOCK, &newmask, &oldmask);

    if (log_fp == NULL) {
        if (logclass & LC_SWITCH) {
            ls_syslog(LOG_INFO, "\
%s: now opening %s as child is switching", __func__, fname);
        }
        if ((log_fp = fopen(fname, "a+")) == NULL) {
            sigprocmask(SIG_SETMASK, &oldmask, NULL);
            ls_syslog(LOG_ERR, "\
%s: fopen() %s failed: %m", __func__, elogFname);
            return -1;
        }
    }
    sigprocmask(SIG_SETMASK, &oldmask, NULL);

    chmod(fname, 0644);
    logPtr = my_calloc(1, sizeof(struct eventRec), __func__);

    /* Use the same version as the main protocol.
     */
    sprintf(logPtr->version, "%d", OPENLAVA_XDR_VERSION);

    return 0;
}

static int
putEventRec(const char *fname)
{
    int cc;

    now = time(0);
    logPtr->eventTime = now;

    cc = putEventRec1(fname);

    return cc;
}
static int
putEventRecTime(const char *fname, time_t eventTime)
{
    int cc;

    logPtr->eventTime = eventTime;

    cc = putEventRec1(fname);

    return cc;

}

static int
putEventRec1(const char *fname)
{
    if (lsb_puteventrec(log_fp, logPtr) < 0) {
        ls_syslog(LOG_ERR, "\
%s: lsb_puteventrec() failed %s", __func__, lsb_sysmsg());
        return -1;
    }

    if (mbdParams->maxStreamRecords > 0)
        streamEvent(logPtr);

    free(logPtr);
    if (fflush(log_fp) != 0) {
        ls_syslog(LOG_ERR, "%s: fflush() failed %m", __func__);
        return -1;
    }

    return 0;
}

static void
logFinishedjob(struct jData *job)
{
    static char           fname[] = "logFinishedjob()";
    struct submitReq      *jobBill = &(job->shared->jobBill);
    struct jobFinishLog   *jobFinishLog;
    int                   i;
    float                 *hostFactor;

    if (IS_POST_FINISH(job->jStatus)) {
        return;
    }

    if (joblog_fp == NULL) {
        if ((joblog_fp = fopen(jlogFname, "a")) == NULL) {
            ls_syslog(LOG_ERR, "\
%s: failed to fopen() %s %m", __func__, jlogFname);
            return;
        }
    }
    chmod(jlogFname, 0644);

    logPtr = my_calloc(1, sizeof(struct eventRec), "logFinishedjob");
    jobFinishLog = &logPtr->eventLog.jobFinishLog;

    logPtr->type = EVENT_JOB_FINISH;
    sprintf(logPtr->version, "%d", OPENLAVA_XDR_VERSION);
    jobFinishLog->jobId = LSB_ARRAY_JOBID(job->jobId);
    jobFinishLog->idx = LSB_ARRAY_IDX(job->jobId);
    jobFinishLog->userId = job->userId;
    strcpy(jobFinishLog->userName, job->userName);
    jobFinishLog->options = jobBill->options;
    jobFinishLog->numProcessors = jobBill->numProcessors;
    jobFinishLog->maxNumProcessors = jobBill->maxNumProcessors;
    jobFinishLog->submitTime = jobBill->submitTime;
    jobFinishLog->beginTime = jobBill->beginTime;
    jobFinishLog->termTime = jobBill->termTime;
    jobFinishLog->startTime = job->startTime;

    strcpy(jobFinishLog->queue, jobBill->queue);
    jobFinishLog->resReq = jobBill->resReq;
    jobFinishLog->dependCond = jobBill->dependCond;
    jobFinishLog->preExecCmd = jobBill->preExecCmd;
    jobFinishLog->mailUser = jobBill->mailUser;

    if (strcmp(jobBill->projectName, "") == 0) {
        jobFinishLog->projectName = getDefaultProject();
    } else
        jobFinishLog->projectName = jobBill->projectName;

    strcpy(jobFinishLog->fromHost, jobBill->fromHost);
    strcpy(jobFinishLog->cwd, jobBill->cwd);

    if ((jobBill->options & SUB_IN_FILE) == SUB_IN_FILE)
        strcpy(jobFinishLog->inFile, jobBill->inFile);
    else
        strcpy(jobFinishLog->inFile, "");

    if ((jobBill->options & SUB_OUT_FILE) == SUB_OUT_FILE)
        strcpy(jobFinishLog->outFile, jobBill->outFile);
    else
        strcpy(jobFinishLog->outFile, "");

    if ((jobBill->options & SUB_ERR_FILE) == SUB_ERR_FILE)
        strcpy(jobFinishLog->errFile, jobBill->errFile);
    else
        strcpy(jobFinishLog->errFile, "");

    if ((jobBill->options2 & SUB2_IN_FILE_SPOOL) == SUB2_IN_FILE_SPOOL) {
        strcpy(jobFinishLog->inFileSpool, jobBill->inFileSpool);
        strcpy(jobFinishLog->inFile, jobBill->inFile);
    } else {
        strcpy(jobFinishLog->inFileSpool, "");
    }

    if ((jobBill->options2 & SUB2_JOB_CMD_SPOOL) == SUB2_JOB_CMD_SPOOL)
        strcpy(jobFinishLog->commandSpool, jobBill->commandSpool);
    else
        strcpy(jobFinishLog->commandSpool, "");

    strcpy(jobFinishLog->jobFile, jobBill->jobFile);

    jobFinishLog->jStatus = job->jStatus;
    jobFinishLog->jStatus &= MASK_INT_JOB_STAT;
    jobFinishLog->endTime = job->endTime;
    logPtr->eventTime = job->endTime;

    jobFinishLog->exitStatus = job->exitStatus;

    jobFinishLog->numAskedHosts = jobBill->numAskedHosts;
    if (jobFinishLog->numAskedHosts > 0)
        jobFinishLog->askedHosts = jobBill->askedHosts;

    jobFinishLog->numExHosts = job->numHostPtr;
    if (jobFinishLog->numExHosts > 0) {
        jobFinishLog->execHosts = my_calloc(job->numHostPtr,
                                            sizeof(char *),
                                            "logFinishedjob");
        for (i = 0; i < jobFinishLog->numExHosts; i++)
            jobFinishLog->execHosts[i] = job->hPtr[i]->host;
    }
    if (jobFinishLog->numExHosts > 0)
        jobFinishLog->hostFactor = job->hPtr[0]->cpuFactor;
    else {
        hostFactor = getHostFactor(jobFinishLog->fromHost);
        if (hostFactor == NULL) {
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S,
                      fname,
                      lsb_jobid2str(job->jobId),
                      "getHostFactor",
                      jobFinishLog->fromHost);
            jobFinishLog->hostFactor = 1.0;
        } else
            jobFinishLog->hostFactor = *hostFactor;
    }
    if (jobBill->options & SUB_JOB_NAME)
        strcpy(jobFinishLog->jobName, fullJobName(job));
    else
        strcpy(jobFinishLog->jobName, "");

    strcpy(jobFinishLog->command, jobBill->command);
    jobFinishLog->loginShell = jobBill->loginShell;

    if (job->lsfRusage != NULL)
        jobFinishLog->lsfRusage = *(job->lsfRusage);
    else
        cleanLsfRusage(&jobFinishLog->lsfRusage);

    jobFinishLog->maxRMem = job->runRusage.mem;
    jobFinishLog->maxRSwap = job->runRusage.swap;

    if (lsb_puteventrec(joblog_fp, logPtr) < 0)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_MM,
                  fname,
                  lsb_jobid2str(job->jobId),
                  "lsb_puteventrec");

    if (jobFinishLog->numExHosts)
        free(jobFinishLog->execHosts);
    free(logPtr);

    /* don't close the lsb.acct file.
     * but flush it.
     */
    fflush(joblog_fp);
}

/* switchELog()
 *
 * Fork a child to switch the lsb.events
 */
void
switchELog(void)
{
    pid_t pid;
    int cc;

    if (numRemoveJobs < maxjobnum)
        return;

    /* For development only when modify the switching
     * algorithm
     */
    if (daemonParams[MBD_SWITCH_NOFORK].paramValue) {
        /* Close the lsb.events the first OpenEventFile()
         * after the switch will reopen it.
         */
        _fclose_(&log_fp);
        do_switch();
        return;
    }

    if (logclass & LC_SWITCH) {
        ls_syslog(LOG_INFO, "\
%s: time to switch numRemoveJobs %d maxjobnum %d", __func__,
                  numRemoveJobs, maxjobnum);
    }
    /* switch child still running
     */
    if (swchild->pid > 0) {
        if (logclass & LC_SWITCH) {
            ls_syslog(LOG_INFO, "\
%s: swich child pid %d still running, no switch", __func__, swchild->pid);
        }
        return;
    }
    /* close the events file first, the child
     * will open its own and mbd will open it
     * when logging the next event
     */
    _fclose_(&log_fp);

    numRemoveJobs = 0;
    swchild->child_gone = false;
    swchild->pid = pid = fork_mbd();
    if (pid == 0) {
        if (make_switch_pidfile() < 0) {
            ls_syslog(LOG_ERR, "\
%s: failed in make_switch_pidfile() %m", __func__);
        }
        cc = do_switch();
        rm_switch_pidfile();
        if (cc < 0) {
            ls_syslog(LOG_ERR, "\
%s: switching events file has failed", __func__);
            exit(-1);
        }
        exit(0);
    }
    if (pid < 0) {
        ls_syslog(LOG_ERR, "\
%s: fork() failed switch is synchronous", __func__);
        swchild->pid = 0;
        swchild->child_gone = false;
        do_switch();
        return;
    }

    if (logclass & LC_SWITCH) {
        ls_syslog(LOG_INFO, "\
%s: switch child started pid %d gone %d", __func__,
                  swchild->pid, swchild->child_gone);
    }
}

/* do_switch()
 */
static int
do_switch(void)
{
    struct timeval before;
    struct timeval after;

    if (logclass & LC_SWITCH)
        gettimeofday(&before, NULL);

    /* Now  switch
     */
    if (switch_log() == 0) {

        numRemoveJobs = 0;
        if (logclass & LC_SWITCH) {
            gettimeofday(&after, NULL);
            ls_syslog(LOG_INFO,"\
%s: %d ms", __func__,  (int)((after.tv_sec - before.tv_sec) * 1000 +
                             (after.tv_usec - before.tv_usec)/1000));
        }
        return 0;
    }

    if (logclass & LC_SWITCH) {
        gettimeofday(&after, NULL);
        ls_syslog(LOG_INFO,"\
%s: %d ms", __func__,  (int)((after.tv_sec - before.tv_sec) * 1000 +
                             (after.tv_usec - before.tv_usec)/1000));
    }
    numRemoveJobs = maxjobnum / 2;
    return -1;
}

void
checkAcctLog(void)
{
    int             needArchive = 0;
    time_t          now;
    struct stat     jbuf;
    static time_t   lastAcctCreationTime = -1;

#define DAYSECONDS      (24*60*60)
#define KILOBYTE        1024

    if (acctArchiveInDays > 0 || acctArchiveInSize > 0) {
        if (stat(jlogFname, &jbuf) != 0){
            lastAcctCreationTime = -1;
            return;
        }

        if (acctArchiveInDays > 0) {
            now = time(0);

            if (lastAcctCreationTime <= 0) {
                lastAcctCreationTime = lsb_getAcctFileTime(jlogFname);
            }

            if ((lastAcctCreationTime > 0)
                && ((now - lastAcctCreationTime)
                    > (acctArchiveInDays*DAYSECONDS))) {
                needArchive = 1;
            }
        }

        if ((!needArchive) && (acctArchiveInSize > 0)
            && (jbuf.st_size > (acctArchiveInSize*KILOBYTE))){
            needArchive = 1;
        }

        if (needArchive){
            switchAcctLog();
            if (acctArchiveInDays > 0){
                lastAcctCreationTime = now;
            }
        }
    }
}

int
switchAcctLog(void)
{
    static char fname[] = "switchAcctLog";
    int errnoSv;
    int totalAcctFile;

    if (joblog_fp)
        _fclose_(&joblog_fp);

    if (createAcct0File() == -1) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "createAcct0File");
        goto Acct_exiterr;
    }


    if (unlink(jlogFname) == -1 ) {
        ls_syslog(LOG_ERR,
                  "%s: unlink(%s) failed: %m",
                  fname,
                  jlogFname);
    }

    if ((joblog_fp = fopen(jlogFname, "a")) == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fopen",
                  jlogFname);
        goto Acct_exiterr;
    }

    /* after switch keep the joblog_fp file open.
     */

    errnoSv = errno;
    chmod(jlogFname, 0644);
    errno = errnoSv;

    if ((totalAcctFile = renameAcctLogFiles(maxAcctArchiveNum)) > 0)
        return 0;

    ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "renameAcctLogFiles");

Acct_exiterr:
    lsb_merr("Fail to switch lsb.acct; see mbatchd error log for details");
    return -1;
}

int
switch_log(void)
{
    char tmpfn[MAXFILENAMELEN];
    int i;
    int lineNum = 0;
    int errnoSv;
    LS_LONG_INT jobId = 0;
    FILE *efp;
    FILE *tmpfp;
    struct jData *jp;
    struct jData *jarray;
    long pos;
    int preserved = false;
    int totalEventFile;

    sprintf(tmpfn, "%s/logdir/lsb.events",
            daemonParams[LSB_SHAREDIR].paramValue);

    ls_syslog(LOG_INFO, "\
%s: switching event log file: %s", __func__, tmpfn);

    if (createEvent0File() == -1) { ;
        ls_syslog(LOG_ERR, "%s: failed in createEvent0File()", __func__);
        goto exiterr;
    }

    sprintf(tmpfn, "%s/logdir/lsb.events.tmp",
            daemonParams[LSB_SHAREDIR].paramValue);

    if ((tmpfp = fopen(tmpfn, "w")) == NULL) {
        ls_syslog(LOG_ERR, "%s: failed fopen %s: %m", __func__, tmpfn);
        goto exiterr;
    }

    if (fchmod(fileno(tmpfp), 0644) != 0) {
        ls_syslog(LOG_ERR, "%s: fchmod on %s failed: %m", __func__, tmpfn);
    }

    efp = fopen(elogFname, "r");
    if (efp == NULL) {
        ls_syslog(LOG_ERR, "%s: fopen(%s) failed: %m", __func__, elogFname);
        _fclose_(&tmpfp);
        goto exiterr;
    }

    fprintf(tmpfp, "#                                     \n");

    initHostCtrlTable();
    initQueueCtrlTable();

    if (lsberrno == LSBE_EOF)
        lsberrno = LSBE_NO_ERROR;

    while (lsberrno != LSBE_EOF) {

        if ((logPtr = lsb_geteventrec(efp, &lineNum)) == NULL) {
            if (lsberrno != LSBE_EOF) {
                ls_syslog(LOG_ERR, "\
%s: reading line %d in file %s: %s", __func__, lineNum,
                          elogFname, lsb_sysmsg());

                if (lsberrno == LSBE_NO_MEM) {
                    mbdDie(MASTER_MEM);
                }

            } else {

                break;
            }
            continue;
        }

        eventTime = logPtr->eventTime;

        switch (logPtr->type) {
            case EVENT_JOB_NEW:
            case EVENT_JOB_MODIFY:
                jobId = LSB_JOBID(logPtr->eventLog.jobNewLog.jobId,
                                  logPtr->eventLog.jobNewLog.idx);
                break;
            case EVENT_JOB_MODIFY2: {
                struct idxList *idxListP;
                int tmpJobId;
                if (getJobIdIndexList(logPtr->eventLog.jobModLog.jobIdStr,
                                      &tmpJobId, &idxListP) == LSBE_NO_ERROR)
                    jobId = tmpJobId;
                freeIdxList(idxListP);
                break;
            }
            case EVENT_PRE_EXEC_START:
            case EVENT_JOB_START:
                jobId = LSB_JOBID(logPtr->eventLog.jobStartLog.jobId,
                                  logPtr->eventLog.jobStartLog.idx);
                break;
            case EVENT_JOB_STATUS:
                jobId = LSB_JOBID(logPtr->eventLog.jobStatusLog.jobId,
                                  logPtr->eventLog.jobStatusLog.idx);
                break;
            case EVENT_JOB_SWITCH:
                jobId = LSB_JOBID(logPtr->eventLog.jobSwitchLog.jobId,
                                  logPtr->eventLog.jobSwitchLog.idx);
                break;
            case EVENT_JOB_MOVE:
                jobId = LSB_JOBID(logPtr->eventLog.jobMoveLog.jobId,
                                  logPtr->eventLog.jobMoveLog.idx);
                break;
            case EVENT_MIG:
                jobId = LSB_JOBID(logPtr->eventLog.migLog.jobId,
                                  logPtr->eventLog.migLog.idx);
                break;
            case EVENT_JOB_ATTR_SET:
                jobId = LSB_JOBID(logPtr->eventLog.jobAttrSetLog.jobId,
                                  logPtr->eventLog.jobAttrSetLog.idx);
                break;
            case EVENT_CHKPNT:
                jobId = LSB_JOBID(logPtr->eventLog.chkpntLog.jobId,
                                  logPtr->eventLog.chkpntLog.idx);
                break;
            case EVENT_JOB_SIGACT:
                jobId = LSB_JOBID(logPtr->eventLog.sigactLog.jobId,
                                  logPtr->eventLog.sigactLog.idx);
                break;
            case EVENT_JOB_SIGNAL:
                jobId = LSB_JOBID(logPtr->eventLog.signalLog.jobId,
                                  logPtr->eventLog.signalLog.idx);
                break;
            case EVENT_JOB_REQUEUE:
                jobId = LSB_JOBID(logPtr->eventLog.jobRequeueLog.jobId,
                                  logPtr->eventLog.jobRequeueLog.idx);
                break;
            case EVENT_JOB_CLEAN:
                jobId = LSB_JOBID(logPtr->eventLog.jobCleanLog.jobId,
                                  logPtr->eventLog.jobCleanLog.idx);
                break;
            case EVENT_MBD_UNFULFILL:
                jobId = LSB_JOBID(logPtr->eventLog.unfulfillLog.jobId,
                                  logPtr->eventLog.unfulfillLog.idx);
                break;
            case EVENT_LOG_SWITCH:
            case EVENT_LOAD_INDEX:
                preserved  = TRUE;
                break;
            case EVENT_JOB_EXECUTE:
                jobId = LSB_JOBID(logPtr->eventLog.jobExecuteLog.jobId,
                                  logPtr->eventLog.jobExecuteLog.idx);
                break;
            case EVENT_JOB_START_ACCEPT:
                jobId = LSB_JOBID(logPtr->eventLog.jobStartAcceptLog.jobId,
                                  logPtr->eventLog.jobStartAcceptLog.idx);
                break;
            case EVENT_JOB_MSG:
                jobId = LSB_JOBID(logPtr->eventLog.jobMsgLog.jobId,
                                  logPtr->eventLog.jobMsgLog.idx);
                break;
            case EVENT_JOB_MSG_ACK:
                jobId = LSB_JOBID(logPtr->eventLog.jobMsgAckLog.jobId,
                                  logPtr->eventLog.jobMsgAckLog.idx);
                break;
            case EVENT_HOST_CTRL:
                saveHostCtrlEvent(&(logPtr->eventLog.hostCtrlLog),
                                  eventTime);
                break;
            case EVENT_QUEUE_CTRL:
                saveQueueCtrlEvent(&(logPtr->eventLog.queueCtrlLog),
                                   eventTime);
            case EVENT_NEW_JGRP:
                /* If you can switch it don't preserve it
                 */
                if (can_switch_jgrp(&logPtr->eventLog.jgrpLog))
                    preserved = false;
                else
                    preserved = true;
                break;
            case EVENT_DEL_JGRP:
                preserved = false;
            default:
                break;
        }

        /* The head of the job array
         */
        jarray = checkJobInCore(LSB_ARRAY_JOBID(jobId));
        /* the job element
         */
        jp = checkJobInCore(jobId);

        if ((preserved)
            || (jarray != NULL
                && ((canSwitch(logPtr, jp) == FALSE)
                    || (logPtr->type == EVENT_JOB_NEW)
                    || (logPtr->type == EVENT_JOB_MODIFY)
                    || (logPtr->type == EVENT_JOB_MODIFY2)
                    || (logPtr->type == EVENT_JOB_SWITCH)
                    || (logPtr->type == EVENT_JOB_MOVE)
                    || (logPtr->type == EVENT_JOB_CLEAN)))) {

            if (lsb_puteventrec(tmpfp, logPtr) == -1) {
                ls_syslog(LOG_ERR, "\
%s: lsb_puteventrec(%s) failed: %s", __func__, tmpfn, lsb_sysmsg());
                _fclose_(&efp);
                unlink (tmpfn);
                goto exiterr;
            }
        }
        jobId = 0;
        preserved = false;
    }
    _fclose_(&efp);

    pos = ftell(tmpfp);
    rewind(tmpfp);
    fprintf(tmpfp, "#%ld", pos);
    if (_fclose_(&tmpfp)) {
        ls_syslog(LOG_ERR, "%s: fclose(%s) failed: %m", __func__, tmpfn);
        goto exiterr;
    }

    i = rename(tmpfn, elogFname);
    errnoSv = errno;
    chmod(elogFname, 0644);

    errno = errnoSv;
    if (i == -1) {
        ls_syslog(LOG_ERR, "\
%s: rename(%s, %s) failed: %m", __func__, tmpfn, elogFname);
        goto exiterr;
    }

    writeHostCtrlEvent();
    destroyHostCtrlTable();

    writeQueueCtrlEvent();
    destroyQueueCtrlTable();

    if (mSchedStage == M_STAGE_REPLAY)
        log_logSwitch(nextJobId_t);
    else
        log_logSwitch(nextJobId);

    if ((totalEventFile = renameElogFiles()) > 0)  {

        if (fork() == 0) {
            char  indexFile[MAXFILENAMELEN];

            sprintf(indexFile, "%s/logdir/%s",
                    daemonParams[LSB_SHAREDIR].paramValue,
                    LSF_JOBIDINDEX_FILENAME);

            if (updateJobIdIndexFile(indexFile, elogFname, totalEventFile) < 0) {
                if (lsberrno == LSBE_SYS_CALL)
                    ls_syslog(LOG_ERR, "\
%s: updateJobIdIndexFile(%s) failed: %s %m",
                              __func__, indexFile, lsb_sysmsg());
                else
                    ls_syslog(LOG_ERR, "\
%s: updateJobIdIndexFile(%s) failed: %s",
                              __func__, indexFile, lsb_sysmsg());
            }
            exit(0);
        }
        return 0;
    } else {
        ls_syslog(LOG_ERR, "%s: renameElogFiles() failed", __func__);
    }

exiterr:
    lsb_merr("\
Fail to switch lsb.events; see mbatchd error log for details");
    return -1;
}

static int
createAcct0File(void)
{
    static char fname[] = "createAcct0File";
    char acct0File[MAXFILENAMELEN];
    struct stat st;
    char buf[MSGSIZE];
    FILE *acctPtr, *acct0Ptr;
    int nread, cc, size;

    sprintf(acct0File, "%s/logdir/lsb.acct.0",
            daemonParams[LSB_SHAREDIR].paramValue);

    stat(jlogFname, &st);
    size = st.st_size;

    if ((acctPtr = fopen(jlogFname, "r")) == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fopen", jlogFname);
        _fclose_(&acctPtr);
        return -1;
    }

    if ((acct0Ptr  = fopen(acct0File, "w")) == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fopen", acct0File);
        _fclose_(&acctPtr);
        return -1;
    }
    chmod(acct0File, 0644);

    for (cc = 0, nread = 0; nread < size;) {

        if ((cc = fread(buf, 1, MSGSIZE, acctPtr)) > 0) {
            if (nread + cc > size)
                cc = size - nread;

            if (fwrite(buf, 1, cc, acct0Ptr) == 0) {
                ls_syslog(LOG_ERR, I18N_FUNC_FAIL_M, fname, "fwrite");
                _fclose_(&acctPtr);
                _fclose_(&acct0Ptr);
                return -1;
            }
            nread += cc;
        } else if (feof(acctPtr)){
            break;
        } else {
            ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fread", jlogFname);
            _fclose_(&acctPtr);
            _fclose_(&acct0Ptr);
            return -1;
        }
    }
    _fclose_(&acctPtr);
    _fclose_(&acct0Ptr);
    return 0;
}


static int
createEvent0File(void)
{
    static char fname[] = "createEvent0File";
    char event0File[MAXFILENAMELEN];
    char buf[MSGSIZE];
    char ch;
    struct stat st;
    int nread, cc, size;
    long int pos;
    FILE *eventPtr, *event0Ptr;

    sprintf(event0File, "%s/logdir/lsb.events.0",
            daemonParams[LSB_SHAREDIR].paramValue);

    stat(elogFname, &st);
    size = st.st_size;

    if ((eventPtr = fopen(elogFname, "r")) == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fopen", elogFname);
        _fclose_(&eventPtr);
        return -1;
    }

    if ((event0Ptr  = fopen(event0File, "w")) == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fopen", event0File);
        _fclose_(&eventPtr);
        return -1;
    }
    chmod(event0File, 0644);

    fprintf(event0Ptr, "#%ld                       \n", time(0));

    if (fscanf(eventPtr, "%c%ld ", &ch, &pos) != 2 || ch != '#') {
        pos = 0;
    }

    /* For the first switch skip the #80 string
     */
    if (pos == 80)
        pos = 4;

    if (fseek(eventPtr, pos, SEEK_SET) != 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "fseek");
        return -1;
    }

    size -= pos;

    for (cc = 0, nread = 0; nread < size;) {
        if ((cc = fread(buf, 1, MSGSIZE, eventPtr)) > 0) {
            if (nread + cc > size)
                cc = size - nread;

            if (fwrite(buf, 1, cc, event0Ptr) == 0) {
                ls_syslog(LOG_ERR, I18N_FUNC_FAIL_M, fname, "fwrite");
                _fclose_(&eventPtr);
                _fclose_(&event0Ptr);
                return -1;
            }
            nread += cc;
        } else if (feof(eventPtr)){
            break;
        } else {
            ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fread", elogFname);
            _fclose_(&eventPtr);
            _fclose_(&event0Ptr);
            return -1;
        }
    }
    _fclose_(&eventPtr);
    _fclose_(&event0Ptr);

    return 0;
}


static int
renameElogFiles(void)
{
    static char fname[] = "renameElogFiles";
    int i;
    int max;
    char tmpfn[MAXFILENAMELEN], eventfn[MAXFILENAMELEN];
    struct stat st;

    if (logclass & (LC_TRACE))
        ls_syslog(LOG_DEBUG, "%s: Entering ... ", fname);

    i = 0;
    do {
        sprintf(tmpfn, "%s/logdir/lsb.events.%d",
                daemonParams[LSB_SHAREDIR].paramValue, ++i);
    } while (stat(tmpfn, &st) == 0);

    if (errno != ENOENT) {
        ls_syslog(LOG_WARNING, I18N_FUNC_S_FAIL_M, fname, "stat", tmpfn);
    }

    max = i;
    while (i--) {

        sprintf(tmpfn, "%s/logdir/lsb.events.%d",
                daemonParams[LSB_SHAREDIR].paramValue, i);
        sprintf(eventfn, "%s/logdir/lsb.events.%d",
                daemonParams[LSB_SHAREDIR].paramValue, i + 1);

        if (rename(tmpfn, eventfn) == -1 && errno != ENOENT) {
            ls_syslog(LOG_ERR, I18N_FUNC_S_S_FAIL_M, fname,
                      "rename", tmpfn, eventfn);
            return -1;
        }
    }

    return max;
}

void
logJobInfo(struct submitReq * req, struct jData *jp, struct lenData * jf)
{
    static char fname[] = "logJobInfo";
    int                     errnoSv;
    char                    logFn[MAXFILENAMELEN];
    FILE                   *fp;
    mode_t                  omask = umask(077);
    sigset_t                newmask, oldmask;

    sprintf(logFn, "%s/logdir/info/%s",
            daemonParams[LSB_SHAREDIR].paramValue,
            jp->shared->jobBill.jobFile);

    sigemptyset(&newmask);
    sigaddset(&newmask, SIGCHLD);
    sigprocmask(SIG_BLOCK, &newmask, &oldmask);
    if ((fp = fopen(logFn, "w")) == NULL) {
        errnoSv = errno;
        sigprocmask(SIG_SETMASK, &oldmask, NULL);
        umask(omask);
        errno = errnoSv;
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fopen",
                  logFn);
        mbdDie(MASTER_FATAL);
    }
    sigprocmask(SIG_SETMASK, &oldmask, NULL);
    umask(omask);

    if (fwrite(jf->data, 1, jf->len, fp) != jf->len) {
        ls_syslog(LOG_ERR, "%s: fwrite(%s, len=%d) failed: %m",
                  fname,
                  logFn,
                  jf->len);
        _fclose_(&fp);
        goto error;
    }

    if (_fclose_(&fp) < 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fclose",
                  logFn);
        goto error;
    }

    return;

error:
    unlink(logFn);
    mbdDie(MASTER_FATAL);
}

int
rmLogJobInfo(struct jData *jp, int check)
{
    char logFn[MAXFILENAMELEN];
    struct stat st;
    struct submitReq *req;

    req = &jp->shared->jobBill;

    if (req->options2 & (SUB2_JOB_CMD_SPOOL | SUB2_IN_FILE_SPOOL)) {
        struct passwd pwUser;

        memset(&pwUser, 0, sizeof(pwUser));
        pwUser.pw_name = jp->userName;
        pwUser.pw_uid = jp->userId;

        if (req->options2 & SUB2_JOB_CMD_SPOOL) {

            childRemoveSpoolFile(req->commandSpool,
                                 FORK_REMOVE_SPOOL_FILE, &pwUser);
        }

        if (req->options2 & SUB2_IN_FILE_SPOOL) {

            childRemoveSpoolFile(req->inFileSpool,
                                 FORK_REMOVE_SPOOL_FILE, &pwUser);
        }
    }

    sprintf(logFn, "%s/logdir/info/%s",
            daemonParams[LSB_SHAREDIR].paramValue, req->jobFile);

    if (stat(logFn, &st) != 0) {
        sprintf(logFn, "%s/logdir/info/%d",
                daemonParams[LSB_SHAREDIR].paramValue,
                LSB_ARRAY_JOBID(jp->jobId));
    }

    if (stat(logFn, &st) == 0) {

        if (unlink(logFn) == -1) {
            if (check == FALSE)
                ls_syslog(LOG_ERR, "\
%s: job %s unlink() %s failed: %m", __func__, lsb_jobid2str(jp->jobId), logFn);
            return -1;
        }

    } else if (errno != ENOENT) {
        if (check == FALSE)
            ls_syslog(LOG_ERR, "\
%s: job %s stat() %s failed: %m", __func__, lsb_jobid2str(jp->jobId), logFn);
        return -1;
    }

    return 0;
}

int
readLogJobInfo(struct jobSpecs *jobSpecs, struct jData *jpbw,
               struct lenData *jf, struct lenData *aux_auth_data)
{
#define ENVEND "$LSB_TRAPSIGS\n"
    static char fname[] = "readLogJobInfo()";
    char logFn[MAXFILENAMELEN];
    LS_STAT_T st;
    int fd, i, numEnv,cc;
    char *buf, *sp, *edata, *eventAttrs = NULL;
    char *newBuf;

    jobSpecs->numEnv = 0;
    jobSpecs->env = NULL;
    jobSpecs->eexec.len = 0;
    jobSpecs->eexec.data = NULL;
    jf->len = 0;
    jf->data = NULL;

    sprintf (logFn, "%s/logdir/info/%s",
             daemonParams[LSB_SHAREDIR].paramValue,
             jpbw->shared->jobBill.jobFile);

    fd = open(logFn, O_RDONLY);
    if (fd < 0) {

        sprintf(logFn, "%s/logdir/info/%d",
                daemonParams[LSB_SHAREDIR].paramValue,
                LSB_ARRAY_JOBID(jpbw->jobId));
        fd = open(logFn, O_RDONLY);
    }

    if (fd < 0) {
        if (errno != ENOENT) {
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M,
                      fname,
                      lsb_jobid2str(jpbw->jobId),
                      "open",
                      logFn);
        }
        return -1;
    }

    fstat(fd, &st);
    buf = my_malloc(st.st_size, fname);
    if ((cc = read(fd, buf, st.st_size)) != st.st_size) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M,
                  fname,
                  lsb_jobid2str(jpbw->jobId),
                  "read",
                  logFn);
        close(fd);
        FREEUP(buf);
        return -1;
    }
    close(fd);

    for (sp = buf + strlen(SHELLLINE), numEnv = 0;
         strncmp(sp, ENVEND, sizeof(ENVEND) - 1); numEnv++) {

        sp = strstr(sp, TAILCMD);

        if (sp == NULL) {
            ls_syslog(LOG_ERR, "\
%s: Info file is corrupted for job <%d>; could not find %s (numEnv=%d)",
                      fname, LSB_ARRAY_JOBID(jpbw->jobId),
                      TAILCMD, numEnv);
            FREEUP(buf);
            return -1;
        }
        sp = strchr(sp, '\n');

        if (sp == NULL) {
            ls_syslog(LOG_ERR, "\
%s: Info file is corrupted for job <%d>; could not find newline character (numEnv=%d)",
                      fname, LSB_ARRAY_JOBID(jpbw->jobId), numEnv);
            FREEUP(buf);
            return -1;
        }
        sp++;
    }

    /* Save the effective rusage which dispatched
     * the job
     */
    if (jpbw->run_rusage)
        ++numEnv;

    if (jpbw->qPtr->jobStarter)
        numEnv++;

    if (numEnv) {

        jobSpecs->env = my_calloc(numEnv, sizeof(char *), fname);
        jobSpecs->numEnv = 0;

        for (sp = buf + strlen(SHELLLINE), i = 0;
             strncmp(sp, ENVEND, sizeof(ENVEND) - 1);) {

            char *spp, *tailst;
            char saveChar;

            tailst = strstr(sp, TAILCMD);
            saveChar = *tailst;
            *tailst = '\0';

            jobSpecs->env[i] = safeSave(sp);
            *tailst = saveChar;


            spp = strchr(jobSpecs->env[i], '=');
            if (spp == NULL) {
                ls_syslog(LOG_ERR,"\
%s: Info file seems corrupted, bad system syntax variable=%s in job=%d, trying to recover by skipping the variable",
                          fname,
                          jobSpecs->env[i],
                          LSB_ARRAY_JOBID(jpbw->jobId));
                FREEUP(jobSpecs->env[i]);
            } else {
                spp += 2;
                for (; *spp != '\0'; spp++)
                    *(spp - 1) = *spp;
                *(spp - 1) = '\0';
                i++;
            }

            sp = strchr(tailst, '\n') + 1;
            if (sp == NULL) {
                ls_syslog(LOG_ERR,"\
%s: Info file seems corrupted cannot locate new line, job=%d numEnv=%d",
                          fname, LSB_ARRAY_JOBID(jpbw->jobId),
                          numEnv);
            }
        }

        if (jpbw->run_rusage) {
            jobSpecs->env[i] = strdup(jpbw->run_rusage);
            ++i;
        }

        if (jpbw->qPtr->jobStarter) {
            jobSpecs->env[i] = my_malloc(strlen(jpbw->qPtr->jobStarter) + 1 +
                                         sizeof("LSB_JOB_STARTER="),
                                         "readLogJobInfo/job_starter");
            sprintf(jobSpecs->env[i], "LSB_JOB_STARTER=%s",
                    jpbw->qPtr->jobStarter);
            i++;
        }

        if (eventAttrs) {
            jobSpecs->env[i] = safeSave(eventAttrs);
            ls_syslog(LOG_DEBUG, "%s", eventAttrs);
            FREEUP (eventAttrs);
            i++;
        }
        jobSpecs->numEnv = i;
    } else {
        jobSpecs->env = NULL;
        jobSpecs->numEnv = 0;
    }
    edata = strstr(buf, EDATASTART);

    if (edata) {
        sp = edata + sizeof(EDATASTART) - 1;
        jobSpecs->eexec.len = atoi(sp);
        sp += strlen(sp) + 1;
        if (jobSpecs->eexec.len > 0) {
            jobSpecs->eexec.data = my_malloc(jobSpecs->eexec.len,
                                             "readLogJobinfo/eexec.data");
            memcpy(jobSpecs->eexec.data, sp, jobSpecs->eexec.len);
        } else {
            jobSpecs->eexec.len = 0;
            jobSpecs->eexec.data = NULL;
        }
    } else {
        ls_syslog(LOG_DEBUG, "%s: File <%s> of job <%d> has no <%s>; assume it is a pre-2.2 file", fname, logFn, LSB_ARRAY_JOBID(jpbw->jobId), EDATASTART);
        jobSpecs->eexec.len = 0;
        jobSpecs->eexec.data = NULL;
    }

    i = 0;

    sp = strstr(buf, EXITCMD);

    if (!sp) {

        ls_syslog(LOG_ERR, "\
%s: failed to find EXITCMD tag in jobfile, for job <%d>",
                  fname, LSB_ARRAY_JOBID(jpbw->jobId));
        return -1;
    }

    sp += strlen(EXITCMD);
    *sp = '\0';

    if ((jpbw->qPtr->jobStarter != NULL) &&
        (jpbw->qPtr->jobStarter[0] != '\0')) {

        if (checkJobStarter(buf, jpbw->qPtr->jobStarter)) {

            jf->data = buf;
            jf->len = strlen(jf->data) + 1;
            return 0;
        } else if ((newBuf = instrJobStarter1(buf, strlen(buf) + 1, CMDSTART,
                                              WAITCLEANCMD, jpbw->qPtr->jobStarter)) == NULL) {
            ls_syslog(LOG_ERR, "\
%s: Failed to insert job stater into jobfile data, job <%s>, queue <%s>",
                      fname, lsb_jobid2str(jpbw->jobId), jpbw->qPtr->queue);
            FREEUP(buf);
            return -1;
        } else {
            FREEUP(buf);
            jf->data = newBuf;
            jf->len = strlen(jf->data) + 1;
            return 0;
        }
    }

    jf->data = buf;
    jf->len = strlen(jf->data) +1;

    return 0;

}

char *
readJobInfoFile (struct jData *jp, int *len)
{
    static char fname[] = "readJobInfoFile";
    char logFn[MAXFILENAMELEN];
    LS_STAT_T st;
    int fd;
    char *buf;

    sprintf(logFn, "%s/logdir/info/%s",
            daemonParams[LSB_SHAREDIR].paramValue,
            jp->shared->jobBill.jobFile);

    fd = open(logFn, O_RDONLY);
    if (fd < 0) {

        sprintf(logFn, "%s/logdir/info/%d",
                daemonParams[LSB_SHAREDIR].paramValue,
                LSB_ARRAY_JOBID(jp->jobId));
        fd = open(logFn, O_RDONLY);
    }

    if (fd < 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M,
                  fname,
                  "open",
                  logFn);
        return NULL;
    }
    fstat(fd, &st);

    buf = (char *) my_malloc(st.st_size, fname);
    *len = st.st_size;
    if (read(fd, buf, st.st_size) != st.st_size) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M,
                  fname,
                  lsb_jobid2str(jp->jobId),
                  "read",
                  logFn);
        close(fd);
        free(buf);
        return NULL;
    }
    close(fd);
    return (buf);
}

void
writeJobInfoFile(struct jData *jp, char *jf, int len)
{

    static char             fname[] = "writeJobInfoFile";
    char                    logFn[MAXFILENAMELEN];
    int                     fd, errnoSv;

    sprintf(logFn, "%s/logdir/info/%s",
            daemonParams[LSB_SHAREDIR].paramValue,
            jp->shared->jobBill.jobFile);

    fd = open(logFn, O_CREAT | O_TRUNC | O_WRONLY, 0600);
    if (fd < 0) {
        sprintf(logFn, "%s/logdir/info/%d",
                daemonParams[LSB_SHAREDIR].paramValue,
                LSB_ARRAY_JOBID(jp->jobId));
        fd = open(logFn, O_CREAT | O_TRUNC | O_WRONLY, 0600);
    }
    if (fd < 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "open", logFn);
        mbdDie(MASTER_FATAL);
    }

    if (b_write_fix(fd, jf, len) != len) {
        errnoSv = errno;
        unlink(logFn);
        close(fd);
        errno = errnoSv;
        ls_syslog(LOG_ERR, I18N_FUNC_S_S_FAIL_M, fname, "fprintf",
                  logFn, jf);
        mbdDie(MASTER_FATAL);
    }

    close(fd);
}


int
replaceJobInfoFile(char *jobFileName,
                   char *newCommand,
                   char *jobStarter,
                   int options)
{
    static char fname[] = "replaceJobInfoFile";
    char jobFile[MAXFILENAMELEN];
    char workFile[MAXFILENAMELEN];
    char line[MAXLINELEN];
    char *ptr;
    int  nbyte;
    FILE *fdi, *fdo;

    sprintf(jobFile, "\
%s/logdir/info/%s", daemonParams[LSB_SHAREDIR].paramValue, jobFileName);
    sprintf(workFile, "\
%s/logdir/info/%s.tmp", daemonParams[LSB_SHAREDIR].paramValue,
            jobFileName);

    if ((fdi = fopen(jobFile, "r")) == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fopen", jobFileName);
        return -1;
    }

    if ((fdo = fopen(workFile, "w")) == NULL) {
        _fclose_(&fdi);
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fopen", workFile);
        return -1;
    }

    while ((ptr = fgets(line, MAXLINELEN, fdi)) != NULL) {
        if (strcmp(line, CMDSTART) != 0) {
            fputs(line, fdo);
        } else {
            char outCmdArgs[2*MAXLINELEN];
            char *pFileCmdArgs;

            fputs(line, fdo);

            if (options & REPLACE_1ST_CMD_ONLY) {
                char *oldCmdArgs;


                if ((ptr = fgets(line, MAXLINELEN, fdi)) == NULL) {
                    _fclose_(&fdo);
                    _fclose_(&fdi);
                    remove(workFile);
                    ls_syslog(LOG_ERR, "%s: Unexpected the end of (%s)",
                              fname,
                              jobFileName);
                    return -1;
                }

                if (jobStarter) {
                    oldCmdArgs = strstr(line, jobStarter);
                    if (oldCmdArgs) {
                        oldCmdArgs += strlen(jobStarter);
                    } else {
                        oldCmdArgs = line;
                    }
                } else {
                    oldCmdArgs = line;
                }

                if (replace1stCmd_(oldCmdArgs, newCommand,
                                   outCmdArgs, sizeof(outCmdArgs)) < 0) {
                    _fclose_(&fdo);
                    _fclose_(&fdi);
                    remove(workFile);
                    ls_syslog(LOG_ERR, "\
%s: The command line is too long when replacing the command of (%s) by the one of (%s)",
                              fname, oldCmdArgs,
                              newCommand);
                    return -1;

                }

                pFileCmdArgs = outCmdArgs;
            } else {
                pFileCmdArgs = newCommand;
            }

            if (jobStarter) {
                fprintf(fdo, "%s %s", jobStarter, pFileCmdArgs);
            } else {
                fprintf(fdo, "%s", pFileCmdArgs);
            }
            break;
        }
    }
    if (ptr == NULL) {
        _fclose_(&fdo);
        _fclose_(&fdi);
        remove(workFile);
        ls_syslog(LOG_ERR, "%s: Unexpected the end of (%s)",
                  __func__, jobFileName);
        return -1;
    }


    while ((ptr=fgets(line, MAXLINELEN, fdi)) != NULL) {
        if (strcmp(line, CMDEND) == 0) {
            ptr=fgets(line, MAXLINELEN, fdi);
            break;
        } else {
            if (options & REPLACE_1ST_CMD_ONLY) {
                fputs(line, fdo);
            }
        }
    }

    if (ptr == NULL) {
        _fclose_(&fdo);
        _fclose_(&fdi);
        remove(workFile);
        ls_syslog(LOG_ERR, "%s: Unexpected the end of (%s)",
                  fname,
                  jobFileName);
        return -1;
    }

    fputs(WAITCLEANCMD, fdo);

    while ((nbyte=fread(line,1, MAXLINELEN, fdi)) > 0)
        if (fwrite(line,1, nbyte, fdo) != nbyte) {
            _fclose_(&fdo);
            _fclose_(&fdi);
            remove(workFile);
            ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "fwrite", workFile);
            return -1;
        }

    _fclose_(&fdo);
    _fclose_(&fdi);
    remove(jobFile);
    rename(workFile, jobFile);
    return 0;
}
void
log_mbdStart(void)
{
    char hname[MAXHOSTNAMELEN];
    struct qData *qp;
    int num_queues;
    int num_hosts;

    if (openEventFile("log_mbdStart") < 0)
        mbdDie(MASTER_FATAL);

    logPtr->type = EVENT_MBD_START;
    if (masterHost)
        strcpy(logPtr->eventLog.mbdStartLog.master, masterHost);
    else if (gethostname(hname, MAXHOSTNAMELEN) >= 0)
        strcpy(logPtr->eventLog.mbdDieLog.master, hname);
    else
        strcpy(logPtr->eventLog.mbdStartLog.master, "unknown");

    if (clusterName)
        strcpy(logPtr->eventLog.mbdStartLog.cluster, clusterName);
    else
        strcpy(logPtr->eventLog.mbdStartLog.cluster, "unknown");


    num_queues = numofqueues;
    num_hosts = numofhosts();
    for (qp = qDataList->forw; qp != qDataList; qp = qp->forw) {
        if (strcmp(qp->queue, "lost_and_found") == 0) {
            num_queues--;
            break;
        }
    }
    if (h_getEnt_(&hostTab, "lost_and_found") != NULL) {
        num_hosts--;
    }
    logPtr->eventLog.mbdStartLog.numQueues = num_queues;
    logPtr->eventLog.mbdStartLog.numHosts = num_hosts;

    putEventRec("log_mbdStart");
}


static int
replay_modifyjob(char *filename, int lineNum)
{
    static char             fname[] = "replay_modifyjob";
    struct jData           *job, *jpbw;

    job = replay_jobdata (filename, lineNum, fname);
    if ((jpbw = getJobData (job->jobId)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6709,
                                         "%s: File %s at line %d: Job <%s> not found in job list"),
                  fname, filename, lineNum, lsb_jobid2str(job->jobId));
        return false;
    }

    handleJParameters(jpbw, job, &job->shared->jobBill, TRUE, 0, 0);
    if (jpbw->jgrpNode && ((job->shared->jobBill.options & SUB_JOB_NAME) ||
                           (job->shared->jobBill.options2 & SUB2_MODIFY_CMD))) {
        treeClip(jpbw->jgrpNode);
        treeFree(jpbw->jgrpNode);
        putOntoTree(jpbw, JOB_NEW);
    }
    else {
        jpbw->runCount = 1;
    }
    if (jpbw->nodeType == JGRP_NODE_ARRAY) {
        struct jData *jPtr;
        for (jPtr = jpbw->nextJob; jPtr != NULL; jPtr = jPtr->nextJob)
            updLocalJData(jPtr, jpbw);
    }
    freeJData (job);
    return true;
}

static int
replay_modifyjob2(char *filename, int lineNum)
{
    struct jobModLog       *jobModLog;
    struct modifyReq        modifyReq;
    struct lsfAuth          auth;
    int                     j;

    memset((void *)&auth, '\0', sizeof (auth));

    jobModLog = &logPtr->eventLog.jobModLog;

    auth.options = 0;
    auth.uid = jobModLog->userId;
    strcpy(auth.lsfUserName, jobModLog->userName);

    modifyReq.jobIdStr = jobModLog->jobIdStr;
    modifyReq.jobId = atoi(jobModLog->jobIdStr);
    modifyReq.delOptions = jobModLog->delOptions;
    modifyReq.delOptions2 = jobModLog->delOptions2;
    modifyReq.submitReq.options = jobModLog->options;
    modifyReq.submitReq.options2 = jobModLog->options2;
    modifyReq.submitReq.submitTime = jobModLog->submitTime;
    modifyReq.submitReq.umask = jobModLog->umask;
    modifyReq.submitReq.numProcessors = jobModLog->numProcessors;
    modifyReq.submitReq.beginTime = jobModLog->beginTime;
    modifyReq.submitReq.termTime = jobModLog->termTime;
    modifyReq.submitReq.sigValue = jobModLog->sigValue;
    modifyReq.submitReq.restartPid = jobModLog->restartPid;
    modifyReq.submitReq.jobName = jobModLog->jobName;
    modifyReq.submitReq.queue = jobModLog->queue;
    modifyReq.submitReq.numAskedHosts = jobModLog->numAskedHosts;
    if (jobModLog->numAskedHosts > 0)
        modifyReq.submitReq.askedHosts = jobModLog->askedHosts;
    modifyReq.submitReq.resReq = jobModLog->resReq;
    for (j = 0; j < LSF_RLIM_NLIMITS; j++)
        modifyReq.submitReq.rLimits[j] = jobModLog->rLimits[j];



    modifyReq.submitReq.hostSpec = jobModLog->hostSpec;
    modifyReq.submitReq.dependCond = jobModLog->dependCond;
    modifyReq.submitReq.subHomeDir = jobModLog->subHomeDir;
    modifyReq.submitReq.inFile = jobModLog->inFile;
    modifyReq.submitReq.outFile = jobModLog->outFile;
    modifyReq.submitReq.errFile = jobModLog->errFile;
    modifyReq.submitReq.command = jobModLog->command;
    modifyReq.submitReq.inFileSpool = jobModLog->inFileSpool;
    modifyReq.submitReq.commandSpool = jobModLog->commandSpool;
    modifyReq.submitReq.chkpntPeriod = jobModLog->chkpntPeriod;
    modifyReq.submitReq.chkpntDir = jobModLog->chkpntDir;
    modifyReq.submitReq.nxf = jobModLog->nxf;
    modifyReq.submitReq.xf = jobModLog->xf ;
    modifyReq.submitReq.jobFile = jobModLog->jobFile ;
    modifyReq.submitReq.fromHost = jobModLog->fromHost ;
    modifyReq.submitReq.cwd = jobModLog->cwd ;
    modifyReq.submitReq.preExecCmd = jobModLog->preExecCmd ;
    modifyReq.submitReq.mailUser = jobModLog->mailUser ;
    modifyReq.submitReq.projectName = jobModLog->projectName ;
    modifyReq.submitReq.niosPort = jobModLog->niosPort ;
    modifyReq.submitReq.maxNumProcessors = jobModLog->maxNumProcessors ;
    modifyReq.submitReq.loginShell = jobModLog->loginShell ;
    modifyReq.submitReq.schedHostType = jobModLog->schedHostType ;
    modifyReq.submitReq.userPriority = jobModLog->userPriority;
    modifyReq.submitReq.job_description = jobModLog->job_description;

    modifyJob(&modifyReq, NULL, &auth);
    return true;

}


static struct jData    *
replay_jobdata(char *filename, int lineNum, char *fname)
{
    struct jData *job;
    struct submitReq *jobBill;
    struct jobNewLog *jobNewLog;
    struct qData *qp;
    int i;
    int errcode;
    struct lsfAuth auth;

    memset(&auth, 0, sizeof (auth));

    jobNewLog = &logPtr->eventLog.jobNewLog;

    qp = getQueueData(jobNewLog->queue);
    if (qp == NULL) {
        ls_syslog(LOG_DEBUG, "\
%s: File %s at line %d: queue %s not found, saving job %d in queue %s",
                  __func__, filename, lineNum,
                  jobNewLog->queue, jobNewLog->jobId, LOST_AND_FOUND);
        qp = getQueueData(LOST_AND_FOUND);
        if (qp == NULL)
            qp = lostFoundQueue();
    }

    job = initJData(my_calloc(1, sizeof(struct jShared), __func__));

    if (job->jobSpoolDir) {
        FREEUP(job->jobSpoolDir);
    }

    job->qPtr = qp;
    job->userId = jobNewLog->userId;

    jobBill = &(job->shared->jobBill);
    jobBill->options = jobNewLog->options;
    jobBill->options2 = jobNewLog->options2;
    if (jobBill->options2 & SUB2_USE_DEF_PROCLIMIT) {

        jobBill->numProcessors =
            jobBill->maxNumProcessors =
            (qp->defProcLimit > 0 ? qp->defProcLimit : 1);
    }
    else {
        jobBill->numProcessors = jobNewLog->numProcessors;
        jobBill->maxNumProcessors = jobNewLog->maxNumProcessors;
    }
    jobBill->submitTime = jobNewLog->submitTime;
    jobBill->beginTime = jobNewLog->beginTime;
    jobBill->termTime = jobNewLog->termTime;
    jobBill->sigValue = jobNewLog->sigValue;
    jobBill->chkpntPeriod = jobNewLog->chkpntPeriod;
    job->chkpntPeriod = jobNewLog->chkpntPeriod;
    jobBill->restartPid = jobNewLog->restartPid;
    job->restartPid = jobNewLog->restartPid;

    for (i = 0; i < LSF_RLIM_NLIMITS; i++)
        jobBill->rLimits[i] = jobNewLog->rLimits[i];

    jobBill->umask = jobNewLog->umask;
    job->cpuTime = 0.0;
    job->jobId = jobNewLog->jobId;
    job->dispTime = now + retryIntvl * msleeptime;
    job->newReason = PEND_SYS_NOT_READY;
    job->oldReason = PEND_SYS_NOT_READY;
    if (debug)
        job->dispTime = 0;

    job->userName = safeSave(jobNewLog->userName);
    job->schedHost = safeSave (jobNewLog->schedHostType);
    jobBill->schedHostType = safeSave (jobNewLog->schedHostType);
    jobBill->queue = safeSave(qp->queue);
    jobBill->fromHost = safeSave(jobNewLog->fromHost);
    jobBill->jobFile = safeSave(jobNewLog->jobFile);
    jobBill->hostSpec = safeSave(jobNewLog->hostSpec);
    jobBill->inFile = safeSave(jobNewLog->inFile);
    jobBill->outFile = safeSave(jobNewLog->outFile);
    jobBill->errFile = safeSave(jobNewLog->errFile);
    jobBill->command = safeSave(jobNewLog->command);
    job->jobSpoolDir = safeSave (jobNewLog->jobSpoolDir);

    jobBill->inFileSpool = safeSave(jobNewLog->inFileSpool);
    jobBill->commandSpool = safeSave(jobNewLog->commandSpool);
    jobBill->preExecCmd = safeSave(jobNewLog->preExecCmd);
    jobBill->mailUser = safeSave(jobNewLog->mailUser);
    jobBill->dependCond = safeSave(jobNewLog->dependCond);
    jobBill->cwd = safeSave(jobNewLog->cwd);
    jobBill->subHomeDir = safeSave(jobNewLog->subHomeDir);
    jobBill->chkpntDir = safeSave(jobNewLog->chkpntDir);
    jobBill->resReq = safeSave(jobNewLog->resReq);
    jobBill->loginShell = safeSave(jobNewLog->loginShell);


    jobBill->numAskedHosts = jobNewLog->numAskedHosts;
    if (jobBill->numAskedHosts > 0) {
        int  numAskedHosts = 0;
        struct askedHost *askedHosts;
        int returnErr, badReqIndx, others;

        jobBill->askedHosts = my_calloc(jobBill->numAskedHosts,
                                        sizeof(char *), fname);
        for (i = 0; i < jobBill->numAskedHosts; i++)
            jobBill->askedHosts[i] = safeSave(jobNewLog->askedHosts[i]);

        returnErr = chkAskedHosts(jobBill->numAskedHosts,
                                  jobBill->askedHosts,
                                  jobBill->numProcessors, &numAskedHosts,
                                  &askedHosts, &badReqIndx, &others, 0);
        if (returnErr == LSBE_NO_ERROR) {
            if (numAskedHosts > 0) {
                job->askedPtr = my_calloc (numAskedHosts,
                                           sizeof(struct askedHost), fname);
                for (i = 0; i < numAskedHosts; i++) {
                    job->askedPtr[i].hData = askedHosts[i].hData;
                    job->askedPtr[i].priority = askedHosts[i].priority;
                }
            }
            job->numAskedPtr = numAskedHosts;
            job->askedOthPrio = others;
            FREEUP(askedHosts);
        }
    }

    if (strcmp(jobNewLog->projectName, "") == 0)
        jobBill->projectName = safeSave(getDefaultProject());
    else
        jobBill->projectName = safeSave(jobNewLog->projectName);

    if (jobBill->options & SUB_JOB_NAME)
        jobBill->jobName = safeSave(jobNewLog->jobName);
    else
        jobBill->jobName = safeSave("");

    auth.uid = jobNewLog->userId;
    strcpy(auth.lsfUserName, jobNewLog->userName);

    if (strcmp(jobBill->dependCond, "") == 0) {
        job->shared->dptRoot = NULL;
    } else {
        setIdxListContext(jobBill->jobName);

        job->shared->dptRoot
            = parseDepCond(jobBill->dependCond, &auth, &errcode,
                           NULL, &job->jFlags, 0);
        freeIdxListContext();
        if (!job->shared->dptRoot)  {
            job->jFlags |= JFLAG_DEPCOND_INVALID;
        } else
            job->jFlags &= ~JFLAG_DEPCOND_INVALID;
    }

    if (jobBill->resReq  && jobBill->resReq[0] != '\0') {
        int useLocal = TRUE;

        if (job->numAskedPtr > 0 || job->askedOthPrio >= 0)
            useLocal = FALSE;

        useLocal = useLocal ? USE_LOCAL : 0;
        if ((job->shared->resValPtr =
             checkResReq (jobBill->resReq,
                          useLocal | PARSE_XOR | CHK_TCL_SYNTAX)) == NULL)
            ls_syslog(LOG_ERR, "\
%s: File %s at line %d: Bad resource requirement %s job %s",
                      __func__, filename, lineNum, jobBill->resReq,
                      lsb_jobid2str(job->jobId));
    }
    jobBill->nxf = jobNewLog->nxf;
    if (jobBill->nxf > 0) {
        jobBill->xf = my_calloc(jobBill->nxf,
                                sizeof(struct xFile), fname);
        memcpy(jobBill->xf, jobNewLog->xf, jobBill->nxf * sizeof(struct xFile));
    } else {
        jobBill->xf = NULL;
    }

    jobBill->niosPort = jobNewLog->niosPort;

    jobBill->userPriority = jobNewLog->userPriority;
    job->jobPriority = jobBill->userPriority;

    jobBill->userGroup = strdup(jobNewLog->userGroup);

    job->abs_run_limit = jobNewLog->abs_run_limit;

    jobBill->job_group = strdup(jobNewLog->job_group);

    jobBill->job_description = strdup(jobNewLog->job_description);

    /* Save the new job priority in case a previous job
     * was bbot
     */
    job->priority = jobNewLog->priority;

    return job;
}

void
log_logSwitch(int lastJobId)
{
    if (openEventFile("log_logSwitch") < 0) {
        LS_LONG_INT tmpJobId;
        tmpJobId = lastJobId;
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S, __func__,
                  lsb_jobid2str(tmpJobId), "openEventFile");
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_LOG_SWITCH;
    logPtr->eventLog.logSwitchLog.lastJobId = lastJobId;
    if (putEventRec("log_logSwitch") < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S, __func__,
                  lastJobId, "putEventRec");
        return;
    }
}


void
log_signaljob(struct jData * jp, struct signalReq * signalReq, int userId,
              char *userName)
{
    static char             fname[] = "log_signaljob";
    int  sigValue;
    int defSigValue;

    if ((signalReq->sigValue != SIG_DELETE_JOB)
        && (signalReq->sigValue != SIG_KILL_REQUEUE)
        && (signalReq->sigValue != SIG_ARRAY_REQUEUE)) {


        defSigValue = getDefSigValue_(signalReq->sigValue,
                                      jp->qPtr->terminateActCmd);
        if (defSigValue == signalReq->sigValue) {
            if (IS_PEND(jp->jStatus))
                defSigValue = SIGKILL;
            else
                return;
        }
    }

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S, fname,
                  lsb_jobid2str(jp->jobId), "openEventFile");
        mbdDie(MASTER_FATAL);
    }
    logPtr->type = EVENT_JOB_SIGNAL;
    logPtr->eventLog.signalLog.jobId = LSB_ARRAY_JOBID(jp->jobId);
    logPtr->eventLog.signalLog.idx = LSB_ARRAY_IDX(jp->jobId);
    logPtr->eventLog.signalLog.userId = userId;
    strcpy(logPtr->eventLog.signalLog.userName, userName);
    if (signalReq->sigValue == SIG_DELETE_JOB) {
        logPtr->eventLog.signalLog.signalSymbol = "DELETEJOB";
        logPtr->eventLog.signalLog.runCount = signalReq->chkPeriod;
    } else if (signalReq->sigValue == SIG_KILL_REQUEUE) {
        logPtr->eventLog.signalLog.signalSymbol = "KILLREQUEUE";
        logPtr->eventLog.signalLog.runCount = signalReq->chkPeriod;
    } else if (signalReq->sigValue == SIG_ARRAY_REQUEUE) {
        logPtr->eventLog.signalLog.signalSymbol
            = getSignalSymbol(signalReq);

        logPtr->eventLog.signalLog.runCount = signalReq->actFlags;
    } else {
        sigValue = sig_encode(defSigValue);
        logPtr->eventLog.signalLog.signalSymbol
            = getSigSymbol(sigValue);
        logPtr->eventLog.signalLog.runCount = jp->runCount;
    }

    if (putEventRec("log_signaljob") < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S, fname,
                  lsb_jobid2str(jp->jobId), "putEventRec");
        mbdDie(MASTER_FATAL);
    }
}

static char *
getSignalSymbol(const struct signalReq *sigPtr)
{
    static char      nameBuf[32];


    if (sigPtr->chkPeriod == JOB_STAT_PSUSP) {

        sprintf(nameBuf, "REQUEUE_PSUSP");

    } else {
        sprintf(nameBuf, "REQUEUE_PEND");
    }

    return(nameBuf);

}

void
log_jobmsg(struct jData *jPtr, struct lsbMsg *jmsg)
{
    char file[PATH_MAX];
    FILE *fp;

    sprintf(file, "%s/logdir/messages/%s",
            daemonParams[LSB_SHAREDIR].paramValue,
            lsb_jobid2str2(jPtr->jobId));

    fp = open_job_msg_file(file);
    if (fp == NULL) {
        ls_syslog(LOG_ERR, "%s: failed in openmsg_file() job %s",
                  __func__, lsb_jobid2str(jPtr->jobId));
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_JOB_MSG;
    logPtr->eventLog.jobMsgLog.jobId = LSB_ARRAY_JOBID(jPtr->jobId);
    logPtr->eventLog.jobMsgLog.idx = LSB_ARRAY_IDX(jPtr->jobId);
    logPtr->eventLog.jobMsgLog.msg = jmsg->msg;

    if (put_job_msg(fp, __func__) < 0) {
        ls_syslog(LOG_ERR, "%s: failed in putEventRec() job %s",
                  __func__, lsb_jobid2str(jPtr->jobId));
        mbdDie(MASTER_FATAL);
    }

    _fclose_(&fp);
}

static int
replay_signaljob(char *filename, int lineNum)
{
    static char fname[] = "replay_signaljob";
    struct jData *jp;
    LS_LONG_INT jobId;
    int cc;

    jobId = LSB_JOBID(logPtr->eventLog.signalLog.jobId,
                      logPtr->eventLog.signalLog.idx);
    if ((jp = getJobData(jobId)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6709,
                                         "%s: File %s at line %d: Job <%d> not found in job list "),
                  fname, filename, lineNum, logPtr->eventLog.signalLog.jobId);
        return false;
    }


    cc = replay_arrayrequeue(jp,
                             &(logPtr->eventLog.signalLog));
    if (cc == 0) {
        return true;
    }

    if (strcmp(logPtr->eventLog.signalLog.signalSymbol, "DELETEJOB") == 0){
        jp->pendEvent.sigDel = TRUE;

    }
    else if (strcmp(logPtr->eventLog.signalLog.signalSymbol, "KILLREQUEUE")
             == 0) {
        jp->pendEvent.sigDel |= DEL_ACTION_REQUEUE;
    }
    else
        getSigVal(logPtr->eventLog.signalLog.signalSymbol);

    if (jp->nodeType == JGRP_NODE_ARRAY) {
        return true;
    }

    if (strcmp(logPtr->eventLog.signalLog.signalSymbol, "DELETEJOB") == 0) {

        if (IS_FINISH(jp->jStatus)){
            jp->runCount = logPtr->eventLog.signalLog.runCount;

        }else if (IS_PEND(jp->jStatus)) {
            if (logPtr->eventLog.signalLog.runCount == 0) {
                jp->runCount = 1;
                jStatusChange(jp, JOB_STAT_EXIT, logPtr->eventTime, fname);
            } else
                jp->runCount = logPtr->eventLog.signalLog.runCount;

        } else {
            if (logPtr->eventLog.signalLog.runCount == 0) {
                jp->runCount = 1;
                jp->pendEvent.sig = SIGKILL;
                eventPending = TRUE;
            } else
                jp->runCount = logPtr->eventLog.signalLog.runCount + 1;
        }
    }
    return true;

}


static int
replay_arrayrequeue(struct jData *jPtr,
                    const struct signalLog *sigLogPtr)
{
    struct signalReq      sigReq;
    char                  *p;

    sigReq.sigValue = SIG_ARRAY_REQUEUE;


    p = sigLogPtr->signalSymbol;

    if (strcmp("REQUEUE_PSUSP", p) == 0) {

        sigReq.chkPeriod = JOB_STAT_PSUSP;
        sigReq.actFlags   = sigLogPtr->runCount;

    } else if (strcmp("REQUEUE_PEND", p) == 0) {

        sigReq.chkPeriod = JOB_STAT_PEND;
        sigReq.actFlags  = sigLogPtr->runCount;

    } else {
        return -1;
    }

    arrayRequeue(jPtr, &sigReq, NULL);

    return 0;

}

static void
ckSchedHost (void)
{
    static char fname[] = "ckSchedHost";
    int list;
    struct jData *jp;
    struct hData *hp;

    if (logclass & LC_SCHED)
        ls_syslog(LOG_DEBUG3, "%s: Entering this routine....", fname);

    for (list = 0; list < ALLJLIST; list++) {
        for (jp = jDataList[list]->forw; (jp != jDataList[list]);
             jp = jp->forw) {
            if ((hp = getHostData (jp->shared->jobBill.fromHost)) != NULL) {
                if (strcmp (jp->shared->jobBill.schedHostType, "") == 0
                    || strcmp (jp->schedHost, "") == 0) {
                    free(jp->schedHost);
                    free(jp->shared->jobBill.schedHostType);
                    jp->schedHost = safeSave(hp->hostType);
                    jp->shared->jobBill.schedHostType = safeSave(hp->hostType);
                    if (logclass & LC_SCHED)
                        ls_syslog(LOG_DEBUG3, "%s: job=%s fromHost=%s schedHost=%s", fname, lsb_jobid2str(jp->jobId), jp->shared->jobBill.fromHost, jp->schedHost);
                }
            }
        }
    }
}


static bool_t
replay_jobforce(char* file, int line)
{

    static char                fname[] = "replay_jobforce()";
    struct jobForceRequestLog* jobForceRequestLog;
    struct jData*              job;
    LS_LONG_INT                jobId;

    jobForceRequestLog = &(logPtr->eventLog.jobForceRequestLog),

        jobId = LSB_JOBID(jobForceRequestLog->jobId, jobForceRequestLog->idx);
    job = getJobData(jobId);
    if (job == NULL) {
        ls_syslog(LOG_ERR, "\
%s: JobId <%d> at line <%d> cannot be found by master daemon",
                  fname, jobForceRequestLog->jobId, line);
        return false;
    }

    job->jFlags |= (jobForceRequestLog->options & RUNJOB_OPT_NOSTOP) ?
        JFLAG_URGENT_NOSTOP : JFLAG_URGENT;

    return true;

}

static int
canSwitch(struct eventRec *logPtr, struct jData *jp)
{
    if (jp) {
        if (jp->startTime > 0 && logPtr->eventTime < jp->startTime)
            return TRUE;
        else
            return FALSE;
    } else
        return TRUE;

    return FALSE;
}

void
log_jobattrset(struct jobAttrInfoEnt *jobAttr, int uid)
{
    static char fname[] = "log_jobattrset()";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S, fname,
                  lsb_jobid2str(jobAttr->jobId), "openEventFile");
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_JOB_ATTR_SET;
    logPtr->eventLog.jobAttrSetLog.uid = uid;
    logPtr->eventLog.jobAttrSetLog.jobId = LSB_ARRAY_JOBID(jobAttr->jobId);
    logPtr->eventLog.jobAttrSetLog.idx = LSB_ARRAY_IDX(jobAttr->jobId);
    logPtr->eventLog.jobAttrSetLog.port = (int) jobAttr->port;
    logPtr->eventLog.jobAttrSetLog.hostname = jobAttr->hostname;

    if (putEventRec(fname) < 0)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S, fname,
                  lsb_jobid2str(jobAttr->jobId), "putEventRec");
}


static int
replay_jobattrset(char *filename, int lineNum)
{
    static char                fname[] = "replay_jobattrset()";
    struct jobAttrSetLog*      jobAttrSetLog;
    struct jData*              job;
    LS_LONG_INT                jobId;

    jobAttrSetLog = &(logPtr->eventLog.jobAttrSetLog);

    jobId = LSB_JOBID(jobAttrSetLog->jobId, jobAttrSetLog->idx);
    if ((job = getJobData(jobId)) == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6866,
                                         "%s: JobId <%d> at line <%d> cannot be found by master daemon"), /* catgets 6866 */
                  fname, jobAttrSetLog->jobId, lineNum);
        return false;
    }
    job->port = (u_short) jobAttrSetLog->port;

    return true;
}

static struct jData *
checkJobInCore(LS_LONG_INT jobId)
{
    struct jData *jp = NULL;
    struct listSet *ptr;

    if ((jp = getJobData(jobId)) != NULL)
        return jp;

    for (ptr = voidJobList; ptr; ptr = ptr->next) {
        if (jobId == ((struct jData *)ptr->elem)->jobId ||
            jobId == LSB_ARRAY_JOBID(((struct jData *)ptr->elem)->jobId)) {
            return (struct jData *)ptr->elem;
        }
    }
    return NULL;
}

static char *
instrJobStarter1(char *data,
                 int  datalen,
                 char *begin,
                 char *end,
                 char *jstr)
{
    static char fname[] = "instrJobStarter1()";
    char        *jstr_header = NULL;
    char        *jstr_tailer = NULL;
    char        *ptr1 = NULL;
    char        *ptr2 = NULL;
    int         int_input = 0;
    int         withKeyword = FALSE;
    char        *tmpBuf;

    tmpBuf = (char *) malloc(datalen + strlen(jstr) + 1);
    if (tmpBuf == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "malloc", "tmpBuf");
        return NULL;
    }
    memset((char *)tmpBuf, 0, datalen + strlen(jstr) + 1);


    ptr1 = strstr(jstr, JOB_STARTER_KEYWORD);
    if (ptr1 == NULL) {
        withKeyword = FALSE;
        jstr_header = (char *) malloc(strlen(jstr)+sizeof(char));
        if (jstr_header == NULL) {
            ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "malloc",
                      "jstr_header");
            FREEUP(tmpBuf);
            return NULL;
        } else {
            strcpy(jstr_header, jstr);
        }
        jstr_tailer = "";
    } else {
        withKeyword = TRUE;
        if (jstr == ptr1) {
            jstr_header = NULL;
            jstr_tailer = ptr1+strlen(JOB_STARTER_KEYWORD);
        } else {
            jstr_header = (char *) malloc((ptr1-jstr)+sizeof(char));
            if (jstr_header == NULL) {
                ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname,
                          "malloc","jstr_header");
                FREEUP(tmpBuf);
                return NULL;
            } else {
                memset(jstr_header, '\0', (ptr1-jstr)+sizeof(char));
                strncpy(jstr_header, jstr, (ptr1-jstr));
            }
            jstr_tailer = ptr1+strlen(JOB_STARTER_KEYWORD);
        }
    }


    ptr1 = strstr(data, begin);

    if (ptr1 == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "strstr", "CMDSTART");
        FREEUP(jstr_header);
        FREEUP(tmpBuf);
        return NULL;
    }

    ptr1 = strchr(ptr1, '\n');

    if (ptr1 == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "strstr", "CMDSTART");
        FREEUP(jstr_header);
        FREEUP(tmpBuf);
        return NULL;
    }


    if ((ptr2 = strstr(ptr1, "\n$LSB_JOBFILENAME.shell"))!= NULL) {
        int_input = 1;
        ptr1 = ptr2;
    }

    ptr1 = ptr1 + sizeof(char);


    strncat(tmpBuf, data, ptr1-data);

    if (jstr_header != NULL){
        strncat(tmpBuf, jstr_header, strlen(jstr_header));
        if (jstr_tailer[0]=='\0' && !withKeyword) {

            strncat(tmpBuf, " ", 1);
        }
    }

    if (int_input == 1)
        ptr2 = strchr (ptr1, '\n');
    else
        ptr2 = strstr(ptr1, end);

    if (ptr2 == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "strstr","WAITCLEANCMD");
        FREEUP(jstr_header);
        FREEUP(tmpBuf);
        return NULL;
    }

    strncat(tmpBuf, ptr1, ptr2-ptr1);

    if (strlen(jstr_tailer) != 0) {
        strncat(tmpBuf, jstr_tailer, strlen(jstr_tailer));
    }
    strcat(tmpBuf, ptr2);
    FREEUP(jstr_header);
    return(tmpBuf);
}


static int
checkJobStarter(char *data, char *jStarter)
{
    char *ptr;
    char *header;
    char *tailer;

    ptr = strstr(jStarter, JOB_STARTER_KEYWORD);
    if (!ptr) {

        if (strstr(data, jStarter))
            return true;
        else
            return false;
    } else {
        if (jStarter == ptr) {
            tailer = ptr + strlen(JOB_STARTER_KEYWORD);
            SKIPSPACE(tailer);
            if (*tailer == '\0')
                return false;
            if (strstr(data, tailer))
                return true;
            else
                return false;
        } else {
            int retcode;
            char saver;

            header = jStarter;
            saver = *ptr;
            *ptr = '\0';
            if (!strstr(data, header)) {
                retcode = FALSE;
            } else {
                tailer = ptr + strlen(JOB_STARTER_KEYWORD);
                SKIPSPACE(tailer);
                if (*tailer == '\0')
                    retcode = TRUE;
                else if (!strstr(data, tailer))
                    retcode = FALSE;
                else
                    retcode = TRUE;
            }
            *ptr = saver;
            return(retcode);
        }
    }
}

static void
initHostCtrlTable(void)
{
    h_initTab_(&hostCtrlEvents, 64);

}

static void
destroyHostCtrlTable(void)
{
    h_freeTab_(&hostCtrlEvents, destroyHostCtrlEvent);

}

static void
destroyHostCtrlEvent(void *ptr)
{
    struct hostCtrlEvent *ctrlPtr;

    ctrlPtr = (struct hostCtrlEvent *)ptr;

    free(ctrlPtr->hostCtrlLog);
    free(ctrlPtr);

}

/* saveHostCtrlEvent()
 */
static void
saveHostCtrlEvent(struct hostCtrlLog *hostCtrlLog,
                  const time_t       eventTime)
{
    static char             fname[] = "saveHostCtrlEvent()";
    hEnt                    *hEntry;
    int                     new;
    struct hData            *hPtr;
    struct hostCtrlEvent    *ctrlPtr;


    if (hostCtrlLog->opCode != HOST_CLOSE) {
        return;
    }

    hPtr = getHostData(hostCtrlLog->host);
    if (hPtr == NULL) {
        return;
    }


    if (! (hPtr->hStatus & HOST_STAT_DISABLED)) {
        return;
    }

    hEntry = h_getEnt_(&hostCtrlEvents, hostCtrlLog->host);
    if (hEntry == NULL) {


        ctrlPtr =
            (struct hostCtrlEvent *)
            my_calloc(1,
                      sizeof(struct hostCtrlEvent),
                      fname);

        ctrlPtr->hostCtrlLog =
            (struct hostCtrlLog *)
            my_calloc(1,
                      sizeof(struct hostCtrlLog),
                      fname);

        ctrlPtr->time = eventTime;
        memcpy(ctrlPtr->hostCtrlLog,
               hostCtrlLog,
               sizeof(struct hostCtrlLog));

        hEntry = h_addEnt_(&hostCtrlEvents,
                           ctrlPtr->hostCtrlLog->host,
                           &new);
        hEntry->hData = ctrlPtr;

    } else {


        ctrlPtr = (struct hostCtrlEvent *)hEntry->hData;

        ctrlPtr->time = eventTime;
        memcpy((int *)ctrlPtr->hostCtrlLog,
               (int *)hostCtrlLog,
               sizeof(struct hostCtrlLog));
    }

}

static void
writeHostCtrlEvent(void)
{
    struct hostCtrlEvent *ctrlPtr;
    hEnt *ent;
    sTab next;

    ent = h_firstEnt_(&hostCtrlEvents, &next);
    while (ent) {

        ctrlPtr = ent->hData;
        ctrlPtr->hostCtrlLog->opCode = HOST_CLOSE;

        log_hostStatusAtSwitch(ctrlPtr);
        ent = h_nextEnt_(&next);
    }

}

static void
log_hostStatusAtSwitch(const struct hostCtrlEvent *ctrlPtr)
{
    static char    fname[] = "log_hostStatusAtSwitch";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_HOST_FAIL,
                  fname,
                  ctrlPtr->hostCtrlLog->host,
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_HOST_CTRL;

    logPtr->eventLog.hostCtrlLog.opCode
        = ctrlPtr->hostCtrlLog->opCode;

    strcpy(logPtr->eventLog.hostCtrlLog.host,
           ctrlPtr->hostCtrlLog->host);

    logPtr->eventLog.hostCtrlLog.userId
        = ctrlPtr->hostCtrlLog->userId;

    strcpy(logPtr->eventLog.hostCtrlLog.userName,
           ctrlPtr->hostCtrlLog->userName);

    strcpy(logPtr->eventLog.hostCtrlLog.message, ctrlPtr->hostCtrlLog->message);

    if (putEventRecTime(fname, ctrlPtr->time) < 0) {
        ls_syslog(LOG_ERR, I18N_HOST_FAIL,
                  fname,
                  ctrlPtr->hostCtrlLog->host,
                  "putEventRecTime");
    }

}

static void
initQueueCtrlTable(void)
{
    h_initTab_(&closedQueueEvents, 64);
    h_initTab_(&inactiveQueueEvents, 64);

}

static void
destroyQueueCtrlTable(void)
{
    h_freeTab_(&closedQueueEvents, destroyQueueCtrlEvent);
    h_freeTab_(&inactiveQueueEvents, destroyQueueCtrlEvent);

}

static void
destroyQueueCtrlEvent(void *ptr)
{
    struct queueCtrlEvent    *ctrlPtr;

    ctrlPtr = (struct queueCtrlEvent *)ptr;

    free(ctrlPtr->queueCtrlLog);
    free(ctrlPtr);

}
static void
saveQueueCtrlEvent(struct queueCtrlLog *queueCtrlLog,
                   const time_t        eventTime)
{
    struct qData    *qPtr;


    if ( (queueCtrlLog->opCode != QUEUE_CLOSED)
         && (queueCtrlLog->opCode != QUEUE_INACTIVATE)) {
        return;
    }

    qPtr = getQueueData(queueCtrlLog->queue);
    if (qPtr == NULL) {
        return;
    }


    if ( (queueCtrlLog->opCode == QUEUE_CLOSED)
         && ( ! (qPtr->qStatus & QUEUE_STAT_OPEN))) {

        queueTableUpdate(&closedQueueEvents, queueCtrlLog);

    }


    if ( (queueCtrlLog->opCode == QUEUE_INACTIVATE)
         && ( ! (qPtr->qStatus & QUEUE_STAT_ACTIVE))) {

        queueTableUpdate(&inactiveQueueEvents, queueCtrlLog);

    }

}
/* queueTableUpdate()
 */
static void
queueTableUpdate(hTab                 *tbPtr,
                 struct queueCtrlLog  *queueCtrlLog)
{
    static char              fname[] = "queueTableUpdate";
    int                      new;
    hEnt                     *hEntry;
    struct queueCtrlEvent    *ctrlPtr;

    hEntry = h_getEnt_(tbPtr, queueCtrlLog->queue);
    if (hEntry == NULL) {

        ctrlPtr =  my_calloc(1,
                             sizeof(struct queueCtrlEvent),
                             fname);

        ctrlPtr->queueCtrlLog = my_calloc(1,
                                          sizeof(struct queueCtrlLog),
                                          fname);
        ctrlPtr->time = eventTime;
        memcpy(ctrlPtr->queueCtrlLog,
               queueCtrlLog,
               sizeof(struct queueCtrlLog));

        hEntry = h_addEnt_(tbPtr,
                           ctrlPtr->queueCtrlLog->queue,
                           &new);
        hEntry->hData = ctrlPtr;

    } else {


        ctrlPtr = (struct queueCtrlEvent *)hEntry->hData;

        ctrlPtr->time = eventTime;
        memcpy((int *)ctrlPtr->queueCtrlLog,
               (int *)queueCtrlLog,
               sizeof(struct queueCtrlLog));
    }

}

/* writeQueueCtrlEvent()
 */
static void
writeQueueCtrlEvent(void)
{
    struct queueCtrlEvent *ctrlPtr;
    hEnt *ent;
    sTab next;

    ent = h_firstEnt_(&closedQueueEvents, &next);
    while (ent) {

        ctrlPtr = ent->hData;
        ctrlPtr->queueCtrlLog->opCode = QUEUE_CLOSED;
        log_queueStatusAtSwitch(ctrlPtr);

        ent = h_nextEnt_(&next);
    }

    ent = h_firstEnt_(&inactiveQueueEvents, &next);
    while (ent) {

        ctrlPtr = ent->hData;
        ctrlPtr->queueCtrlLog->opCode = QUEUE_INACTIVATE;
        log_queueStatusAtSwitch(ctrlPtr);
        ent = h_nextEnt_(&next);
    }
}

static void
log_queueStatusAtSwitch(const struct queueCtrlEvent *ctrlPtr)
{
    static char    fname[] = "log_queueStatusAtSwitch";

    if (openEventFile(fname) < 0) {
        ls_syslog(LOG_ERR, I18N_QUEUE_FAIL,
                  fname,
                  ctrlPtr->queueCtrlLog->queue,
                  "openEventFile");
        mbdDie(MASTER_FATAL);
    }

    logPtr->type = EVENT_QUEUE_CTRL;

    logPtr->eventLog.queueCtrlLog.opCode
        = ctrlPtr->queueCtrlLog->opCode;

    strcpy(logPtr->eventLog.queueCtrlLog.queue,
           ctrlPtr->queueCtrlLog->queue);

    logPtr->eventLog.queueCtrlLog.userId
        = ctrlPtr->queueCtrlLog->userId;

    strcpy(logPtr->eventLog.queueCtrlLog.userName,
           ctrlPtr->queueCtrlLog->userName);

    if (putEventRecTime(fname, ctrlPtr->time) < 0) {
        ls_syslog(LOG_ERR, I18N_QUEUE_FAIL,
                  fname,
                  ctrlPtr->queueCtrlLog->queue,
                  "putEventRecTime");
    }

}

static int
renameAcctLogFiles(int fileLimit)
{
    static char fname[] = "renameAcctLogFiles";
    int i;
    int max;
    char tmpfn[MAXFILENAMELEN], acctfn[MAXFILENAMELEN];
    struct stat st;

    if (logclass & (LC_TRACE))
        ls_syslog(LOG_DEBUG, "%s: Entering ... ", fname);

    i = 0;
    if (fileLimit > 0){
        do {
            sprintf(tmpfn, "%s/logdir/lsb.acct.%d",
                    daemonParams[LSB_SHAREDIR].paramValue, ++i);
        } while ((stat(tmpfn, &st) == 0) && (i < fileLimit));
    } else {
        do {
            sprintf(tmpfn, "%s/logdir/lsb.acct.%d",
                    daemonParams[LSB_SHAREDIR].paramValue, ++i);
        } while (stat(tmpfn, &st) == 0);
    }

    if ((errno != ENOENT) && (errno != 0)) {
        ls_syslog(LOG_WARNING, I18N_FUNC_S_FAIL_M, fname, "stat", tmpfn);
    }

    max = i;
    while (i--) {

        sprintf(tmpfn, "%s/logdir/lsb.acct.%d",
                daemonParams[LSB_SHAREDIR].paramValue, i);
        sprintf(acctfn, "%s/logdir/lsb.acct.%d",
                daemonParams[LSB_SHAREDIR].paramValue, i + 1);

        if (rename(tmpfn, acctfn) == -1 && errno != ENOENT) {
            ls_syslog(LOG_ERR, I18N_FUNC_S_S_FAIL_M, fname,
                      "rename", tmpfn, acctfn);
            return -1;
        }
    }

    return max;
}

static void
jobMsgInit(void)
{
    char dirbuf[PATH_MAX];
    char fpath[PATH_MAX];
    DIR *dir;
    struct dirent *ent;
    FILE *fp;
    int num;
    int cc;

    sprintf(dirbuf, "%s/logdir/messages",
            daemonParams[LSB_SHAREDIR].paramValue);

    cc = checkDirAccess(dirbuf);
    if (cc < 0) {
        ls_syslog(LOG_ERR, "\
%s: gudness problem accessing message directory", __func__);
        return;
    }

    dir = opendir(dirbuf);
    if (dir == NULL) {
        ls_syslog(LOG_ERR, "%s: cannot open %s? we just checked it: %m",
                  __func__, dirbuf);
        return;
    }

    while ((ent = readdir(dir))) {

        if (strcmp(ent->d_name, ".") == 0
            || strcmp(ent->d_name, "..") == 0)
            continue;

        sprintf(fpath, "%s/%s", dirbuf, ent->d_name);

        fp = fopen(fpath, "r");
        if (fp == NULL) {
            ls_syslog(LOG_ERR, "\
%s: failed to open %s %m", __func__, ent->d_name);
            continue;
        }

        lsberrno = LSBE_NO_ERROR;
        while (lsberrno != LSBE_EOF) {

            logPtr = lsb_geteventrec(fp, &num);
            if (logPtr == NULL) {
                if (lsberrno == LSBE_EOF) {
                    break;
                } else {
                    ls_syslog(LOG_ERR, "\
%s: Error reading record number %d file %s: %M",
                              __func__, num, fpath);
                    continue;
                }
            }
            if (! replay_event(fpath, num)) {
                ls_syslog(LOG_ERR, "\
%s: file %s line %d replay_event() failed, ignoring line", __func__,
                          ent->d_name, num);
                continue;
            }
        }

        fclose(fp);
    }

    closedir(dir);

}

static int
checkDirAccess(const char *dir)
{
    struct stat sbuf;

    if (stat(dir, &sbuf) < 0) {
        if (mkdir(dir, 0700) == -1
            && errno != EEXIST) {
            ls_syslog(LOG_ERR, "%s: mkdir() %s failed: %m", __func__, dir);
            return -1;
        }
    }

    return 0;
}

/* streamEvent()
 *
 * Write the lsb.stream and reset it every
 * million records written. The stream file
 * is an external interface to system events.
 */
static int
streamEvent(struct eventRec *logPtr)
{
    static FILE *fp;
    static int n;
    static char streamFile[PATH_MAX];
    static char streamFile2[PATH_MAX];
    struct eventRec logPtr2;
    char *mode;

    if (mbdParams->maxStreamRecords == 0)
        return 0;

    if (fp == NULL) {

        sprintf(streamFile, "%s/logdir/stream/lsb.stream.0",
                daemonParams[LSB_SHAREDIR].paramValue);

        mode = "w";
        if (logPtr->type == EVENT_MBD_START) {
            /* MBD starting let's count the records
             * in the stream file to test.
             */
            n = countStream(streamFile);
            if (n > 0) {
                mode = "a";
                /* This in theory should never happen unless
                 * MBD dies unexpectedly not writing
                 * EVENT_MBD_DIE
                 */
                if (n == mbdParams->maxStreamRecords)
                    n = n - 1;
            }
        }

        fp = fopen(streamFile, mode);
        if (fp == NULL) {
            ls_syslog(LOG_ERR, "\
%s: failed to open stream %s %m", __func__, streamFile);
            return -1;
        }
    }

    if (lsb_puteventrec(fp, logPtr) < 0) {
        ls_syslog(LOG_ERR, "\
%s: lsb_puteventrec() failed %s", __func__, lsb_sysmsg());
        return -1;
    }

    n = n + 1;

    if ((n % mbdParams->maxStreamRecords) == 0) {

        logPtr2.type = EVENT_STREAM_END;
        sprintf(logPtr2.version, "%d", OPENLAVA_XDR_VERSION);
        logPtr2.eventTime = logPtr->eventTime;
        logPtr2.eventLog.eos.numRecords = n;

        if (lsb_puteventrec(fp, &logPtr2) < 0) {
            ls_syslog(LOG_ERR, "\
%s: lsb_puteventrec() failed %s", __func__, lsb_sysmsg());
            return -1;
        }

        sprintf(streamFile2, "%s/logdir/stream/lsb.stream.1",
                daemonParams[LSB_SHAREDIR].paramValue);

        fclose(fp);
        fp = NULL;
        n = 0;

        if (rename(streamFile, streamFile2) != 0) {
            ls_syslog(LOG_ERR, "\
%s: rename() from %s to %s failed %m", __func__, streamFile, streamFile2);
            /* Keep writing in the old file...
             */
            fp = fopen(streamFile, "a");
            n = n + 1;
            return -1;
        }

        fp = fopen(streamFile, "w");
        if (fp == NULL) {
            ls_syslog(LOG_ERR, "\
%s: failed to open stream %s %m", __func__, streamFile);
            return -1;
        }
    }

    if (fp)
        fflush(fp);

    return 0;
}

/* countStream()
 *
 * Count records in the stream file
 */
static int
countStream(char *file)
{
    FILE *fp;
    int cc;
    int n;

    fp = fopen(file, "r");
    if (fp == NULL)
        return 0;

    n = 0;
    while ((cc = fgetc(fp)) != EOF) {
        if (cc == '\n')
            ++n;
    }
    fclose(fp);

    return n;
}

/* open_msg_file()
 */
static FILE *
open_job_msg_file(const char *fname)
{
    sigset_t newmask;
    sigset_t oldmask;
    FILE *fp;

    sigemptyset(&newmask);
    sigaddset(&newmask, SIGCHLD);
    sigprocmask(SIG_BLOCK, &newmask, &oldmask);

    if ((fp = fopen(fname, "a+")) == NULL) {
        sigprocmask(SIG_SETMASK, &oldmask, NULL);
        ls_syslog(LOG_ERR, "\
%s: fopen() %s failed: %m", __func__, elogFname);
        return NULL;
    }

    sigprocmask(SIG_SETMASK, &oldmask, NULL);
    fseek(fp, 0L, SEEK_END);
    chmod(elogFname, 0644);

    /* allocate the log record
     */
    logPtr = my_calloc(1, sizeof(struct eventRec), __func__);

    /* Use the same version as the main protocol.
     */
    sprintf(logPtr->version, "%d", OPENLAVA_XDR_VERSION);

    return fp;
}

/* put_job_msg()
 */
static int
put_job_msg(FILE *fp, const char *fname)
{
    if (lsb_puteventrec(fp, logPtr) < 0) {
        ls_syslog(LOG_ERR, "\
%s: lsb_puteventrec() failed %s", __func__, lsb_sysmsg());
        return -1;
    }

    /* free the log record
     */
    free(logPtr);

    if (fflush(fp) != 0) {
        ls_syslog(LOG_ERR, "%s: fflush() failed %m", __func__);
        return -1;
    }

    return 0;
}

/* do_switch_child_end()
 *
 * This function is invoked by the parent mbatchd after
 * the switch child has finished. It closes the lsb.events.parent
 * file and merges it into the newly switched lsb.events.
 */
static int
do_switch_child_end(void)
{
    if (logclass & LC_SWITCH) {
        ls_syslog(LOG_INFO, "\
%s: switch child pid %d gone %d", __func__,
                  swchild->pid, swchild->child_gone);
    }

    /* Examine exit status of the switch child
     */
    if (WIFEXITED(swchild->status)) {
        ls_syslog(LOG_INFO, "\
%s: switch child exit with status %d", __func__,
                  WEXITSTATUS(swchild->status));
    }

    if (WIFSIGNALED(swchild->status)) {
        ls_syslog(LOG_INFO, "\
%s: switch child got signal %d and core dump %s", __func__,
                  WTERMSIG(swchild->status),
                  WCOREDUMP(swchild->status) ? "yes" : "no");
    }

    swchild->pid = 0;
    swchild->child_gone = false;
    swchild->status = 0;

    /* This may or may not be open, it depends
     * if a logging event came in before or after
     * the switch child has finished. While the
     * switch child is running log_fp is the FILE
     * to lsb.events.parent.
     */
    _fclose_(&log_fp);

    merge_switch_file();

    return 0;
}

/* make_switch_pidfile()
 *
 * This file serves its purpose should the parent
 * mbatchd restart and come back while the child
 * is switching. If the parent detects this file it
 * waits until the child is done.
 */
static int
make_switch_pidfile(void)
{
    FILE *fp;
    char buf[MAXFILENAMELEN];

    /* remove and older switch.pid if there
     */
    rm_switch_pidfile();

    sprintf(buf, "%s/logdir/switch.pid",
            daemonParams[LSB_SHAREDIR].paramValue);

    fp = fopen(buf, "w");
    if (fp == NULL) {
        ls_syslog(LOG_ERR, "\
%s: fopen() %s failed %m", __func__, buf);
        return -1;
    }

    fprintf(fp, "%d\n", (int)getpid());
    fclose(fp);

    return 0;
}

/* rm_switch_pidfile()
 */
static int
rm_switch_pidfile(void)
{
    char buf[MAXFILENAMELEN];

    sprintf(buf, "%s/logdir/switch.pid",
            daemonParams[LSB_SHAREDIR].paramValue);
    unlink(buf);

    return 0;
}

/* wait_for_switch_child()
 *
 * Detect if switch child is around and wait
 * for it if it is still running.
 */
static void
wait_for_switch_child(void)
{
    char buf[MAXFILENAMELEN];
    struct stat sbuf;
    FILE *fp;
    int pid;

    if (logclass & LC_SWITCH) {
        ls_syslog(LOG_INFO, "\
%s: checking if switch child still running", __func__);
    }
    /* Make the switch child
     */
    swchild = calloc(1, sizeof(struct switch_child));
    swchild->child_gone = false;

    sprintf(buf, "%s/logdir/switch.pid",
            daemonParams[LSB_SHAREDIR].paramValue);

    /* even we have not found the switch pid
     * still see if there is a parent event file
     * to merge. It is quite possible the child
     * cleaned its pid and then the parent reconfig.
     */
    if (stat(buf, &sbuf) < 0)
        goto merge;

    fp = fopen(buf, "r");
    if (fp == NULL) {
        ls_syslog(LOG_ERR, "\
%s: fopen() %s failed but stat() ok.. %m", __func__, buf);
        return;
    }

    fscanf(fp, "%d", &pid);
    fclose(fp);

    if (pid <= 0) {
        ls_syslog(LOG_INFO, "\
%s: strange switch pid %d found...", __func__, pid);
        goto merge;
    }

    ls_syslog(LOG_INFO, "\
%s: detected switch child at %s with pid %d", __func__, buf, pid);

    while ((kill(pid, 0) == 0)) {
        ls_syslog(LOG_INFO, "\
%s: switch child still around pid %d", __func__, pid);
        millisleep_(2000);
    }

    ls_syslog(LOG_INFO, "%s: switch child gone...", __func__);

merge:
    unlink(buf);
    /* The child is gone see if we have to merge
     * the parent events and the switched events.
     */
    merge_switch_file();
}

/* merge_switch_file()
 */
static int
merge_switch_file(void)
{
    FILE *fp_child;
    FILE *fp_parent;
    struct stat sbuf;
    int cc;
    int nread;
    int size;
    char buf[BUFSIZ];

    if (logclass & LC_SWITCH) {
        ls_syslog(LOG_INFO, "\
%s: check if there are parent events to merge", __func__);
    }

    /* If there is no lsb.events.parent there were
     * no events while the child was switching, there
     * are no files to merge.
     */
    if (stat(elogFname2, &sbuf) < 0) {
        if (errno != ENOENT) {
            ls_syslog(LOG_ERR, "\
%s: stat() failed on %s file %m", __func__, elogFname2);
        }
        return -2;
    }
    size = sbuf.st_size;
    /* fopen the switched lsb.events
     */
    fp_child = fopen(elogFname, "a+");
    if (fp_child == NULL) {
        ls_syslog(LOG_ERR, "\
%s: failed to open %s %m", __func__, elogFname);
        return -1;
    }

    /* fopen the lsb.events.parent
     */
    fp_parent = fopen(elogFname2, "r");
    if (fp_parent == NULL) {
        ls_syslog(LOG_ERR, "\
%s: failed to open %s %m", __func__, elogFname2);
        _fclose_(&fp_child);
        return -1;
    }

    /* Merge the switched file with the temporary
     * events managed by mbatchd parent.
     */
    for (cc = 0, nread = 0; nread < size;) {

        if ((cc = fread(buf, 1, BUFSIZ, fp_parent)) > 0) {
            if (nread + cc > size)
                cc = size - nread;

            if (fwrite(buf, 1, cc, fp_child) == 0) {
                ls_syslog(LOG_ERR, "\
%s: fwrite(%s) %d bytes failed %m", __func__, elogFname, cc);
                _fclose_(&fp_parent);
                _fclose_(&fp_child);
                return -1;
            }
            nread += cc;
        } else if (feof(fp_parent)){
            break;
        } else {
            ls_syslog(LOG_ERR, "%s: fread(%s) failed %m", __func__, elogFname);
            _fclose_(&fp_parent);
            _fclose_(&fp_child);
            return -1;
        }
    }

    if (logclass & LC_SWITCH) {
        ls_syslog(LOG_INFO, "\
%s: merged %d bytes from %s into %s", __func__, nread,
                  elogFname2, elogFname);
    }

    _fclose_(&fp_parent);
    _fclose_(&fp_child);
    unlink(elogFname2);

    return 0;
}
