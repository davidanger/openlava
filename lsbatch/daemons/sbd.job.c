/*
 * Copyright (C) 2014-2016 David Bigagli
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 *
 */
#include "sbd.h"
#include <time.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <grp.h>
#include <pwd.h>
#ifdef HAVE_HWLOC_H
#include <numa.h>
#endif
#include "../../lsf/intlib/jidx.h"
#include "../../lsf/lib/lib.rcp.h"
#include "../daemons/daemons.h"

#define MAX_PREEXEC_ENVS  ((MAXLINELEN + 1)/3 + 2)


extern char **environ;

extern int lsbStdoutDirect;
static void shouldCopyFromLsbatch(struct jobCard *jp,
                                  int *cpyStdoutFromLsbatch,
                                  int *cpyStderrFromLsbatch );
static int isLink(char *filename);

static void getJobTmpDir( char * tmpDirName, struct jobCard *jobCardPtr );
static void createJobTmpDir(struct jobCard *jobCardPtr);

static void execJob(struct jobCard *jobCardPtr, int chfd);
static int finishJob(struct jobCard *jobCardPtr);
static int setLimits(struct jobSpecs *jobSpecsPtr);
static int mysetLimits(struct jobSpecs *jobSpecsPtr);
static int send_results (struct jobCard *);
static int sendNotification(struct jobCard *);
static int setPGid(struct jobCard *jc);
static char *getLoginShell(char *jfDada, char *jobFile,
                           struct hostent *hp, int readFile);
static int createTmpJobFile(struct jobSpecs *jobSpecsPtr,
                            struct hostent *hp, char *stdinFile);
static void runQPre(struct jobCard *);

static void collectPreStatus(struct jobCard *, int, char *);
static int requeueJob (struct jobCard *);
static char **execArgs(struct jobSpecs *jp, char **execArgv);
static void jobFinishRusage(struct jobCard *jp);
static void initJRusage(struct jRusage *);
static int getJobVersion (struct jobSpecs *);
extern int sbdlog_newstatus (struct jobCard *jp);
extern void copyJUsage(struct jRusage *to, struct jRusage *from);
extern int jRunSuspendAct(struct jobCard *jp, int sigValue, int jState,
                          int reasons, int subReasons, logType logFlag);
extern int resumeJob (struct jobCard *jp, int sigValue,
                      int suspendReasons, logType logFlag);
static int sbdStartupStopJob (struct jobCard *jp, int reasons, int subReasons);

static int chPrePostUser(struct jobCard *jp);
static void sbdChildCloseChan (int execptChan);
static int REShasPTYfix(char *);
static void setJobArrayEnv(char *, int);
extern int getpwnamRetry;
struct passwd *my_getpwnam(const char *name, char *caller);
extern int lsbMemEnforce;
extern char *yybuff;
extern int lsbJobCpuLimit;
extern int lsbJobMemLimit;

extern void ls_closelog_ext(void);
extern int cpHostent(struct hostent *, const struct hostent *);
static int acctMapTo(struct jobCard *jobCard);
static void setup_mem_cgroup(struct jobCard *);
static void rm_mem_cgroup(struct jobCard *);
static void setup_cpu_bind(struct jobCard *, int*);
static int* select_cpu_to_bind(struct jobCard *);

static struct passwd *
__getpwnam__(const char *name)
{
    int counter = 1;
    struct passwd * pw = NULL;

    while (((pw = getpwnam(name)) == NULL) && (counter < getpwnamRetry)) {
        if (logclass & LC_EXEC)
            ls_syslog(LOG_DEBUG1, "\
%s: getpwnam(%s) failed %d times:%m", __func__, name, counter);

        ++counter;
        millisleep_(1000);
    }
    return pw;
}

static void
sbdChildCloseChan(int exceptChan)
{
    struct clientNode *cliPtr, *nextClient;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG1,"sbdChildCloseChan: Entering...");

    for (cliPtr = clientList->forw;
         cliPtr != clientList; cliPtr=nextClient) {
        nextClient = cliPtr->forw;

        if (cliPtr->chanfd != exceptChan)
            shutDownClient(cliPtr);
    }

    CLOSECD(statusChan);
}


sbdReplyType
job_exec(struct jobCard *jobCardPtr, int chfd)
{
    struct jobSpecs *jobSpecsPtr;
    int pid;
    int *cores;

    jobSpecsPtr = &(jobCardPtr->jobSpecs);
    if (logclass & LC_EXEC) {
        ls_syslog(LOG_DEBUG, "\
%s: the Job's JobSpoolDir is %s \n",
                  __func__, jobSpecsPtr->jobSpoolDir);
    }

    jobSpecsPtr->reasons = 0;
    jobSpecsPtr->subreasons = 0;

    cores = select_cpu_to_bind(jobCardPtr);
    pid = fork();

    if (pid < 0) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh fork() failed starting job %s: %m",
                  __func__, lsb_jobid2str(jobSpecsPtr->jobId));
        FREEUP(cores);
        return ERR_FORK_FAIL;
    }

    /* centos6 does not support hwloc API to bind memory on process pid, so
     * let the child process bind itself to local memory
     */
    if (pid == 0) {
#ifdef HAVE_HWLOC_H
        int localonly;

        /* Hard code for Cadence case!!!
         * Replace following code with "affinity" interface.
         */
        if ((daemonParams[SBD_BIND_CPU].paramValue || hostAffinity) &&
            strstr(jobSpecsPtr->resReq, "affinity")) {
            if (strstr(jobSpecsPtr->resReq, "membind=localonly"))
                localonly = 1;
            else
                localonly = 0;
            bind_to_numa_mem(cores, localonly);
        }
#endif
        closeBatchSocket();
        sbdChildCloseChan (chfd);
        execJob(jobCardPtr, chfd);
        exit(-1);
    }

    jobSpecsPtr->jobPid = pid;

    if (jobSpecsPtr->options & SUB_RESTART)
        jobSpecsPtr->jobPGid = 0;
    else
        jobSpecsPtr->jobPGid = pid;

    jobCardPtr->missing = FALSE;
    jobCardPtr->notReported = 0;
    jobCardPtr->needReportRU = FALSE;

    /* Setup the memory cgroup for the child
     * we just started
     */
    setup_mem_cgroup(jobCardPtr);

    /* Setup the cpu binding
     */
    setup_cpu_bind(jobCardPtr, cores);

    return ERR_NO_ERROR;
}

static int
sendNotification(struct jobCard *jobCardPtr)
{
    static char fname[] = "sendNotification";
    struct jobSpecs *jobSpecsPtr = &(jobCardPtr->jobSpecs);
    FILE *mail;
    char myhostnm[MAXHOSTNAMELEN];
    int  k;
    char *temp1;

    if (jobSpecsPtr->options & SUB_MAIL_USER)
        mail = smail(jobSpecsPtr->mailUser, jobSpecsPtr->fromHost);
    else
        mail = smail(jobSpecsPtr->userName, jobSpecsPtr->fromHost);


    if (gethostname(myhostnm, MAXHOSTNAMELEN) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jobSpecsPtr->jobId), "gethostname");
        STRNCPY(myhostnm, "localhost", sizeof(myhostnm));
    }
    fprintf(mail, _i18n_msg_get(ls_catd , NL_SETN, 400,
                                "Sender: OpenLava System <%s@%s>\n"), /* catgets 400 */
            lsbManager, myhostnm);
    fprintf(mail, _i18n_msg_get(ls_catd , NL_SETN, 401,
                                "Subject: Job %s: <%s> started.\n"), /* catgets 401 */
            lsb_jobid2str(jobSpecsPtr->jobId), jobSpecsPtr->jobName);

    fprintf(mail, "\n");
    fprintf(mail, _i18n_msg_get(ls_catd , NL_SETN, 402,
                                "Job <%s> was submitted from host <%s> by user <%s>. "),/* catgets 402 */
            jobSpecsPtr->jobName, jobSpecsPtr->fromHost, jobSpecsPtr->userName);
    temp1 = putstr_(_i18n_msg_get(ls_catd , NL_SETN, 404,
                                  "unknown time in the past")); /* catgets 404 */
    fprintf(mail, _i18n_msg_get(ls_catd , NL_SETN, 405,
                                "Started at %sunder user <%s>, "),  /* catgets 405 */
            jobSpecsPtr->startTime? _i18n_ctime(ls_catd, CTIME_FORMAT_DEFAULT,
                                                &jobSpecsPtr->startTime) : temp1, jobCardPtr->execUsername );
    free(temp1);

    fprintf(mail, _i18n_msg_get(ls_catd , NL_SETN, 407,
                                "on host(s) <%s>, while in queue <%s>.\n"),  /* catgets 407 */
            myhostnm, jobSpecsPtr->queue);
    for (k = 1; k < jobSpecsPtr->numToHosts; k++)
        fprintf(mail, "                   <%s>\n", jobSpecsPtr->toHosts[k]);
    now=time(0);
    fprintf(mail, _i18n_msg_get(ls_catd , NL_SETN, 408,
                                "Notification reported at %s"), /* catgets 408 */
            _i18n_ctime(ls_catd, CTIME_FORMAT_DEFAULT, &now));

    mclose(mail);
    return 0;
}

void
getJobTmpDir(char *tmpDirName, struct jobCard *jPtr)
{
    char jobId[16];

    strcpy(tmpDirName, lsTmpDir_) ;
    strcat(tmpDirName, "/" );
    sprintf(jobId, "%s", lsb_jobidinstr(jPtr->jobSpecs.jobId) );
    strcat(tmpDirName, jobId );
    strcat(tmpDirName, ".tmpdir" );
}

static void
createJobTmpDir(struct jobCard *jobCardPtr)
{
    char tmpDirName[MAXFILENAMELEN];
    mode_t previousUmask;

    getJobTmpDir(tmpDirName, jobCardPtr);
    previousUmask = umask(077);

    if ((mkdir(tmpDirName, 0700) == -1)
        && errno != EEXIST) {
        ls_syslog(LOG_ERR, "\
%s: Unable to create job tmp directory %s for job %s",
                  __func__, tmpDirName,
                  lsb_jobid2str(jobCardPtr->jobSpecs.jobId));
        tmpDirName[0] = 0;
    }

    umask(previousUmask);

    if (daemonParams[LSB_SET_TMPDIR].paramValue != NULL) {
        if (tmpDirName[0] != 0
            && !strcasecmp(daemonParams[LSB_SET_TMPDIR].paramValue, "y")) {
            putEnv("TMPDIR", tmpDirName );
        }
    }
}

static void
execJob(struct jobCard *jobCardPtr, int chfd)
{
    int i;
    int cc;
    struct jobSpecs *jobSpecsPtr;
    struct hostent *fromHp;
    struct lenData jf;
    char *shellPath = NULL;
    sigset_t newmask;
    XDR xdrs;
    char buf[MSGSIZE];
    struct LSFHeader replyHdr;

    jobSpecsPtr =  &(jobCardPtr->jobSpecs);
    jobSpecsPtr->jobPid = getpid();
    jobSpecsPtr->jobPGid = jobSpecsPtr->jobPid;
    jobCardPtr->stdinFile = NULL;

    if (rcvJobFile(chfd, &jf) == -1) {
        ls_syslog(LOG_ERR, "\
%s: failed receiving job file job %s", __func__,
                  lsb_jobid2str(jobSpecsPtr->jobId));
        jobSetupStatus(JOB_STAT_PEND, PEND_JOB_NO_FILE, jobCardPtr);
    }

    if (daemonParams[LSB_BSUBI_OLD].paramValue
        || !PURE_INTERACTIVE(jobSpecsPtr)) {

        xdrmem_create(&xdrs, buf, MSGSIZE, XDR_DECODE);
        cc = readDecodeHdr_(chfd, buf, chanRead_, &xdrs, &replyHdr);
        if (cc < 0) {
            ls_syslog(LOG_WARNING, "\
%s: Fail to get go-ahead from mbatchd; abort job %s cc %d %m",
                      __func__, lsb_jobid2str(jobSpecsPtr->jobId), cc);

            jobSetupStatus(JOB_STAT_PEND, PEND_JOB_START_FAIL, jobCardPtr);
        }

        xdr_destroy(&xdrs);
        chanClose_(chfd);
    }

    ls_syslog(LOG_DEBUG, "\
%s: Got job start ok from mbatchd for job <%s>",
              __func__, lsb_jobid2str(jobSpecsPtr->jobId));

    if (acctMapTo(jobCardPtr) < 0)  {
        jobSetupStatus(JOB_STAT_PEND, PEND_NO_MAPPING, jobCardPtr);
    }

    if ((jobCardPtr->jobSpecs.jobPGid = setPGid(jobCardPtr)) < 0)
        jobSetupStatus(JOB_STAT_PEND, PEND_JOB_EXEC_INIT, jobCardPtr);

    putEnv(LS_EXEC_T, "START");

    if (setJobEnv(jobCardPtr) < 0) {
        ls_syslog(LOG_DEBUG, "\
%s: setJobEnv() failed for job <%s>", __func__,
                  lsb_jobid2str(jobSpecsPtr->jobId));
        jobSetupStatus(JOB_STAT_PEND, PEND_JOB_ENV, jobCardPtr);
    }

    umask(jobCardPtr->jobSpecs.umask);

    runQPre(jobCardPtr);
    nice(NICE_LEAST);

    if (!debug)
        nice(NICE_MIDDLE);

    nice(0);

    errno = 0;
    if (nice(jobCardPtr->jobSpecs.nice) == -1 && errno != 0) {
        ls_syslog(LOG_ERR, "\
%s: nice(%d) failed %m", __func__, jobSpecsPtr->nice);
    }

    if (setLimits(jobSpecsPtr) < 0)
        jobSetupStatus(JOB_STAT_PEND, PEND_JOB_EXEC_INIT, jobCardPtr);

    if (jobSpecsPtr->lsfLimits[LSF_RLIMIT_FSIZE].rlim_maxl != 0xffffffff
        || jobSpecsPtr->lsfLimits[LSF_RLIMIT_FSIZE].rlim_maxh != 0x7fffffff
        || jobSpecsPtr->lsfLimits[LSF_RLIMIT_FSIZE].rlim_curl != 0xffffffff
        || jobSpecsPtr->lsfLimits[LSF_RLIMIT_FSIZE].rlim_curh != 0x7fffffff)
        ls_closelog_ext();

    if (setIds(jobCardPtr) < 0) {
        jobSetupStatus(JOB_STAT_PEND, PEND_JOB_EXEC_INIT, jobCardPtr);
    }

    if ((fromHp = Gethostbyname_(jobSpecsPtr->fromHost)) == NULL) {
        jobSetupStatus(JOB_STAT_PEND, PEND_JOB_EXEC_INIT, jobCardPtr);
    }

    if (initPaths(jobCardPtr, fromHp, &jf) < 0) {
        jobSetupStatus(JOB_STAT_PEND, PEND_JOB_PATHS, jobCardPtr);
    }

    for (i = 1; i < NSIG; i++)
        Signal_(i, SIG_DFL);

    Signal_(SIGHUP, SIG_IGN);

    sigemptyset(&newmask);
    sigprocmask(SIG_SETMASK, &newmask, NULL);

    /* unblock all signals */
    alarm(0);

    closeExceptFD(-1);

    createJobTmpDir(jobCardPtr);

    if (jobSpecsPtr->options & SUB_PRE_EXEC)
        runUPre(jobCardPtr);

    if ((jobSpecsPtr->options & SUB_LOGIN_SHELL)
        || UID_MAPPED(jobCardPtr)) {

        if (createTmpJobFile(jobSpecsPtr,
                             fromHp,
                             jobCardPtr->stdinFile) < 0)
            jobSetupStatus(JOB_STAT_PEND, PEND_JOB_NO_FILE, jobCardPtr);
    }

    if (!(jobCardPtr->jobSpecs.options & SUB_RESTART)) {

        if (!(!daemonParams[LSB_BSUBI_OLD].paramValue &&
              PURE_INTERACTIVE(&jobCardPtr->jobSpecs) &&
              !UID_MAPPED(jobCardPtr))) {
            jobSetupStatus(JOB_STAT_RUN, 0, jobCardPtr);
        }
    }

    if (jobCardPtr->jobSpecs.options & SUB_NOTIFY_BEGIN)
        sendNotification(jobCardPtr);

    if (jobSpecsPtr->options & SUB_RESTART)
        execRestart(jobCardPtr, fromHp);


    if (! (jobSpecsPtr->options & SUB_RESTART)) {
        char *jobArgv[4];
        char **execArgv;
        char tmpJobFile[MAXLINELEN];

        if (!(jobSpecsPtr->options & SUB_LOGIN_SHELL)
            && !UID_MAPPED(jobCardPtr)) {

            sprintf(tmpJobFile, "%s%s", jobSpecsPtr->jobFile, JOBFILEEXT);
            jobArgv[0] = tmpJobFile;
            jobArgv[1] = NULL;

            execArgv = execArgs(jobSpecsPtr, jobArgv);

            ls_syslog(LOG_DEBUG, "\
%s: job %s execArgv[0] is %s, execArg[1] is %s",
                      __func__,
                      lsb_jobid2str(jobSpecsPtr->jobId),
                      execArgv[0],
                      execArgv[1]);

            if ((jobSpecsPtr->options & SUB_INTERACTIVE)
                && (jobSpecsPtr->options & SUB_PTY))  {
                chuser(batchId);
                if (!REShasPTYfix(execArgv[0])) {
                    setuid(jobSpecsPtr->execUid);
                }
            }

            execv(execArgv[0], execArgv);

            fprintf(stderr, "\
LSF: Unable to execute jobfile %s job %s: %s\n",
                    execArgv[0],
                    lsb_jobid2str(jobSpecsPtr->jobId),
                    strerror(errno));
        } else {

            if ((jobSpecsPtr->options & SUB_INTERACTIVE)
                && (jobSpecsPtr->options & SUB_PTY)) {
                chuser(batchId);
                setuid(jobSpecsPtr->execUid);
            }

            shellPath = "/bin/sh";
            if (jobSpecsPtr->options & SUB_LOGIN_SHELL) {
                if (jobSpecsPtr->loginShell != NULL
                    && jobSpecsPtr->loginShell[0] != '\0')
                    shellPath = jobSpecsPtr->loginShell;
                else
                    shellPath = getLoginShell(NULL,
                                              jobSpecsPtr->jobFile,
                                              fromHp, 1);
            }

            if (logclass & LC_EXEC)
                ls_syslog(LOG_DEBUG2, "\
%s: options=%x sock=%d shellPath=%s",
                          __func__, jobSpecsPtr->options,
                          chanSock_(chfd), shellPath);

            putenv("PATH=/bin:/usr/bin:/local/bin:/usr/local/bin");

            chdir(jobSpecsPtr->execHome);
            if (shellPath != NULL) {
                putEnv("SHELL", shellPath);

                jobArgv[0] = "-";

                jobArgv[1] = NULL;
                execv(shellPath, jobArgv);

                fprintf(stderr, "\
LSF: Unable to execute login shell %s for job %s: %s\n",
                        shellPath,
                        lsb_jobid2str(jobSpecsPtr->jobId),
                        strerror(errno));
            } else {
                fprintf(stderr, "\
LSF: Unable to find login shell for job %s.\n",
                        lsb_jobid2str(jobSpecsPtr->jobId));
            }
        }
    }

    exit(-1);
}



static char **
execArgs(struct jobSpecs *jp, char **execArgv)
{
    int i = 0;
    static char portStr[10];
    static char *argv[64];
    static char debugStr[10];

    argv[i++] = getDaemonPath_("/res", daemonParams[LSF_SERVERDIR].paramValue);
    if (debug) {
        sprintf(debugStr, "-%d", debug);
        argv[i++] = debugStr;
    }

    if (env_dir != NULL) {
        argv[i] = "-d";
        i++;
        argv[i] = env_dir;
        i++;
    }


    if (jp->options & SUB_INTERACTIVE) {
        argv[i++] = "-p";
        sprintf(portStr, "%d", jp->niosPort);
        argv[i++] = portStr;

        if (jp->options & SUB_PTY)
            argv[i++] = "-P";
        if ((jp->options & SUB_IN_FILE) || (jp->options2 & SUB2_IN_FILE_SPOOL))
            argv[i++] = "-i";
        if (jp->options & SUB_OUT_FILE)
            argv[i++] = "-o";
        if ((jp->options & SUB_ERR_FILE) || (jp->options & SUB_OUT_FILE))
            argv[i++] = "-e";
    }

    argv[i++] = "-m";
    argv[i++] = jp->fromHost;
    for (; *execArgv; execArgv++) {
        argv[i++] = *execArgv;
    }

    argv[i] = NULL;

    return argv;
}


void
resetEnv(void)
{
    char **env;

    env = (char **) my_calloc(2, sizeof(char *), "resetEnv");

    env[0] = NULL;
    environ = env;
}


int
setJobEnv(struct jobCard *jp)
{
    static char fname[] = "setJobEnv()";
    char val[MAXLINELEN];
    int i;
    char *hosts, *sp;
    char *tz, tzsave[255];
    char *eexecT, eexecTStr[100];
    char *eauthAuxData, eauthAuxDataStr[MAXPATHLEN];
    char shellFile[MAXFILENAMELEN];

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG, "setJobEnv: Job <%s> numEnv %d ...",
                  lsb_jobid2str(jp->jobSpecs.jobId), jp->jobSpecs.numEnv);


    if ((tz = getenv("TZ")))
        strcpy(tzsave, tz);

    if ((eexecT = getenv(LS_EXEC_T)))
        strcpy(eexecTStr, eexecT);


    if ((eauthAuxData = getenv ("LSF_EAUTH_AUX_DATA")) != NULL) {
        if (logclass & LC_TRACE) {
            ls_syslog(LOG_DEBUG, "%s: LSF_EAUTH_AUX_DATA=%s",
                      fname, eauthAuxData);
        }
        strcpy (eauthAuxDataStr, eauthAuxData);
    }


    resetEnv();


    for (i = 0; i < jp->jobSpecs.numEnv; i++) {
        if ((sp = strchr (jp->jobSpecs.env[i], '=')) == NULL)
            continue;

        if ( (jp->jobSpecs.options & SUB_LOGIN_SHELL)
             && (strstr(jp->jobSpecs.env[i], "LSB_ACCT_MAP") == NULL)
             && (strstr(jp->jobSpecs.env[i], "LSF_VERSION") == NULL)
             && (strstr(jp->jobSpecs.env[i], "LSB_UNIXGROUP") == NULL)
             && (strstr(jp->jobSpecs.env[i], "LSB_JOB_STARTER") == NULL)
             && (strstr(jp->jobSpecs.env[i], "LSF_INTERACTIVE_STDERR") == NULL)
             && (strstr(jp->jobSpecs.env[i], "LOGNAME") == NULL)
             && (strstr(jp->jobSpecs.env[i], "LSF_CUR_SECURITY_LABEL") == NULL)
             && (strstr(jp->jobSpecs.env[i], "LSF_JOB_SECURITY_LABEL") == NULL)) {

            if (!(jp->jobSpecs.options & SUB_INTERACTIVE))
                continue;
            if ((strstr (jp->jobSpecs.env[i], "TERMCAP") == NULL)
                && (strstr (jp->jobSpecs.env[i], "TERM") == NULL)) {
                continue;
            }

        }

        *sp = '\0';
        sp++;
        putEnv(jp->jobSpecs.env[i], sp);
        sp--;
        *sp = '=';
    }

    if (tz)
        putEnv("TZ", tzsave);

    sprintf(val, "%d", LSB_ARRAY_JOBID(jp->jobSpecs.jobId));
    putEnv("LSB_JOBID", val);
    sprintf(val, "%d", LSB_ARRAY_IDX(jp->jobSpecs.jobId));
    putEnv ("LSB_JOBINDEX", val);

    if (atoi(val) != 0) {
        setJobArrayEnv(jp->jobSpecs.jobName,
                       LSB_ARRAY_IDX(jp->jobSpecs.jobId));
    }

    sprintf(val, "%s", jp->jobSpecs.jobFile);
    putEnv("LSB_JOBFILENAME", val);
    if (strlen(jp->jobSpecs.chkpntDir) == 0) {

        if (jp->jobSpecs.jobSpoolDir[0] == '\0' ) {
            strcpy(shellFile,jp->jobSpecs.jobFile);
        } else {
            sprintf(shellFile, "%s/%s",
                    jp->jobSpecs.jobSpoolDir,
                    jp->jobSpecs.jobFile);
        }
    } else {
        sprintf(shellFile, "%s/%s", jp->jobSpecs.chkpntDir, jp->jobSpecs.jobFile);
    }

    if (strlen(jp->jobSpecs.chkpntDir) > 0) {
        char chkDir[MAXFILENAMELEN];
        char cwd[MAXFILENAMELEN];
        char *strPtr;




        if (((strPtr = strrchr(jp->jobSpecs.chkpntDir, '/')) != NULL)
            && (islongint_(strPtr+1))) {
            if (jp->jobSpecs.chkpntDir[0] == '/') {

                if(jp->jobSpecs.options & SUB_RESTART) {
                    *strPtr = '\0';
                    sprintf(chkDir, "%s/%s",
                            jp->jobSpecs.chkpntDir,
                            lsb_jobidinstr(jp->jobSpecs.jobId));
                    *strPtr = '/';
                } else {
                    sprintf(chkDir, "%s",jp->jobSpecs.chkpntDir);
                }
            } else {


                if (jp->jobSpecs.cwd[0] == '/') {
                    strcpy(cwd, jp->jobSpecs.cwd);
                } else {
                    if (jp->jobSpecs.cwd[0] == '\0')
                        strcpy(cwd, jp->jobSpecs.subHomeDir);
                    else
                        sprintf(cwd, "%s/%s", jp->jobSpecs.subHomeDir,
                                jp->jobSpecs.cwd);
                }
                if (jp->jobSpecs.options & SUB_RESTART) {
                    *strPtr = '\0';
                    sprintf(chkDir, "%s/%s/%s",
                            cwd,
                            jp->jobSpecs.chkpntDir,
                            lsb_jobidinstr(jp->jobSpecs.jobId));
                    *strPtr = '/';
                } else {
                    sprintf(chkDir, "%s",jp->jobSpecs.chkpntDir);
                }
            }
            putEnv("LSB_CHKPNT_DIR", chkDir);
            sprintf(shellFile, "%s/%s", chkDir, jp->jobSpecs.jobFile);
        } else {
            sprintf(shellFile, "%s/%s", jp->jobSpecs.chkpntDir, jp->jobSpecs.jobFile);
            ls_syslog(LOG_ERR, I18N(430,
                                    "Failed to set LSB_CHKPNT_DIR, error in the chkpnt directory: %s"), /* catgets 430 */
                      jp->jobSpecs.chkpntDir);
        }

        sprintf(val,"%ld",jp->jobSpecs.chkPeriod);
        putEnv("LSB_CHKPNT_PERIOD",val);
    }

    putEnv("LSB_CHKFILENAME", shellFile);

    sprintf(val, "trap # %d %d %d %d %d",
            SIGTERM, SIGUSR1, SIGUSR2, SIGINT, SIGHUP);

    putEnv("LSB_TRAPSIGS", val);

#define MAX_LSB_HOSTS_LEN 4096
    if (jp->jobSpecs.numToHosts <= (MAX_LSB_HOSTS_LEN/2))  {
        hosts = (char *) my_malloc(jp->jobSpecs.numToHosts * MAXHOSTNAMELEN, "setJobEnv");
        hosts[0] = '\0';
        for (i=0; i<jp->jobSpecs.numToHosts-1; i++) {
            strcat(hosts, jp->jobSpecs.toHosts[i]);
            strcat(hosts, " ");
        }
        strcat(hosts, jp->jobSpecs.toHosts[i]);

        if (strlen(hosts) < MAX_LSB_HOSTS_LEN)
            putEnv("LSB_HOSTS", hosts);
        free(hosts);

    }

    if ((jp->jobSpecs.options & SUB_OUT_FILE)) {
        sprintf(val, "%s", jp->jobSpecs.outFile);
        putEnv("LSB_OUTPUTFILE", val);
    }

    if ((jp->jobSpecs.options & SUB_ERR_FILE)) {
        sprintf(val, "%s", jp->jobSpecs.errFile);
        putEnv("LSB_ERRORFILE", val);
    }


    {
        struct nameList *hostList;
        hostList = lsb_compressStrList(jp->jobSpecs.toHosts,
                                       jp->jobSpecs.numToHosts);
        putEnv("LSB_MCPU_HOSTS",
               lsb_printNameList(hostList, PRINT_MCPU_HOSTS));
    }

    sprintf(val, "%s", jp->jobSpecs.queue);
    putEnv("LSB_QUEUE", val);
    sprintf(val, "%s", jp->jobSpecs.jobName);
    putEnv("LSB_JOBNAME", val);
    strcpy(val, "Y");
    if (jp->jobSpecs.options & SUB_RESTART)
        putEnv("LSB_RESTART", val);

    putEnv("LSFUSER", jp->execUsername);
    putEnv("USER", jp->execUsername);

    sprintf(val, "%d", LSB_PRE_ABORT);
    putEnv("LSB_EXIT_PRE_ABORT", val);

    if (jp->jobSpecs.requeueEValues &&
        jp->jobSpecs.requeueEValues[0] != '\0')
        putEnv("LSB_EXIT_REQUEUE", jp->jobSpecs.requeueEValues);


    if (eexecT)
        putEnv(LS_EXEC_T, eexecTStr);

    if (jp->jobSpecs.options & SUB_INTERACTIVE)
        putEnv("LSB_INTERACTIVE", "Y");


    if ((getenv("WINDIR") != NULL) || (getenv("windir") != NULL)) {
        char tmppath[MAXPATHLEN];

        sprintf(tmppath,"/bin:/usr/bin:/sbin:/usr/sbin");
        if (daemonParams[LSF_BINDIR].paramValue != NULL) {
            strcat(tmppath,":");
            strcat(tmppath,daemonParams[LSF_BINDIR].paramValue);
        }
        putEnv("PATH",tmppath);
    } else {


        char *tmppath = NULL;
        int len;
        char *envpath;
        int cc = TRUE;

        if (daemonParams[LSF_BINDIR].paramValue != NULL) {
            envpath = getenv("PATH");
            if (envpath != NULL) {

                len = strlen(daemonParams[LSF_BINDIR].paramValue);
                cc  = strncmp(envpath,daemonParams[LSF_BINDIR].paramValue,len);
                if (cc != 0)
                    len += strlen(envpath) + 2;
            }else {
                len = strlen(daemonParams[LSF_BINDIR].paramValue) +2;
            }
            if (cc != 0) {
                tmppath = malloc(len);
                if (tmppath == NULL) {
                    ls_syslog(LOG_ERR, I18N_FUNC_FAIL_M, fname, "malloc");
                    exit(-1);
                }
                strcpy(tmppath, daemonParams[LSF_BINDIR].paramValue);
                if (envpath != NULL) {
                    strcat(tmppath,":");
                    strcat(tmppath, envpath);
                }
                putEnv("PATH", tmppath);
                FREEUP(tmppath);
            }else {
                putEnv("PATH", envpath);
            }
        }
    }

    sprintf(val, "%d", (int)getpid());
    putEnv("LS_JOBPID", val);

    if (isAbsolutePathSub(jp, jp->jobSpecs.cwd)) {
        putEnv("LS_SUBCWD", jp->jobSpecs.cwd);
    } else {
        char cwd[MAXFILENAMELEN];

        sprintf(cwd, "%s/%s", jp->jobSpecs.subHomeDir, jp->jobSpecs.cwd);
        putEnv("LS_SUBCWD", cwd);
    }

    putEnv("LSB_SUB_HOST", jp->jobSpecs.fromHost);


    putEnv("LSF_ENVDIR",env_dir);

    if (daemonParams[LSF_SERVERDIR].paramValue != NULL) {
        putEnv("LSF_SERVERDIR", daemonParams[LSF_SERVERDIR].paramValue);
    }
    if (daemonParams[LSF_BINDIR].paramValue != NULL) {
        putEnv("LSF_BINDIR", daemonParams[LSF_BINDIR].paramValue);
    }
    if (daemonParams[LSF_LIBDIR].paramValue != NULL) {
        char path[MAXFILENAMELEN];

        putEnv("LSF_LIBDIR", daemonParams[LSF_LIBDIR].paramValue);

        sprintf(path, "%s/%s", daemonParams[LSF_LIBDIR].paramValue, "uid");
        putEnv("XLSF_UIDDIR", path);
    }


    if (eauthAuxData != NULL) {
        putEnv("LSF_EAUTH_AUX_DATA", eauthAuxDataStr);
    }

    sprintf (val, "%d", jp->w_status);
    putEnv ("LSB_JOBEXIT_STAT", val);

    sprintf (val, "%d", jp->jobSpecs.maxNumProcessors);
    putEnv ("LSB_MAX_NUM_PROCESSORS", val);

    sprintf (val, "%d", jp->jobSpecs.numToHosts);
    putEnv ("LSB_DJOB_NUMPROC", val);

    putEnv ("LSB_JOB_EXECUSER", jp->execUsername);

    putEnv ("LSB_EFFECTIVE_RSRCREQ", jp->jobSpecs.resReq);

    runEexec_("", jp->jobSpecs.jobId, &jp->jobSpecs.eexec, NULL);
    return 0;
}


static int
setLimits(struct jobSpecs *jobSpecsPtr)
{
    return(mysetLimits(jobSpecsPtr));
}

static int
mysetLimits(struct jobSpecs *jobSpecsPtr)
{
    struct rlimit rlimit;
    static char fname[] = "mysetLimits";

#ifdef  RLIMIT_CPU




    if ( (lsbJobCpuLimit != 1 ) ||
         (jobSpecsPtr->lsfLimits[LSF_RLIMIT_CPU].rlim_maxl == 0xffffffff &&
          jobSpecsPtr->lsfLimits[LSF_RLIMIT_CPU].rlim_maxh == 0x7fffffff &&
          jobSpecsPtr->lsfLimits[LSF_RLIMIT_CPU].rlim_curl == 0xffffffff &&
          jobSpecsPtr->lsfLimits[LSF_RLIMIT_CPU].rlim_curh == 0x7fffffff))
    {
        rlimitDecode_(&jobSpecsPtr->lsfLimits[LSF_RLIMIT_CPU],
                      &rlimit, LSF_RLIMIT_CPU);

        if (setrlimit(RLIMIT_CPU, &rlimit) < 0) {
            if (!debug)
                ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5406,
                                                 "%s: setrlimit(RLIMIT_CPU) failed for job <%s>: %m: soft %ld hard %ld"), /* catgets 5406 */
                          fname,
                          lsb_jobid2str(jobSpecsPtr->jobId),
                          (long)rlimit.rlim_cur,
                          rlimit.rlim_max);
        }
    }
#endif


#ifdef  RLIMIT_FSIZE
    rlimitDecode_(&jobSpecsPtr->lsfLimits[LSF_RLIMIT_FSIZE],
                  &rlimit, LSF_RLIMIT_FSIZE);
    if (setrlimit(RLIMIT_FSIZE, &rlimit) < 0) {
        if (! debug)
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5407,
                                             "%s: setrlimit(RLIMIT_FSIZE) failed for job <%s>: %m: soft %ld hard %ld"), /* catgets 5407 */
                      fname,
                      lsb_jobid2str(jobSpecsPtr->jobId),
                      (long)rlimit.rlim_cur,
                      rlimit.rlim_max);
    }
#endif

#ifdef RLIMIT_DATA
    rlimitDecode_(&jobSpecsPtr->lsfLimits[LSF_RLIMIT_DATA],
                  &rlimit, LSF_RLIMIT_DATA);
    if (setrlimit(RLIMIT_DATA, &rlimit) < 0) {
        if (! debug)
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5409,
                                             "%s: setrlimit(RLIMIT_DATA) failed for job <%s>: %m: soft %ld hard %ld"), /* catgets 5409 */
                      fname,
                      lsb_jobid2str(jobSpecsPtr->jobId),
                      (long)rlimit.rlim_cur,
                      rlimit.rlim_max);
    }
#endif

#ifdef  RLIMIT_STACK
    rlimitDecode_(&jobSpecsPtr->lsfLimits[LSF_RLIMIT_STACK],
                  &rlimit, LSF_RLIMIT_STACK);
    if (setrlimit(RLIMIT_STACK, &rlimit) < 0) {
        if (! debug)
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5410,
                                             "%s: setrlimit(RLIMIT_STACK) failed for job <%s>: %m: soft %ld hard %ld"), /* catgets 5410 */
                      fname,
                      lsb_jobid2str(jobSpecsPtr->jobId),
                      (long)rlimit.rlim_cur,
                      rlimit.rlim_max);
    }
#endif

#ifdef RLIMIT_CORE
    rlimitDecode_(&jobSpecsPtr->lsfLimits[LSF_RLIMIT_CORE],
                  &rlimit, LSF_RLIMIT_CORE);
    if (setrlimit(RLIMIT_CORE, &rlimit) < 0) {
        if (! debug)
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5411,
                                             "%s: setrlimit(RLIMIT_CORE) failed for job <%s>: %m: soft %ld hard %ld"), /* catgets 5411 */
                      fname,
                      lsb_jobid2str(jobSpecsPtr->jobId),
                      (long)rlimit.rlim_cur,
                      rlimit.rlim_max);
    }
#endif

#ifdef RLIMIT_RSS

    if (lsbJobMemLimit != 1)
    {
        rlimitDecode_(&jobSpecsPtr->lsfLimits[LSF_RLIMIT_RSS],
                      &rlimit, LSF_RLIMIT_RSS);
        if (setrlimit(RLIMIT_RSS, &rlimit) < 0) {
            if (! debug)
                ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5412,
                                                 "%s: setrlimit(RLIMIT_RSS) failed for job <%s>: %m: soft %ld hard %ld"),  /* catgets 5412 */
                          fname,
                          lsb_jobid2str(jobSpecsPtr->jobId),
                          (int)rlimit.rlim_cur,
                          rlimit.rlim_max);
        }
    }
#endif

    return 0;
}

int
job_finish(struct jobCard *jobCard, int report)
{
    static char fname[] = "job_finish";
    int pid;

    if (logclass & LC_EXEC)
        ls_syslog(LOG_DEBUG,
                  "%s: Entering ... jobId<%s> status<0x%x>", fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  jobCard->jobSpecs.jStatus);


    if (report && jobCard->jobSpecs.actPid)
        return -1;

    unlockHosts(jobCard, jobCard->jobSpecs.numToHosts);

    if (report
        && status_job(BATCH_STATUS_JOB, jobCard, jobCard->jobSpecs.jStatus,
                      (jobCard->jobSpecs.startTime > bootTime) ?
                      ERR_NO_ERROR : ERR_HOST_BOOT) < 0) {
        jobCard->notReported++;
        return -1;
    }

    if ((jobCard->jobSpecs.jStatus & JOB_STAT_PEND) &&
        (jobCard->jobSpecs.reasons == PEND_JOB_START_FAIL ||
         jobCard->jobSpecs.reasons == PEND_JOB_NO_FILE)) {
        deallocJobCard(jobCard);
        return 0;
    }

    /* Clean up the cgroup hierarchy of this job.
     */
    rm_mem_cgroup(jobCard);

    if ( !jobCard->postJobStarted ) {
        pid = fork();

        if (pid < 0) {
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                      lsb_jobid2str(jobCard->jobSpecs.jobId), "fork");
            jobCard->notReported++;
            return -1;
        }

        if (pid == 0) {

            closeBatchSocket();
            finishJob(jobCard);
            return 0;
        }

        if (jobCard->userJobSucc == TRUE) {
            jobCard->postJobStarted = 1;
            jobCard->jobSpecs.jobPid = pid;
            jobCard->jobSpecs.jobPGid = pid;
            jobCard->jobSpecs.jStatus = JOB_STAT_RUN;
            return 0;
        }
    }

    deallocJobCard(jobCard);

    return 0;
}

void
unlockHosts (struct jobCard *jp, int num)
{
    static char fname[] = "unlockHosts()";
    int i;

    if (jp == NULL || num <= 0)
        return;

    if (jp->jobSpecs.jAttrib & Q_ATTRIB_EXCLUSIVE) {
        for (i = 0; i < num; i++) {
            if (i > 0 && strcmp (jp->jobSpecs.toHosts[i],
                                 jp->jobSpecs.toHosts[i-1]) == 0)

                continue;
            if (unlockHost_(jp->jobSpecs.toHosts[i]) < 0
                && lserrno != LSE_LIM_NLOCKED)
                ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                          lsb_jobid2str(jp->jobSpecs.jobId),
                          "unlockHost_",
                          jp->jobSpecs.toHosts[i]);
        }
        jp->jobSpecs.jAttrib &= ~Q_ATTRIB_EXCLUSIVE;
    }

}

static int
finishJob(struct jobCard *jobCard)
{
    static char fname[]="finishJob";
    int doSendResults;
    int hasError = 0;

    char tmpDirName[MAXFILENAMELEN];

    doSendResults = (!jobCard->mbdRestarted &&
                     !(jobCard->jobSpecs.jStatus & JOB_STAT_PEND));

    putEnv(LS_EXEC_T, "END");


    if (postJobSetup(jobCard) == -1 && doSendResults) {
        chuser(batchId);
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId), "postJobSetup");
        lsb_merr1(_i18n_msg_get(ls_catd , NL_SETN, 412,
                                "Unable to setup user's environment to report results for job %s\n"), /* catgets 412 */
                  lsb_jobid2str(jobCard->jobSpecs.jobId));
        exit(-1);
    }

    getJobTmpDir(tmpDirName, jobCard);

    if (rmDirAndFilesEx(tmpDirName, 1) != 0 ) {
        ls_syslog(LOG_DEBUG, "\
%s: Fail to  delete TMPDIR=%s directory for job <%s>",
                  fname, tmpDirName, lsb_jobid2str(jobCard->jobSpecs.jobId));
    }

    if (doSendResults) {
        if ( send_results(jobCard) != 0 ) {
            hasError = -1;
            ls_syslog(LOG_ERR, I18N(5500,"Has error send result for job<%s>"), /* catgets 5500 */
                      lsb_jobid2str(jobCard->jobSpecs.jobId));
        }
    }

    if (runQPost(jobCard) != 0) {
        hasError = -1;
        ls_syslog(LOG_ERR, "\
%s: failed to run queue post for job %s", __func__,
                  lsb_jobid2str(jobCard->jobSpecs.jobId));
    }

    if (jobCard->jobSpecs.reasons & EXIT_REQUEUE) {
        jobCard->jobSpecs.reasons &= ~EXIT_REQUEUE;
    }

    if ((!jobCard->mbdRestarted)
        && (!((jobCard->jobSpecs.reasons ==  PEND_QUE_PRE_FAIL)
              &&(jobCard->jobSpecs.jStatus == JOB_STAT_PEND)))) {
        if ( rmJobBufFiles(jobCard) != 0 ) {
            hasError = -1;
            ls_syslog(LOG_ERR,I18N(5502, "Has error to remove buffer file for job<%s>"),/* catgets 5502 */
                      lsb_jobid2str(jobCard->jobSpecs.jobId));
        }
    }

    exit(hasError);
    return 0;
}



void
status_report(void)
{
    struct jobCard *jp, *next;
    static char mailed = TRUE;
    int rep;
    int allReported = TRUE;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_INFO,"%s: Entering..", __func__);

    for (jp = jobQueHead->back; jp != jobQueHead; jp = next) {
        next = jp->back;

        if (!IS_START(jp->jobSpecs.jStatus)
            && !(jp->jobSpecs.jStatus & JOB_STAT_PEND)
            && !IS_FINISH(jp->jobSpecs.jStatus)
            && !IS_POST_FINISH(jp->jobSpecs.jStatus) ) {
            ls_syslog(LOG_ERR, "\
%s: Illegal job status %d of job %s found; re-life",
                      __func__, jp->jobSpecs.jStatus,
                      lsb_jobid2str(jp->jobSpecs.jobId));
            relife();
        }

        if (logclass & LC_TRACE)
            ls_syslog(LOG_INFO,"\
%s: job %s notReproted %d missing %d needReportRU %d startTime %d status %x",
                      lsb_jobid2str(jp->jobSpecs.jobId), jp->notReported,
                      jp->missing, jp->needReportRU,
                      jp->jobSpecs.startTime, jp->jobSpecs.jStatus);

        if (jp->notReported < 0)
            continue;


        if (jp->missing)
            continue;

        if (!jp->notReported && !jp->needReportRU)
            continue;

        if ((now >= jp->jobSpecs.startTime)
            && (now < jp->jobSpecs.startTime + 10))
            continue;
        /* don't retry other jobs either */

        if (IS_START(jp->jobSpecs.jStatus)) {
            mbdReqType reqType;

            if (!jp->notReported && jp->needReportRU)
                reqType = BATCH_RUSAGE_JOB;
            else
                reqType = BATCH_STATUS_JOB;

            rep = status_job(reqType,
                             jp,
                             jp->jobSpecs.jStatus,
                             ERR_NO_ERROR);

            if (reqType == BATCH_STATUS_JOB) {
                if (rep >= 0) {
                    if (jp->notReported > 0)
                        jp->notReported = 0;
                } else {
                    allReported = FALSE;
                    jp->notReported++;
                    if (jp->notReported == 40 && !mailed) {
                        static char buf[256];
                        mailed = TRUE;
                        sprintf(buf, "\
%s: unable to report job %s status to master; retried %d times\n",
                                __func__, lsb_jobid2str(jp->jobSpecs.jobId),
                                jp->notReported);
                        lsb_merr(buf);
                    }
                }
            }
        }
    }

    if (allReported == TRUE)
        mailed = FALSE;
}

void
rmvJobStarterStr(char *line, char *jobStarter)
{
    static char *header=NULL, *tailer=NULL;
    static int header_len = 0;
    char *cmd;
    if (jobStarter == NULL || jobStarter[0] == '\0')
        return;
    if (header == NULL) {
        header = jobStarter;
        if ((tailer = strstr(jobStarter, JOB_STARTER_KEYWORD)) != NULL) {

            *tailer = '\0';

            tailer += strlen (JOB_STARTER_KEYWORD);

        }
        header_len = strlen (header);
    }
    if (strncmp(line, header, header_len) != 0)

        return;
    if (tailer != NULL) {
        if ((cmd = strstr (line, tailer)) !=NULL) {

            *cmd = '\n';
            *(cmd+1) = '\0';
        }
    }
    cmd = line + strlen(header);
    while (*cmd == ' ' && *cmd != '\0') cmd++;
    memmove (line, cmd, (strlen(cmd)+1)*sizeof(char));
}


static int
isLink(char * filename)
{
    static char fname[] = "isLink";
    struct stat statBuf;
    int lstatReturnValue;
    int returnValue = FALSE;

    lstatReturnValue = lstat(filename, &statBuf);
    if( lstatReturnValue  == 0 ) {

        if( S_ISLNK(statBuf.st_mode) ) {
            returnValue = TRUE;
        }
    }

    if (logclass & LC_TRACE) {
        if( filename ) {
            ls_syslog(LOG_DEBUG1,
                      "%s: filename:%s, lstatReturnValue:%d, returnValue:%d",
                      fname, filename, lstatReturnValue, returnValue);
        } else {
            ls_syslog(LOG_DEBUG1,
                      "%s: filename is null, lstatReturnValue:%d, returnValue:%d",
                      fname, lstatReturnValue, returnValue);
        }
    }

    return returnValue;
}

static void
shouldCopyFromLsbatch(struct jobCard *jp,
                      int *cpyStdoutFromLsbatch,
                      int *cpyStderrFromLsbatch )
{
    static char fname[] = "shouldCopyFromLsbatch";

    *cpyStdoutFromLsbatch = TRUE;
    *cpyStderrFromLsbatch = TRUE;

    if ( lsbStdoutDirect ) {

        char filename[MAXFILENAMELEN];


        if( jp->jobSpecs.options & SUB_OUT_FILE ) {
            sprintf(filename, "%s.out", jp->jobSpecs.jobFile);


            if( isLink(filename) ) {
                *cpyStdoutFromLsbatch = FALSE;
            }
        }


        if( jp->jobSpecs.options & SUB_ERR_FILE ) {
            sprintf(filename, "%s.err", jp->jobSpecs.jobFile);


            if( isLink(filename) ) {
                *cpyStderrFromLsbatch = FALSE;
            }

        }
    }

    if (logclass & LC_TRACE) {
        char errMsg[MAXLINELEN];

        sprintf(errMsg,
                "%s: leaving... cpyStdoutFromLsbatch=%d cpyStderrFromLsbatch=%d",
                fname, *cpyStdoutFromLsbatch, *cpyStderrFromLsbatch );
        sbdSyslog(LOG_DEBUG, errMsg);
    }
}


static int
send_results(struct jobCard *jp)
{
    static char fname[] = "send_results()";
    struct hostent *hp;
    FILE *mail = NULL, *fp, *errout = NULL, *output = NULL, *notif = NULL;
    int sendWarning = FALSE;
    char *result;
    char line[MSGSIZE*2];
    char fileName[MAXFILENAMELEN];
    char *myhostnm;
    int k, nItems, i;
    char ps[MSGSIZE*4];
    char withps = FALSE;
    char *sp, *jobStarter;
    int errFileError = FALSE;
    char ofileHost[MAXFILENAMELEN];
    struct stat ostat;
    int ofIdx;
    char rcpMsg[MSGSIZE];
    LS_WAIT_T w_status;
    float cpuTime = jp->cpuTime;
    int hasError=0;
    char outputIsDirectory = FALSE;
    char errIsDirectory  = FALSE;
    char outputFileName[MAXFILENAMELEN];
    char errFileName[MAXFILENAMELEN];
    char jobIdFile[ 16 ];
    struct stat stb;
    char mailSizeStr[MAXFILENAMELEN];
    struct stat outfileStat;
    long mailSizeLimit = 0;
    LS_LONG_INT submitJid;
    char jobIdStr[32];
    int copyStdoutFromLsbatch = TRUE;
    int copyStderrFromLsbatch = TRUE;
    int errorOpeningOutputFile = FALSE;

    shouldCopyFromLsbatch(jp, &copyStdoutFromLsbatch, &copyStderrFromLsbatch);


    outfileStat.st_size = -1;
    if ( !(jp->jobSpecs.options & SUB_OUT_FILE)) {

        sprintf(mailSizeStr, "%s.out", jp->jobSpecs.jobFile);

        if (stat(mailSizeStr, &outfileStat) < 0) {
            ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "stat", mailSizeStr);
            outfileStat.st_size = -1;
        }
    }

    sprintf(mailSizeStr, "%jd", outfileStat.st_size);

    if (daemonParams[LSB_MAILSIZE_LIMIT].paramValue != NULL) {
        mailSizeLimit = atol(daemonParams[LSB_MAILSIZE_LIMIT].paramValue);

        if (mailSizeLimit <= 0) {
            ls_syslog(LOG_ERR,
                      I18N(5414, "%s: Illegal value for LSB_MAILSIZE_LIMIT, ignoring"), /* catgets 5414 */
                      fname);
        }
    }

    LS_STATUS(w_status) = jp->w_status;

    ps[0] = '\0';

    if ((myhostnm = ls_getmyhostname()) == NULL) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jp->jobSpecs.jobId), "ls_getmyhostname");
        myhostnm = "localhost";
    }

    if (jp->w_status)
        result = putstr_(I18N_Exited);
    else
        result = putstr_(I18N_Done);

    hp = Gethostbyname_(jp->jobSpecs.fromHost);
    if (jp->jobSpecs.options & SUB_INTERACTIVE) {

        if (!(jp->jobSpecs.options & SUB_OUT_FILE)) {
            strcpy(jp->jobSpecs.outFile, LSDEVNULL);
            jp->jobSpecs.options |= SUB_OUT_FILE;
        }
    }

    if (jp->jobSpecs.options & SUB_OUT_FILE) {
        char chr;
        int lastSlash = FALSE;
        int outDirOk = FALSE;


        chr = jp->jobSpecs.outFile[strlen(jp->jobSpecs.outFile) - 1];

        if (chr == '/' || chr == '\\' ) {
            outputIsDirectory = TRUE;
            lastSlash = TRUE;
        }

        if (stat(jp->jobSpecs.outFile, &stb) == 0 && S_ISDIR(stb.st_mode)) {
            outputIsDirectory = TRUE;
            outDirOk = TRUE;
        }

        if ( !outputIsDirectory ) {

            strcpy( outputFileName, jp->jobSpecs.outFile );
            outDirOk = TRUE;
        } else {


            strcpy( outputFileName, jp->jobSpecs.outFile );
            if ( !outDirOk ) {

                if ( mkdir(outputFileName, 0700) == 0) {
                    outDirOk = TRUE;
                }
            }

            if ( !lastSlash ) {
                strcat( outputFileName, "/" );
            }
            sprintf(jobIdFile, "%s.out", lsb_jobidinstr(jp->jobSpecs.jobId) );

            strcat( outputFileName, jobIdFile );
        }


        if (outDirOk ) {
            output = myfopen_(outputFileName, "a", hp);
        }

        if (  output == NULL ) {
            errorOpeningOutputFile = TRUE;

            if( copyStdoutFromLsbatch ) {
                sprintf(line, _i18n_msg_get(ls_catd , NL_SETN, 414,
                                            "Fail to open output file %s: %s.\nOutput is stored in this mail.\n"), /* catgets 414 */
                        jp->jobSpecs.outFile, strerror(errno));

            } else {
                sprintf(line, _i18n_msg_get(ls_catd , NL_SETN, 483,
                                            "Fail to open output file %s: %s.\n"), /* catgets 483 */
                        jp->jobSpecs.outFile, strerror(errno));
            }

            strcat(ps, line);
            withps = TRUE;

            sprintf(mailSizeStr, "%s", "y");
            putEnv("LSB_OUTPUT_TARGETFAILED", mailSizeStr);
            sprintf(mailSizeStr, "%s.out", jp->jobSpecs.jobFile);
            if (stat(mailSizeStr, &outfileStat) < 0) {
                ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_M, fname, "stat", mailSizeStr);
                outfileStat.st_size = -1;
            }
            sprintf(mailSizeStr, "%jd", outfileStat.st_size);
            putEnv("LSB_MAILSIZE", mailSizeStr);
            if (jp->jobSpecs.options & SUB_MAIL_USER)
                output = (mail = smail(jp->jobSpecs.mailUser,
                                       jp->jobSpecs.fromHost));
            else
                output = (mail = smail(jp->jobSpecs.userName,
                                       jp->jobSpecs.fromHost));
        }
    } else {

        putEnv("LSB_MAILSIZE", mailSizeStr);
        if (jp->jobSpecs.options & SUB_MAIL_USER)
            output = (mail=smail(jp->jobSpecs.mailUser,
                                 jp->jobSpecs.fromHost));
        else
            output = (mail=smail(jp->jobSpecs.userName,
                                 jp->jobSpecs.fromHost));
    }

    if (jp->jobSpecs.options & SUB_NOTIFY_END) {
        if (jp->jobSpecs.options & SUB_MAIL_USER)
            notif = (output == mail) ? output :
                smail(jp->jobSpecs.mailUser, jp->jobSpecs.fromHost);
        else
            notif = (output == mail) ? output :
                smail(jp->jobSpecs.userName, jp->jobSpecs.fromHost);
    } else {
        if (jp->jobSpecs.options & SUB_INTERACTIVE) {
            notif = myfopen_( LSDEVNULL, "a", hp);


            sendWarning = TRUE;

        } else {
            notif = output;
        }
    }

    if (notif == output) {

        sendWarning = TRUE;
    }

    if (jp->jobSpecs.options & SUB_ERR_FILE) {
        char chr;
        int lastSlash = FALSE;
        int errDirOk = FALSE;


        chr = jp->jobSpecs.errFile[strlen(jp->jobSpecs.errFile) - 1];

        if (chr == '/' || chr == '\\' ) {
            errIsDirectory = TRUE;
            lastSlash = TRUE;
        }

        if (stat(jp->jobSpecs.errFile, &stb) == 0 && S_ISDIR(stb.st_mode)) {
            errIsDirectory = TRUE;
            errDirOk = TRUE;
        }


        if ( !errIsDirectory ) {

            strcpy( errFileName, jp->jobSpecs.errFile );
            errDirOk = TRUE;
        } else {


            strcpy( errFileName, jp->jobSpecs.errFile );
            if ( !errDirOk ) {

                if ( mkdir(errFileName, 0700) == 0) {
                    errDirOk = TRUE;
                }
            }

            if ( !lastSlash ) {
                strcat( errFileName, "/" );
            }
            sprintf(jobIdFile, "%s.err", lsb_jobidinstr(jp->jobSpecs.jobId) );

            strcat( errFileName, jobIdFile );
        }



        if ((errout = myfopen_(errFileName, "a", hp)) == NULL) {
            sprintf(line, _i18n_msg_get(ls_catd , NL_SETN, 415,
                                        "Fail to open stderr file %s: %s.\nThe stderr output is included in this report.\n"), /* catgets 415 */
                    jp->jobSpecs.errFile, strerror(errno));
            strcat(ps, line);
            withps = TRUE;
            errout = notif;
            errFileError = TRUE;
        }
    } else {
        errout = output;
    }




    if ( !copyStdoutFromLsbatch
         && (output==notif)
         && !errorOpeningOutputFile ) {

        fputs("\n------------------------------------------------------------\n", notif);
    }

    fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 416,
                                 "Sender: OpenLava System <%s@%s>\n"), /* catgets 416 */
            lsbManager,
            myhostnm);

    submitJid = jp->jobSpecs.jobId;

    sprintf(jobIdStr, "%s", lsb_jobid2str(submitJid));

    fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 417,
                                 "Subject: Job %s: <%s> %s\n"), /* catgets 417 */
            jobIdStr,
            jp->jobSpecs.jobName, result);
    free(result);
    fprintf(notif, "\n");
    fprintf(notif,  _i18n_msg_get(ls_catd , NL_SETN, 418,
                                  "Job <%s> was submitted from host <%s> by user <%s>.\n"), /* catgets 418 */
            jp->jobSpecs.jobName, jp->jobSpecs.fromHost,
            jp->jobSpecs.userName);

    if (jp->jobSpecs.numToHosts <= 1)
        fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 420,
                                     "Job was executed on host(s) <%s>, in queue <%s>, as user <%s>.\n"), /* catgets 420 */
                myhostnm, jp->jobSpecs.queue, jp->execUsername);
    else {
        struct nameList *hostList;
        char str[MAXLINELEN];
        hostList = lsb_compressStrList(jp->jobSpecs.toHosts,
                                       jp->jobSpecs.numToHosts);

        sprintf(str, "%d*%s", hostList->counter[0], hostList->names[0]);
        fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 420,
                                     "Job was executed on host(s) <%s>, in queue <%s>, as user <%s>.\n"), /* catgets 420 */
                str, jp->jobSpecs.queue, jp->execUsername);

        for (k=1; k < hostList->listSize; k++)
            fprintf(notif, "                            <%d*%s>\n",
                    hostList->counter[k], hostList->names[k]);
    }
    if (jp->jobSpecs.execHome[0] == '\0')
        fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 421,
                                     "Execution home directory was not known.\n")); /* catgets 421 */
    else
        fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 422,
                                     "<%s> was used as the home directory.\n"), /* catgets 422 */
                jp->jobSpecs.execHome);

    if (jp->jobSpecs.execCwd[0] == '\0')
        fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 423,
                                     "Execution working directory was not known.\n")); /* catgets 423 */
    else
        fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 424,
                                     "<%s> was used as the working directory.\n"), /* catgets 424 */
                jp->jobSpecs.execCwd);

    if (jp->jobSpecs.startTime != 0)
        sp = putstr_(_i18n_ctime(ls_catd, CTIME_FORMAT_DEFAULT,
                                 &jp->jobSpecs.startTime));
    else
        sp = putstr_(_i18n_msg_get(ls_catd , NL_SETN, 404,
                                   "unknown time in the past\n"));

    fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 426,
                                 "Started at %s"), sp); /* catgets 426 */
    free(sp);
    now = time(0);
    fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 487,
                                 "Results reported at %s"), /* catgets 487 */
            _i18n_ctime(ls_catd, CTIME_FORMAT_DEFAULT, &now));
    sprintf(fileName,"%s%s",jp->jobSpecs.jobFile,JOBFILEEXT);
    if ((fp = myfopen_(fileName, "r", hp)) == NULL) {
        fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 427,
                                     "Cannot open your job file: %s\n"), /* catgets 427 */
                fileName);
    } else {
        while (fgets(line, MAXLINELEN, fp) != NULL) {
            if (!strcmp(line, CMDSTART))
            {
                break;
            }
        }
        if (line == NULL) {
            fprintf(notif, _i18n_msg_get(ls_catd , NL_SETN, 428,
                                         "Corrupted jobfile <%s>\n"), /* catgets 428 */
                    jp->jobSpecs.jobFile);
        } else {
            int find =FALSE;
            jobStarter=getenv("LSB_JOB_STARTER");
            fputs(_i18n_printf("\n%s\n", _i18n_msg_get(ls_catd , NL_SETN, 429,
                                                       "Your job looked like:")), /* catgets 429 */
                  notif);
            fputs("\n------------------------------------------------------------\n", notif);

            fprintf(notif, "# LSBATCH: %s\n",
                    I18N(430, "User input"));  /* catgets 430 */
            k = 0;

            while (fgets(line, MAXLINELEN, fp)!= NULL) {
                if (strstr(line, "# LOGIN_SHELL") != NULL)
                    continue;
                if (strstr(line, "SCRIPT_\n") != NULL) {
                    if (find == FALSE) {

                        find = TRUE;
                        continue;
                    }

                    break;
                }

                if (strstr(line, "ExitStat=$") != 0) {
                    break;
                } else {
                    if (++k > 50)
                    {
                        fputs(_i18n_printf("\n(... %s ...)\n", I18N_more),
                              notif);
                        break;
                    }
                    if (k == 1)

                        rmvJobStarterStr(line, jobStarter);
                    ls_syslog(LOG_DEBUG, "Job Command: %s",
                              jp->jobSpecs.command);
                    ls_syslog(LOG_DEBUG, "Job line: %s",
                              line);

                    if ( ( k == 1 )
                         && (jp->jobSpecs.options2 & SUB2_JOB_CMD_SPOOL ) ) {
                        char *pSep=NULL;
                        if ( ( pSep = strchr(jp->jobSpecs.command, ';') )
                             == NULL ) {

                            strcpy(line, jp->jobSpecs.command);
                        }
                        else {
                            strncpy( line, jp->jobSpecs.command,
                                     (pSep - jp->jobSpecs.command) );
                        }
                        strcat( line, "\n");
                    }
                    fputs(line, notif);
                }
            }
            fprintf(notif, "------------------------------------------------------------\n\n");
        }
        _fclose_(&fp);
    }

    if (cpuTime > MIN_CPU_TIME) {
        if (WIFEXITED(w_status) && !WEXITSTATUS(w_status))
            fprintf(notif, "Successfully completed");
        else {
            if (WIFSIGNALED(w_status)) {
                int sig = WTERMSIG(w_status);

                if (sig > LSF_NSIG) {
                    fprintf(notif, "\
Exited with unknown status 0%o", jp->w_status);
                } else {
                    fprintf(notif, "\
Exited with signal termination: %s", strsignal(sig));
                }
                if (WCOREDUMP(w_status))
                    fprintf(notif, ", and core dumped");
            } else {
                if (jp->jobSpecs.reasons & EXIT_REQUEUE) {
                    fprintf(notif, "\
Job was killed by user or admin, and will be re-queued with the same jobid.");
                }
                else {
                    fprintf(notif, "\
Exited with exit code %d", WEXITSTATUS(w_status));
                }
            }
        }
    } else {

        fprintf(notif, I18N_Exited);
    }

    if (jp->jobSpecs.lastCpuTime + cpuTime > MIN_CPU_TIME) {
        fprintf(notif, ".\n\n%s\n\n", "Resource usage summary:");
        fprintf(notif, "    %s   :%10.2f ", "CPU time",
                (cpuTime + jp->jobSpecs.lastCpuTime));

        fprintf(notif, "%s.\n", "sec");

        if (jp->maxRusage.mem > 1024)
            fprintf(notif, "\
    %s :%10d MB\n", "Max Memory", (jp->maxRusage.mem + 512) / 1024);
        else
            if (jp->maxRusage.mem > 0)
                fprintf(notif, "\
    %s :%10d KB\n", "Max Memory", jp->maxRusage.mem);

        if (jp->maxRusage.swap > 1024)
            fprintf(notif, "    %s   :%10d MB\n\n", "Max Swap",
                    (jp->maxRusage.swap + 512) / 1024);
        else
            if (jp->maxRusage.swap > 0)
                fprintf(notif, "    %s   :%10d KB\n\n", "Max Swap",
                        jp->maxRusage.swap);
        if (jp->maxRusage.npids > 0)
            fprintf(notif, "    %s  :%10d\n", "Max Processes",
                    jp->maxRusage.npids);
    }

    if (! copyStdoutFromLsbatch
        && (output==notif)
        && !errorOpeningOutputFile ) {

        fprintf(notif, "\n");
        fprintf(notif, "%s\n\n", "\
The output (if any) is above this job summary.");
    }

    if (notif != output && strcmp(jp->jobSpecs.outFile, "/dev/null")) {
        if (errout == output)
            sprintf(line, "\
Read file <%s> for stdout and stderr ouput of this job.\n",
                    jp->jobSpecs.outFile);
        else
            sprintf(line, "\
Read file <%s> for stdout output of this job.\n", jp->jobSpecs.outFile);
        strcat(ps, line);
        withps = TRUE;
    }

    if (!((jp->jobSpecs.options & SUB_OUT_FILE)
          && !strcmp(jp->jobSpecs.outFile,"/dev/null"))
        && copyStdoutFromLsbatch) {
        sprintf(fileName, "%s.out", jp->jobSpecs.jobFile);

        if ((fp = myfopen_(fileName, "r", hp)) == NULL) {
            sprintf(line, "Unable to read output data from the stdout buffer file <%s>: your job was probably aborted prematurely.\n", fileName);
            strcat(ps, line);
            withps = TRUE;
        } else {

            if ((output == mail) && (mailSizeLimit > 0)
                && (outfileStat.st_size > mailSizeLimit*1024)) {

                fprintf(output, "\n");
                fprintf(output, "Output is larger than limit of %ld KB set by administrator.\n", mailSizeLimit);
                fprintf(output, I18N(480, "Output will be saved at %s.out.\n"), /* catgets 480 */
                        jp->jobSpecs.jobFile);


                jp->jobSpecs.jAttrib |= JOB_SAVE_OUTPUT;
            } else {
                if (output == notif) {
                    fprintf(output, "\n");
                    fprintf(output, "%s\n\n", _i18n_msg_get(ls_catd , NL_SETN, 447, "The output (if any) follows:")); /* catgets 447 */
                }
                while ((nItems = fread(line, sizeof(char), MAXLINELEN, fp))) {
                    if (fwrite(line, sizeof(char), nItems, output) == 0) {

                        sprintf(line,
                                "\nWARNING: writing output file %s failed for job %s\nError message: %s", lsb_jobid2str(jp->jobSpecs.jobId),
                                jp->jobSpecs.outFile, strerror(errno));

                        if (sendWarning) {


                            if (jp->jobSpecs.options & SUB_MAIL_USER) {
                                merr_user(jp->jobSpecs.mailUser,
                                          jp->jobSpecs.fromHost, line,
                                          I18N_Warning);
                            } else {
                                merr_user(jp->jobSpecs.userName,
                                          jp->jobSpecs.fromHost,
                                          line,
                                          I18N_Warning);
                            }
                        } else {

                            fprintf(notif, "%s", line);
                        }


                        break;
                    }
                }
            }
            _fclose_(&fp);
        }
    }



    if (!(jp->jobSpecs.options & SUB_ERR_FILE)) {

        sprintf(fileName, "%s.err", jp->jobSpecs.jobFile);
        if ((fp = myfopen_(fileName, "r", hp)) != NULL) {
            while ((nItems = fread(line, sizeof(char), MAXLINELEN, fp))) {
                fwrite(line, sizeof(char), nItems, errout);
            }
            _fclose_(&fp);
        }
    } else if (strcmp(jp->jobSpecs.errFile, "/dev/null")) {

        sprintf(fileName, "%s.err", jp->jobSpecs.jobFile);
        if (copyStderrFromLsbatch && (fp= myfopen_(fileName,"r",hp))== NULL) {
            sprintf(line, _i18n_msg_get(ls_catd , NL_SETN, 448,
                                        "Unable to read stderr data from stderr buffer file; your job was probably aborted prematurely.\n")); /* catgets 448 */
            strcat(ps, line);
            withps = TRUE;
        } else {
            if (errFileError)
                fprintf(errout, "\n\n%s\n\n",
                        _i18n_msg_get(ls_catd , NL_SETN, 449,
                                      "PS: The stderr output (if any) follows:")); /* catgets 449 */
            else {
                sprintf(line, _i18n_msg_get(ls_catd , NL_SETN, 450,
                                            "Read file <%s> for stderr output of this job.\n"), /* catgets 450 */
                        jp->jobSpecs.errFile);
                strcat(ps, line);
                withps = TRUE;
            }
            while ( copyStderrFromLsbatch
                    && (nItems = fread(line, sizeof(char), MAXLINELEN, fp))) {
                fwrite(line, sizeof(char), nItems, errout);
            }
            _fclose_(&fp);
        }
    }

    fflush(output);
    fflush(errout);
    fflush(notif);

    ofileHost[0] = '\0';
    if (jp->jobSpecs.nxf && output != mail && notif == output) {
        char *host;
        fstat(fileno(output), &ostat);
        if ((host = ls_getmnthost(jp->jobSpecs.outFile)))
            strcpy(ofileHost, host);
    }
    ofIdx = -1;

    for (i = 0; i < jp->jobSpecs.nxf; i++) {
        struct stat st;
        char *host;

        if (jp->jobSpecs.xf[i].options & XF_OP_EXEC2SUB) {

            if (ofileHost[0] != '\0' &&
                ofIdx == -1  ) {
                stat(jp->jobSpecs.xf[i].execFn, &st);
                if ((host = ls_getmnthost(jp->jobSpecs.xf[i].execFn)) &&
                    ostat.st_dev == st.st_dev &&
                    ostat.st_ino == st.st_ino &&
                    !strcmp(host, ofileHost)) {
                    ofIdx = i;
                    continue;
                }
            }

            if (rcpFile(&jp->jobSpecs, jp->jobSpecs.xf+i,
                        jp->jobSpecs.fromHost, XF_OP_EXEC2SUB, rcpMsg) == 0) {
                if (rcpMsg[0] != '\0') {
                    sprintf(line, _i18n_msg_get(ls_catd , NL_SETN, 451,
                                                "Copy file <%s> to <%s> on submission host <%s>: %s.\n"), /* catgets 451 */
                            jp->jobSpecs.xf[i].execFn,
                            jp->jobSpecs.xf[i].subFn,
                            jp->jobSpecs.fromHost,
                            rcpMsg);
                    strcat(ps, line);
                    withps = TRUE;
                }
                continue;
            }

            sprintf(line, _i18n_msg_get(ls_catd , NL_SETN, 452,
                                        "Unable to copy file <%s> to <%s> on submission host <%s>: %s.\n"), /* catgets 452 */
                    jp->jobSpecs.xf[i].execFn,
                    jp->jobSpecs.xf[i].subFn,
                    jp->jobSpecs.fromHost,
                    rcpMsg);
            hasError = -1;
            strcat(ps, line);
            withps = TRUE;
        }
    }
    if (withps) {
        if (notif == output)
            fprintf(notif, "\n\nPS:\n\n%s\n", ps);
        else
            fprintf(notif, "\n%s\n", ps);
    }



    if( !errorOpeningOutputFile && (output==notif) &&  ferror(output) ) {

        sprintf(line,
                "\nWARNING: writing to output file %s failed for job %s\nError message: %s",
                jp->jobSpecs.outFile,
                lsb_jobid2str(jp->jobSpecs.jobId),
                strerror(errno));

        if (jp->jobSpecs.options & SUB_MAIL_USER) {
            merr_user(jp->jobSpecs.mailUser,
                      jp->jobSpecs.fromHost, line,
                      I18N_Warning);
        } else {
            merr_user(jp->jobSpecs.userName,
                      jp->jobSpecs.fromHost,
                      line,
                      I18N_Warning);
        }
    }


    mclose(output);
    if (errout != output)
        mclose(errout);
    if (notif != output)
        mclose(notif);

    if (ofIdx >= 0) {
        if (rcpFile(&jp->jobSpecs, jp->jobSpecs.xf+ofIdx,
                    jp->jobSpecs.fromHost, XF_OP_EXEC2SUB, rcpMsg) < 0) {
            sprintf(ps, _i18n_msg_get(ls_catd , NL_SETN, 454,
                                      "We are unable to copy your output file <%s> to <%s> on submission host <%s> for job <%s>: %s.\n\n"), /* catgets 454 */
                    jp->jobSpecs.xf[ofIdx].execFn,
                    jp->jobSpecs.xf[ofIdx].subFn,
                    jp->jobSpecs.fromHost,
                    lsb_jobid2str(jp->jobSpecs.jobId),
                    rcpMsg);
            hasError = -1;

            if (jp->jobSpecs.options & SUB_MAIL_USER)
                merr_user(jp->jobSpecs.mailUser,
                          jp->jobSpecs.fromHost, ps,
                          I18N_Warning);
            else
                merr_user(jp->jobSpecs.userName,
                          jp->jobSpecs.fromHost,
                          ps,
                          I18N_Warning);
        } else {
            if (rcpMsg[0] != '\0') {
                sprintf(line, _i18n_msg_get(ls_catd , NL_SETN, 456,
                                            "%s: Copy output file <%s> to <%s> on submission host <%s> for job <%s>: %s"), /* catgets 456 */
                        fname,
                        jp->jobSpecs.xf[ofIdx].execFn,
                        jp->jobSpecs.xf[ofIdx].subFn,
                        jp->jobSpecs.fromHost,
                        lsb_jobid2str(jp->jobSpecs.jobId),
                        rcpMsg);
                sbdSyslog(LOG_DEBUG, line);
            }
        }
    }

    return (hasError);
}
/* addJob()
 *
 * Upon sbd's start up, initialize sbatchd's job list
 * using job specs from mbatchd
 */
struct jobCard *
addJob(struct jobSpecs *jobSpecs, int mbdVersion)
{
    static char fname[] = "addJob";
    struct jobCard *jp = NULL;
    struct passwd *pw = NULL;
    int reply;
    int cc;

    jp = my_calloc (1, sizeof (struct jobCard), fname);
    memcpy(&jp->jobSpecs, jobSpecs, sizeof(struct jobSpecs));

    if (jobSpecs->execUsername[0] == '\0') {
        jp->execGid = 0;
        jp->execUsername[0] = '\0';
        jp->jobSpecs.execUid   = -1;
    } else {


        if ((pw = getpwnam(jobSpecs->execUsername)) == NULL ||
            pw->pw_name == NULL) {
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                      lsb_jobid2str(jobSpecs->jobId), "getpwnam",
                      jobSpecs->execUsername);
            FREEUP (jp);
            return NULL;
        }
        jp->execGid = pw->pw_gid;
        strcpy(jp->execUsername, jobSpecs->execUsername);


        jp->jobSpecs.execUid   = pw->pw_uid;
    }


    if (mbdVersion < 3)
        jp->execJobFlag = JOB_EXEC_STARTED;
    else {
        if (jp->jobSpecs.execHome[0] != '\0')
            jp->execJobFlag = JOB_EXEC_STARTED;
        else
            jp->execJobFlag = 0;
    }


    if (jp->jobSpecs.preCmd && jp->jobSpecs.preCmd[0] != '\0' &&
        (jp->jobSpecs.execHome[0] != '\0' || jp->jobSpecs.execCwd[0] != '\0'))
        jp->execJobFlag |= JOB_EXEC_QPRE_OK | JOB_EXEC_QPRE_KNOWN;


    jp->jobSpecs.actValue = jobSpecs->actValue;

    cc = initJobCard(jp, jobSpecs, &reply);
    if (cc < 0) {
        lsb_merr1(_i18n_msg_get(ls_catd , NL_SETN, 457,
                                "Got garbage jobSpecs for job <%s> from mbatchd upon start: die\n"), lsb_jobid2str(jobSpecs->jobId)); /* catgets 457 */
        die(SLAVE_FATAL);
    }

    if (daemonParams[SBD_BIND_CPU].paramValue
            || hostAffinity) {
        int num;
        int *cores = NULL;

        cores = find_bound_core(jobSpecs->jobPid, &num);
        if (num > 0 && cores) {
            ls_syslog(LOG_DEBUG, "\
%s: job %d pid %d bound to core [%s]", __func__, jobSpecs->jobId,
                      jobSpecs->jobPid, covert_cores_to_str(num, cores));
            jp->cores = cores;
            jp->numCores = num;
            if (jp->jobSpecs.hostShares > 0)
                set_core_shares(jp->jobSpecs.queue,
                                jp->jobSpecs.hostShares,
                                num,
                                cores);
        }
    }

    if (jp->jobSpecs.jAttrib & Q_ATTRIB_EXCLUSIVE)
        if (lockHosts (jp) < 0) {
            ls_syslog(LOG_ERR, "\
%s: lockHosts() failed for job <%s>; Host used by the job will not be locked",
                      __func__, lsb_jobid2str(jp->jobSpecs.jobId));
        }
    renewJobStat (jp);


    if (!(daemonParams[LSB_RENICE_NEVER_AT_RESTART].paramValue)) {
        if (reniceJob(jp) < 0)
            ls_syslog(LOG_DEBUG, "%s: renice job <%s> failed",
                      fname, lsb_jobid2str(jp->jobSpecs.jobId));
    }


    jp->runTime = jobSpecs->runTime;

    if (jp->runTime < 0) {
        jp->runTime = 0;
    }

    return jp;
}

void
renewJobStat (struct jobCard *jp)
{
    int k;
    int mbdJobStatus;
    int mbdReasons;
    int mbdSubReasons;
    int sbdLogFound = 0;


    mbdJobStatus = jp->jobSpecs.jStatus;
    mbdReasons = jp->jobSpecs.reasons;
    mbdSubReasons = jp->jobSpecs.subreasons;


    sbdLogFound = sbdread_jobstatus (jp);

    k = 0;

    if (jp->jobSpecs.actPid) {

        kill(-jp->jobSpecs.actPid, SIGCONT);
        k = jobsig(jp, SIGCONT, FALSE);
    } else {


        if (mbdJobStatus & JOB_STAT_EXIT) {




            if ((mbdReasons & EXIT_RESTART)
                || ((mbdReasons == EXIT_ZOMBIE)
                    &&  !(jp->jobSpecs.options & SUB_CHKPNTABLE)
                    && (jp->jobSpecs.options & SUB_RERUNNABLE))) {
                jp->mbdRestarted = TRUE;
            }
            if (jp->jobSpecs.jobPid == 0) {

                jp->jobSpecs.jStatus = mbdJobStatus;
                return;
            }
            jobSigStart (jp, SIG_TERM_OTHER, 0, 0, SIGLOG);
        } else {
            if ( sbdLogFound == 0) {

                switch (MASK_STATUS(jp->jobSpecs.jStatus)) {
                    case JOB_STAT_PEND:
                        k = 0;

                        if (!(mbdJobStatus & JOB_STAT_PEND))
                            jp->notReported++;
                        break;

                    case JOB_STAT_RUN:
                        jp->migCnt = 1;
                        if (!(mbdJobStatus & JOB_STAT_RUN))
                            jp->notReported++;
                        break;

                    case JOB_STAT_SSUSP:
                    case JOB_STAT_USUSP:

                        k = sbdStartupStopJob(jp, jp->jobSpecs.reasons,
                                              jp->jobSpecs.subreasons);

                        if (!(IS_SUSP (mbdJobStatus)))
                            jp->notReported++;
                        break;

                }

            } else {


                switch (MASK_STATUS(mbdJobStatus)) {
                    case JOB_STAT_PEND:
                        k = 0;
                        break;
                    case JOB_STAT_RUN:
                        jp->migCnt = 1;
                        k = resumeJob (jp, SIG_RESUME_OTHER, 0, SIGLOG);
                        break;

                    case JOB_STAT_SSUSP:
                    case JOB_STAT_USUSP:
                        k = sbdStartupStopJob (jp, mbdReasons, mbdSubReasons);
                        break;
                }
            }
        }
    }

    if (k < 0)
        jobGone(jp);
}


static int
sbdStartupStopJob (struct jobCard *jp, int reasons, int subReasons)
{
    int sigValue = 0;

    if (reasons & SUSP_USER_STOP)
        sigValue = SIG_SUSP_USER;
    else if (reasons & SUSP_QUEUE_WINDOW)
        sigValue = SIG_SUSP_WINDOW;
    else if (reasons & LOAD_REASONS)
        sigValue = SIG_SUSP_LOAD;
    else if (reasons & OTHER_REASONS)
        sigValue = SIG_SUSP_OTHER;

    if (sigValue == 0)
        return 0;

    reasons |= SUSP_SBD_STARTUP;

    return (jRunSuspendAct(jp, sigValue,
                           ((reasons & SUSP_USER_STOP) ? JOB_STAT_USUSP
                            : JOB_STAT_SSUSP),
                           reasons, subReasons, NO_SIGLOG));

}

void
jobGone(struct jobCard *jp)
{
    if (logclass & LC_EXEC)
        ls_syslog(LOG_DEBUG, "\
%s: Checking if job %s pid %d gone, jp->w_status=%d", __func__,
                  lsb_jobid2str(jp->jobSpecs.jobId),
                  jp->jobSpecs.jobPid, jp->w_status);

    if (! jp->missing) {
        jp->missing = TRUE;
        need_checkfinish = TRUE;
        if (jp->notReported > 0)
            jp->notReported = 0;
        return;
    }

    if (getJobVersion (&jp->jobSpecs) < 4) {
        if (jp->exitPid == 0) {
            if ( jp->postJobStarted == 0 ) {
                jobFileExitStatus(jp);
            }
        }
        if (jp->exitPid >= 0)
            return;
    } else {

        jp->exitPid = -1;
        if (jp->postJobStarted == 0)
            jobFinishRusage(jp);
    }

    /* Clean up the allocated core
     */
    if (jp->cores != NULL) {
        free_core(jp->numCores, jp->cores, false);
        FREEUP(jp->cores);
        jp->numCores= 0;
    }

    if (jp->jobSpecs.actPid == 0 && jp->exitPid == -1) {

        if (requeueJob(jp) == TRUE) {
            SBD_SET_STATE(jp, JOB_STAT_PEND);
            jp->jobSpecs.reasons = PEND_SBD_JOB_REQUEUE;
        } else {
            if (jp->postJobStarted != 0) {
                if (jp->w_status) {
                    SBD_SET_STATE(jp, JOB_STAT_PERR);
                }
                else {
                    SBD_SET_STATE(jp, JOB_STAT_PDONE);
                }
            } else {
                if (jp->w_status) {
                    SBD_SET_STATE(jp, JOB_STAT_EXIT);
                }
                else {
                    SBD_SET_STATE(jp, JOB_STAT_DONE);
                }
            }
        }
    }

    jp->notReported++;
}

void
refreshJob(struct jobSpecs *jobSpecs)
{
    static char fname[] = "refreshJob()";
    struct jobCard *jp;
    char *word, *cp;
    int i, j;

    for (jp = jobQueHead->forw; jp != jobQueHead; jp = jp->forw) {
        if (jp->jobSpecs.jobId != jobSpecs->jobId)
            continue;


        jp->jobSpecs.reasons = jobSpecs->reasons;
        jp->jobSpecs.subreasons = jobSpecs->subreasons;


        if (!(jp->postJobStarted) && (jp->jobSpecs.jStatus != jobSpecs->jStatus)) {

            jp->jobSpecs.jStatus = (jobSpecs->jStatus & ~JOB_STAT_MIG) |
                (jp->jobSpecs.jStatus & JOB_STAT_MIG);

            renewJobStat(jp);
        }
        strcpy(jp->jobSpecs.queue, jobSpecs->queue);
        strcpy(jp->jobSpecs.resumeCond, jobSpecs->resumeCond);
        strcpy(jp->jobSpecs.stopCond, jobSpecs->stopCond);
        strcpy(jp->jobSpecs.suspendActCmd, jobSpecs->suspendActCmd);
        strcpy(jp->jobSpecs.resumeActCmd, jobSpecs->resumeActCmd);
        strcpy(jp->jobSpecs.terminateActCmd, jobSpecs->terminateActCmd);

        for (i=0; i < LSB_SIG_NUM; i++)
            jp->jobSpecs.sigMap[i] = jobSpecs->sigMap[i];

        jp->jobSpecs.priority = jobSpecs->priority;
        jp->jobSpecs.nice = jobSpecs->nice;
        jp->jobSpecs.migThresh = jobSpecs->migThresh;
        jp->jobSpecs.chkPeriod = jobSpecs->chkPeriod;


        freeToHostsEtc(&jp->jobSpecs);
        saveSpecs(&jp->jobSpecs, jobSpecs);

        jp->jobSpecs.jAttrib = jobSpecs->jAttrib;
        if (jp->jobSpecs.jAttrib & Q_ATTRIB_EXCLUSIVE) {
            if (lockHosts (jp) < 0) {
                ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5421,
                                                 "%s: lockHosts() failed for job <%s>; Host used by the job will not be locked"), fname, lsb_jobid2str(jp->jobSpecs.jobId));
            }
        }
        for (j = 0; j < LSF_RLIM_NLIMITS; j++)
            memcpy(&jp->jobSpecs.lsfLimits[j],
                   &jobSpecs->lsfLimits[j],
                   sizeof (struct lsfLimit));
        setRunLimit (jp, FALSE);

        if ((strcmp(jp->jobSpecs.windows, jobSpecs->windows)) != 0) {
            freeWeek(jp->week);
            cp = jobSpecs->windows;
            while ((word = getNextWord_(&cp)) != NULL) {
                if (addWindow(word, jp->week, "refreshJobs jobSpecs") < 0) {
                    ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                              lsb_jobid2str(jp->jobSpecs.jobId), "addWindow", word);
                    lsb_merr("Got garbage job bill from mbatchd on restart: die\n");
                    die(SLAVE_FATAL);
                }
            }
            strcpy(jp->jobSpecs.windows, jobSpecs->windows);
            jp->windEdge = now;
        }
        lsbFreeResVal (&jp->resumeCondVal);
        if (jobSpecs->resumeCond && jobSpecs->resumeCond[0] != '\0') {
            if ((jp->resumeCondVal = checkThresholdCond(jobSpecs->resumeCond))
                == NULL)
                ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                          lsb_jobid2str(jp->jobSpecs.jobId), "checkThresholdCond", "resumeCond");
        }

        lsbFreeResVal(&jp->stopCondVal);
        if (jobSpecs->stopCond  && jobSpecs->stopCond[0] != '\0') {
            if ((jp->stopCondVal = checkThresholdCond (jobSpecs->stopCond))
                == NULL)
                ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                          lsb_jobid2str(jp->jobSpecs.jobId), "checkThresholdCond", "stopCond");
        }


        jp->needReportRU = TRUE;

        if (!(daemonParams[LSB_RENICE_NEVER_AT_RESTART].paramValue)) {
            if (reniceJob(jp) < 0)
                ls_syslog(LOG_DEBUG, "refreshJob: reniceJob job <%s> failed",
                          lsb_jobid2str(jp->jobSpecs.jobId));
        }
    }
}


void
inJobLink(struct jobCard *jp)
{
    struct jobCard *jobp;

    for (jobp = jobQueHead->forw; jobp != jobQueHead; jobp = jobp->forw) {
        if (jp->jobSpecs.priority < jobp->jobSpecs.priority)
            break;
        else if ((jp->jobSpecs.priority == jobp->jobSpecs.priority)
                 && (jp->jobSpecs.startTime - jobp->jobSpecs.startTime) >= 0)
            break;
    }

    inList((struct  listEntry *)jobp, (struct listEntry *)jp);

}

int
setIds(struct jobCard *jobCardPtr)
{
    struct jobSpecs *jobSpecsPtr = &(jobCardPtr->jobSpecs);

    if (debug)
        return 0;

    if (initgroups(jobCardPtr->execUsername, jobCardPtr->execGid) < 0) {
        ls_syslog(LOG_ERR, "%s: initgroups() jobid %s user %s failed: %m",
                  __func__, lsb_jobid2str(jobSpecsPtr->jobId),
                  jobCardPtr->execUsername);
        return -1;
    }

    if (setgid(jobCardPtr->execGid) < 0) {
        ls_syslog(LOG_ERR, "%s: setgid() failed jobid %s gid %d",
                  __func__, lsb_jobid2str(jobSpecsPtr->jobId),
                  jobCardPtr->execGid);
        return -1;
    }

    if ((jobSpecsPtr->options & SUB_INTERACTIVE) &&
        (jobSpecsPtr->options & SUB_PTY)) {
        chuser(jobSpecsPtr->execUid);
    } else if (setuid(jobSpecsPtr->execUid) < 0) {
        ls_syslog(LOG_ERR, "%s: setuid() failed jobid %s user %s %m",
                  __func__, lsb_jobid2str(jobSpecsPtr->jobId),
                  jobCardPtr->execUsername);
        return -1;
    }

    ls_syslog(LOG_DEBUG, "\
%s: Job %s uid %d euid %d", __func__,
              lsb_jobid2str(jobSpecsPtr->jobId),
              getuid(), geteuid());
    return 0;
}

void
deallocJobCard(struct jobCard *jobCard)
{
    static char fname[] = "deallocJobCard()";
    char fileBuf[MAXFILENAMELEN];

    sprintf(fileBuf, "%s/.%s.%s.fail", LSTMPDIR, jobCard->jobSpecs.jobFile,
            lsb_jobidinstr(jobCard->jobSpecs.jobId));

    if (unlink(fileBuf) < 0 && errno != ENOENT)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  "unlink",
                  fileBuf);

    sprintf(fileBuf, "%s/.%s.%s.chk", LSTMPDIR, jobCard->jobSpecs.jobFile,
            lsb_jobidinstr(jobCard->jobSpecs.jobId));
    if (unlink(fileBuf) < 0 && errno != ENOENT)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  "unlink",
                  fileBuf);

    sprintf(fileBuf, "%s/.%s.%s.resume", LSTMPDIR, jobCard->jobSpecs.jobFile,
            lsb_jobidinstr(jobCard->jobSpecs.jobId));
    if (unlink(fileBuf) < 0 && errno != ENOENT)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  "unlink",
                  fileBuf);

    sprintf(fileBuf, "%s/.%s.%s.suspend", LSTMPDIR, jobCard->jobSpecs.jobFile,
            lsb_jobidinstr(jobCard->jobSpecs.jobId));
    if (unlink(fileBuf) < 0 && errno != ENOENT)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  "unlink",
                  fileBuf);

    sprintf(fileBuf, "%s/.%s.%s.terminate", LSTMPDIR, jobCard->jobSpecs.jobFile,
            lsb_jobidinstr(jobCard->jobSpecs.jobId));
    if (unlink(fileBuf) < 0 && errno != ENOENT)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  "unlink",
                  fileBuf);

    sprintf(fileBuf, "%s/.%s.acct", LSTMPDIR, jobCard->jobSpecs.jobFile);
    if (unlink(fileBuf) < 0 && errno != ENOENT)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  "unlink",
                  fileBuf);

    sprintf(fileBuf, "%s/.%s.%d.stt",
            LSTMPDIR,
            lsb_jobidinstr(jobCard->jobSpecs.jobId),
            jobCard->jobSpecs.jobPid);
    if (unlink(fileBuf) < 0 && errno != ENOENT)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  "unlink",
                  fileBuf);

    sprintf(fileBuf, "%s/.%s.sbd/jobstatus.%s", LSTMPDIR, clusterName,
            lsb_jobidinstr(jobCard->jobSpecs.jobId));

    if (unlink(fileBuf) < 0 && errno != ENOENT)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  "unlink",
                  fileBuf);


    sprintf(fileBuf, "/tmp/.sbd/%s.rusage", jobCard->jobSpecs.jobFile);
    ls_syslog(LOG_DEBUG, "deallocJobCard: unlink(%s) for MPI job <%s>: %m",
              fileBuf, lsb_jobid2str(jobCard->jobSpecs.jobId));

    if (unlink(fileBuf) < 0 && errno != ENOENT)
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  "unlink",
                  fileBuf);

    offList ((struct listEntry *)jobCard);
    freeWeek (jobCard->week);
    freeToHostsEtc (&jobCard->jobSpecs);

    FREEUP(jobCard->cores);

    if (jobCard->runRusage.npgids > 0) {
        FREEUP(jobCard->runRusage.pgid);
    }

    if (jobCard->runRusage.npids > 0) {
        FREEUP(jobCard->runRusage.pidInfo);
    }

    if (jobCard->mbdRusage.npgids > 0) {
        FREEUP(jobCard->mbdRusage.pgid);
    }

    if (jobCard->mbdRusage.npids > 0) {
        FREEUP(jobCard->mbdRusage.pidInfo);
    }

    if (jobCard->client) {
        shutDownClient(jobCard->client);
    }

    lsbFreeResVal (&jobCard->resumeCondVal);
    lsbFreeResVal (&jobCard->stopCondVal);

    free (jobCard);
    jobcnt--;
}

void
freeToHostsEtc (struct jobSpecs *jobSpecs)
{
    int i;

    if (jobSpecs == NULL)
        return;

    for (i = 0; i < jobSpecs->numToHosts; i++) {
        FREEUP(jobSpecs->toHosts[i]);
    }
    FREEUP(jobSpecs->toHosts);

    freeThresholds(&jobSpecs->thresholds);

    if (jobSpecs->nxf)
        FREEUP(jobSpecs->xf);

    if (jobSpecs->numEnv > 0) {
        for (i = 0; i < jobSpecs->numEnv; i++)
            FREEUP (jobSpecs->env[i]);
        FREEUP (jobSpecs->env);
        jobSpecs->numEnv = 0;
    }

    if (jobSpecs->eexec.len > 0) {
        FREEUP(jobSpecs->eexec.data);
        jobSpecs->eexec.len = 0;
    }
    FREEUP (jobSpecs->loginShell);
    FREEUP (jobSpecs->schedHostType);
    FREEUP (jobSpecs->execHosts);

}

void
saveSpecs (struct jobSpecs *jobSpecs, struct jobSpecs *specs)
{
    static char fname[] = "saveSpecs";
    int i;

    jobSpecs->toHosts = (char **)my_calloc(specs->numToHosts,
                                           sizeof (char *), fname);
    for (i = 0; i < specs->numToHosts; i++) {
        jobSpecs->toHosts[i] = safeSave (specs->toHosts[i]);
    }
    jobSpecs->numToHosts = specs->numToHosts;

    saveThresholds (jobSpecs, &specs->thresholds);
    if (specs->nxf)
        jobSpecs->xf = (struct xFile *)
            my_calloc (specs->nxf, sizeof(struct xFile), fname);
    else
        jobSpecs->xf = NULL;
    for (i = 0; i < specs->nxf; i++)
        memcpy ((char *) &(jobSpecs->xf[i]), (char *) &specs->xf[i],
                sizeof (struct xFile));
    jobSpecs->nxf = specs->nxf;

    if (specs->numEnv > 0) {
        jobSpecs->env = (char **)
            my_calloc (specs->numEnv, sizeof(char *), fname);
        for (i = 0; i < specs->numEnv; i++)
            jobSpecs->env[i] = safeSave (specs->env[i]);
        jobSpecs->numEnv = specs->numEnv;
    } else {
        jobSpecs->numEnv = 0;
        jobSpecs->env = NULL;
    }

    jobSpecs->eexec.len = specs->eexec.len;
    if (jobSpecs->eexec.len > 0) {
        jobSpecs->eexec.data = my_malloc(jobSpecs->eexec.len, fname);
        memcpy(jobSpecs->eexec.data, specs->eexec.data, jobSpecs->eexec.len);
    } else {
        jobSpecs->eexec.data = NULL;
        jobSpecs->eexec.len = 0;
    }
    jobSpecs->loginShell = safeSave (specs->loginShell);
    jobSpecs->schedHostType = safeSave (specs->schedHostType);
    if (specs->execHosts != NULL)
        jobSpecs->execHosts = safeSave (specs->execHosts);
    jobSpecs->hostShares= specs->hostShares;

}



void
setRunLimit(struct jobCard *jp, int initRunTime)
{
    if (jp->jobSpecs.lsfLimits[LSF_RLIMIT_RUN].rlim_curh != 0) {
        jp->jobSpecs.lsfLimits[LSF_RLIMIT_RUN].rlim_curl = 0x7fffffff;
    }

    if (initRunTime)
        jp->runTime = 0;
}

static int
setPGid(struct jobCard *jc)
{
    static char fname[] = "setPGid()";


    if (setpgid(0, getpid()) <0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jc->jobSpecs.jobId), "setpgid");
        return -1;
    }

    return (getpid());
}



static char *
getLoginShell (char *jfData, char *jobFile, struct hostent *hp, int readFile)
{
    static char shellPath[MAXFILENAMELEN];
    int i = 0;
    char *sp;
    char line[MAXLINELEN];
    FILE *fp;


    if (!readFile) {
        if (jfData == NULL)
            return NULL;
        if ((sp = strstr(jfData, "\n# LOGIN_SHELL ")) == NULL)
            return NULL;

        sp += strlen("\n# LOGIN_SHELL ");
        while (*sp != '\n') {
            shellPath[i] = *sp;
            i++; sp++;
        }
        shellPath[i] = '\0';
        return (shellPath);
    }


    if ((fp = myfopen_(jobFile, "r", hp)) == NULL) {
        fprintf(stderr, _i18n_msg_get(ls_catd , NL_SETN, 459,
                                      "Cannot open your job file: %s\n"), /* catgets 459 */
                jobFile);
        exit (-1);
    }

    while (fgets(line, MAXLINELEN, fp) != NULL) {
        if (!strcmp(line, CMDSTART))
        {
            break;
        }
    }
    if (line == NULL) {
        fprintf(stderr, _i18n_msg_get(ls_catd , NL_SETN, 460,
                                      "Corrupted jobfile <%s>\n"), /* catgets 460 */
                jobFile);
        exit (-1);
    }

    if (fgets(line, MAXLINELEN, fp) != NULL) {

        if ((sp = strstr(line, "# LOGIN_SHELL ")) == NULL) {
            _fclose_(&fp);
            return NULL;
        }
        sp += strlen("# LOGIN_SHELL ");
        while (*sp != '\n') {
            shellPath[i] = *sp;
            i++; sp++;
        }
        shellPath[i] = '\0';
        _fclose_(&fp);
        return (shellPath);
    }
    _fclose_(&fp);
    return NULL;

}

static int
createTmpJobFile(struct jobSpecs *jobSpecsPtr, struct hostent *hp,
                 char *stdinFile)
{
    static char fname[] = "createTmpJobFile";
    char errMsg[MAXLINELEN];
    char path[MAXFILENAMELEN];
    char cmdBuf[MAXLINELEN];
    char *sp = NULL;
    int fd, len, size, i;
    char **execArgv, *argv[2];

    if (logclass & LC_EXEC) {
        sprintf(errMsg, "%s: entering, jobFile=<%s>", fname, jobSpecsPtr->jobFile);
        sbdSyslog(LOG_DEBUG3, errMsg);
    }

    sprintf(path, "%s.tmp", jobSpecsPtr->jobFile);
    if ((fd = myopen_(path, O_CREAT | O_TRUNC | O_WRONLY,
                      0600, hp)) == -1) {
        sprintf(cmdBuf, I18N_JOB_FAIL_S_S_M, fname,
                lsb_jobid2str(jobSpecsPtr->jobId), "myopen_",
                path);
        goto Error;
    }
    if (stdinFile == NULL)
        stdinFile = "/dev/null";


    sprintf(cmdBuf, "cd %s", jobSpecsPtr->execCwd);

    argv[0] = jobSpecsPtr->jobFile;
    argv[1] = NULL;

    execArgv = execArgs(jobSpecsPtr, argv);



    size = 2 * (strlen(jobSpecsPtr->jobFile) + strlen(stdinFile))
        + strlen(cmdBuf) + 25;

    for (i = 0; execArgv[i]; i++)
        size += 2 * (strlen(execArgv[i]) + 1);

    sp = malloc (size);
    if (sp == NULL) {
        sprintf(cmdBuf, I18N_JOB_FAIL_S_M, fname,
                lsb_jobid2str(jobSpecsPtr->jobId), "malloc");
        goto Error;
    }

    sprintf(sp, "%s\nexec", cmdBuf);
    for (i = 0; execArgv[i]; i++) {
        strcat(sp, " ");
        strcat(sp, execArgv[i]);
    }

    strcat(sp, " <");
    strcat(sp, stdinFile);
    strcat(sp, "\n");

    for (i = 0; execArgv[i]; i++) {
        strcat(sp, " ");
        strcat(sp, execArgv[i]);
    }

    strcat(sp, " <");
    strcat(sp, stdinFile);
    strcat(sp, "\n");

    len = strlen(sp);
    if (write(fd, sp, len) != len) {
        sprintf(cmdBuf, I18N_FUNC_S_FAIL_M, fname, "write",
                jobSpecsPtr->jobFile);
        goto Error;
    }
    FREEUP(sp);
    if (close(fd) == -1) {
        sprintf(cmdBuf, I18N_FUNC_S_FAIL_M, fname, "close",
                jobSpecsPtr->jobFile);
        goto Error;
    }

    sprintf(path, "%s.tmp", jobSpecsPtr->jobFile);
    if ((fd = myopen_(path, O_RDONLY, 0600, hp)) == -1) {
        sprintf(cmdBuf, I18N_JOB_FAIL_S_S_M, fname,
                lsb_jobid2str(jobSpecsPtr->jobId), "myopen_", path);
        goto Error;
    }

    if (dup2(fd, 0) == -1) {
        sprintf(cmdBuf, I18N_FUNC_D_FAIL_M, fname, "dup2",
                fd);
        goto Error;
    }

    if (logclass & LC_EXEC) {
        sprintf(errMsg, "%s: leaving", fname);
        sbdSyslog(LOG_DEBUG3, errMsg);
    }
    return 0;

Error:
    if (logclass & LC_EXEC) {
        sprintf(errMsg, "%s: %s", fname, cmdBuf);
        sbdSyslog(LOG_DEBUG3, errMsg);
    }
    fprintf(stderr, "%s\n", cmdBuf);
    FREEUP(sp);

    return -1;

}

/* acctMapTo()
 *
 * Validate the existence of the user on the execution host.
 *
 */
static int
acctMapTo(struct jobCard *jobCard)
{
    struct passwd *pw;

    if ((pw =  __getpwnam__(jobCard->jobSpecs.userName)) == NULL) {

        ls_syslog(LOG_ERR, "\
%s: No valid user name found for job <%s>, userName <%s>. getpwnam() failed:%m",
                  __func__,
                  lsb_jobid2str(jobCard->jobSpecs.jobId),
                  jobCard->jobSpecs.userName);
        return -1;
    }

    strcpy(jobCard->execUsername, jobCard->jobSpecs.userName);
    jobCard->execGid  = pw->pw_gid;
    jobCard->jobSpecs.execUid  = pw->pw_uid;
    strcpy(jobCard->jobSpecs.execUsername, jobCard->jobSpecs.userName);

    return 0;
}

static void
runQPre(struct jobCard *jp)
{
    static char *fname = "runQPre";
    pid_t pid;
    int i;
    char errMsg[MAXLINELEN];

    if (!jp->jobSpecs.preCmd || jp->jobSpecs.preCmd[0] == '\0')
        return;

    if ((pid = fork()) == 0) {
        /* child */
        sigset_t newmask;
        char *myargv[6];
        int maxfds = sysconf(_SC_OPEN_MAX);

        chdir("/tmp");

        if (chPrePostUser(jp) != 0) {
            ls_syslog(LOG_ERR, "\
%s: queue's pre-exec chPrePostUser failed for job <%d>",
                      fname, jp->jobSpecs.jobId);
            exit(-1);
        }

        for (i = 1; i < NSIG; i++)
            Signal_(i, SIG_DFL);

        sigemptyset(&newmask);
        sigprocmask(SIG_SETMASK, &newmask, NULL);

        alarm(0);

        putEnv("PATH", "/bin:/usr/bin:/sbin:/usr/sbin");

        i = open ("/dev/null", O_RDWR, 0);
        dup2(i, 0);
        dup2(i, 1);
        dup2(i, 2);

        for (i = 3; i < maxfds; i++)
            close(i);

        myargv[0] = "/bin/sh";
        myargv[1] = "-c";
        myargv[2] = jp->jobSpecs.preCmd;
        myargv[3] = NULL;

        execvp ("/bin/sh", myargv);
        sprintf(errMsg, "\
%s: queue's pre-exec command %s failed for job %s: %s",
                fname, jp->jobSpecs.preCmd,
                lsb_jobid2str(jp->jobSpecs.jobId),
                strerror(errno));
        sbdSyslog(LOG_ERR, errMsg);
        exit (-1);
    }

    if (pid < 0) {
        ls_syslog(LOG_ERR, "\
%s: Fork to run pre-exec for job <%d> failed: %m",
                  fname, jp->jobSpecs.jobId);
        jobSetupStatus(JOB_STAT_PEND, PEND_QUE_PRE_FAIL, jp);
    }

    collectPreStatus(jp, pid, "runQPre");
    jp->execJobFlag |= JOB_EXEC_QPRE_OK;
}

int
runQPost(struct jobCard *jp)
{
    int i;
    int pid;
    int maxfds;
    char val[MAXLINELEN];
    char *myargv[6];
    sigset_t newmask;

    if (logclass & LC_TRACE) {
        chuser(batchId);
        ls_syslog(LOG_DEBUG, "\
%s: queue's post-command %s pre-command %s execJobFlag %x for job %d status %x", __func__, jp->jobSpecs.postCmd ? jp->jobSpecs.postCmd : "NULL",
                  jp->jobSpecs.preCmd ? jp->jobSpecs.preCmd : "NULL",
                  jp->execJobFlag,
                  lsb_jobid2str(jp->jobSpecs.jobId),
                  jp->w_status);
        chuser(jp->jobSpecs.execUid);
    }

    if (!jp->jobSpecs.postCmd || jp->jobSpecs.postCmd[0] == '\0')
        return 0;

    if (jp->jobSpecs.preCmd && jp->jobSpecs.preCmd[0] != '\0') {

        if (!(jp->execJobFlag & JOB_EXEC_QPRE_KNOWN)) {

            chuser(batchId);
            ls_syslog(LOG_WARNING, "\
%s: qpost is not run for job %d because status of qpre unknown",
                      __func__, jp->jobSpecs.jobId);
            return -1;
        }

        if (!(jp->execJobFlag & JOB_EXEC_QPRE_OK)) {

            chuser(batchId);
            ls_syslog(LOG_WARNING, "\
%s: qpost is not run for job % because qpre failed",
                      __func__,
                      lsb_jobid2str(jp->jobSpecs.jobId),
                      chuser(jp->jobSpecs.execUid));
            return -1;
        }
    }

    if ((pid = fork()) == -1) {
        chuser(batchId);
        ls_syslog(LOG_ERR, "\
%s: fork() failed for job %d: %m",
                  __func__, jp->jobSpecs.jobId);
        chuser(jp->jobSpecs.execUid);
        return -1;
    }

    if (pid) {
        if (waitpid(pid, NULL, 0) < 0) {
            chuser(batchId);
            ls_syslog(LOG_ERR, "\
%s: waitpid(%d) failed for qpre for job <%d>: %m",
                      __func__, pid, jp->jobSpecs.jobId);
            chuser(jp->jobSpecs.execUid);
            return -1;
        }
        return 0;
    }

    chdir("/tmp");
    chuser(batchId);

    if (chPrePostUser(jp) < 0) {
        ls_syslog(LOG_ERR, "\
%s: queue's post-exec chPrePostUser failed for job <%d>",
                  __func__, jp->jobSpecs.jobId);
        exit(-1);
    }

    for (i = 1; i < NSIG; i++)
        Signal_(i, SIG_DFL);

    sigemptyset(&newmask);
    sigprocmask(SIG_SETMASK, &newmask, NULL);
    alarm(0);

    putEnv("PATH", "/bin:/usr/bin:/sbin:/usr/sbin");

    i = open ("/dev/null", O_RDWR, 0);
    dup2(i, 0);
    dup2(i, 1);
    dup2(i, 2);

    maxfds = sysconf(_SC_OPEN_MAX);
    for (i = 3; i < maxfds; i++) {
        close(i);
    }

    sprintf (val, "%d", jp->w_status);
    putEnv ("LSB_JOBEXIT_STAT", val);

    if (jp->jobSpecs.jStatus & JOB_STAT_PEND)
        putEnv("LSB_JOBPEND", " ");

    myargv[0] = "/bin/sh";
    myargv[1] = "-c";
    myargv[2] = jp->jobSpecs.postCmd;
    myargv[3] = NULL;

    execvp ("/bin/sh", myargv);

    sprintf(val, "\
%s: queue's post-exec command %s failed for job %s: %s",
            __func__, jp->jobSpecs.postCmd,
            lsb_jobid2str(jp->jobSpecs.jobId),
            strerror(errno));
    sbdSyslog(LOG_ERR, val);

    exit(-1);
}

/* chPrePostUser()
 * Change the userid to the execution user.
 */
static int
chPrePostUser(struct jobCard *jp)
{
    uid_t prepostUid;

    if (initgroups(jp->execUsername, jp->execGid) < 0) {
        ls_syslog(LOG_ERR, "\
%s: initgroups() failed for user %s uid %d gid %d %m", __func__,
                  jp->execUsername, jp->jobSpecs.execUid,
                  jp->execGid);
    }

    if (setgid(jp->execGid) < 0) {
        ls_syslog(LOG_ERR, "\
%s: setgid() failed for user %s uid %d gid %d %m", __func__,
                  jp->execUsername, jp->jobSpecs.execUid,
                  jp->execGid);
    }

    if (getUid(jp->jobSpecs.prepostUsername, &prepostUid) != 0) {
        prepostUid = jp->jobSpecs.execUid;
    }

    if (setuid(prepostUid) < 0) {
        ls_syslog(LOG_ERR, "\
%s: setuid() failed for uid %d gid %d %m", __func__,
                  prepostUid, jp->execGid);
        return -1;
    }

    return 0;
}


int
postJobSetup(struct jobCard *jp)
{
    static char fname[] = "postJobSetup";
    struct hostent *hp;

    closeBatchSocket();
    sbdChildCloseChan (-1);

    Signal_(SIGCHLD, SIG_DFL);
    Signal_(SIGTERM, SIG_IGN);
    Signal_(SIGINT,  SIG_IGN);

    if (setsid() == -1) {

        if (getpid() != getpgrp()) {
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                      lsb_jobid2str(jp->jobSpecs.jobId), "setsid");
        }
    }

    if (setJobEnv (jp) < 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jp->jobSpecs.jobId), "setJobEnv");
        return -1;
    }

    if (jp->jobSpecs.execUsername[0] == '\0') {

        if (acctMapTo(jp) < 0) {
            ls_syslog(LOG_DEBUG, "%s: acctMapTo() failed for job <%s>",
                      fname, lsb_jobid2str(jp->jobSpecs.jobId));
            return -1;
        }

        putEnv ("LSFUSER", jp->execUsername);
        putEnv("USER", jp->execUsername);
        chuser(jp->jobSpecs.execUid);

        chuser(batchId);
    }

    if ((hp = Gethostbyname_(jp->jobSpecs.fromHost)) == NULL)
        ls_syslog(LOG_ERR, "\
%s: Gethostbyname_() failed %s job %s",
                  __func__, jp->jobSpecs.fromHost,
                  lsb_jobid2str(jp->jobSpecs.jobId));

    if (!debug) {

        if (initgroups(jp->execUsername, jp->execGid) < 0)
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                      lsb_jobid2str(jp->jobSpecs.jobId), "initgroups",
                      jp->execUsername);

        if (setgid(jp->execGid) < 0)
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_D_M, fname,
                      fname, lsb_jobid2str(jp->jobSpecs.jobId),
                      "setgid", (int)jp->execGid);
    }

    chuser(jp->jobSpecs.execUid);

    if (initPaths(jp, hp, NULL) < 0) {
        if (!(jp->jobSpecs.jStatus & JOB_STAT_PRE_EXEC)
            && jp->jobSpecs.reasons != PEND_QUE_PRE_FAIL) {

            chuser(batchId);
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                      lsb_jobid2str(jp->jobSpecs.jobId), "initPaths");
            chuser(jp->jobSpecs.execUid);
        }
        return -2;
    }

    return 0;
}


void
runUPre(struct jobCard *jp)
{
    static char fname[]="runUPre";
    int pid, i;
    char errMsg[MAXLINELEN];

    if ((pid = fork()) < 0) {
        sprintf(errMsg, I18N_JOB_FAIL_S_M, fname,
                lsb_jobid2str(jp->jobSpecs.jobId), "fork");
        sbdSyslog(LOG_ERR, errMsg);
        jobSetupStatus(JOB_STAT_PEND, PEND_SBD_NO_PROCESS, jp);
    }

    if (pid == 0) {

        closeBatchSocket();
        if (getuid() == batchId) {

            chuser(batchId);
            setuid(jp->jobSpecs.execUid);
        }

        for (i = 1; i < NSIG; i++)
            Signal_(i, SIG_DFL);

        Signal_(SIGHUP, SIG_IGN);

        execl("/bin/sh", "/bin/sh", "-c", jp->jobSpecs.preExecCmd, NULL);
        sprintf(errMsg, I18N_JOB_FAIL_S_M, fname,
                lsb_jobid2str(jp->jobSpecs.jobId), "execl");
        sbdSyslog(LOG_ERR, errMsg);
        sbdSyslog(LOG_ERR, jp->jobSpecs.preExecCmd);
        exit(-1);
    }

    collectPreStatus(jp, pid, "runUPre");
    jp->jobSpecs.jStatus &= ~JOB_STAT_PRE_EXEC;
}

static void
collectPreStatus(struct jobCard *jp, int pid, char *context)
{
    static char fname[] = "collectPreStatus()";
    int id;
    LS_WAIT_T status;
    struct lsfRusage lsfRusage;
    struct rusage   rusage;
    char errMsg[MAXLINELEN];

    while ((id = wait3(&status, 0, &rusage)) != pid) {
        if (id < 0) {
            sprintf(errMsg, I18N_JOB_FAIL_S_D_M, fname,
                    lsb_jobid2str(jp->jobSpecs.jobId), "wait3", pid);
            sbdSyslog(LOG_ERR, errMsg);
            jobSetupStatus(JOB_STAT_PEND, PEND_JOB_EXEC_INIT, jp);
        }
        sprintf(errMsg, _i18n_msg_get(ls_catd , NL_SETN, 482,
                                      "%s: %s: wait3() got %d, not pid %d for job <%s>"), /* catgets 482 */
                fname, context, id, pid, lsb_jobid2str(jp->jobSpecs.jobId));
        sbdSyslog(LOG_DEBUG, errMsg);
    }

    if (LS_STATUS(status)) {
        if (logclass & LC_TRACE)
            ls_syslog(LOG_DEBUG, "collectPreStatus: %s: job <%s> status <%d>",
                      context, lsb_jobid2str(jp->jobSpecs.jobId),
                      LS_STATUS(status));

        ls_ruunix2lsf (&rusage, &lsfRusage);
        jp->w_status = LS_STATUS(status);
        jp->cpuTime = lsfRusage.ru_utime + lsfRusage.ru_stime;
        memcpy ((char *) &jp->lsfRusage, (char *) &lsfRusage,
                sizeof(struct lsfRusage));

        if ((WIFEXITED(status) && WEXITSTATUS(status) == LSB_PRE_ABORT) ||
            WIFSIGNALED(status)  )
            jobSetupStatus(JOB_STAT_EXIT, 0, jp);
        else {
            if (!strcmp(context, "runUPre"))
                jobSetupStatus(JOB_STAT_PEND, PEND_JOB_PRE_EXEC, jp);
            else
                jobSetupStatus(JOB_STAT_PEND, PEND_QUE_PRE_FAIL, jp);
        }
    }
}


static int
requeueJob (struct jobCard *jp)
{
    static char fname[]="requeueJob";
    char   *sp, *cp;
    int w_status;
    LS_WAIT_T status;


    LS_STATUS(status) = jp->w_status;
    w_status = WEXITSTATUS(status);

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG, "%s: Exit status for job <%s> is <%d>, normal termination is <%d>, jp->w_status <%d>, requeue exit status is <%s>",
                  fname, lsb_jobid2str(jp->jobSpecs.jobId),
                  w_status, WIFEXITED(status),
                  jp->w_status, jp->jobSpecs.requeueEValues);

    if (!WIFEXITED(status)  ||
        !jp->jobSpecs.requeueEValues || jp->jobSpecs.requeueEValues[0]
        == '\0')
        return false;

    sp = jp->jobSpecs.requeueEValues;

    while ((cp = getNextWord_(&sp)) != NULL) {

        if ((!isdigit(cp[0])) || (atoi(cp) != w_status))
            continue;
        return true;
    }
    return false;
}



int
reniceJob(struct jobCard *jp)
{
    char fname[] = "reniceJob/sbd.job.c";
    int which, who, i;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG,
                  "%s: Job <%s> nice value %d",
                  fname, lsb_jobid2str(jp->jobSpecs.jobId), jp->jobSpecs.nice);




    if (jp->runRusage.npgids == 0) {
        if (jp->jobSpecs.jobPGid == 0) {

            who = jp->jobSpecs.jobPid;
            which = PRIO_PROCESS;
        } else {
            which = PRIO_PGRP;
            who = jp->jobSpecs.jobPGid;
        }


        if (setpriority(which, who, jp->jobSpecs.nice) == -1) {
            ls_syslog(LOG_DEBUG,
                      "%s: prio_pgrp which %d who %d nice %d for job %s: %m",
                      fname, which, who, jp->jobSpecs.nice,
                      lsb_jobid2str(jp->jobSpecs.jobId));
            which = PRIO_PROCESS;
            who = jp->jobSpecs.jobPid;

            if (setpriority(which, who, jp->jobSpecs.nice) == -1) {
                ls_syslog(LOG_DEBUG,
                          "%s: prio_process which %d who %d nice %d for job %s: %m",
                          fname, which, who, jp->jobSpecs.nice,
                          lsb_jobid2str(jp->jobSpecs.jobId));
                return -1;
            }
        }
    } else {
        which = PRIO_PGRP;
        for (i = 0; i < jp->runRusage.npgids; i++) {
            who = jp->runRusage.pgid[i];
            if (setpriority(which, who, jp->jobSpecs.nice) == -1)
                ls_syslog(LOG_DEBUG,
                          "%s: prio_pgrp which %d who %d nice %d for job %s: %m",
                          fname, which, who, jp->jobSpecs.nice,
                          lsb_jobid2str(jp->jobSpecs.jobId));
        }
    }

    return 0;
}


int
updateRUsageFromSuper(struct jobCard *jp, char *mbuf)
{
    static char fname[] = "updateRUsageFromSuper";
    static struct jRusage jusage;
    int i, ret, cnt;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG, "%s: Entering this routine ...", fname);



    ret = sscanf(mbuf, "%d%d%d%d%d%n",
                 &jusage.mem, &jusage.swap, &jusage.utime,
                 &jusage.stime, &jusage.npids, &cnt);

    if ( ret != 5 ) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5462,
                                         "%s: sscanf() got %d values from [%s], continuing"), /* catgets 5462 */
                  fname, ret, mbuf);
        return LSBE_PROTOCOL;
    }

    mbuf += cnt + 1;

    if (jusage.npids > 0) {
        FREEUP(jusage.pidInfo);
        jusage.pidInfo = my_calloc(jusage.npids, sizeof(struct pidInfo),
                                   fname);

        for (i = 0; i < jusage.npids; i++) {
            if ((ret = sscanf(mbuf, "%d%d%d%d%n",
                              &jusage.pidInfo[i].pid, &jusage.pidInfo[i].ppid,
                              &jusage.pidInfo[i].pgid, &jusage.pidInfo[i].jobid,
                              &cnt)) != 4) {
                ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5463,
                                                 "%s: sscanf() %s for job <%s> i=%d, ret=%d failed: %m"), /* catgets 5463 */
                          fname, "pid", lsb_jobid2str(jp->jobSpecs.jobId), i, ret);
                FREEUP(jusage.pidInfo);
                return LSBE_NO_JOB;
            }
            mbuf += cnt + 1;
        }
    }

    if ((ret = sscanf(mbuf, "%d%n", &jusage.npgids, &cnt)) != 1) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5463,
                                         "%s: sscanf() %s for job <%s> i=%d, ret=%d failed: %m"),
                  fname, "npgids", lsb_jobid2str(jp->jobSpecs.jobId), 0, ret);
        FREEUP(jusage.pidInfo);
        return LSBE_NO_JOB;
    }

    mbuf += cnt + 1;
    if (jusage.npgids > 0) {
        FREEUP(jusage.pgid);
        jusage.pgid = (int *) my_malloc(jusage.npgids * sizeof(int), fname);



        for (i = 0; i < jusage.npgids; i++) {
            if ((ret = sscanf(mbuf, "%d%n", &jusage.pgid[i], &cnt)) != 1) {
                ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5463,
                                                 "%s: sscanf() %s for job <%s> i=%d, ret=%d failed: %m"),
                          fname, "pgid", lsb_jobid2str(jp->jobSpecs.jobId), i, ret);
                FREEUP(jusage.pgid);
                FREEUP(jusage.pidInfo);
                return LSBE_NO_JOB;
            }
            mbuf += cnt + 1;
        }
    }


    updateJUsage(jp, &jusage);

    if (logclass & (LC_TRACE| LC_EXEC)) {
        ls_syslog(LOG_DEBUG,
                  "%s: ru job %s mem %d swap %d utime %d stime %d npids %d",
                  fname, lsb_jobid2str(jp->jobSpecs.jobId),
                  jp->runRusage.mem, jp->runRusage.swap, jp->runRusage.utime,
                  jp->runRusage.stime, jp->runRusage.npgids);
        for (i = 0; i < jp->runRusage.npgids; i++)
            ls_syslog(LOG_DEBUG, "... pgid[%d]=%d", i, jp->runRusage.pgid[i]);
    }


    return LSBE_NO_ERROR;

}

extern void updateJUsage(struct jobCard *jPtr, struct jRusage *jRusage)
{

    if (jPtr->newPam == TRUE) {

        if (jPtr->runRusage.utime == -1
            && jPtr->runRusage.stime == -1) {
            jPtr->runRusage.utime = jPtr->runRusage.stime = 0;
        }


        jPtr->runRusage.utime  +=  jRusage->utime;
        jPtr->runRusage.stime  +=  jRusage->stime;

    } else {


        jPtr->runRusage.utime += (jRusage->utime - jPtr->wrkRusage.utime);
        jPtr->runRusage.stime += (jRusage->stime - jPtr->wrkRusage.stime);
    }



    jPtr->runRusage.mem = MAX(jPtr->runRusage.mem,
                              jRusage->mem);
    jPtr->runRusage.swap = MAX(jPtr->runRusage.swap,
                               jRusage->swap);

    if (logclass & LC_EXEC) {
        ls_syslog(LOG_INFO,"\
%s: current rusage of job %d utime=%d stime=%d mem=%d swap=%d",
                  __func__, jPtr->jobSpecs.jobId, jPtr->runRusage.utime,
                  jPtr->runRusage.stime, jPtr->runRusage.mem,
                  jPtr->runRusage.swap);
    }


    jPtr->wrkRusage.utime  = jRusage->utime;
    jPtr->wrkRusage.stime  = jRusage->stime;


    copyPidInfo(jPtr, jRusage);

    if (jPtr->newPam == TRUE) {
        writePidInfoFile(jPtr, jRusage);
    }


    jPtr->newPam = FALSE;

}

/* copyPidInfo()
 */
void
copyPidInfo(struct jobCard *jPtr, struct jRusage *jRusage)
{

    FREEUP(jPtr->runRusage.pidInfo);
    jPtr->runRusage.npids = 0;
    FREEUP(jPtr->runRusage.pgid);
    jPtr->runRusage.npgids = 0;

    if (jRusage->npids > 0) {

        jPtr->runRusage.pidInfo = calloc(jRusage->npids,
                                         sizeof(struct pidInfo));

        jPtr->runRusage.npids = jRusage->npids;
        memcpy(jPtr->runRusage.pidInfo,
               jRusage->pidInfo,
               (jRusage->npids)*(sizeof(struct pidInfo)));
    }


    if (jRusage->npgids > 0) {

        jPtr->runRusage.pgid = calloc(jRusage->npgids, sizeof(int));
        jPtr->runRusage.npgids = jRusage->npgids;
        memcpy(jPtr->runRusage.pgid,
               jRusage->pgid,
               (jRusage->npgids)*(sizeof(int)));
    }
}

void
writePidInfoFile(struct jobCard *jPtr,
                 struct jRusage *jRusage)
{
    char buf[MAXFILENAMELEN];
    FILE *fp;
    int i;

    if (jPtr->jobSpecs.jobFile[0] == '/') {
        sprintf(buf, "%s/.%s.pidInfo", LSTMPDIR,
                strrchr(jPtr->jobSpecs.jobFile, '/') + 1);
    } else {
        sprintf(buf, "%s/.%s.pidInfo", LSTMPDIR, jPtr->jobSpecs.jobFile);
    }

    fp = fopen(buf, "w");
    if (fp == NULL) {
        ls_syslog(LOG_ERR, "\
%s: Unable to fopen() pidInfo file (%s), %m.", __func__, buf);
        return;
    }

    for (i = 0; i < jRusage->npids; i++) {
        fprintf(fp, "%d %d %d\n",
                jRusage->pidInfo[i].pid,
                jRusage->pidInfo[i].ppid,
                jRusage->pidInfo[i].pgid);
    }

    _fclose_(&fp);

}

static void
jobFinishRusage(struct jobCard *jp)
{
    char rufn[MAXFILENAMELEN];
    char rufn30[MAXFILENAMELEN];
    char tmpDirName[MAXFILENAMELEN];
    struct lsfAcctRec *rec;
    int lineNum = 0;
    FILE *fp;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG, "%s: Entering this routine ...", __func__);

    if (!jp->collectedChild) {

        LS_WAIT_T wStatus;

        LS_STATUS(wStatus) = 0xff00;
        jp->w_status = LS_STATUS(wStatus);
    }

    getJobTmpDir( (char *) tmpDirName, jp );

    if (jp->jobSpecs.jobFile[0] == '/') {
        sprintf(rufn, "%s/.%s.acct", tmpDirName,
                strrchr(jp->jobSpecs.jobFile, '/') + 1);
        sprintf(rufn30, "%s/.%s.acct", LSTMPDIR,
                strrchr(jp->jobSpecs.jobFile, '/') + 1);
    }
    else {
        sprintf(rufn, "%s/.%s.acct", tmpDirName, jp->jobSpecs.jobFile);
        sprintf(rufn30, "%s/.%s.acct", LSTMPDIR, jp->jobSpecs.jobFile);
    }

    if ((fp = fopen(rufn, "r")) == NULL
        && (fp = fopen(rufn30, "r")) == NULL) {
    }
    if (fp == NULL) {
        ls_syslog(LOG_DEBUG, "%s: fopen(%s) failed: %m",
                  __func__, rufn);
    } else {
        if ((rec = ls_getacctrec(fp, &lineNum)) == NULL) {
            ls_syslog(LOG_ERR, "%s: ls_getacctrec() %s failed",
                      __func__, rufn);

        } else {
            if (logclass & LC_EXEC) {
                LS_WAIT_T w_status;
                LS_STATUS(w_status) = rec->exitStatus;
                ls_syslog(LOG_DEBUG, "\
%s: Job %s status %d exitcode %d",
                          __func__, lsb_jobid2str(jp->jobSpecs.jobId),
                          rec->exitStatus,
                          WEXITSTATUS(w_status));
            }

            if (jp->collectedChild) {

                if (jp->lsfRusage.ru_utime > rec->lsfRu.ru_utime)
                    rec->lsfRu.ru_utime = jp->lsfRusage.ru_utime;
                if (jp->lsfRusage.ru_stime > rec->lsfRu.ru_stime)
                    rec->lsfRu.ru_stime = jp->lsfRusage.ru_stime;
            }
            cleanLsfRusage(&jp->lsfRusage);
            jp->lsfRusage = rec->lsfRu;
            jp->w_status = rec->exitStatus;
        }
        _fclose_(&fp);
    }
    if (jp->runRusage.utime > jp->lsfRusage.ru_utime)
        jp->lsfRusage.ru_utime = jp->runRusage.utime;
    if (jp->runRusage.stime > jp->lsfRusage.ru_stime)
        jp->lsfRusage.ru_stime = jp->runRusage.stime;

    jp->cpuTime = jp->lsfRusage.ru_utime + jp->lsfRusage.ru_stime;

}

int
initJobCard(struct jobCard *jp, struct jobSpecs *jobSpecs, int *reply)
{
    char *cp;
    char *word;
    int j;

    jp->resumeCondVal = NULL;
    jp->stopCondVal = NULL;

    cp = jp->jobSpecs.windows;
    for (j = 0; j < 8; j++)
        jp->week[j] = NULL;

    while ((word = getNextWord_(&cp)) != NULL) {
        if (addWindow(word, jp->week, "addJob jobSpecs") < 0) {
            ls_syslog(LOG_ERR,"\
%s: addWindow(%s) failed for job %s", __func__, word, lsb_jobid2str(jobSpecs->jobId));
            freeWeek(jp->week);
            *reply = ERR_BAD_REQ;
            return -1;
        }
    }

    if (jobSpecs->resumeCond && jobSpecs->resumeCond[0] != 0) {
        jp->resumeCondVal = checkThresholdCond(jobSpecs->resumeCond);
        if (! jp->resumeCondVal) {
            ls_syslog(LOG_ERR,"\
%s: resumeCondVal() %s failed for job %s", __func__,
                      jobSpecs->resumeCond, lsb_jobid2str(jobSpecs->jobId));
            freeWeek(jp->week);
            *reply = ERR_BAD_REQ;
            return -1;
        }
    }

    if (jobSpecs->stopCond && jobSpecs->stopCond[0] != 0) {
        jp->stopCondVal = checkThresholdCond(jobSpecs->stopCond);
        if (! jp->stopCondVal) {
            ls_syslog(LOG_ERR,"\
%s: stopCondVal() %s failed for job %s", __func__,
                      jobSpecs->resumeCond, lsb_jobid2str(jobSpecs->jobId));
            freeWeek(jp->week);
            *reply = ERR_BAD_REQ;
            return -1;
        }
    }

    setRunLimit(jp, TRUE);
    jp->windEdge = now;
    jp->active = FALSE;
    jp->windWarnTime = 0;
    jp->w_status = 0;
    jp->cpuTime = 0.0;
    jp->notReported = 0;
    jp->exitPid = 0;
    jp->missing = FALSE;
    jp->needReportRU = FALSE;
    jp->mbdRestarted = FALSE;
    jp->lastChkpntTime = now;

    jp->migCnt = 1;

    jp->cleanupPid = 0;
    jp->collectedChild = FALSE;

    cleanLsfRusage (&jp->lsfRusage);
    jp->client = NULL;
    jp->regOpFlag = 0;
    jp->jSupStatus = -1;
    initJRusage(&jp->runRusage);
    initJRusage(&jp->mbdRusage);
    initJRusage(&jp->maxRusage);
    jp->lastStatusMbdTime = now;
    jp->actStatus = ACT_NO;

    jp->jobSpecs.execHosts = NULL;

    jp->crossPlatforms = -1;
    if (jobSpecs->options2 >= 0) {
        if (jobSpecs->options2 & SUB2_HOST_UX)
            jp->crossPlatforms = FALSE;
        else
            jp->crossPlatforms = TRUE;

    }


    saveSpecs(&jp->jobSpecs, jobSpecs);
    jobcnt++;
    inJobLink (jp);
    jp->spooledExec = NULL;

    jp->postJobStarted = 0;
    jp->userJobSucc = FALSE;
    jp->cores = NULL;
    jp->numCores= 0;

    return 0;
}

void
saveThresholds (struct jobSpecs *jobSpecs, struct thresholds *thresholds)
{
    static char fname[] = "saveThresholds";
    int i, j;


    jobSpecs->thresholds.loadSched = (float **)
        my_calloc(thresholds->nThresholds, sizeof(float *), fname);
    jobSpecs->thresholds.loadStop = (float **)
        my_calloc(thresholds->nThresholds, sizeof(float *), fname);
    for (i = 0; i < thresholds->nThresholds; i++) {
        jobSpecs->thresholds.loadSched[i] = (float *)
            my_calloc(thresholds->nIdx, sizeof (float), fname);
        jobSpecs->thresholds.loadStop[i] = (float *)
            my_calloc(thresholds->nIdx, sizeof (float), fname);
    }
    for (i = 0; i < thresholds->nThresholds; i++) {
        for (j = 0; j < thresholds->nIdx; j++) {
            jobSpecs->thresholds.loadSched[i][j] = thresholds->loadSched[i][j];
            jobSpecs->thresholds.loadStop[i][j] = thresholds->loadStop[i][j];
        }
    }

    jobSpecs->thresholds.nIdx = thresholds->nIdx;
    jobSpecs->thresholds.nThresholds = thresholds->nThresholds;

}
void
freeThresholds (struct thresholds *thresholds)
{
    int i;

    if (thresholds->nThresholds <= 0)
        return;
    for (i = 0; i < thresholds->nThresholds; i++) {
        FREEUP (thresholds->loadSched[i]);
        FREEUP (thresholds->loadStop[i]);
    }
    FREEUP (thresholds->loadSched);
    FREEUP (thresholds->loadStop);
    thresholds->nIdx = 0;
    thresholds->nThresholds = 0;

}

static void
initJRusage(struct jRusage *jRusage)
{
    jRusage->mem = -1;
    jRusage->swap = -1;
    jRusage->utime = -1;
    jRusage->stime = -1;
    jRusage->npids = 0;
    jRusage->pidInfo = NULL;
    jRusage->npgids = 0;
    jRusage->pgid = NULL;
}


static int
getJobVersion (struct jobSpecs *jobSpecs)
{
    int i, version;
    char *sp;


    if (jobSpecs->numEnv <= 0)

        return 2;

    for (i = 0; i < jobSpecs->numEnv; i++) {
        if ((sp = strstr (jobSpecs->env[i], "LSF_VERSION=")) == NULL)
            continue;
        sp += strlen("LSF_VERSION=");
        version = atoi (sp);
        if (logclass & LC_EXEC)
            ls_syslog(LOG_DEBUG3,"getJobVersion: LSF_VERSION of job <%s> is <%d>", lsb_jobid2str(jobSpecs->jobId), version);
        if (version <= 0)
            return -1;
        return (version);
    }
    return (3);

}

int
lockHosts (struct jobCard *jp)
{
    static char fname[] = "lockHosts";
    int i;


    if (!(jp->jobSpecs.jAttrib & Q_ATTRIB_EXCLUSIVE))
        return 0;

    for (i = 0; i < jp->jobSpecs.numToHosts; i++) {
        if (i > 0 && strcmp (jp->jobSpecs.toHosts[i],
                             jp->jobSpecs.toHosts[i-1]) == 0)

            continue;
        if (lockHost_(0, jp->jobSpecs.toHosts[i]) < 0
            && lserrno != LSE_LIM_ALOCKED) {
            ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL_MM, fname,
                      "lockHost_",
                      "jp->jobSpecs.toHosts[i]");
            unlockHosts (jp, i);
            return -1;
        }
    }
    return 0;
}


static int
REShasPTYfix(char *resPath)
{
    FILE *fp;
    char str[256], cmd[MAXFILENAMELEN + 32];

    sprintf(cmd, "%s -PTY_FIX", resPath);

    if ((fp = popen(cmd, "r")) == NULL)
        return FALSE;

    if (fscanf(fp, "%s", str) != 1) {
        pclose(fp);
        return false;
    }

    pclose(fp);

    if (strcmp(str, "PTY_FIX"))
        return false;

    return true;

}

static void
setJobArrayEnv(char *jobName, int jobIndex)
{
    struct idxList *idxList = NULL, *idx;
    char   *index,  val[MAXLINELEN];
    int found = FALSE;
    int maxJLimit = 0;

    index = strchr(jobName, '[');
    if (!index)
        return;
    yybuff = index;
    if (idxparse(&idxList, &maxJLimit)) {
        ls_syslog(LOG_ERR, "%s: idxparse() error", __func__);
        return;
    }

    for (idx = idxList; idx; idx = idx->next) {
        int nstep;

        if ((jobIndex >= idx->start) && (jobIndex <= idx->end)) {
            nstep = (jobIndex - idx->start)/idx->step;
            if ((idx->start + nstep * idx->step) == jobIndex) {
                found = TRUE;
                break;
            }
        }
    }

    if (found) {
        sprintf(val, "%d", idx->step);
        putEnv("LSB_JOBINDEX_STEP", val);
        sprintf(val, "%d", idx->end);
        putEnv("LSB_JOBINDEX_END", val);
    } else {
        ls_syslog(LOG_ERR, "\
%s: Job %d not found in job index list", __func__, jobIndex);
    }

    while (idxList) {
        idx = idxList->next;
        FREEUP(idxList);
        idxList = idx;
    }

}

/* setup_mem_cgroup()
 */
static void
setup_mem_cgroup(struct jobCard *jPtr)
{
    struct rlimit rlimit;
    char job_id[64];

    rlimitDecode_(&jPtr->jobSpecs.lsfLimits[LSF_RLIMIT_RSS],
                  &rlimit,
                  LSF_RLIMIT_RSS);

    sprintf(job_id, "%s", lsb_jobid2str(jPtr->jobSpecs.jobId));

    lsb_constrain_mem(job_id, rlimit.rlim_cur, jPtr->jobSpecs.jobPid);

    if (logclass & LC_EXEC) {
        ls_syslog(LOG_INFO, "%s: job %s cur %lu max %lu", __func__,
                  job_id,rlimit.rlim_cur, rlimit.rlim_max);
    }
}

/* rm_mem_cgroup()
 */
static void
rm_mem_cgroup(struct jobCard *jPtr)
{
    char job_id[64];

    sprintf(job_id, "%s", lsb_jobid2str(jPtr->jobSpecs.jobId));
    lsb_rmcgroup_mem(job_id, jPtr->jobSpecs.jobPid);

    if (logclass & LC_EXEC) {
        ls_syslog(LOG_INFO, "%s: cleanup job %s", __func__, job_id);
    }
}

/* select_cpu_to_bind()
 */
static int*
select_cpu_to_bind(struct jobCard *jPtr)
{
    int *selected;

    if (! daemonParams[SBD_BIND_CPU].paramValue
        && !hostAffinity)
        return NULL;

    if (jPtr->jobSpecs.hostShares > 0) {
        selected = get_core_shares(jPtr->jobSpecs.queue,
                                   jPtr->jobSpecs.hostShares,
                                   &jPtr->numCores);
    } else {
        selected = find_free_core(jPtr->jobSpecs.numToHosts);
        jPtr->numCores = jPtr->jobSpecs.numToHosts;
    }

    if (!selected) {
        ls_syslog(LOG_ERR, "\
%s: failed to find free core for job %d", __func__,
                  jPtr->jobSpecs.jobId);
        jPtr->numCores = 0;
    }
    return selected;
}

/* setup_cpu_bind()
 */
static void
setup_cpu_bind(struct jobCard *jPtr, int* selected)
{
    int cc;

    if (!selected)
        return;

    cc = bind_to_core(jPtr->jobSpecs.jobPid,
                      jPtr->numCores,
                      selected);
    if (cc < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed to bind to core %d job %d pid %d", __func__,
                  selected[0], jPtr->jobSpecs.jobId, jPtr->jobSpecs.jobPid);
        FREEUP(selected);
        return;
    }

    if (logclass & LC_EXEC) {
        ls_syslog(LOG_INFO, "%\
s: job %d pid %d bound to core %d", __func__, jPtr->jobSpecs.jobId,
                  jPtr->jobSpecs.jobPid, selected[0]);
    }

    jPtr->cores = selected;
}
