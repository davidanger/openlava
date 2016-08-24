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

extern int urgentJob;
extern int jRusageUpdatePeriod;
extern int  rusageUpdateRate;
extern int  rusageUpdatePercent;
extern int lsbJobCpuLimit;
extern int lsbJobMemLimit;

static struct jRusage *blaunch_jru;

static int replyHdrWithRC(int, int, int);
static struct jobCard *find_job_card(int);

void
do_newjob(XDR *xdrs, int chfd, struct LSFHeader *reqHdr)
{
    static char        fname[] = "do_newjob()";
    char               reply_buf[MSGSIZE];
    XDR                xdrs2;
    struct jobSpecs    jobSpecs;
    struct jobReply    jobReply;
    struct jobCard     *jp;
    sbdReplyType       reply;
    struct LSFHeader   replyHdr;
    char               *replyStruct;
    struct lsfAuth     *auth = NULL;

    memset(&jobReply, 0, sizeof(struct jobReply));

    if (!xdr_jobSpecs(xdrs, &jobSpecs, reqHdr)) {
        reply = ERR_BAD_REQ;
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_jobSpecs");
        goto sendReply;
    }


    for (jp = jobQueHead->forw; (jp != jobQueHead); jp = jp->forw) {
        if (jp->jobSpecs.jobId == jobSpecs.jobId) {

            jobReply.jobId = jp->jobSpecs.jobId;
            jobReply.jobPid = jp->jobSpecs.jobPid;
            jobReply.jobPGid = jp->jobSpecs.jobPGid;
            jobReply.jStatus = jp->jobSpecs.jStatus;
            reply = ERR_NO_ERROR;
            goto sendReply;
        }
    }

    jp = calloc(1, sizeof(struct jobCard));
    if (jp == NULL) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jobSpecs.jobId), "calloc");
        reply = ERR_MEM;
        goto sendReply;
    }

    memcpy(&jp->jobSpecs, &jobSpecs, sizeof(struct jobSpecs));

    jp->jobSpecs.jStatus &= ~JOB_STAT_MIG;
    jp->jobSpecs.startTime = now;
    jp->jobSpecs.reasons = 0;
    jp->jobSpecs.subreasons = 0;
    /* Initialize the core number
     */
    jp->cores = NULL;
    jp->numCores= 0;

    if (jp->jobSpecs.jAttrib & Q_ATTRIB_EXCLUSIVE) {
        if (lockHosts (jp) < 0) {
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S, fname,
                      lsb_jobid2str(jp->jobSpecs.jobId), "lockHosts");
            unlockHosts (jp, jp->jobSpecs.numToHosts);
            reply = ERR_LOCK_FAIL;
            freeWeek(jp->week);
            FREEUP(jp);
            goto sendReply;
        }
    }

    jp->runTime = 0;
    if (initJobCard(jp, &jobSpecs, (int *)&reply) < 0) {

        if (jp->jobSpecs.jAttrib & Q_ATTRIB_EXCLUSIVE) {
            unlockHosts (jp, jp->jobSpecs.numToHosts);
        }
        FREEUP(jp);
        goto sendReply;
    }

    jp->execJobFlag = 0;

    if (jp->runTime < 0) {
        jp->runTime = 0;
    }
    jp->execGid = 0;
    jp->execUsername[0] = '\0';
    jp->jobSpecs.execUid = -1;
    jp->jobSpecs.execUsername[0] = '\0';

    if (jp->jobSpecs.jobSpoolDir[0] != '\0') {
        char *tmp;

        if ((tmp = getUnixSpoolDir (jp->jobSpecs.jobSpoolDir)) == NULL) {

            jp->jobSpecs.jobSpoolDir[0] = '\0';
        }
    }

    if ((logclass & LC_TRACE) && jp->jobSpecs.jobSpoolDir[0] != 0) {
        ls_syslog(LOG_DEBUG,
                  "%s: the SpoolDir for  job <%s>  is %s \n",
                  fname, lsb_jobid2str(jp->jobSpecs.jobId),
                  jp->jobSpecs.jobSpoolDir);
    }
    if (jp->jobSpecs.options & SUB_PRE_EXEC)
        SBD_SET_STATE(jp, (JOB_STAT_RUN | JOB_STAT_PRE_EXEC))
        else
            SBD_SET_STATE(jp, JOB_STAT_RUN);

    reply = job_exec(jp, chfd);

    if (reply != ERR_NO_ERROR) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S, fname,
                  lsb_jobid2str(jp->jobSpecs.jobId), "job_exec");
        if (jp->jobSpecs.jAttrib & Q_ATTRIB_EXCLUSIVE) {
            unlockHosts (jp, jp->jobSpecs.numToHosts);
        }
        deallocJobCard(jp);
    } else {
        jobReply.jobId = jp->jobSpecs.jobId;
        jobReply.jobPid = jp->jobSpecs.jobPid;
        jobReply.jobPGid = jp->jobSpecs.jobPGid;
        jobReply.jStatus = jp->jobSpecs.jStatus;
    }


sendReply:
    xdr_lsffree(xdr_jobSpecs, (char *)&jobSpecs, reqHdr);
    xdrmem_create(&xdrs2, reply_buf, MSGSIZE, XDR_ENCODE);
    initLSFHeader_(&replyHdr);
    replyHdr.opCode = reply;
    replyStruct = (reply == ERR_NO_ERROR) ? (char *) &jobReply : (char *) NULL;
    if (!xdr_encodeMsg(&xdrs2, replyStruct, &replyHdr, xdr_jobReply, 0, auth)) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_jobReply");
        lsb_merr(_i18n_msg_get(ls_catd , NL_SETN, 5804,
                               "Fatal error: xdr_jobReply() failed; sbatchd relifing")); /* catgets 5804 */
        relife();
    }

    if (chanWrite_(chfd, reply_buf, XDR_GETPOS(&xdrs2)) <= 0) {
        ls_syslog(LOG_ERR, "\
%s: chanWrite() jobReply len %d to master failed: %m", __func__, XDR_GETPOS(&xdrs2));
    }

    xdr_destroy(&xdrs2);


    if (reply == ERR_NO_ERROR && !daemonParams[LSB_BSUBI_OLD].paramValue &&
        PURE_INTERACTIVE(&jp->jobSpecs)) {
        if (status_job (BATCH_STATUS_JOB, jp, jp->jobSpecs.jStatus,
                        ERR_NO_ERROR) < 0) {
            jp->notReported++;
        }
    }
}

void
do_switchjob(XDR * xdrs, int chfd, struct LSFHeader * reqHdr)
{
    static char        fname[] = "do_switchjob()";
    char               reply_buf[MSGSIZE];
    XDR                xdrs2;
    struct jobSpecs    jobSpecs;
    struct jobReply    jobReply;
    int                i;
    sbdReplyType       reply;
    char               *cp;
    char               *word;
    char               found = FALSE;
    struct LSFHeader   replyHdr;
    char               *replyStruct;
    struct jobCard     *jp;
    struct lsfAuth     *auth = NULL;

    memset(&jobReply, 0, sizeof(struct jobReply));

    if (!xdr_jobSpecs(xdrs, &jobSpecs, reqHdr)) {
        reply = ERR_BAD_REQ;
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_jobSpecs");
        goto sendReply;
    }
    for (jp = jobQueHead->back; jp != jobQueHead; jp = jp->back) {
        if (jp->jobSpecs.jobId == jobSpecs.jobId) {
            found = TRUE;
            break;
        }
    }
    if (!found) {
        reply = ERR_NO_JOB;
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5807,
                                         "%s: mbatchd trying to switch a non-existent job <%s>"), fname, lsb_jobid2str(jobSpecs.jobId)); /* catgets 5807 */
        goto sendReply;
    }
    if (jp->jobSpecs.jStatus & (JOB_STAT_DONE | JOB_STAT_EXIT)) {
        reply = ERR_JOB_FINISH;
        goto sendReply;
    }


    cp = jobSpecs.windows;
    freeWeek(jp->week);
    while ((word = getNextWord_(&cp)) != NULL) {
        if (addWindow(word, jp->week, "switchJob jobSpecs") < 0) {
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_M, fname,
                      lsb_jobid2str(jp->jobSpecs.jobId), "addWindow", word);
            freeWeek(jp->week);
            reply = ERR_BAD_REQ;
            goto sendReply;
        }
    }
    jp->windEdge = now;


    if ((jp->jobSpecs.jAttrib & Q_ATTRIB_EXCLUSIVE)
        && !(jobSpecs.jAttrib & Q_ATTRIB_EXCLUSIVE))
        for (i = 0; i < jp->jobSpecs.numToHosts; i++)
            if (unlockHost_(jp->jobSpecs.toHosts[i]) < 0
                && lserrno != LSE_LIM_NLOCKED)
                ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S_MM, fname,
                          lsb_jobid2str(jp->jobSpecs.jobId), "unlockHost_", jp->jobSpecs.toHosts[i]);



    strcpy(jp->jobSpecs.queue, jobSpecs.queue);
    strcpy(jp->jobSpecs.windows, jobSpecs.windows);
    jp->jobSpecs.priority = jobSpecs.priority;
    jp->jobSpecs.nice = jobSpecs.nice;
    jp->jobSpecs.jAttrib = jobSpecs.jAttrib;

    freeThresholds (&jp->jobSpecs.thresholds);
    saveThresholds (&jp->jobSpecs, &jobSpecs.thresholds);


    memcpy((char *) &jp->jobSpecs.lsfLimits[LSF_RLIMIT_RUN],
           (char *) &jobSpecs.lsfLimits[LSF_RLIMIT_RUN],
           sizeof(struct lsfLimit));


    strcpy (jp->jobSpecs.requeueEValues, jobSpecs.requeueEValues);
    strcpy (jp->jobSpecs.resumeCond, jobSpecs.resumeCond);
    strcpy (jp->jobSpecs.stopCond, jobSpecs.stopCond);

    lsbFreeResVal (&jp->resumeCondVal);
    if (jobSpecs.resumeCond && jobSpecs.resumeCond[0] != '\0') {
        if ((jp->resumeCondVal = checkThresholdCond (jobSpecs.resumeCond))
            == NULL)
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S, fname,
                      lsb_jobid2str(jp->jobSpecs.jobId),
                      "checkThresholdCond", jobSpecs.resumeCond);
    }

    lsbFreeResVal (&jp->stopCondVal);
    if (jobSpecs.stopCond && jobSpecs.stopCond[0] != '\0') {
        if ((jp->stopCondVal = checkThresholdCond (jobSpecs.stopCond))
            == NULL)
            ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_S, fname,
                      lsb_jobid2str(jp->jobSpecs.jobId),
                      "checkThresholdCond", jobSpecs.stopCond);
    }

    if (jobSpecs.options & SUB_LOGIN_SHELL) {
        FREEUP (jp->jobSpecs.loginShell);
        jp->jobSpecs.loginShell = safeSave (jobSpecs.loginShell);
    }

    strcpy (jp->jobSpecs.suspendActCmd, jobSpecs.suspendActCmd);
    strcpy (jp->jobSpecs.resumeActCmd, jobSpecs.resumeActCmd);
    strcpy (jp->jobSpecs.terminateActCmd, jobSpecs.terminateActCmd);

    setRunLimit (jp, FALSE);
    offList ((struct listEntry *)jp);
    inJobLink (jp);

    if (reniceJob(jp) < 0)
        ls_syslog(LOG_DEBUG, "%s: renice job <%s> failed",
                  fname, lsb_jobid2str(jp->jobSpecs.jobId));

    reply = ERR_NO_ERROR;
    jobReply.jobId = jp->jobSpecs.jobId;
    jobReply.jobPid = jp->jobSpecs.jobPid;
    jobReply.jobPGid = jp->jobSpecs.jobPGid;
    jobReply.jStatus = jp->jobSpecs.jStatus;

sendReply:
    xdr_lsffree(xdr_jobSpecs, (char *)&jobSpecs, reqHdr);
    xdrmem_create(&xdrs2, reply_buf, MSGSIZE, XDR_ENCODE);
    initLSFHeader_(&replyHdr);
    replyHdr.opCode = reply;
    if (reply == ERR_NO_ERROR)
        replyStruct = (char *) &jobReply;
    else {
        replyStruct = (char *) 0;
    }

    if (!xdr_encodeMsg(&xdrs2, replyStruct, &replyHdr, xdr_jobReply, 0, auth)) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jp->jobSpecs.jobId),
                  "xdr_jobReply");
        relife();
    }

    if (chanWrite_(chfd, reply_buf, XDR_GETPOS(&xdrs2)) <= 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jp->jobSpecs.jobId), "chanWrite_");
    }

    xdr_destroy(&xdrs2);

    return;

}

void
do_modifyjob(XDR * xdrs, int chfd, struct LSFHeader * reqHdr)
{
    static char        fname[] = "do_switchjob()";
    char               reply_buf[MSGSIZE];
    XDR                xdrs2;
    struct jobSpecs    jobSpecs;
    struct jobReply    jobReply;
    sbdReplyType       reply;
    char               found = FALSE;
    struct LSFHeader   replyHdr;
    char               *replyStruct;
    struct jobCard     *jp;
    struct lsfAuth     *auth = NULL;

    memset(&jobReply, 0, sizeof(struct jobReply));

    if (!xdr_jobSpecs(xdrs, &jobSpecs, reqHdr)) {
        reply = ERR_BAD_REQ;
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_jobSpecs");
        goto sendReply;
    }
    for (jp = jobQueHead->back; jp != jobQueHead; jp = jp->back)
        if (jp->jobSpecs.jobId == jobSpecs.jobId) {
            found = TRUE;
            break;
        }
    if (!found) {
        reply = ERR_NO_JOB;
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5808,
                                         "%s: mbatchd trying to modify a non-existent job <%s>"), fname, lsb_jobid2str(jobSpecs.jobId)); /* catgets 5808 */
        goto sendReply;
    }
    if (jp->jobSpecs.jStatus & (JOB_STAT_DONE | JOB_STAT_EXIT)) {
        reply = ERR_JOB_FINISH;
        goto sendReply;
    }
    if ((lsbJobCpuLimit != 1) &&
        ((jp->jobSpecs.lsfLimits[LSF_RLIMIT_CPU].rlim_maxl
          != jobSpecs.lsfLimits[LSF_RLIMIT_CPU].rlim_maxl) ||
         (jp->jobSpecs.lsfLimits[LSF_RLIMIT_CPU].rlim_maxh
          != jobSpecs.lsfLimits[LSF_RLIMIT_CPU].rlim_maxh) ||
         (jp->jobSpecs.lsfLimits[LSF_RLIMIT_CPU].rlim_curl
          != jobSpecs.lsfLimits[LSF_RLIMIT_CPU].rlim_curl) ||
         (jp->jobSpecs.lsfLimits[LSF_RLIMIT_CPU].rlim_curh
          != jobSpecs.lsfLimits[LSF_RLIMIT_CPU].rlim_curh)
            )) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd, NL_SETN, 5809, "%s, LSB_JOB_CPULIMIT is not set for the host, job <%s>, CPU limit not modified"), fname, lsb_jobid2str(jobSpecs.jobId));
    } else {
        memcpy((char *) &jp->jobSpecs.lsfLimits[LSF_RLIMIT_CPU],
               (char *) &jobSpecs.lsfLimits[LSF_RLIMIT_CPU],
               sizeof(struct lsfLimit));
    }
    if ((lsbJobMemLimit != 1) &&
        ((jp->jobSpecs.lsfLimits[LSF_RLIMIT_RSS].rlim_maxl
          != jobSpecs.lsfLimits[LSF_RLIMIT_RSS].rlim_maxl) ||
         (jp->jobSpecs.lsfLimits[LSF_RLIMIT_RSS].rlim_maxh
          != jobSpecs.lsfLimits[LSF_RLIMIT_RSS].rlim_maxh) ||
         (jp->jobSpecs.lsfLimits[LSF_RLIMIT_RSS].rlim_curl
          != jobSpecs.lsfLimits[LSF_RLIMIT_RSS].rlim_curl) ||
         (jp->jobSpecs.lsfLimits[LSF_RLIMIT_RSS].rlim_curh
          != jobSpecs.lsfLimits[LSF_RLIMIT_RSS].rlim_curh)
            )) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd, NL_SETN, 5810, "%s, LSB_JOB_MEMLIMIT is not set for the host, job <%s>, memory limit not modified"), fname, lsb_jobid2str(jobSpecs.jobId));
    } else {
        memcpy((char *) &jp->jobSpecs.lsfLimits[LSF_RLIMIT_RSS],
               (char *) &jobSpecs.lsfLimits[LSF_RLIMIT_RSS],
               sizeof(struct lsfLimit));
    }

    memcpy((char *) &jp->jobSpecs.lsfLimits[LSF_RLIMIT_RUN],
           (char *) &jobSpecs.lsfLimits[LSF_RLIMIT_RUN],
           sizeof(struct lsfLimit));
    setRunLimit(jp, FALSE);
    if (strcmp(jp->jobSpecs.outFile, jobSpecs.outFile) ||
        !(strcmp(jobSpecs.outFile, "/dev/null")))
    {
        strcpy(jp->jobSpecs.outFile, jobSpecs.outFile);
        if (strcmp(jobSpecs.outFile, "/dev/null") ||
            (jobSpecs.options & SUB_OUT_FILE)) {
            jp->jobSpecs.options |= SUB_OUT_FILE;
        }
        else {
            jp->jobSpecs.options &= ~SUB_OUT_FILE;
        }
    }
    if (strcmp(jp->jobSpecs.errFile, jobSpecs.errFile))
    {
        strcpy(jp->jobSpecs.errFile, jobSpecs.errFile);
        if (!strcmp(jp->jobSpecs.errFile, "/dev/null")
            && !(jobSpecs.options & SUB_ERR_FILE)) {
            jp->jobSpecs.options &= ~SUB_ERR_FILE;
        }
    }

    if (jobSpecs.options & SUB_RERUNNABLE) {
        jp->jobSpecs.options |= SUB_RERUNNABLE;
    } else {
        jp->jobSpecs.options &= ~SUB_RERUNNABLE;
    }

sendReply:
    xdr_lsffree(xdr_jobSpecs, (char *)&jobSpecs, reqHdr);
    xdrmem_create(&xdrs2, reply_buf, MSGSIZE, XDR_ENCODE);
    initLSFHeader_(&replyHdr);
    replyHdr.opCode = reply;
    if (reply == ERR_NO_ERROR)
        replyStruct = (char *) &jobReply;
    else {
        replyStruct = (char *) 0;
    }

    if (!xdr_encodeMsg(&xdrs2, replyStruct, &replyHdr, xdr_jobReply, 0, auth)) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jp->jobSpecs.jobId),
                  "xdr_jobReply");
        relife();
    }

    if (chanWrite_(chfd, reply_buf, XDR_GETPOS(&xdrs2)) <= 0) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jp->jobSpecs.jobId), "chanWrite_");
    }

    xdr_destroy(&xdrs2);

    return;

}
void
do_probe(XDR * xdrs, int chfd, struct LSFHeader * reqHdr)
{
    static char         fname[] = "do_probe()";
    char                reply_buf[MSGSIZE];
    XDR                 xdrs2;
    struct LSFHeader    replyHdr;
    struct sbdPackage   sbdPackage;
    struct jobSpecs     *jobSpecs;
    int                 i;
    struct lsfAuth      *auth = NULL;

    if (reqHdr->length == 0)
        return;

    initLSFHeader_(&replyHdr);
    replyHdr.opCode = ERR_NO_ERROR;
    jobSpecs = NULL;

    if (!xdr_sbdPackage(xdrs, &sbdPackage, reqHdr)) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_sbdPackage");
        relife();
    } else {
        if (sbdPackage.numJobs) {
            jobSpecs = my_calloc(sbdPackage.numJobs,
                                 sizeof(struct jobSpecs), fname);
            for (i = 0; i < sbdPackage.numJobs; i++) {
                if (!xdr_arrayElement(xdrs, (char *) &(jobSpecs[i]),
                                      reqHdr, xdr_jobSpecs)) {
                    replyHdr.opCode = ERR_BAD_REQ;
                    ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5815,
                                                     "%s: %s(%d) failed for %d jobs"), /* catgets 5815 */
                              fname, "xdr_arrayElement", i, sbdPackage.numJobs);
                    break;
                }
                refreshJob(&(jobSpecs[i]));
                xdr_lsffree(xdr_jobSpecs, (char *)&jobSpecs[i], reqHdr);
            }
        }
    }
    if (replyHdr.opCode == ERR_NO_ERROR)
        if (!xdr_sbdPackage1(xdrs, &sbdPackage, reqHdr)) {
            ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_sbdPackage1");
            relife();
        }
    if (replyHdr.opCode == ERR_NO_ERROR) {
        if (myStatus & NO_LIM) {
            replyHdr.opCode = ERR_NO_LIM;
        }
    }
    xdrmem_create(&xdrs2, reply_buf, MSGSIZE, XDR_ENCODE);

    if (!xdr_encodeMsg(&xdrs2, NULL, &replyHdr, NULL, 0, auth)) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_encodeMsg");
        relife();
    }

    if (chanWrite_(chfd, reply_buf, XDR_GETPOS(&xdrs2)) <= 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL_M, fname, "chanWrite_");
    }

    xdr_destroy(&xdrs2);

    if (jobSpecs != NULL)
        free(jobSpecs);


    getManagerId(&sbdPackage);

    mbdPid = sbdPackage.mbdPid;
    sbdSleepTime = sbdPackage.sbdSleepTime;
    retryIntvl = sbdPackage.retryIntvl;
    preemPeriod = sbdPackage.preemPeriod;
    pgSuspIdleT = sbdPackage.pgSuspIdleT;
    maxJobs = sbdPackage.maxJobs;
    uJobLimit = sbdPackage.uJobLimit;
    rusageUpdateRate = sbdPackage.rusageUpdateRate;
    rusageUpdatePercent = sbdPackage.rusageUpdatePercent;
    jobTerminateInterval = sbdPackage.jobTerminateInterval;
    hostAffinity = sbdPackage.affinity;

    for (i = 0; i < sbdPackage.nAdmins; i++)
        FREEUP(sbdPackage.admins[i]);
    FREEUP(sbdPackage.admins);

    return;
}

void
do_sigjob(XDR * xdrs, int chfd, struct LSFHeader * reqHdr)
{
    static char        fname[] = "do_sigjob()";
    char               reply_buf[MSGSIZE];
    XDR                xdrs2;
    struct jobSig      jobSig;
    sbdReplyType       reply;
    struct jobReply    jobReply;
    struct LSFHeader   replyHdr;
    char               *replyStruct;
    struct jobCard     *jp = NULL;
    char               found = FALSE;
    int                cc;
    int                sigValue;
    int                savedActReasons;
    int                savedActSubReasons;
    struct lsfAuth     *auth = NULL;

    memset(&jobReply, 0, sizeof(struct jobReply));
    if (!xdr_jobSig(xdrs, &jobSig, reqHdr)) {
        reply = ERR_BAD_REQ;
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_jobSig");

        goto Reply1;
    }

    jobSig.sigValue = sig_decode(jobSig.sigValue);
    sigValue = jobSig.sigValue;

    if (logclass & LC_SIGNAL)
        ls_syslog(LOG_DEBUG, "do_sigJob: sigValue =%d", sigValue);

    for (jp = jobQueHead->forw; (jp != jobQueHead); jp = jp->forw) {
        if (jp->jobSpecs.jobId != jobSig.jobId)
            continue;
        found = TRUE;
        break;
    }
    if (found == FALSE) {
        reply = ERR_NO_JOB;
        jp = NULL;
        goto Reply1;
    }

    if (jobSig.reasons & SUSP_MBD_LOCK) {

        jp->jobSpecs.reasons = jobSig.reasons;
        jp->jobSpecs.subreasons = jobSig.subReasons;
        savedActReasons = jp->actReasons;
        savedActSubReasons = jp->actSubReasons;
        jp->actReasons = jobSig.reasons;
        jp->actSubReasons = jobSig.subReasons;
    }


    if (jp->postJobStarted) {
        reply = ERR_NO_ERROR;
        goto Reply1;
    }

    if (IS_FINISH(jp->jobSpecs.jStatus)) {
        reply = ERR_NO_ERROR;
        goto Reply1;
    }

    if (jp->jobSpecs.jobPGid == -1) {
        SBD_SET_STATE(jp, JOB_STAT_EXIT);
        reply = ERR_NO_ERROR;
        goto Reply;
    }

    if (!JOB_STARTED(jp)) {
        if (isSigTerm(sigValue) == TRUE) {
            if ((cc = jobSigStart (jp, sigValue, jobSig.actFlags, jobSig.chkPeriod, NO_SIGLOG)) < 0)
                reply = ERR_SIG_RETRY;
            else
                reply = ERR_NO_ERROR;

            goto Reply;
        }

        reply = ERR_SIG_RETRY;

        if (logclass & LC_EXEC)
            ls_syslog(LOG_DEBUG, "%s: Retry signal %s for job <%s>",
                      fname, getLsbSigSymbol(sigValue),
                      lsb_jobid2str(jp->jobSpecs.jobId));
        goto Reply1;
    }

    if (IS_PEND(jp->jobSpecs.jStatus)) {
        reply = ERR_SIG_RETRY;
        goto Reply1;
    }

    if (jp->jobSpecs.actPid || (jp->jobSpecs.jStatus & JOB_STAT_MIG)) {

        if ((cc = jobSigStart(jp,
                              sigValue,
                              jobSig.actFlags,
                              jobSig.chkPeriod,
                              NO_SIGLOG)) < 0)
            reply = ERR_SIG_RETRY;
        else {
            jp->jobSpecs.jStatus &= ~JOB_STAT_MIG;
            reply = ERR_NO_ERROR;
        }
        goto Reply;
    }

    if ((cc = jobSigStart(jp,
                          sigValue,
                          jobSig.actFlags,
                          jobSig.chkPeriod,
                          NO_SIGLOG)) < 0)
        reply = ERR_SIG_RETRY;
    else
        reply = ERR_NO_ERROR;

Reply:
    sbdlog_newstatus(jp);

Reply1:

    xdrmem_create(&xdrs2, reply_buf, MSGSIZE, XDR_ENCODE);

    initLSFHeader_(&replyHdr);
    replyHdr.opCode = reply;
    if (reply == ERR_NO_ERROR) {
        jobReply.jobPid = jp->jobSpecs.jobPid;
        jobReply.actPid = jp->jobSpecs.actPid;
        jobReply.jobId = jp->jobSpecs.jobId;
        jobReply.jobPGid = jp->jobSpecs.jobPGid;
        jobReply.jStatus = jp->jobSpecs.jStatus;
        jobReply.reasons = jp->jobSpecs.reasons;
        jobReply.actStatus = jp->actStatus;
        replyStruct = (char *) &jobReply;
    } else {
        if (reply != ERR_NO_JOB)
            if  ((jp != NULL) && (jobSig.reasons & SUSP_MBD_LOCK)) {
                jp->actReasons = savedActReasons;
                jp->actSubReasons = savedActSubReasons;
            }
        replyStruct = (char *) 0;
    }

    if (!xdr_encodeMsg(&xdrs2, replyStruct, &replyHdr, xdr_jobReply, 0, auth)) {
        ls_syslog(LOG_ERR, I18N_JOB_FAIL_S_M, fname,
                  lsb_jobid2str(jp->jobSpecs.jobId), "xdr_jobReply");
        relife();
    }

    if (chanWrite_(chfd, reply_buf, XDR_GETPOS(&xdrs2)) <= 0) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5821,
                                         "%s: Sending jobReply (len=%d) to master failed for job <%s>: %m"), fname, XDR_GETPOS(&xdrs2), lsb_jobid2str(jobSig.jobId)); /* catgets 5821 */
    }
    if (jp != NULL)
        jp->actStatus = ACT_NO;

    xdr_destroy(&xdrs2);

    return;
}

void
do_reboot(XDR * xdrs, int chfd, struct LSFHeader * reqHdr)
{
    static char        fname[] = "do_reboot()";
    char               reply_buf[MSGSIZE / 8];
    XDR                xdrs2;
    sbdReplyType       reply;
    struct LSFHeader   replyHdr;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG, "%s: Entering this routine...", fname);

    reply = ERR_NO_ERROR;
    xdrmem_create(&xdrs2, reply_buf, MSGSIZE / 8, XDR_ENCODE);

    initLSFHeader_(&replyHdr);
    if (reqHdr->opCode == CMD_SBD_REBOOT)
        replyHdr.opCode = LSBE_NO_ERROR;
    else
        replyHdr.opCode = reply;

    if (!xdr_encodeMsg(&xdrs2, (char *) 0, &replyHdr, 0, 0, NULL)) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_encodeMsg");
        xdr_destroy(&xdrs2);
        relife();
        return;
    }

    if (chanWrite_(chfd, reply_buf, XDR_GETPOS(&xdrs2)) <= 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL_M, fname, "chanWrite_");
        xdr_destroy(&xdrs2);
        relife();
        return;
    }
    ls_syslog(LOG_NOTICE, _i18n_msg_get(ls_catd , NL_SETN, 5828,
                                        "Slave batch daemon reboot command received")); /* catgets 5828 */
    relife();
    ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5829,
                                     "Unable to relife during rebooting: %m")); /* catgets 5829 */
    xdr_destroy(&xdrs2);

}


int
ctrlSbdDebug(struct debugReq  *pdebug)
{
    static char   fname[]="ctrlSbdDebug()";
    int           opCode;
    int           level;
    int           newClass;
    int           options;
    char          logFileName[MAXLSFNAMELEN];
    char          lsfLogDir[MAXPATHLEN];
    char          *dir;
    char          dynDbgEnv[MAXPATHLEN];

    memset(logFileName, 0, sizeof(logFileName));
    memset(lsfLogDir, 0, sizeof(lsfLogDir));

    opCode = pdebug->opCode;
    level = pdebug->level;
    newClass = pdebug->logClass;
    options = pdebug->options;

    if (pdebug->logFileName[0] != '\0') {
        if (((dir = strrchr(pdebug->logFileName,'/')) != NULL) ||
            ((dir = strrchr(pdebug->logFileName,'\\')) != NULL)) {
            dir++;
            ls_strcat(logFileName, sizeof(logFileName), dir);
            *(--dir) = '\0';
            ls_strcat(lsfLogDir, sizeof(lsfLogDir), pdebug->logFileName);
        }
        else {
            ls_strcat(logFileName, sizeof(logFileName), pdebug->logFileName);
            if (daemonParams[LSF_LOGDIR].paramValue
                && *(daemonParams[LSF_LOGDIR].paramValue)) {
                ls_strcat(lsfLogDir, sizeof(lsfLogDir),
                          daemonParams[LSF_LOGDIR].paramValue);
            }
            else {
                lsfLogDir[0] = '\0';
            }
        }
        ls_strcat(logFileName, sizeof(logFileName), ".sbatchd");
    }
    else {
        ls_strcat(logFileName, sizeof(logFileName), "sbatchd");
        if (daemonParams[LSF_LOGDIR].paramValue
            && *(daemonParams[LSF_LOGDIR].paramValue)) {
            ls_strcat(lsfLogDir, sizeof(lsfLogDir),
                      daemonParams[LSF_LOGDIR].paramValue);
        } else {
            lsfLogDir[0] = '\0';
        }
    }

    if (options==1) {
        struct config_param *plp;
        for (plp = daemonParams; plp->paramName != NULL; plp++) {
            if (plp->paramValue != NULL)
                FREEUP(plp->paramValue);
        }

        if (initenv_(daemonParams, env_dir) < 0){
            ls_openlog("sbatchd", daemonParams[LSF_LOGDIR].paramValue,
                       (debug > 1), daemonParams[LSF_LOG_MASK].paramValue);
            ls_syslog(LOG_ERR, I18N_FUNC_FAIL_MM, fname, "initenv_");
            die(SLAVE_FATAL);
            return -1;
        }

        getLogClass_(daemonParams[LSB_DEBUG_SBD].paramValue,
                     daemonParams[LSB_TIME_SBD].paramValue);
        closelog();
        if (debug > 1)
            ls_openlog("sbatchd", daemonParams[LSF_LOGDIR].paramValue, TRUE,
                       daemonParams[LSF_LOG_MASK].paramValue);
        else
            ls_openlog("sbatchd", daemonParams[LSF_LOGDIR].paramValue, FALSE,
                       daemonParams[LSF_LOG_MASK].paramValue);

        if (logclass & LC_TRACE)
            ls_syslog(LOG_DEBUG, "%s: logclass=%x", fname, logclass);

        cleanDynDbgEnv();

        return(LSBE_NO_ERROR);
    }

    if (opCode==SBD_DEBUG) {
        putMaskLevel(level, &(daemonParams[LSF_LOG_MASK].paramValue));

        if (newClass>=0) {
            logclass = newClass;

            sprintf(dynDbgEnv, "%d", logclass);
            putEnv("DYN_DBG_LOGCLASS", dynDbgEnv);
        }

        if ( pdebug->level>=0 ){
            closelog();
            if (debug > 1)
                ls_openlog(logFileName, lsfLogDir, TRUE,
                           daemonParams[LSF_LOG_MASK].paramValue);
            else
                ls_openlog(logFileName, lsfLogDir, FALSE,
                           daemonParams[LSF_LOG_MASK].paramValue);

            putEnv("DYN_DBG_LOGDIR", lsfLogDir);
            putEnv("DYN_DBG_LOGFILENAME", logFileName);
            sprintf(dynDbgEnv, "%d", pdebug->level);
            putEnv("DYN_DBG_LOGLEVEL", dynDbgEnv);
        }
    }
    else if (opCode == SBD_TIMING) {
        if (level>=0)
            timinglevel = level;
        if (pdebug->logFileName[0] != '\0') {
            if (debug > 1)
                ls_openlog(logFileName, lsfLogDir,
                           TRUE, daemonParams[LSF_LOG_MASK].paramValue);
            else
                ls_openlog(logFileName, lsfLogDir,
                           FALSE, daemonParams[LSF_LOG_MASK].paramValue);
        }
    }
    else {
        ls_perror("No this debug command!\n");
        return -1;
    }
    return (LSBE_NO_ERROR);
}

void
do_sbdDebug(XDR * xdrs, int chfd, struct LSFHeader * reqHdr)
{
    static char       fname[] = "do_sbdDebug()";
    struct debugReq    debugReq;
    char               reply_buf[MSGSIZE / 8];
    XDR                xdrs2;
    sbdReplyType       reply;
    struct LSFHeader   replyHdr;

    if (!xdr_debugReq(xdrs, &debugReq, reqHdr)) {
        reply = LSBE_XDR;
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_debugReq");
    }
    else
        reply = ctrlSbdDebug(&debugReq);
    xdrmem_create(&xdrs2, reply_buf, MSGSIZE / 8, XDR_ENCODE);
    initLSFHeader_(&replyHdr);
    replyHdr.opCode = reply;
    if (!xdr_encodeMsg(&xdrs2, (char *) 0, &replyHdr, 0, 0, NULL)) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_encodeMsg");
        xdr_destroy(&xdrs2);
        return;
    }

    if (chanWrite_(chfd, reply_buf, XDR_GETPOS(&xdrs2)) <= 0) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5833,
                                         "%s: Sending  reply to master failed : %m"), /* catgets 5833 */
                  fname);
        xdr_destroy(&xdrs2);
        return;
    }
    xdr_destroy(&xdrs2);
    return;

}

void
do_shutdown(XDR * xdrs, int chfd, struct LSFHeader * reqHdr)
{
    static char        fname[] = "do_shutdown()";
    char               reply_buf[MSGSIZE / 8];
    XDR                xdrs2;
    sbdReplyType       reply;
    struct LSFHeader   replyHdr;

    reply = ERR_NO_ERROR;

    xdrmem_create(&xdrs2, reply_buf, MSGSIZE / 8, XDR_ENCODE);

    initLSFHeader_(&replyHdr);
    if (reqHdr->opCode == CMD_SBD_SHUTDOWN)
        replyHdr.opCode = LSBE_NO_ERROR;
    else
        replyHdr.opCode = reply;

    if (!xdr_encodeMsg(&xdrs2, (char *) 0, &replyHdr, 0, 0, NULL)) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_encodeMsg");
        xdr_destroy(&xdrs2);
        relife();
        return;
    }

    if (chanWrite_(chfd, reply_buf, XDR_GETPOS(&xdrs2)) <= 0) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5835,
                                         "%s: Sending shutdown reply to master failed: %m"), /* catgets 5835 */
                  fname);
        xdr_destroy(&xdrs2);
        relife();
        return;
    }
    xdr_destroy(&xdrs2);
    ls_syslog(LOG_NOTICE, _i18n_msg_get(ls_catd , NL_SETN, 5836,
                                        "Slave batch daemon shutdown command received")); /* catgets 5836 */
    die(SLAVE_SHUTDOWN);

}


void
do_jobSetup(XDR * xdrs, int chfd, struct LSFHeader * reqHdr)
{
    static char       fname[] = "do_jobSetup()";
    struct jobSetup   jsetup;
    struct jobCard    *jp = NULL;
    char              found = FALSE;
    struct jobCard    savejp;

    if (logclass & LC_EXEC)
        ls_syslog(LOG_DEBUG, "%s: Entering ...", fname);

    if (!xdr_jobSetup(xdrs, &jsetup, reqHdr)) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, fname, "xdr_jobSetup");
        return;
    }

    for (jp = jobQueHead->forw; (jp != jobQueHead); jp = jp->forw) {
        if (jp->jobSpecs.jobId != jsetup.jobId)
            continue;
        found = TRUE;
        break;
    }

    if (found == FALSE) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 5838,
                                         "%s: Job <%s> is not found"), /* catgets 5838 */
                  fname, lsb_jobid2str(jsetup.jobId));
        replyHdrWithRC(LSBE_NO_JOB, chfd, jsetup.jobId);
        return;
    }
    if (jp->jobSpecs.actPid)
        return;

    memcpy((char *) &savejp, (char *) jp, sizeof(savejp));

    jp->execJobFlag |= JOB_EXEC_QPRE_KNOWN;
    if (jsetup.execJobFlag & JOB_EXEC_QPRE_OK)
        jp->execJobFlag |= JOB_EXEC_QPRE_OK;

    jp->jobSpecs.jobPid = jsetup.jobPid;
    jp->jobSpecs.jobPGid = jsetup.jobPGid;
    jp->jobSpecs.execUid = jsetup.execUid;
    strcpy(jp->jobSpecs.execUsername, jsetup.execUsername);
    jp->execGid = jsetup.execGid;
    strcpy(jp->execUsername, jsetup.execUsername);
    strcpy(jp->jobSpecs.execCwd, jsetup.execCwd);
    strcpy(jp->jobSpecs.execHome, jsetup.execHome);

    if (jsetup.jStatus & JOB_STAT_RUN) {
        if (!(jsetup.jStatus & JOB_STAT_PRE_EXEC))
            jp->jobSpecs.jStatus &= ~JOB_STAT_PRE_EXEC;

        if (status_job(BATCH_STATUS_JOB, jp, jp->jobSpecs.jStatus,
                       ERR_NO_ERROR) < 0) {
            memcpy((char *) jp, (char *) &savejp, sizeof(savejp));
            return;
        }
        jp->execJobFlag |= JOB_EXEC_STARTED;

    } else {
        jp->jobSpecs.reasons = jsetup.reason;
        jp->collectedChild = TRUE;
        jp->notReported = 0;
        jp->exitPid = -1;
        jp->needReportRU = FALSE;
        jp->jobSpecs.jStatus = jsetup.jStatus;
        jp->w_status = jsetup.w_status;
        jp->lsfRusage = jsetup.lsfRusage;
        jp->cpuTime = jsetup.cpuTime;
        if (job_finish(jp, TRUE) < 0) {
            memcpy((char *) jp, (char *) &savejp, sizeof(savejp));
            return;
        }
    }

    if (replyHdrWithRC(LSBE_NO_ERROR, chfd, jsetup.jobId) < 0) {
        ls_syslog(LOG_DEBUG, "%s: Reply header failed for job <%s>",
                  fname, lsb_jobid2str(jsetup.jobId));
    }
    if (logclass & LC_EXEC)
        ls_syslog(LOG_DEBUG1, "%s: JobId %s jstatus %d reason %x  jobPid %d jobPGid %d execUid %d execGid <%d> execUser <%s> execHome <%s> execCwd <%s> execJobFlag %x cpuTime %f w_status %d",
                  fname, lsb_jobid2str(jsetup.jobId), jsetup.jStatus,
                  jsetup.reason, jsetup.jobPid,
                  jsetup.jobPGid, jsetup.execUid, jsetup.execGid,
                  jsetup.execUsername, jsetup.execHome, jsetup.execCwd,
                  jsetup.execJobFlag, jsetup.cpuTime, jsetup.w_status);
}

void
do_jobSyslog(XDR * xdrs, int chfd, struct LSFHeader * reqHdr)
{
    struct jobSyslog sysMsg;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG, "%s: Entering ...", __func__);

    if (!xdr_jobSyslog(xdrs, &sysMsg, reqHdr)) {
        ls_syslog(LOG_ERR, "%s: failed in xdr_jobSyslog()", __func__);
        return;
    }

    if (replyHdrWithRC(LSBE_NO_ERROR, chfd, -1) < 0)
        ls_syslog(LOG_ERR, "%s: replyHdrWithRC()", __func__);

    ls_syslog(sysMsg.logLevel, sysMsg.msg);
}

static int
replyHdrWithRC(int rc, int chfd, int jobId)
{
    XDR xdrs2;
    char reply_buf[sizeof(struct LSFHeader)];
    struct LSFHeader replyHdr;

    xdrmem_create(&xdrs2, reply_buf, sizeof(reply_buf), XDR_ENCODE);

    replyHdr.opCode = rc;
    replyHdr.length = 0;

    if (!xdr_LSFHeader(&xdrs2, &replyHdr)) {
        ls_syslog(LOG_ERR, "\
%s: xdr_LSFHeader() failed for job <%d>", __func__, jobId);
        xdr_destroy(&xdrs2);
        return -1;
    }

    if (chanWrite_(chfd, reply_buf, XDR_GETPOS(&xdrs2)) <= 0) {
        ls_syslog(LOG_ERR, "\
%s: chanWrite_(%d) failed for job <%d>: %m", __func__,
                  XDR_GETPOS(&xdrs2), jobId);
        xdr_destroy(&xdrs2);
        return -1;
    }

    xdr_destroy(&xdrs2);

    return 0;
}


/* do_blaunch_rusage()
 *
 * Process the rusage update from blaunch job
 */
void
do_blaunch_rusage(XDR *xdrs, int chfd, struct LSFHeader *hdr)
{
    struct jobCard *jPtr;
    int jobID;

    /* Get the job id
     */
    if (! xdr_int(xdrs, &jobID)) {
        ls_syslog(LOG_ERR, "\%s: failed dencoding jobid %s", __func__, jobID);
        xdr_destroy(xdrs);
        return;
    }

    /* find the job
     */
    jPtr = find_job_card(jobID);
    if (! jPtr) {
        replyHdrWithRC(LSBE_NO_JOB, chfd, -1);
        xdr_destroy(xdrs);
        return;
    }

    if (jPtr->jobSpecs.jStatus & (JOB_STAT_DONE | JOB_STAT_EXIT)) {
        ls_syslog(LOG_ERR, "%s: job %d is finished", __func__, jobID);
        replyHdrWithRC(LSBE_JOB_FINISH, chfd, jobID);
        return;
    }

    free_jrusage(&blaunch_jru);

    /* decode the rusage
     */
    blaunch_jru = calloc(1, sizeof(struct jRusage));

    if (! xdr_jRusage(xdrs, blaunch_jru, hdr)) {
        ls_syslog(LOG_ERR, "\
%: failed decoding jobid % or stepid %d", __func__, jobID);
        replyHdrWithRC(LSBE_XDR, chfd, jobID);
        xdr_destroy(xdrs);
        return;
    }

    /* update the job
     */
    replyHdrWithRC(LSBE_NO_ERROR, chfd, jobID);

    /* Report the rusage
     */
    jPtr->needReportRU = true;

    xdr_destroy(xdrs);
}

struct jRusage *
get_blaunch_jrusage(void)
{
    return blaunch_jru;
}

static struct jobCard *
find_job_card(int jobID)
{
    struct jobCard *jp;

    for (jp = jobQueHead->back; jp != jobQueHead; jp = jp->back) {

        if (jp->jobSpecs.jobId == jobID)
            return jp;
    }

    return NULL;
}

void
free_jrusage(struct jRusage **jru)
{
    if (jru == NULL
        || *jru == NULL)
        return;

    _free_((*jru)->pidInfo);
    _free_((*jru)->pgid);
    _free_(*jru);
    *jru = NULL;
}
