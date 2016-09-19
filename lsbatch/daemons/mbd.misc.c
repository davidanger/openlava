/*
 * Copyright (C) 2011-2016 David Bigagli
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA
 *
 */

#include "mbd.h"
#include "fairshare.h"

#define CHECKQUSABLE(qp, oldReason, newReason)                  \
    {                                                           \
        if (qp != NULL && newReason == 0 && oldReason != 0 &&   \
            (mSchedStage & M_STAGE_QUE_CAND)) {                 \
            qp->flags |= QUEUE_UPDATE_USABLE;                   \
        }                                                       \
    }

extern int getQUsable(struct qData *);
extern int schedule;
extern int dispatch;

static void updUAcct(struct jData *, struct uData *,
                     struct hTab **, int,
                     int, int, int, int, struct hData *,
                     void (*)(struct userAcct *, void *),
                     void *);
static void updHAcct(struct jData *, struct qData *,
                     struct uData *, struct hTab **,
                     int, int, int, int,
                     void (*)(struct hostAcct *, void *),
                     void *);
static void updHostData(char, struct jData *,
                        int, int, int, int, int);
static void updUserData1(struct jData *, struct uData *,
                         int, int, int, int, int, int);
static void addValue(int *currentValue, int num,
                     struct jData *jp,
                     char *fname, char *counter);
static void initUData(struct uData *);
static void addOneAbs(int *, int, int);
static struct userAcct *addUAcct(struct hTab **, struct uData *, int,
                                 int, int, int, int);
extern char *lsfDefaultProject;
extern int getQUsable(struct qData *);

void
updCounters(struct jData *jData, int oldStatus, time_t eventTime)
{
    int num, numReq;

    if (IS_FINISH (oldStatus))
        return;

    if (MASK_STATUS (jData->jStatus & ~JOB_STAT_UNKWN)
        == MASK_STATUS (oldStatus & ~JOB_STAT_UNKWN))
        return;

    if (IS_PEND (jData->jStatus) && IS_PEND (oldStatus))
        return;

    num = jData->numHostPtr;
    numReq = jData->shared->jobBill.maxNumProcessors;

    switch (MASK_STATUS (jData->jStatus & ~JOB_STAT_UNKWN)) {
        case JOB_STAT_RUN:
            if ((oldStatus & JOB_STAT_PEND) || (oldStatus & JOB_STAT_PSUSP) ) {
                updQaccount (jData, -numReq+num, -numReq, num, 0, 0, 0);
                updUserData(jData, -numReq+num, -numReq, num, 0, 0, 0);
                updLimitSlotData(jData, -numReq+num, -numReq, num, 0, 0, 0);
                updLimitJobData(jData, -1, -1, 1, 0, 0, 0);
                updHostData(TRUE, jData, 1, 1, 0, 0, 0);
            } else if (oldStatus & JOB_STAT_SSUSP) {
                updQaccount (jData, 0, 0, num, -num, 0, 0);
                updUserData(jData, 0, 0, num, -num, 0, 0);
                updLimitSlotData(jData, 0, 0, num, -num, 0, 0);
                updLimitJobData(jData, 0, 0, 1,-1, 0, 0);
                updHostData(FALSE, jData, 0, 1, -1, 0, 0);
            } else if (oldStatus & JOB_STAT_USUSP) {
                updQaccount (jData, 0, 0, num, 0, -num, 0);
                updUserData(jData, 0, 0, num, 0, -num, 0);
                updLimitSlotData(jData, 0, 0, num, 0, -num, 0);
                updLimitJobData(jData, 0, 0, 1, 0, -1, 0);
                updHostData(FALSE, jData, 0, 1, 0, -1, 0);
            } else if ( (oldStatus & ( JOB_STAT_RUN | JOB_STAT_WAIT)) ) {
                ls_syslog(LOG_DEBUG2, "%s: Job %s RWAIT to RUN",
                          __func__, lsb_jobid2str(jData->jobId));
            } else {
                ls_syslog(LOG_ERR, "\
%s: Job <%s> transited from %d to JOB_STAT_RUN", __func__,
                          lsb_jobid2str(jData->jobId), oldStatus);
            }
            break;
        case JOB_STAT_SSUSP:
            if (oldStatus & JOB_STAT_RUN) {
                updQaccount (jData, 0, 0, -num, num, 0, 0);
                updUserData(jData, 0, 0, -num, num, 0, 0);
                updLimitSlotData(jData, 0, 0, -num, num, 0, 0);
                updLimitJobData(jData, 0, 0, -1, 1, 0, 0);
                updHostData(FALSE, jData, 0, -1, 1, 0, 0);
            } else if (oldStatus & JOB_STAT_USUSP) {
                updQaccount (jData, 0, 0, 0, num, -num, 0);
                updUserData(jData, 0, 0, 0, num, -num, 0);
                updLimitSlotData(jData, 0, 0, -num, num, 0, 0);
                updLimitJobData(jData, 0, 0, -1, 1, 0, 0);
                updHostData(FALSE, jData, 0, 0, 1, -1, 0);
            } else if (oldStatus & JOB_STAT_PEND) {
                updQaccount (jData, -numReq+num, -numReq, 0, num, 0, 0);
                updUserData(jData, -numReq+num, -numReq, 0, num, 0, 0);
                updLimitSlotData(jData, -numReq+num, -numReq, 0, num, 0, 0);
                updLimitJobData(jData, -1, -1, 0, 1, 0, 0);
                updHostData(TRUE, jData, 1, 0, 1, 0, 0);
            } else {
                ls_syslog(LOG_ERR, "\
%s: Job <%s> transited from %d to JOB_STAT_SSUSP", __func__,
                          lsb_jobid2str(jData->jobId), oldStatus);
            }
            break;
        case JOB_STAT_USUSP:
            if (oldStatus & JOB_STAT_RUN) {
                updQaccount (jData, 0, 0, -num, 0, num, 0);
                updUserData(jData, 0, 0, -num, 0, num, 0);
                updLimitSlotData(jData, 0, 0, -num, 0, num, 0);
                updLimitJobData(jData, 0, 0, -1, 0, 1, 0);
                updHostData(FALSE, jData, 0, -1, 0, 1, 0);
            } else if (oldStatus & JOB_STAT_SSUSP) {
                updQaccount (jData, 0, 0, 0, -num, num, 0);
                updUserData(jData, 0, 0, 0, -num, num, 0);
                updLimitSlotData(jData, 0, 0, 0, -num, num, 0);
                updLimitJobData(jData, 0, 0, 0, -1, 1, 0);
                updHostData(FALSE, jData, 0, 0, -1, 1, 0);
            } else if (oldStatus & JOB_STAT_PEND) {
                updQaccount (jData, -numReq+num, -numReq, 0, 0, num, 0);
                updUserData(jData, -numReq+num, -numReq, 0, 0, num, 0);
                updLimitSlotData(jData, -numReq+num, -numReq, 0, 0, num, 0);
                updLimitJobData(jData, -1, -1, 0, 0, 1, 0);
                updHostData(TRUE, jData, 1, 0, 0, 1, 0);
            } else {
                ls_syslog(LOG_ERR, "\
%s: Job <%s> transited from %d to JOB_STAT_USUSP", __func__,
                          lsb_jobid2str(jData->jobId), oldStatus);
            }
            break;
        case JOB_STAT_EXIT:
        case JOB_STAT_DONE:
            if (oldStatus & JOB_STAT_WAIT) {
                if ( logclass & (LC_TRACE | LC_EXEC )) {
                    ls_syslog(LOG_DEBUG, "\
%s: last job in the chunk <%s> exits: status WAIT", __func__,
                              lsb_jobid2str(jData->jobId));
                }
                updQaccount (jData, -num, 0, -num, 0, 0, 0);
                updHostData(TRUE, jData, -1, -1, 0, 0, 0);
                updUserData(jData, -num, 0, -num, 0, 0, 0);
                updLimitSlotData(jData, -num, 0, -num, 0, 0, 0);
                updLimitJobData(jData, -1, 0, -1, 0, 0, 0);
            } else if (oldStatus & JOB_STAT_RUN) {
                updQaccount (jData, -num, 0, -num, 0, 0, 0);
                updHostData(TRUE, jData, -1, -1, 0, 0, 0);
                updUserData(jData, -num, 0, -num, 0, 0, 0);
                updLimitSlotData(jData, -num, 0, -num, 0, 0, 0);
                updLimitJobData(jData, -1, 0, -1, 0, 0, 0);
            } else if (oldStatus & JOB_STAT_USUSP) {
                updQaccount (jData, -num, 0, 0, 0, -num, 0);
                updHostData(TRUE, jData, -1, 0, 0, -1, 0);
                updUserData(jData, -num, 0, 0, 0, -num, 0);
                updLimitSlotData(jData, -num, 0, 0, 0, -num, 0);
                updLimitJobData(jData, -1, 0, 0, 0, -1, 0);
            } else if (oldStatus & JOB_STAT_SSUSP) {
                updQaccount (jData, -num, 0, 0, -num, 0, 0);
                updHostData(TRUE, jData, -1, 0, -1, 0, 0);
                updUserData(jData, -num, 0, 0, -num, 0, 0);
                updLimitSlotData(jData, -num, 0, 0, -num, 0, 0);
                updLimitJobData(jData, -1, 0, 0, -1, 0, 0);
            } else if (IS_PEND (oldStatus)) {
                updQaccount (jData, -numReq, -numReq, 0, 0, 0, 0);
                updUserData(jData, -numReq, -numReq, 0, 0, 0, 0);
                updLimitSlotData(jData, -numReq, -numReq, 0, 0, 0, 0);
                updLimitJobData(jData, -1, -1, 0, 0, 0, 0);
            }
            else {
                ls_syslog(LOG_ERR, "%s: Job <%s> transited from %x to %x",
                          __func__, lsb_jobid2str(jData->jobId), oldStatus, jData->jStatus);
            }
            break;
        case JOB_STAT_PEND:
            if (oldStatus & JOB_STAT_RUN) {
                updQaccount (jData, numReq-num, numReq, -num, 0, 0, 0);
                updHostData(TRUE, jData, -1, -1, 0, 0, 0);
                updUserData(jData, numReq-num, numReq, -num, 0, 0, 0);
                updLimitSlotData(jData, numReq-num, numReq, -num, 0, 0, 0);
                updLimitJobData(jData, -1, 1, -1, 0, 0, 0);
            }
            else if (oldStatus & JOB_STAT_USUSP) {
                updQaccount (jData, -num+numReq, numReq, 0, 0, -num, 0);
                updHostData(TRUE, jData, -1, 0, 0, -1, 0);
                updUserData(jData, -num+numReq, numReq, 0, 0, -num, 0);
                updLimitSlotData(jData, -num+numReq, numReq, 0, 0, -num, 0);
                updLimitJobData(jData, -1, 1, 0, 0, -1, 0);
            }
            else if (oldStatus & JOB_STAT_SSUSP) {
                updQaccount (jData, -num+numReq, numReq, 0, -num, 0, 0);
                updHostData(TRUE, jData, -1, 0, -1, 0, 0);
                updUserData(jData, -num+numReq, numReq, 0, -num, 0, 0);
                updLimitSlotData(jData, -num+numReq, numReq, 0, -num, 0, 0);
                updLimitJobData(jData, -1, 1, 0, -1, 0, 0);
            } else {
                ls_syslog(LOG_ERR, "%s: Job <%s> transited from %d to %d",
                          __func__, lsb_jobid2str(jData->jobId),
                          oldStatus, jData->jStatus);
            }
            break;
        case JOB_STAT_RUN | JOB_STAT_WAIT:
            if (oldStatus & JOB_STAT_RUN ) {
                ls_syslog(LOG_DEBUG2, "%s: Job %s RUN to RWAIT",
                          __func__, lsb_jobid2str(jData->jobId));
            } else if ( oldStatus & JOB_STAT_PEND ) {
                ls_syslog(LOG_DEBUG2, "%s: Job %s PEND to RWAIT",
                          __func__, lsb_jobid2str(jData->jobId));
            } else {
                ls_syslog(LOG_ERR, "%s: Job %s transited from %d to %d",
                          __func__, lsb_jobid2str(jData->jobId),
                          oldStatus, jData->jStatus);
            }
            break;
        default:
            if ( IS_POST_FINISH(jData->jStatus) ) {
                break;
            }
            ls_syslog(LOG_ERR, "%s: Job <%s> transited from %d to %d",
                      __func__, lsb_jobid2str(jData->jobId),
                      oldStatus, jData->jStatus);
    }
}

void
updSwitchJob (struct jData *jp, struct qData *qfp, struct qData *qtp,
              int oldNumReq)
{
    int num = jp->numHostPtr;
    int numReq = jp->shared->jobBill.maxNumProcessors;
    int reserved = FALSE;

    if (jp->jStatus & JOB_STAT_RESERVE) {
        jp->qPtr = qfp;
        jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                      jp->shared->jobBill.projectName,
                                      jp->qPtr->queue);
        jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                      jp->userName,
                                      jp->qPtr->queue);
        if (jp->numHostPtr > 0)
            jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                          jp->hPtr[0]->host,
                                          jp->qPtr->queue);

        updResCounters (jp, jp->jStatus & ~JOB_STAT_RESERVE);
        jp->qPtr = qtp;
        jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                      jp->shared->jobBill.projectName,
                                      jp->qPtr->queue);
        jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                      jp->userName,
                                      jp->qPtr->queue);
        if (jp->numHostPtr > 0)
            jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                          jp->hPtr[0]->host,
                                          jp->qPtr->queue);
        jp->jStatus &= ~JOB_STAT_RESERVE;
        reserved = TRUE;
    }
    switch (MASK_STATUS(jp->jStatus)) {
        case JOB_STAT_PEND:
        case JOB_STAT_PSUSP:
            jp->qPtr = qfp;
            jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                          jp->shared->jobBill.projectName,
                                          jp->qPtr->queue);
            jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                          jp->userName,
                                          jp->qPtr->queue);
            if (jp->numHostPtr > 0)
                jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                              jp->hPtr[0]->host,
                                              jp->qPtr->queue);

            if (mSchedStage != M_STAGE_REPLAY) {
                updQaccount (jp, -oldNumReq, -oldNumReq, 0, 0, 0, 0);
                updUserData(jp, -oldNumReq, -oldNumReq, 0, 0, 0, 0);
                updLimitSlotData(jp, -oldNumReq, -oldNumReq, 0, 0, 0, 0);
                updLimitJobData(jp, -1, -1, 0, 0, 0, 0);
            }
            jp->qPtr = qtp;
            jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                          jp->shared->jobBill.projectName,
                                          jp->qPtr->queue);
            jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                          jp->userName,
                                          jp->qPtr->queue);
            if (jp->numHostPtr > 0)
                jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                              jp->hPtr[0]->host,
                                              jp->qPtr->queue);

            if (mSchedStage != M_STAGE_REPLAY) {
                updQaccount (jp, numReq, numReq, 0, 0, 0, 0);
                updUserData(jp, numReq, numReq, 0, 0, 0, 0);
                updLimitSlotData(jp, numReq, numReq, 0, 0, 0, 0);
                updLimitJobData(jp, 1, 1, 0, 0, 0, 0);
            }
            break;
        case JOB_STAT_RUN:
        case (JOB_STAT_RUN | JOB_STAT_UNKWN):
            jp->qPtr = qfp;
            jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                          jp->shared->jobBill.projectName,
                                          jp->qPtr->queue);
            jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                          jp->userName,
                                          jp->qPtr->queue);
            if (jp->numHostPtr > 0)
                jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                              jp->hPtr[0]->host,
                                              jp->qPtr->queue);

            if (mSchedStage != M_STAGE_REPLAY) {
                updQaccount (jp, -num, 0, -num, 0, 0, 0);
                updHostData(TRUE, jp, -1, -1, 0, 0, 0);
                updUserData(jp,-num, 0, -num, 0, 0, 0);
                updLimitSlotData(jp,-num, 0, -num, 0, 0, 0);
                updLimitJobData(jp,-1, 0, -1, 0, 0, 0);
            }
            jp->qPtr = qtp;
            jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                          jp->shared->jobBill.projectName,
                                          jp->qPtr->queue);
            jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                          jp->userName,
                                          jp->qPtr->queue);
            if (jp->numHostPtr > 0)
                jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                              jp->hPtr[0]->host,
                                              jp->qPtr->queue);

            if (mSchedStage != M_STAGE_REPLAY) {
                updQaccount (jp, num, 0, num, 0, 0, 0);
                updHostData(TRUE, jp, 1, 1, 0, 0, 0);
                updUserData(jp, num, 0, num, 0, 0, 0);
                updLimitSlotData(jp, num, 0, num, 0, 0, 0);
                updLimitJobData(jp, 1, 0, 1, 0, 0, 0);
            }
            break;
        case JOB_STAT_SSUSP:
        case (JOB_STAT_SSUSP | JOB_STAT_UNKWN):
            jp->qPtr = qfp;
            jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                          jp->shared->jobBill.projectName,
                                          jp->qPtr->queue);
            jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                          jp->userName,
                                          jp->qPtr->queue);
            if (jp->numHostPtr > 0)
                jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                              jp->hPtr[0]->host,
                                              jp->qPtr->queue);

            if (mSchedStage != M_STAGE_REPLAY) {
                updQaccount (jp, -num, 0, 0, -num, 0, 0);
                updHostData(TRUE, jp, -1, 0, -1, 0, 0);
                updUserData(jp,-num, 0, 0, -num, 0, 0);
                updLimitSlotData(jp,-num, 0, 0, -num, 0, 0);
                updLimitJobData(jp,-1, 0, 0, -1, 0, 0);
            }
            jp->qPtr = qtp;
            jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                          jp->shared->jobBill.projectName,
                                          jp->qPtr->queue);
            jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                          jp->userName,
                                          jp->qPtr->queue);
            if (jp->numHostPtr > 0)
                jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                              jp->hPtr[0]->host,
                                              jp->qPtr->queue);

            if (mSchedStage != M_STAGE_REPLAY) {
                updQaccount (jp, num, 0, 0, num, 0, 0);
                updHostData(TRUE, jp, 1, 0, 1, 0, 0);
                updUserData(jp, num, 0, 0, num, 0, 0);
                updLimitSlotData(jp, num, 0, 0, num, 0, 0);
                updLimitJobData(jp, 1, 0, 0, 1, 0, 0);
            }
            break;
        case JOB_STAT_USUSP:
        case (JOB_STAT_USUSP | JOB_STAT_UNKWN):
            jp->qPtr = qfp;
            jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                          jp->shared->jobBill.projectName,
                                          jp->qPtr->queue);
            jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                          jp->userName,
                                          jp->qPtr->queue);
            if (jp->numHostPtr > 0)
                jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                              jp->hPtr[0]->host,
                                              jp->qPtr->queue);

            if (mSchedStage != M_STAGE_REPLAY) {
                updQaccount (jp, -num, 0, 0, 0, -num, 0);
                updHostData(TRUE, jp, -1, 0, 0, -1, 0);
                updUserData(jp,-num, 0, 0, 0, -num, 0);
                updLimitSlotData(jp,-num, 0, 0, 0, -num, 0);
                updLimitJobData(jp,-1, 0, 0, 0, -1, 0);
            }
            jp->qPtr = qtp;
            jp->pqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_PROJECT,
                                          jp->shared->jobBill.projectName,
                                          jp->qPtr->queue);
            jp->uqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_USER,
                                          jp->userName,
                                          jp->qPtr->queue);
            if (jp->numHostPtr > 0)
                jp->hqPtr = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                                              jp->hPtr[0]->host,
                                              jp->qPtr->queue);

            if (mSchedStage != M_STAGE_REPLAY) {
                updQaccount (jp, num, 0, 0, 0, num, 0);
                updHostData(TRUE, jp, 1, 0, 0, 1, 0);
                updUserData(jp, num, 0, 0, 0, num, 0);
                updLimitSlotData(jp, num, 0, 0, 0, num, 0);
                updLimitJobData(jp, 1, 0, 0, 0, 1, 0);
            }
            break;
        default:
            break;
    }
    if (reserved == TRUE) {
        updResCounters (jp, jp->jStatus | JOB_STAT_RESERVE);
        jp->jStatus |= JOB_STAT_RESERVE;
    }

}


void
updQaccount(struct jData *jp, int numJobs, int numPEND,
            int numRUN, int numSSUSP, int numUSUSP, int numRESERVE)
{
    static char fname[] = "updQaccount";
    struct qData *qp = jp->qPtr;
    int numSlots;
    int newJob;

    if (logclass & LC_JLIMIT)
        ls_syslog(LOG_DEBUG1, "%s: Entering with job=%s queue=%s numJobs=%d numPEND=%d numRUN=%d numSSUSP=%d numUSUSP=%d numRESERVE=%d", fname, lsb_jobid2str(jp->jobId), qp->queue, numJobs, numPEND, numRUN, numSSUSP, numUSUSP, numRESERVE);


    addValue (&qp->numJobs, numJobs, jp, fname, "numJobs");
    addValue (&qp->numPEND, numPEND, jp, fname, "numPEND");
    addValue (&qp->numRUN, numRUN, jp, fname, "numRUN");
    addValue (&qp->numSSUSP, numSSUSP, jp, fname, "numSSUSP");
    addValue (&qp->numUSUSP, numUSUSP, jp, fname, "numUSUSP");
    addValue (&qp->numRESERVE, numRESERVE, jp, fname, "numRESERVE");

    if (logclass & LC_JLIMIT)
        ls_syslog(LOG_DEBUG2, "%s: job=%s queue=%s numJobs=%d numPEND=%d numRUN=%d numSSUSP=%d numUSUSP=%d numRESERVE=%d", fname, lsb_jobid2str(jp->jobId), qp->queue, qp->numJobs, qp->numPEND, qp->numRUN, qp->numSSUSP, qp->numUSUSP, qp->numRESERVE);
    if (qp->maxJobs == INFINIT_INT)
        numSlots = INFINIT_INT;
    else
        numSlots = qp->maxJobs - (qp->numJobs - qp->numPEND);
    if (qp->reasonTb[1][0] == INFINIT_INT)
        qp->reasonTb[1][0] = 0;
    SET_REASON(numSlots <= 0, qp->reasonTb[1][0], PEND_QUE_JOB_LIMIT);
    if (numSlots <= 0 && (logclass & LC_JLIMIT))
        ls_syslog(LOG_DEBUG3, "%s: Q's MAX reached; reason=%d numSlots=%d", fname, qp->reasonTb[1][0], numSlots);

    newJob = (numRUN == 0 && numSSUSP == 0 && numUSUSP == 0
              && numRESERVE == 0 && numPEND == 0);

    updUAcct(jp, jp->uPtr, &(qp->uAcct), numRUN, numSSUSP, numUSUSP,
             numRESERVE,numPEND, (struct hData *)NULL,
             NULL, (void *) qp);

    /* update fairshare counters
     */
    if (qp->fsSched) {
        (*qp->fsSched->fs_update_sacct)(qp,
					jp,
					numJobs,
					numPEND,
					numRUN,
					numUSUSP,
					numSSUSP);
    }

    if (qp->own_sched) {
        (*qp->own_sched->fs_update_sacct)(qp,
					  jp,
					  numJobs,
					  numPEND,
					  numRUN,
					  numUSUSP,
					  numSSUSP);
    }

    if (!newJob) {
        updHAcct (jp, qp, NULL, &(qp->hAcct), numRUN, numSSUSP,
                  numUSUSP, numRESERVE, NULL, (void *)qp);
    }

    return;
}

static void
updUAcct (struct jData *jData,
          struct uData *up,
          struct hTab **uAcct,
          int  numRUN,
          int numSSUSP,
          int numUSUSP,
          int numRESERVE,
          int numPEND,
          struct hData *hp,
          void (*onNewEntry)(struct userAcct *, void *),
          void *extra)

{
    static char fname[] = "updUAcct";
    struct userAcct *foundU;
    struct qData *qp = jData->qPtr;
    int numSlots;

    if (*uAcct == NULL) {
        *uAcct = (struct hTab *) my_malloc(sizeof(struct hTab), fname);
        h_initTab_(*uAcct, 4);
    }

    if ((foundU = getUAcct(*uAcct, up)) != NULL) {
        addValue (&foundU->numRUN, numRUN, jData, fname, "numRUN");
        addValue (&foundU->numSSUSP, numSSUSP, jData, fname, "numSSUSP");
        addValue (&foundU->numUSUSP, numUSUSP, jData, fname, "numUSUSP");
        addValue (&foundU->numRESERVE, numRESERVE, jData, fname, "numRESERVE");
        addValue (&foundU->numPEND, numPEND, jData, fname, "numPEND");
        if (numRESERVE != 0 && !(jData->jStatus & JOB_STAT_PEND)) {
            addValue (&foundU->numNonPrmptRsv, numRESERVE, jData, fname,
                      "numNonPrmptRsv");
        }
        if (logclass & LC_JLIMIT)
            ls_syslog(LOG_DEBUG3, "%s: job=%s host/queue=%s user=%s RUN=%d SSUSP=%d USUSP=%d RESERVE=%d PEND=%d numNonPrmptRsv=%d", fname, lsb_jobid2str(jData->jobId), ((hp == NULL)? jData->qPtr->queue:hp->host), jData->uPtr->user, foundU->numRUN, foundU->numSSUSP, foundU->numUSUSP, foundU->numRESERVE, foundU->numPEND, foundU->numNonPrmptRsv);
    }

    else if (numRUN != 0 || numSSUSP != 0 ||  numUSUSP != 0
             || numRESERVE != 0 || numPEND != 0) {
        foundU = addUAcct(uAcct, up, numRUN,
                          numSSUSP, numUSUSP, numRESERVE, numPEND);
        foundU->userId = jData->userId;

        if (onNewEntry != NULL)
            (*onNewEntry)(foundU, extra);

        if (logclass & LC_JLIMIT)
            ls_syslog(LOG_DEBUG3, "\
%s: New uAcct for job=%s host/queue=%s user=%s RUN=%d SSUSP=%d USUSP=%d RESERVE=%d, PEND=%d",
                      fname,
                      lsb_jobid2str(jData->jobId),
                      ((hp == NULL)?
                       jData->qPtr->queue:hp->host),
                      ((jData->uPtr == NULL)?
                       jData->userName:jData->uPtr->user),
                      foundU->numRUN,
                      foundU->numSSUSP,
                      foundU->numUSUSP,
                      foundU->numRESERVE,
                      foundU->numPEND);
    } else {

        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 7006,
                                         "%s: expected one of (numRUN, numSSUSP, numUSUSP, numRESERVE and numPEND) to be non-ZERO, but they are all ZERO"), /* catgets 7006 */
                  fname);
        return;
    }

    if (hp == NULL) {

        numSlots = qp->uJobLimit - foundU->numRUN - foundU->numSSUSP
            - foundU->numUSUSP - foundU->numRESERVE;
        SET_REASON(numSlots <= 0, foundU->reason, PEND_QUE_USR_JLIMIT);
        if (numSlots <= 0 && (logclass & LC_JLIMIT))
            ls_syslog(LOG_DEBUG3, "%s: Q's JL/U reached; job=%s queue=%s user=%s", fname, lsb_jobid2str(jData->jobId), jData->qPtr->queue, jData->uPtr->user);

    } else {

        numSlots = hp->uJobLimit - foundU->numRUN - foundU->numSSUSP
            - foundU->numUSUSP - foundU->numRESERVE;
        SET_REASON(numSlots <= 0, up->reasonTb[1][hp->hostId],
                   PEND_HOST_USR_JLIMIT);
        if (numSlots <= 0 && (logclass & LC_JLIMIT))
            ls_syslog(LOG_DEBUG3, "%s: H's JL/U reached; job=%s host=%s user=%s", fname, lsb_jobid2str(jData->jobId), hp->host, jData->uPtr->user);
    }
}

/* updLimitSlotData() */
void
updLimitSlotData(struct jData *jp, int numJobs, int numPEND,
            int numRUN, int numSSUSP, int numUSUSP, int numRESERVE)
{
    struct resData *qp = jp->pqPtr;
    struct resData *uq = jp->uqPtr;
    struct resData *hq = jp->hqPtr;

    if (qp == NULL || uq == NULL)
        return;

    if (logclass & LC_JLIMIT)
        ls_syslog(LOG_DEBUG1,
"%s: Entering with job=%s project=%s user=%s queue=%s numJobs=%d numPEND=%d numRUN=%d numSSUSP=%d numUSUSP=%d numRESERVE=%d",
                            __func__,
                            lsb_jobid2str(jp->jobId),
                            qp->project,
                            uq->user,
                            qp->queue,
                            numJobs,
                            numPEND,
                            numRUN,
                            numSSUSP,
                            numUSUSP,
                            numRESERVE);

    addValue (&qp->numSlots, numJobs, jp, (char *)__func__, "numJobs");
    addValue (&qp->numPENDSlots, numPEND, jp, (char *)__func__, "numPEND");
    addValue (&qp->numRUNSlots, numRUN, jp, (char *)__func__, "numRUN");
    addValue (&qp->numSSUSPSlots, numSSUSP, jp, (char *)__func__, "numSSUSP");
    addValue (&qp->numUSUSPSlots, numUSUSP, jp, (char *)__func__, "numUSUSP");
    addValue (&qp->numRESERVESlots, numRESERVE, jp, (char *)__func__, "numRESERVE");

    addValue (&uq->numSlots, numJobs, jp, (char *)__func__, "numJobs");
    addValue (&uq->numPENDSlots, numPEND, jp, (char *)__func__, "numPEND");
    addValue (&uq->numRUNSlots, numRUN, jp, (char *)__func__, "numRUN");
    addValue (&uq->numSSUSPSlots, numSSUSP, jp, (char *)__func__, "numSSUSP");
    addValue (&uq->numUSUSPSlots, numUSUSP, jp, (char *)__func__, "numUSUSP");
    addValue (&uq->numRESERVESlots, numRESERVE, jp, (char *)__func__, "numRESERVE");

    if (!hq && jp->numHostPtr > 0) {
        hq = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                               jp->hPtr[0]->host,
                               jp->qPtr->queue);
    }

    if (hq) {
        addValue (&hq->numSlots, numJobs, jp, (char *)__func__, "numJobs");
        addValue (&hq->numPENDSlots, numPEND, jp, (char *)__func__, "numPEND");
        addValue (&hq->numRUNSlots, numRUN, jp, (char *)__func__, "numRUN");
        addValue (&hq->numSSUSPSlots, numSSUSP, jp, (char *)__func__, "numSSUSP");
        addValue (&hq->numUSUSPSlots, numUSUSP, jp, (char *)__func__, "numUSUSP");
        addValue (&hq->numRESERVESlots, numRESERVE, jp, (char *)__func__, "numRESERVE");
    }

    if (logclass & LC_JLIMIT)
        ls_syslog(LOG_DEBUG2,
"%s: job=%s project=%s user=%s queue=%s numJobs=%d numPEND=%d numRUN=%d numSSUSP=%d numUSUSP=%d numRESERVE=%d",
                            __func__,
                            lsb_jobid2str(jp->jobId),
                            qp->project,
                            uq->user,
                            qp->queue,
                            qp->numSlots,
                            qp->numPENDSlots,
                            qp->numRUNSlots,
                            qp->numSSUSPSlots,
                            qp->numUSUSPSlots,
                            qp->numRESERVESlots);

    return;
}

/* updLimitJobData() */
void
updLimitJobData(struct jData *jp, int numJobs, int numPEND,
            int numRUN, int numSSUSP, int numUSUSP, int numRESERVE)
{
    struct resData *qp = jp->pqPtr;
    struct resData *uq = jp->uqPtr;
    struct resData *hq = jp->hqPtr;

    if (qp == NULL || uq == NULL)
        return;

    if (logclass & LC_JLIMIT)
        ls_syslog(LOG_DEBUG1,
"%s: Entering with job=%s project=%s user=%s queue=%s numJobs=%d numPEND=%d numRUN=%d numSSUSP=%d numUSUSP=%d numRESERVE=%d",
                            __func__,
                            lsb_jobid2str(jp->jobId),
                            qp->project,
                            uq->user,
                            qp->queue,
                            numJobs,
                            numPEND,
                            numRUN,
                            numSSUSP,
                            numUSUSP,
                            numRESERVE);

    addValue (&qp->numJobs, numJobs, jp, (char *)__func__, "numJobs");
    addValue (&qp->numPENDJobs, numPEND, jp, (char *)__func__, "numPEND");
    addValue (&qp->numRUNJobs, numRUN, jp, (char *)__func__, "numRUN");
    addValue (&qp->numSSUSPJobs, numSSUSP, jp, (char *)__func__, "numSSUSP");
    addValue (&qp->numUSUSPJobs, numUSUSP, jp, (char *)__func__, "numUSUSP");
    addValue (&qp->numRESERVEJobs, numRESERVE, jp, (char *)__func__, "numRESERVE");

    addValue (&uq->numJobs, numJobs, jp, (char *)__func__, "numJobs");
    addValue (&uq->numPENDJobs, numPEND, jp, (char *)__func__, "numPEND");
    addValue (&uq->numRUNJobs, numRUN, jp, (char *)__func__, "numRUN");
    addValue (&uq->numSSUSPJobs, numSSUSP, jp, (char *)__func__, "numSSUSP");
    addValue (&uq->numUSUSPJobs, numUSUSP, jp, (char *)__func__, "numUSUSP");
    addValue (&uq->numRESERVEJobs, numRESERVE, jp, (char *)__func__, "numRESERVE");

    if (!hq && jp->numHostPtr > 0) {
        hq = getLimitUsageData(LIMIT_CONSUMER_PER_HOST,
                               jp->hPtr[0]->host,
                               jp->qPtr->queue);
    }

    if (hq) {
        addValue (&hq->numJobs, numJobs, jp, (char *)__func__, "numJobs");
        addValue (&hq->numPENDJobs, numPEND, jp, (char *)__func__, "numPEND");
        addValue (&hq->numRUNJobs, numRUN, jp, (char *)__func__, "numRUN");
        addValue (&hq->numSSUSPJobs, numSSUSP, jp, (char *)__func__, "numSSUSP");
        addValue (&hq->numUSUSPJobs, numUSUSP, jp, (char *)__func__, "numUSUSP");
        addValue (&hq->numRESERVEJobs, numRESERVE, jp, (char *)__func__, "numRESERVE");
    }

    if (logclass & LC_JLIMIT)
        ls_syslog(LOG_DEBUG2,
"%s: job=%s project=%s user=%s queue=%s numJobs=%d numPEND=%d numRUN=%d numSSUSP=%d numUSUSP=%d numRESERVE=%d",
                            __func__,
                            lsb_jobid2str(jp->jobId),
                            qp->project,
                            uq->user,
                            qp->queue,
                            qp->numJobs,
                            qp->numPENDJobs,
                            qp->numRUNJobs,
                            qp->numSSUSPJobs,
                            qp->numUSUSPJobs,
                            qp->numRESERVEJobs);

    return;
}

/* addUAcct()
 */
static struct userAcct *
addUAcct(struct hTab **uAcct,
         struct uData *up,
         int numRUN,
         int numSSUSP,
         int numUSUSP,
         int numRESERVE,
         int numPEND)
{
    struct hEnt       *ent;
    struct userAcct   *acct;

    if (logclass &(LC_JLIMIT))
        ls_syslog(LOG_DEBUG2, "\
addUAcct: New userAcct for user %s", up->user);

    if (*uAcct == NULL) {
        *uAcct = my_calloc(1, sizeof(struct hTab), "addUAcct");
        h_initTab_(*uAcct, 11);
    }

    acct = my_calloc (1, sizeof(struct userAcct), "addUAcct");

    ent = h_addEnt_(*uAcct, up->user, NULL);
    ent->hData = acct;

    acct->uData = up;
    acct->numRUN = numRUN;
    acct->numSSUSP = numSSUSP;
    acct->numUSUSP = numUSUSP;
    acct->numRESERVE = numRESERVE;
    acct->numPEND = numPEND;
    acct->numAvailSUSP = 0;
    acct->numNonPrmptRsv = 0;
    acct->reason = 0;

    return acct;
}

struct userAcct *
getUAcct (struct hTab *uAcct, struct uData *up)
{
    hEnt   *uAcctEnt;

    if (uAcct == NULL || up == NULL)
        return NULL;

    uAcctEnt = h_getEnt_(uAcct, up->user);
    if (uAcctEnt != NULL)
        return((struct userAcct *)uAcctEnt->hData);
    else
        return NULL;

}


static void
updHAcct(struct jData *jData,
         struct qData *qp,
         struct uData *up,
         struct hTab **hAcct,
         int numRUN,
         int numSSUSP,
         int numUSUSP,
         int numRESERVE,
         void (*onNewEntry)(struct hostAcct *, void *),
         void *extra)
{
    static char fname[] = "updHAcct";
    struct hostAcct *foundH = NULL;
    struct hData *hp;
    int i, numSlots;

    if (jData->hPtr == NULL)
        return;

    if (*hAcct == NULL) {
        *hAcct = my_malloc(sizeof(struct hTab), "updHAcct");
        h_initTab_(*hAcct, 4);
    }

    if (qp != NULL)
        qp->flags &= ~QUEUE_UPDATE_USABLE;

    for (i = 0; i < jData->numHostPtr; i++) {
        if (jData->hPtr[i]->hStatus & HOST_STAT_REMOTE)
            continue;

        hp = jData->hPtr[i];
        if ((foundH = getHAcct(*hAcct, hp)) != NULL) {
            addOneAbs(&foundH->numRUN, numRUN, FALSE);
            addOneAbs(&foundH->numSSUSP, numSSUSP, FALSE);
            addOneAbs(&foundH->numUSUSP, numUSUSP, FALSE);
            addOneAbs(&foundH->numRESERVE, numRESERVE, FALSE);
            if (numRESERVE != 0 && !(jData->jStatus & JOB_STAT_PEND)) {
                addOneAbs (&foundH->numNonPrmptRsv, numRESERVE, FALSE);
            }
            if (logclass & LC_JLIMIT)
                ls_syslog(LOG_DEBUG3, "%s: job=%s user/queue=%s host=%s RUN=%d SSUSP=%d USUSP=%d RESERVE=%d NonPrmptRsv=%d", fname, lsb_jobid2str(jData->jobId), ((qp != NULL)? qp->queue:up->user), hp->host, foundH->numRUN, foundH->numSSUSP, foundH->numUSUSP, foundH->numRESERVE, foundH->numNonPrmptRsv);
        } else if (numRUN > 0 || numSSUSP > 0 || numUSUSP > 0
                   || numRESERVE > 0) {
            foundH = addHAcct(hAcct, hp, numRUN, numSSUSP, numUSUSP,
                              numRESERVE);

            if (onNewEntry != NULL)
                (*onNewEntry)(foundH, extra);

            if (logclass & LC_JLIMIT)
                ls_syslog(LOG_DEBUG2, "%s: New hAcct: job=%s user/queue=%s host=%s RUN=%d SSUSP=%d USUSP=%d RESERVE=%d", fname, lsb_jobid2str(jData->jobId), ((qp != NULL)? qp->queue:up->user), hp->host, foundH->numRUN, foundH->numSSUSP, foundH->numUSUSP, foundH->numRESERVE);

        }

        if (qp != NULL) {

            int svReason = qp->reasonTb[1][hp->hostId];
            qp->flags &= ~QUEUE_UPDATE_USABLE;
            numSlots = pJobLimitOk(hp, foundH, qp->pJobLimit);
            if (numSlots <= 0
                && !(hp->hStatus & HOST_STAT_UNREACH)
                && ! (hp->hStatus & HOST_STAT_UNAVAIL)
                && ! LS_ISUNAVAIL (hp->limStatus)) {

                SET_REASON(numSlots <= 0,
                           qp->reasonTb[1][hp->hostId], PEND_QUE_PROC_JLIMIT);
                CHECKQUSABLE (qp, svReason, qp->reasonTb[1][hp->hostId]);
                if (numSlots <= 0 && (logclass & (LC_PEND | LC_JLIMIT)))
                    ls_syslog(LOG_DEBUG2, "%s: Q's JL/P reached. Set reason <%d>; job=%s host=%s queue=%s", fname, qp->reasonTb[1][hp->hostId], lsb_jobid2str(jData->jobId), hp->host, qp->queue);
            } else {
                numSlots = hJobLimitOk(hp, foundH, qp->hJobLimit);
                SET_REASON(numSlots <= 0,
                           qp->reasonTb[1][hp->hostId], PEND_QUE_HOST_JLIMIT);
                CHECKQUSABLE (qp, svReason, qp->reasonTb[1][hp->hostId]);
                if (numSlots <= 0 && (logclass & (LC_PEND | LC_JLIMIT)))
                    ls_syslog(LOG_DEBUG2, "%s: Q's JL/H reached. Set reason <%d>; job=%s host=%s queue=%s", fname, qp->reasonTb[1][hp->hostId], lsb_jobid2str(jData->jobId), hp->host, qp->queue);
            }
            if (numSlots > 0
                && (svReason == PEND_QUE_PROC_JLIMIT
                    || svReason == PEND_QUE_HOST_JLIMIT)) {
                CLEAR_REASON(qp->reasonTb[1][hp->hostId], svReason);
                CHECKQUSABLE (qp, svReason, qp->reasonTb[1][hp->hostId]);
                if (logclass & (LC_PEND | LC_JLIMIT))
                    ls_syslog(LOG_DEBUG2, "%s: Clear reason <%d>; job=%s host=%s queue=%s", fname, svReason, lsb_jobid2str(jData->jobId), hp->host, qp->queue);
            }

        } else {

            numSlots = pJobLimitOk(hp, foundH, up->pJobLimit);
            if (up->flags & USER_GROUP) {

                if (numSlots <= 0
                    && !(hp->hStatus & HOST_STAT_UNREACH)
                    && ! (hp->hStatus & HOST_STAT_UNAVAIL)
                    && ! LS_ISUNAVAIL (hp->limStatus)) {
                    SET_REASON(numSlots <= 0,
                               up->reasonTb[1][hp->hostId],
                               PEND_UGRP_PROC_JLIMIT);

                } else if (numSlots > 0) {

                    CLEAR_REASON(up->reasonTb[1][hp->hostId],
                                 PEND_UGRP_PROC_JLIMIT);
                }

            } else {
                if (numSlots <= 0
                    && !(hp->hStatus & HOST_STAT_UNREACH)
                    && ! (hp->hStatus & HOST_STAT_UNAVAIL)
                    && ! LS_ISUNAVAIL (hp->limStatus)) {
                    SET_REASON(numSlots <= 0,
                               up->reasonTb[1][hp->hostId],
                               PEND_USER_PROC_JLIMIT);

                } else if (numSlots > 0) {

                    CLEAR_REASON(up->reasonTb[1][hp->hostId],
                                 PEND_USER_PROC_JLIMIT);

                }
            }

            if (numSlots <= 0 && (logclass & LC_JLIMIT)
                && !(hp->hStatus & HOST_STAT_UNREACH)
                && ! (hp->hStatus & HOST_STAT_UNAVAIL)
                && ! LS_ISUNAVAIL (hp->limStatus)) {

                ls_syslog(LOG_DEBUG2, "\
%s: U/G's JL/P reached; job=%s host=%s user=%s",
                          fname, lsb_jobid2str(jData->jobId),
                          hp->host, up->user);

            } else if (numSlots > 0 && (logclass & LC_JLIMIT)) {

                ls_syslog(LOG_DEBUG,"\
%s: U/G's JL/P cleared up; job=%s host=%s user=%s",
                          fname, lsb_jobid2str(jData->jobId),
                          hp->host, up->user);
            }
        }
    }

    if (qp != NULL && (qp->flags & QUEUE_UPDATE_USABLE)) {
        getQUsable (qp);
        qp->flags &= ~QUEUE_UPDATE_USABLE;
    }
}

struct hostAcct *
getHAcct(struct hTab *hAcct, struct hData *hp)
{
    hEnt   *hAcctEnt;

    if (hAcct == NULL || hp == NULL)
        return NULL;

    hAcctEnt = h_getEnt_(hAcct, hp->host);
    if (hAcctEnt != NULL)
        return((struct hostAcct *) hAcctEnt->hData);
    else
        return NULL;

}

/* addHAcct()
 */
struct hostAcct *
addHAcct(struct hTab **hAcct,
         struct hData *hp,
         int numRUN,
         int numSSUSP,
         int numUSUSP,
         int numRESERVE)
{
    struct hEnt       *ent;
    int               new;
    struct hostAcct   *acct;

    if (logclass &(LC_JLIMIT))
        ls_syslog(LOG_DEBUG2, "\
addHAcct: New hostAcct for host %s", hp->host);

    if (*hAcct == NULL) {
        *hAcct = my_calloc(1, sizeof(struct hTab), "addHAcct");
        h_initTab_(*hAcct, 11);
    }

    acct = my_calloc(1, sizeof(struct hostAcct), "addHAcct");

    ent = h_addEnt_(*hAcct, hp->host, &new);
    ent->hData = acct;
    acct->hPtr = hp;
    acct->numRUN = 0;
    acct->numSSUSP = 0;
    acct->numUSUSP = 0;
    acct->numRESERVE = 0;
    addOneAbs (&acct->numRUN, numRUN, FALSE);
    addOneAbs (&acct->numSSUSP, numSSUSP, FALSE);
    addOneAbs (&acct->numUSUSP, numUSUSP, FALSE);
    addOneAbs (&acct->numRESERVE, numRESERVE, FALSE);
    acct->numAvailSUSP = 0;
    acct->numNonPrmptRsv = 0;

    return acct;
}

static void
addOneAbs (int *num, int addedNum, int clean)
{

    if (clean == TRUE)
        *num = 0;
    if (addedNum > 0)
        (*num)++;
    else if (addedNum < 0)
        (*num)--;
}

void
updUserData (struct jData *jData, int numJobs, int numPEND,
             int numRUN, int numSSUSP, int numUSUSP, int numRESERVE)
{
    static char fname[] = "updUserData";
    struct uData *up, *ugp;
    static struct uData **grpPtr = NULL;
    int i, numSlots, numNew;
    int newJob;

    if (grpPtr == NULL && numofugroups > 0) {
        grpPtr = (struct uData **) my_calloc(numofugroups,
                                             sizeof(struct uData *), "updUserData");
    }


    if (jData->uPtr == NULL) {
        if (mSchedStage!=M_STAGE_REPLAY)
            ls_syslog (LOG_DEBUG, "updUserData: uData pointer of job <%s> is NULL", lsb_jobid2str(jData->jobId));
        jData->uPtr = getUserData(jData->userName);
    }
    up = jData->uPtr;
    up->flags &= ~USER_GROUP;
    updUserData1 (jData, up, numJobs, numPEND, numRUN,
                  numSSUSP, numUSUSP, numRESERVE);

    newJob = (numRUN == 0 && numSSUSP == 0 && numUSUSP == 0
              && numRESERVE == 0);
    if (!newJob) {
        int svReason = up->reasonTb[1][0];
        if (up->maxJobs == INFINIT_INT)
            numSlots = INFINIT_INT;
        else
            numSlots = up->maxJobs - up->numRUN - up->numSSUSP
                - up->numUSUSP - up->numRESERVE;
        if (up->reasonTb[1][0] == INFINIT_INT)
            up->reasonTb[1][0] = 0;
        SET_REASON(numSlots <= 0, up->reasonTb[1][0], PEND_USER_JOB_LIMIT);
        if (logclass & (LC_PEND | LC_JLIMIT)) {
            if (numSlots <= 0) {
                ls_syslog(LOG_DEBUG2, "%s: Set reason <%d> job=%s user=%s numJobs=%d maxJobs=%d numPEND=%d", fname, up->reasonTb[1][0], lsb_jobid2str(jData->jobId), up->user, up->numJobs, up->maxJobs, up->numPEND);
            }
            else if (svReason == PEND_USER_JOB_LIMIT
                     && (logclass & (LC_PEND | LC_JLIMIT))) {
                ls_syslog(LOG_DEBUG2, "%s: Clear reason <%d> job=%s user=%s numJobs=%d maxJobs=%d numPEND=%d", fname, svReason, lsb_jobid2str(jData->jobId), up->user, up->numJobs, up->maxJobs, up->numPEND);
            }
        }
    }

    if (numofugroups <= 0) {
        up->numSlots = numSlots;
        goto Exit;
    }


    numNew = 0;
    if (!(up->flags & USER_INIT)) {
        for (i = 0; i < numofugroups; i++) {
            if (!gMember(jData->userName, usergroups[i]))
                continue;
            if ((ugp = getUserData(usergroups[i]->group)) == NULL)
                continue;
            ugp->gData = usergroups[i];
            grpPtr[numNew++] = ugp;
        }
        FREEUP(up->gPtr);
        if (numNew > 0) {
            up->gPtr = (struct uData **) my_calloc(numNew,
                                                   sizeof(struct uData *), "updUserData");
        }

        for (i = 0; i < numNew; i++) {
            up->gPtr[i] = grpPtr[i];
        }
        up->numGrpPtr = numNew;
        up->flags |= USER_INIT;
    }


    for (i = 0; i < up->numGrpPtr; i++) {
        ugp = up->gPtr[i];
        if (ugp == NULL)
            continue;
        ugp->flags |= USER_GROUP;
        updUserData1 (jData, ugp, numJobs, numPEND,
                      numRUN, numSSUSP, numUSUSP, numRESERVE);


        if (!newJob) {
            int num;
            int svReason = ugp->reasonTb[1][0];
            if (ugp->maxJobs == INFINIT_INT)
                num = INFINIT_INT;
            else
                num = ugp->maxJobs - ugp->numRUN - ugp->numSSUSP
                    - ugp->numUSUSP - ugp->numRESERVE;
            numSlots = MIN(num, numSlots);
            if (ugp->reasonTb[1][0] == INFINIT_INT)
                ugp->reasonTb[1][0] = 0;
            SET_REASON(num <= 0, ugp->reasonTb[1][0], PEND_UGRP_JOB_LIMIT);
            if (logclass & (LC_PEND | LC_JLIMIT)) {
                if (num <= 0)
                    ls_syslog(LOG_DEBUG2, "%s: Set reason <%d>; job=%s group=%s numJobs=%d maxJobs=%d numPEND=%d", fname, ugp->reasonTb[1][0], lsb_jobid2str(jData->jobId), ugp->user, ugp->numJobs, ugp->maxJobs, ugp->numPEND);
                else
                    if (svReason == PEND_UGRP_JOB_LIMIT
                        && (logclass & (LC_PEND | LC_JLIMIT)))
                        ls_syslog(LOG_DEBUG2, "%s: Clear reason <%d> job=%s group=%s numJobs=%d maxJobs=%d numPEND=%d", fname, PEND_UGRP_JOB_LIMIT, lsb_jobid2str(jData->jobId), ugp->user, ugp->numJobs, ugp->maxJobs, ugp->numPEND);
            }
        }
    }

    up->numSlots = numSlots;

Exit:
    return;
}

static void
updUserData1 (struct jData *jData, struct uData *up, int numJobs,
              int numPEND, int numRUN, int numSSUSP, int numUSUSP,
              int numRESERVE)
{
    static char       fname[] = "updUserData1";
    bool_t            newJob;



    addValue (&up->numJobs, numJobs, jData, fname, "numJobs");
    addValue (&up->numPEND, numPEND, jData, fname, "numPEND");
    addValue (&up->numRUN, numRUN, jData, fname, "numRUN");
    addValue (&up->numSSUSP, numSSUSP, jData, fname, "numSSUSP");
    addValue (&up->numUSUSP, numUSUSP, jData, fname, "numUSUSP");
    addValue (&up->numRESERVE, numRESERVE, jData, fname, "numRESERVE");

    if (logclass & LC_JLIMIT)
        ls_syslog(LOG_DEBUG3, "\
%s: job=%s user/group=%s PEND=%d RUN=%d SSUSP=%d USUSP=%d RESERVE=%d",
                  fname, lsb_jobid2str(jData->jobId), up->user,
                  up->numPEND, up->numRUN, up->numSSUSP,
                  up->numUSUSP, up->numRESERVE);

    newJob = (   numRUN == 0
                 && numSSUSP == 0
                 && numUSUSP == 0
                 && numRESERVE == 0);

    if (   !  newJob
           && up->pJobLimit < INFINIT_FLOAT)
    {

        updHAcct(jData, NULL, up, &(up->hAcct), numRUN, numSSUSP,
                 numUSUSP, numRESERVE,
                 (void (*)(struct hostAcct *, void *))NULL, NULL);
    }

}

static void
updHostData(char updHPart, struct jData *jData, int numJobs, int numRUN,
            int numSSUSP, int numUSUSP, int numRESERVE)
{
    int i;

    for (i = 0; i < jData->numHostPtr; i++) {
        struct hData *hp = jData->hPtr[i];

        if (hp == NULL)
            break;

        if (jData->hPtr[i]->hStatus & HOST_STAT_REMOTE)
            continue;

        if (hp->uJobLimit < INFINIT_INT
            && (numRUN != 0 || numSSUSP != 0 || numUSUSP != 0
                || numRESERVE != 0)) {
            updUAcct(jData, jData->uPtr, &(hp->uAcct), numRUN, numSSUSP,
                     numUSUSP, numRESERVE, 0, jData->hPtr[i],
                     (void (*)(struct userAcct *, void *)) NULL, NULL);
        }

        addValue(&hp->numJobs,
                 numJobs,
                 jData,
                 (char *)__func__,
                 "numJobs");
        addValue(&hp->numRUN,
                 numRUN,
                 jData,
                 (char *)__func__,
                 "numRUN");
        addValue(&hp->numSSUSP,
                 numSSUSP,
                 jData,
                 (char *)__func__,
                 "numSSUSP");
        addValue(&hp->numUSUSP,
                 numUSUSP,
                 jData,
                 (char *)__func__,
                 "numUSUSP");
        addValue(&hp->numRESERVE,
                 numRESERVE,
                 jData,
                 (char *)__func__,
                 "numRESERVE");

        if (logclass & LC_JLIMIT)
            ls_syslog(LOG_DEBUG3, "%s: job=%s host=%s RUN=%d SSUSP=%d USUSP=%d RESERVE=%d", __func__, lsb_jobid2str(jData->jobId), hp->host, hp->numRUN, hp->numSSUSP, hp->numUSUSP, hp->numRESERVE);

        if (hp->numJobs >= hp->maxJobs) {

            hp->hStatus |= HOST_STAT_FULL;
            hReasonTb[1][hp->hostId] = PEND_HOST_JOB_LIMIT;

            if (logclass & (LC_PEND | LC_JLIMIT | LC_PREEMPT)) {
                ls_syslog(LOG_INFO, "\
%s: Set reason <%d> job=%s host=%s numJobs=%d maxJobs=%d",
                          __func__, hReasonTb[1][hp->hostId],
                          lsb_jobid2str(jData->jobId), hp->host,
                          hp->numJobs, hp->maxJobs);
            }

        } else {
            struct qData *qp;

            hp->hStatus &= ~HOST_STAT_FULL;

            if ((logclass & (LC_PEND | LC_JLIMIT | LC_PREEMPT))
                && hReasonTb[1][hp->hostId] == PEND_HOST_JOB_LIMIT) {
                ls_syslog(LOG_INFO, "\
%s: Clear reason <%d>; job=%s host=%s numJobs=%d maxJobs=%d",
                          __func__, hReasonTb[1][hp->hostId],
                          lsb_jobid2str(jData->jobId), hp->host,
                          hp->numJobs, hp->maxJobs);
            }

            CLEAR_REASON(hReasonTb[1][hp->hostId], PEND_HOST_JOB_LIMIT);
            for (qp = qDataList->forw; qp != qDataList; qp = qp->forw)
                CLEAR_REASON(qp->reasonTb[1][hp->hostId],
                             PEND_HOST_JOB_LIMIT);
        }

        if (hp->numJobs <= 0)
            hp->hStatus &= ~HOST_STAT_EXCLUSIVE;

        if (!updHPart)
            continue;
    }
}

static void
addValue(int *currentValue, int num, struct jData *jp, char *fname,
         char *counter)
{
    *currentValue += num;
    if (*currentValue >= 0)
        return;

    ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 7009,
                                     "%s: %s is negative; job=%s queue=%s currentValue=%d, num=%d"), fname, counter, lsb_jobid2str(jp->jobId), jp->qPtr->queue, *currentValue, num); /* catgets 7009 */
    *currentValue = 0;

}

struct uData *
getUserData (char *user)
{
    static char fname[] = "getUserData";
    hEnt   *userEnt;
    struct uData *uData, *defUser;

    userEnt = h_getEnt_(&uDataList, user);
    if (userEnt != NULL)
        return ((struct uData *) userEnt->hData);


    userEnt = h_getEnt_(&uDataList, "default");
    if (userEnt != NULL) {
        defUser = (struct uData *) userEnt->hData;
        uData = addUserData(user, defUser->maxJobs, defUser->pJobLimit,
                            "mbatchd/getUserData", FALSE, FALSE);
        if (uData != NULL)
            return uData;

        ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL, fname, "addUserData",
                  user);
        return ((struct uData *) NULL);
    }


    uData = addUserData(user, INFINIT_INT, INFINIT_FLOAT,
                        "mbatchd/getUserData", FALSE, FALSE);
    if (uData != NULL)
        return uData;

    ls_syslog(LOG_ERR, I18N_FUNC_S_FAIL, fname, "addUserData",
              user);
    return ((struct uData *) NULL);

}


int
checkUsers (struct infoReq *req, struct userInfoReply *reply)
{
    sTab hashSearchPtr;
    hEnt *hashEntryPtr;
    struct uData *uData, *defUser;
    struct userInfoEnt *uInfo;
    int i, j, found = FALSE;

    reply->numUsers = 0;

    if (req->numNames == 0) {
        hashEntryPtr = h_firstEnt_(&uDataList, &hashSearchPtr);
        while (hashEntryPtr) {
            uData = (struct uData *) hashEntryPtr->hData;


            if (uData->flags & USER_OTHERS) {
                hashEntryPtr = h_nextEnt_(&hashSearchPtr);
                continue;
            }

            uInfo = &(reply->users[reply->numUsers]);
            uInfo->user = uData->user;
            if (uData->pJobLimit >= INFINIT_FLOAT)
                uInfo->procJobLimit = INFINIT_FLOAT;
            else{
                uInfo->procJobLimit = uData->pJobLimit;
            }

            uInfo->maxJobs = uData->maxJobs;
            uInfo->numStartJobs = uData->numJobs - uData->numPEND;
            uInfo->numJobs = uData->numJobs;
            uInfo->numPEND = uData->numPEND;
            uInfo->numRUN  = uData->numRUN;
            uInfo->numSSUSP = uData->numSSUSP;
            uInfo->numUSUSP = uData->numUSUSP;
            uInfo->numRESERVE = uData->numRESERVE;
            reply->numUsers++;
            hashEntryPtr = h_nextEnt_(&hashSearchPtr);
        }
        if (reply->numUsers == 0)
            return (LSBE_NO_USER);
        return (LSBE_NO_ERROR);
    }

    if ((defUser = getUserData ("default")) == NULL)
        return (LSBE_NO_USER);

    for (i = 0; i < req->numNames; i++) {
        hashEntryPtr = h_getEnt_(&uDataList, req->names[i]);
        if (hashEntryPtr != NULL) {
            uData = (struct uData *)hashEntryPtr->hData;
        } else {
            if (getpwnam (req->names[i]) == NULL) {
                for (j = 0; j < numofugroups; j++) {
                    if (strcmp (req->names[i], usergroups[j]->group))
                        continue;
                    found = TRUE;
                    break;
                }
            }
            uData = defUser;
        }
        uInfo = &(reply->users[reply->numUsers]);
        if (uData == defUser)
            uInfo->user = req->names[i];
        else
            uInfo->user = uData->user;
        if (found) {
            uInfo->maxJobs = INFINIT_INT;
            uInfo->procJobLimit = INFINIT_FLOAT;
        } else {
            uInfo->maxJobs = uData->maxJobs;
            if (uData->pJobLimit >= INFINIT_FLOAT)
                uInfo->procJobLimit = INFINIT_FLOAT;
            else
                uInfo->procJobLimit = uData->pJobLimit;
        }
        uInfo->numStartJobs = uData->numJobs
            - uData->numPEND - uData->numRESERVE;
        uInfo->numJobs = uData->numJobs;
        uInfo->numPEND = uData->numPEND;
        uInfo->numRUN  = uData->numRUN;
        uInfo->numSSUSP = uData->numSSUSP;
        uInfo->numUSUSP = uData->numUSUSP;
        uInfo->numRESERVE = uData->numRESERVE;
        reply->numUsers++;
        found = FALSE;
    }
    return (LSBE_NO_ERROR);

}

struct uData *
addUserData(char *username, int maxjobs, float pJobLimit,
            char *filename, int override, int config)
{
    hEnt *userEnt;
    int new;
    struct uData *uData;
    static int first = TRUE;

    if (logclass & (LC_JLIMIT))
        ls_syslog(LOG_DEBUG2, "\
%s: new uData for user/group %s with maxJobs=%d pJobLimit=%f",
		  __func__, username, maxjobs, pJobLimit);

    if (first) {
        h_initTab_(&uDataList, 61);
        first = FALSE;
    }

    userEnt = h_addEnt_(&uDataList, username, &new);
    if (new) {
        uData = my_calloc(1,
                          sizeof(struct uData),
                          "addUserData");
        initUData(uData);
    } else if (override) {
        uData = (struct uData *)userEnt->hData;
        FREEUP(uData->user);
    } else {
        if (filename)
            ls_syslog(LOG_ERR, "\
%s: %s: User <%s> is multiply defined; retaining old definition",
                      __func__,
                      filename,
                      username);
        return userEnt->hData;
    }

    if (config == TRUE)
        uData->flags |= USER_UPDATE;

    uData->user = safeSave(username);
    uData->pJobLimit = pJobLimit;
    uData->maxJobs   = maxjobs;
    userEnt->hData = uData;

    return uData;
}

double
get_host_shares(char *host, char *queue)
{
    struct hShareData *hData = NULL;
    struct qShareData *qData = NULL;
    hEnt *he = NULL;
    hEnt *qe = NULL;

    if ((he = h_getEnt_(&hShareTab, host)) == NULL)
        return 0;

    hData = (struct hShareData *)he->hData;
    if ((qe = h_getEnt_(hData->qAcct, queue)) == NULL)
        return 0;

    qData = (struct qShareData *)qe->hData;
    return qData->dshares;
}

void
checkParams(struct infoReq *req, struct parameterInfo *reply)
{
    if (defaultQueues)
        reply->defaultQueues = defaultQueues;
    else
        reply->defaultQueues = "";
    if (defaultHostSpec)
        reply->defaultHostSpec = defaultHostSpec;
    else
        reply->defaultHostSpec = "";
    reply->mbatchdInterval = msleeptime;
    reply->sbatchdInterval = sbdSleepTime;
    reply->jobAcceptInterval = accept_intvl;

    reply->maxDispRetries = max_retry;
    reply->maxSbdRetries = max_sbdFail;
    reply->cleanPeriod = clean_period;
    reply->maxNumJobs = maxjobnum;
    reply->pgSuspendIt = pgSuspIdleT;
    reply->defaultProject = getDefaultProject();
    reply->jobTerminateInterval = jobTerminateInterval;
    reply->maxJobArraySize = maxJobArraySize;

    if (pjobSpoolDir) {
        reply->pjobSpoolDir = pjobSpoolDir;
    } else {
        reply->pjobSpoolDir = "";
    }

    reply->maxUserPriority = maxUserPriority;
    reply->jobPriorityValue = jobPriorityValue;
    reply->jobPriorityTime = jobPriorityTime;
    reply->jobDepLastSub = jobDepLastSub;
    reply->sharedResourceUpdFactor = sharedResourceUpdFactor;
    reply->maxJobId = maxJobId;
    reply->maxAcctArchiveNum = maxAcctArchiveNum;
    reply->acctArchiveInDays = acctArchiveInDays;
    reply->acctArchiveInSize = acctArchiveInSize;
    reply->maxPreemptJobs = mbdParams->maxPreemptJobs;
    reply->maxStreamRecords = mbdParams->maxStreamRecords;
    reply->freshPeriod = freshPeriod;
    reply->maxSbdConnections = maxSbdConnections;
    reply->hist_mins = mbdParams->hist_mins;
    reply->run_abs_limit = mbdParams->run_abs_limit;
    if (mbdParams->preemptableResources)
        reply->preemptableResources = mbdParams->preemptableResources;
    else
        reply->preemptableResources = "";
    reply->preempt_slot_suspend = mbdParams->preempt_slot_suspend;
    reply->slot_decay_factor = mbdParams->slot_decay_factor;
}

void
mbdDie(int sig)
{
    struct jData *jpbw;
    int list;
    sigset_t newmask;
    char myhostname[MAXHOSTNAMELEN];
    char *myhostp = myhostname;

    sigemptyset(&newmask);
    sigaddset(&newmask, SIGCHLD);
    sigaddset(&newmask, SIGTERM);
    sigaddset(&newmask, SIGINT);
    sigprocmask(SIG_BLOCK, &newmask, NULL);


    for (list = 0; list < NJLIST; list++) {
        if (jDataList[list] != NULL) {
            for (jpbw = jDataList[list]->back; jpbw != jDataList[list];
                 jpbw=jpbw->back) {
                if (!(jpbw->pendEvent.notSwitched
                      || jpbw->pendEvent.sig != SIG_NULL
                      || jpbw->pendEvent.sig1 != SIG_NULL
                      || jpbw->pendEvent.notModified))
                    continue;
                if (IS_FINISH(jpbw->jStatus) && (getZombieJob(jpbw->jobId)) == NULL)
                    continue;
                log_unfulfill(jpbw);
            }
        }
    }

    log_mbdDie(sig);

    freeTclLsInfo(tclLsInfo, 0);
    freeTclInterp();

    if (gethostname(myhostp, MAXHOSTNAMELEN) < 0) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL_M, "mbdDie", "gethostname");
        strcpy(myhostp, "localhost");
    }

    die(sig);

}

int
isManager(char *lsfUserName)
{

    int i;

    for (i = 0; i < nManagers; i++) {
        if (strcmp(lsfUserName, lsbManagers[i]) == 0)
            return TRUE;
    }
    return FALSE;
}


int
isAuthManager(struct lsfAuth *auth)
{

    if (mSchedStage == M_STAGE_REPLAY)
        return true;

    return isManager(auth->lsfUserName);
}

char *
getDefaultProject(void)
{

    static char szDefaultProjName[MAX_LSB_NAME_LEN];


    if (lsfDefaultProject) {
        strcpy(szDefaultProjName, lsfDefaultProject);
        return(szDefaultProjName);
    }

    strcpy(szDefaultProjName, "default");

    return(szDefaultProjName);

}

void
updResCounters(struct jData *jData, int newStatus)
{
    int num;
    int numReq;

    if (!IS_PEND (jData->jStatus) && !IS_START (jData->jStatus))
        return;

    if (logclass & (LC_TRACE | LC_JLIMIT))
        ls_syslog(LOG_DEBUG3, "\
%s:jobId %s jStatus %x num %d numReq %d newStatus %x",
                  __func__, lsb_jobid2str(jData->jobId),
                  jData->jStatus,
                  jData->numHostPtr,
                  jData->shared->jobBill.maxNumProcessors,
                  newStatus);

    num = jData->numHostPtr;

    numReq = jData->shared->jobBill.maxNumProcessors;

    switch (MASK_STATUS (jData->jStatus & ~JOB_STAT_UNKWN)) {
        case JOB_STAT_SSUSP:
            if (!(jData->jStatus & JOB_STAT_RESERVE)
                && (newStatus & JOB_STAT_RESERVE)
                && IS_START (newStatus)) {

                updQaccount (jData, 0, 0, 0, -num, 0, num);
                updUserData(jData, 0, 0, 0, -num, 0, num);
                updLimitSlotData(jData, 0, 0, 0, -num, 0, num);
                updLimitJobData(jData, 0, 0, 0, -1, 0, 1);
                updHostData(TRUE, jData, 0, 0, -1, 0, 1);
            }  else if ((jData->jStatus & JOB_STAT_RESERVE)
                        && !(newStatus & JOB_STAT_RESERVE)
                        && (IS_START (newStatus)
                            || (newStatus & JOB_STAT_PEND))) {
                updQaccount (jData, 0, 0, 0, num, 0, -num);
                updUserData(jData, 0, 0, 0, num, 0, -num);
                updLimitSlotData(jData, 0, 0, 0, num, 0, -num);
                updLimitJobData(jData, 0, 0, 0, 1, 0, -1);
                updHostData(TRUE, jData, 0, 0, 1, 0, -1);
            } else if ((jData->jStatus & JOB_STAT_RESERVE)
                       && IS_FINISH(newStatus)) {
                updQaccount (jData, -num, 0, 0, 0, 0, -num);
                updUserData(jData, -num, 0, 0, 0, 0, -num);
                updLimitSlotData(jData, -num, 0, 0, 0, 0, -num);
                updLimitJobData(jData, -1, 0, 0, 0, 0, -1);
                updHostData(TRUE, jData, -1, 0, 0, 0, -1);
            } else {
            ls_syslog(LOG_ERR, "%s: Job <%s> transited from %x to %x",
                      __func__,
                      lsb_jobid2str(jData->jobId),
                      jData->jStatus, newStatus);
            }
            break;
        case JOB_STAT_USUSP:
            if (!(jData->jStatus & JOB_STAT_RESERVE)
                && (newStatus & JOB_STAT_RESERVE)
                && IS_START (newStatus)) {
                updQaccount (jData, 0, 0, 0, 0, -num, num);
                updUserData(jData, 0, 0, 0, 0, -num,  num);
                updLimitSlotData(jData, 0, 0, 0, 0, -num,  num);
                updLimitJobData(jData, 0, 0, 0, 0, -1, 1);
                updHostData(TRUE, jData, 0, 0, 0, -1, 1);
            }  else if ((jData->jStatus & JOB_STAT_RESERVE)
                        && !(newStatus & JOB_STAT_RESERVE)
                        && IS_START (newStatus)) {

                updQaccount (jData, 0, 0, 0, 0, num, -num);
                updUserData(jData, 0, 0, 0, 0, num, -num);
                updLimitSlotData(jData, 0, 0, 0, 0, -num,  num);
                updLimitJobData(jData, 0, 0, 0, 0, -1, 1);
                updHostData(TRUE, jData, 0, 0, 0, 1, -1);
            } else if ((jData->jStatus & JOB_STAT_RESERVE)
                       && IS_FINISH(newStatus)) {
                updQaccount (jData, -num, 0, 0, 0, 0, -num);
                updUserData(jData, -num, 0, 0, 0, 0, -num);
                updLimitSlotData(jData, -num, 0, 0, 0, 0, -num);
                updLimitJobData(jData, -1, 0, 0, 0, 0, -1);
                updHostData(TRUE, jData, -1, 0, 0, 0, -1);
            } else {
                ls_syslog(LOG_ERR, "\
%s: Job <%s> transited from %x to %x",__func__,
                          lsb_jobid2str(jData->jobId),
                          jData->jStatus, newStatus);
            }
            break;
        case JOB_STAT_PEND:
        case JOB_STAT_PSUSP:
            if ((jData->jStatus & JOB_STAT_RESERVE)
                && !(newStatus & JOB_STAT_RESERVE)
                && IS_PEND (newStatus)) {

                updQaccount (jData, 0, num, 0, 0, 0, -num);
                updHostData(TRUE, jData, -1, 0, 0, 0, -1);
                updUserData(jData, 0, num, 0, 0, 0, -num);
                updLimitSlotData(jData, 0, num, 0, 0, 0, -num);
                updLimitJobData(jData, 0, 1, 0, 0, 0, -1);
            }  else if (!(jData->jStatus & JOB_STAT_RESERVE)
                        && (newStatus & JOB_STAT_RESERVE)
                        && IS_PEND (newStatus)) {
                updQaccount (jData, 0, -num, 0, 0, 0, num);
                updHostData(TRUE, jData, 1, 0, 0, 0, 1);
                updUserData(jData, 0, -num, 0, 0, 0, num);
                updLimitSlotData(jData, 0, -num, 0, 0, 0, num);
                updLimitJobData(jData, 0, -1, 0, 0, 0, 1);
            } else if (IS_PEND (jData->jStatus)
                       && (jData->jStatus & JOB_STAT_RESERVE)
                       && IS_FINISH(newStatus)) {
                updQaccount (jData, -numReq, -numReq+num, 0, 0, 0, -num);
                updHostData(TRUE, jData, -1, 0, 0, 0, -1);
                updUserData(jData, -numReq, -numReq+num, 0, 0, 0, -num);
                updLimitSlotData(jData, -numReq, -numReq+num, 0, 0, 0, -num);
                updLimitJobData(jData, -1, -1, 0, 0, 0, -1);
            } else {
                ls_syslog(LOG_ERR, "\
%s: Job <%s> transited from %x to %x", __func__,
                    lsb_jobid2str(jData->jobId), jData->jStatus,
                    newStatus);
            }
            break;
    }
}

static void
initUData(struct uData *uPtr)
{
    uPtr->user = NULL;
    uPtr->maxJobs = INFINIT_INT;
    uPtr->pJobLimit = INFINIT_FLOAT;
    uPtr->hAcct = NULL;
    uPtr->flags = 0;
    uPtr->numJobs   = 0;
    uPtr->numPEND   = 0;
    uPtr->numRUN    = 0;
    uPtr->numSSUSP  = 0;
    uPtr->numUSUSP  = 0;
    uPtr->numRESERVE  = 0;
    uPtr->numSlots  = INFINIT_INT;
    uPtr->numGrpPtr = 0;
    uPtr->gPtr = NULL;
    uPtr->gData = NULL;
    FREEUP(uPtr->reasonTb);
    uPtr->reasonTb = my_calloc(2, sizeof(int *), __func__);
    uPtr->reasonTb[0] = my_calloc(numofhosts() + 1,
                                  sizeof(int), __func__);
    uPtr->reasonTb[1] = my_calloc(numofhosts() + 1,
                                  sizeof(int), __func__);
    uPtr->uDataIndex  = -1;
    uPtr->children    = NULL;
    uPtr->descendants = NULL;
    uPtr->parents     = NULL;
    uPtr->ancestors   = NULL;
    /* User jobs references
     */
    uPtr->jobs = dlink_make();
}

void
updHostLeftRusageMem(struct jData *jobP, int order)
{
    static  char fname[] = "updHostLeftRusageMem";
    int     numHost;
    struct  resVal *resValPtr;
    float   resMem;

    if (logclass & (LC_TRACE))
        ls_syslog(LOG_DEBUG, "%s: Enter this function ...", fname);

    resValPtr = getReserveValues (jobP->shared->resValPtr, jobP->qPtr->resValPtr);
    if (resValPtr != NULL) {
        resMem = resValPtr->val[MEM];

        if (resMem < INFINIT_LOAD && resMem > -INFINIT_LOAD) {


            if (resValPtr->duration != INFINIT_INT)
                return;

            if ((hasResSpanHosts(jobP->shared->resValPtr)
                 || hasResSpanHosts(jobP->qPtr->resValPtr))
                && (jobP->shared->jobBill.numProcessors > 1)
                && (jobP->shared->jobBill.numProcessors == jobP->shared->jobBill.maxNumProcessors)) {
                resMem /= jobP->shared->jobBill.numProcessors;
                if (logclass & (LC_TRACE | LC_SCHED )) {
                    ls_syslog(LOG_DEBUG, "\
%s: Updating job <%s>'s reserved mem to <%f> as all processors of the job are allocated on the same host.",
                    fname,
                    lsb_jobid2str(jobP->jobId),
                    resMem);
                }
            }

            for (numHost = 0; numHost < jobP->numHostPtr; numHost++) {
                if (jobP->hPtr[numHost]->leftRusageMem == INFINIT_LOAD){

                    int i;
                    getLsfHostInfo(FALSE);
                    for (i = 0; i < numLIMhosts; i++) {
                        if (strcmp(LIMhosts[i].hostName,
                                   jobP->hPtr[numHost]->host) == 0)
                            break;
                    }
                    if (i < numLIMhosts && LIMhosts[i].maxMem != 0)
                        jobP->hPtr[numHost]->leftRusageMem = LIMhosts[i].maxMem;
                    else
                        return;
                }
                jobP->hPtr[numHost]->leftRusageMem +=  resMem * order;
                if (logclass & (LC_TRACE | LC_SCHED ))
                    ls_syslog(LOG_DEBUG, "%s: job <%s> reserved mem is %f, execution host <%s>, new leftRusageMem is %f, order is %d", fname, lsb_jobid2str(jobP->jobId),  resMem, jobP->hPtr[numHost]->host, jobP->hPtr[numHost]->leftRusageMem, order);

            }
        }
    }
}

/* fork_mbd()
 */
int
fork_mbd(void)
{
    pid_t pid;
    int cc;
    int n;

    pid = fork();
    if (pid < 0) {
	ls_syslog(LOG_ERR, "\
%s: ohmygosh fork() failed %m", __func__);
	return -1;
    }
    if (pid > 0)
	return pid;

    n = sysconf(_SC_OPEN_MAX);
    for (cc = 3; cc < n; cc++)
	close(cc);

    return pid;
}
