/*
 * Copyright (C) 2015 - 2016 David Bigagli
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301, USA
 *
 */
#include "preempt.h"

static struct jData *
find_first_pend_job(struct qData *);
static bool_t
is_pend_for_license(struct jData *);
static bool_t
is_preemptable_resource(const char *);
static bool_t
is_pend_for_slot(struct jData *);


/* prm_init()
 */
int
prm_init(LIST_T *qList)
{
    if (logclass & LC_PREEMPT)
        ls_syslog(LOG_INFO, "%s: plugin initialized", __func__);

    return 0;
}

/* prm_elect_preempt()
 *
 * Elect jobs to be preempted based on jobs that can trigger
 * preemption.
 */
int
prm_elect_preempt(struct qData *qPtr, link_t *rl, int numjobs)
{
    link_t *jl;
    struct jData *jPtr;
    struct jData *jPtr2;
    uint32_t numPEND;
    uint32_t numSLOTS;
    uint32_t num_harvest;
    linkiter_t iter;

    if (logclass & LC_PREEMPT) {
        ls_syslog(LOG_INFO, "\
%s: entering preemptive queue %s maxPreemptJobs %d preempt_slot_suspend %d",
                  __func__, qPtr->queue, mbdParams->maxPreemptJobs,
                  mbdParams->preempt_slot_suspend);
    }

    /* Jobs that can eventually trigger
     * preemption causing other jobs to
     * be requeued
     */
    jl = make_link();

    /* Gut nicht jobs
     */
    jPtr = find_first_pend_job(qPtr);
    if (jPtr == NULL) {

        fin_link(jl);

        if (logclass & LC_PREEMPT)
            ls_syslog(LOG_INFO, "\
%s: No jobs to trigger preemption in queue %s",
                      __func__, qPtr->queue);
        return 0;
    }

    numPEND = 0;
    while (jPtr) {

        jPtr2 = jPtr->back;
        assert(jPtr->jStatus & JOB_STAT_PEND
               || jPtr->jStatus & JOB_STAT_PSUSP);

        /* mbatchd does preempt for resources different
         * than slots
         */
        if (mbdParams->preemptableResources) {

            if (! is_pend_for_license(jPtr)) {
                if (logclass & LC_PREEMPT) {
                    ls_syslog(LOG_INFO, "\
%s: job %s queue %s can trigger resource %s preemption", __func__,
                              lsb_jobid2str(jPtr->jobId),
                              qPtr->queue, mbdParams->preemptableResources);
                }
                goto dalsi;
            }
        } else {

            if (! is_pend_for_slot(jPtr)) {

                if (logclass & LC_PREEMPT) {
                    ls_syslog(LOG_INFO, "\
%s: job %s queue %s can trigger preemption", __func__,
                              lsb_jobid2str(jPtr->jobId), qPtr->queue);
                }
                goto dalsi;
            }
        }

        ++numPEND;
        /* Save the candidate in jl
         */
        enqueue_link(jl, jPtr);
        if (logclass & LC_PREEMPT) {
            ls_syslog(LOG_INFO, "\
%s: job %s queue %s can trigger preemption", __func__,
                      lsb_jobid2str(jPtr->jobId), qPtr->queue);
        }

        /* Preempt only a sebset of hosts by default 1
         */
        if (numPEND >= mbdParams->maxPreemptJobs)
            break;

    dalsi:
        /* Fine della coda
         */
        if (jPtr2 == (void *)jDataList[PJL]
            || jPtr->qPtr->priority != jPtr2->qPtr->priority)
            break;
        jPtr = jPtr2;
    }

    if (numPEND == 0) {

        fin_link(jl);
        if (logclass & LC_PREEMPT)
            ls_syslog(LOG_INFO, "\
%s: No pending jobs to trigger preemption in queue %s",
                      __func__, qPtr->queue);
        return 0;
    }

    /* Traverse candidate list of jobs in the
     * preemptive queue and search for preemptable jobs.
     */
    while ((jPtr = pop_link(jl))) {
        struct qData *qPtr2;

        /* Initialiaze the iterator on the list
         * of preemptable queue, the list is
         * traversed in the order in which it
         * was configured.
         */
        traverse_init(jPtr->qPtr->preemptable, &iter);
        /* Number of slots this job wants
         */
        numSLOTS = jPtr->shared->jobBill.numProcessors;
        /* Number of slots we were able to harvest
         */
        num_harvest = 0;

        while ((qPtr2 = traverse_link(&iter))) {

            if (qPtr2->numRUN == 0)
                continue;

            if (logclass & LC_PREEMPT)
                ls_syslog(LOG_INFO, "\
%s: job %s queue %s trying to harvest %d slots in queue %s",
                          __func__, lsb_jobid2str(jPtr->jobId),
                          qPtr2->queue, numSLOTS, qPtr->queue, qPtr2->queue);

            /* Search on SJL jobs belonging to the
             * preemptable queue and harvest slots.
             * later we want to eventually break out
             * of this loop somehow.
             */
            for (jPtr2 = jDataList[SJL]->forw;
                 jPtr2 != jDataList[SJL];
                 jPtr2 = jPtr2->forw) {
                int cc;

                if (jPtr2->qPtr != qPtr2)
                    continue;

                if (IS_SUSP(jPtr2->jStatus))
                    continue;

                if (jPtr2->jStatus & JOB_STAT_SIGNAL)
                    continue;

                /* This job was already preempted
                 */
                if (jPtr2->preempted_by > 0)
                    continue;

                num_harvest = num_harvest + jPtr2->shared->jobBill.numProcessors;
                /* Hop in the list of jobs that will be preempted
                 * if we harvest enough slots.
                 */
                push_link(rl, jPtr2);

                if (logclass & LC_PREEMPT) {
                    ls_syslog(LOG_INFO, "\
%s: job %s gives up %d slots got %d want %d", __func__,
                              lsb_jobid2str(jPtr2->jobId),
                              jPtr2->shared->jobBill.numProcessors,
                              num_harvest, numSLOTS);
                }

                jPtr2->preempted_by = jPtr->jobId;

                for (cc = 0; cc < jPtr2->numHostPtr; cc++) {

                    if (logclass & LC_PREEMPT) {
                        ls_syslog(LOG_INFO, "\
%s: job %s preempting exec hosts %s", __func__, lsb_jobid2str(jPtr->jobId),
                                  jPtr2->hPtr[cc]->host);
                    }

                    push_link(jPtr->preempted_hosts, jPtr2->hPtr[cc]);
                }

                if (num_harvest >= numSLOTS) {

                    fin_link(jl);
                    if (logclass & LC_PREEMPT) {
                        ls_syslog(LOG_INFO, "\
%s: job %s did harvest enough slots wanted %d got %d", __func__,
                                  lsb_jobid2str(jPtr->jobId), numSLOTS, num_harvest);
                    }
                    /* Log the job preemption events
                     */
                    log_job_preemption(jPtr, rl);

                    return LINK_NUM_ENTRIES(rl);
                }

            } /* for running jobs in preemptable queue */

        } /* while (preemptable queues) */

        /* We did not find the number of necessary slots
         * so undo the operation.
         */
        if (num_harvest < numSLOTS) {

            if (logclass & LC_PREEMPT) {
                ls_syslog(LOG_INFO, "\
%s: job %s did not harvest enough slots wanted %d got %d", __func__,
                          lsb_jobid2str(jPtr->jobId), numSLOTS, num_harvest);
            }
            while ((jPtr2 = pop_link(rl)))
                jPtr2->preempted_by = 0;
            while (pop_link(jPtr->preempted_hosts))
                ;
        }

        /* Only try a subset of hosts
         */
        if (LINK_NUM_ENTRIES(rl) >= mbdParams->maxPreemptJobs)
            break;

    } /* while jobs on preemptive list */

    assert(LINK_NUM_ENTRIES(rl) == 0);

    return LINK_NUM_ENTRIES(rl);
}

/* find_first_pend_job()
 */
static struct jData *
find_first_pend_job(struct qData *qPtr)
{
    struct jData *jPtr;

    for (jPtr = jDataList[PJL]->back;
         jPtr != (void *)jDataList[PJL];
         jPtr = jPtr->back) {

        if (jPtr->qPtr == qPtr)
            return jPtr;
    }

    return NULL;
}

/* is_pend_for_license()
 */
static bool_t
is_pend_for_license(struct jData *jPtr)
{
    struct resVal *resPtr;
    int cc;
    int rusage;
    int is_set;
    int reason;
    linkiter_t iter;
    struct _rusage_ *r;
    struct resVal r2;

    /* Try the host and then the queue
     */
    resPtr = jPtr->shared->resValPtr;
    if (resPtr == NULL)
        resPtr = jPtr->qPtr->resValPtr;
    if (resPtr == NULL)
        return false;

    rusage = 0;
    for (cc = 0; cc < GET_INTNUM(allLsInfo->nRes); cc++)
        rusage += resPtr->rusage_bit_map[cc];

    if (rusage == 0)
        return false;

    traverse_init(resPtr->rl, &iter);
    while ((r = traverse_link(&iter))) {

        r2.rusage_bit_map = r->bitmap;
        r2.val = r->val;

        for (cc = 0; cc < allLsInfo->nRes; cc++) {

            if (NOT_NUMERIC(allLsInfo->resTable[cc]))
                continue;

            TEST_BIT(cc, r2.rusage_bit_map, is_set);
            if (is_set == 0)
                continue;

            if (r2.val[cc] >= INFINIT_LOAD
                || r2.val[cc] < 0.01)
                continue;

            if (cc < allLsInfo->numIndx)
                continue;

            if (is_preemptable_resource(allLsInfo->resTable[cc].name))
                goto dal;
        }
    }

dal:
    if (jPtr->numReasons == 0)
        return false;

    for (cc = 0; cc < jPtr->numReasons; cc++) {
        GET_LOW(reason, jPtr->reasonTb[cc]);
        if (reason >= PEND_HOST_JOB_RUSAGE)
            return true;
        if (reason >= PEND_HOST_QUE_RUSAGE
            && reason < PEND_HOST_JOB_RUSAGE)
            return true;
    }

    return false;
}

/* is_preemptable_resource()
 */
static bool_t
is_preemptable_resource(const char *res)
{
    static char buf[MAXLSFNAMELEN];
    char *p;
    char *str;

    if (mbdParams->preemptableResources == NULL)
        return false;

    strcpy(buf, mbdParams->preemptableResources);

    str = buf;
    while ((p = getNextWord_(&str))) {
        if (strcmp(p, res) == 0)
            return true;
    }

    return false;
}

/* is_pend_for_slot()
 */
static bool_t
is_pend_for_slot(struct jData *jPtr)
{
    /* mbatchd preempts for slots
     */
    if (!(jPtr->jStatus & JOB_STAT_PEND
          && jPtr->newReason == 0)) {
        return false;
    }

    return true;
}
