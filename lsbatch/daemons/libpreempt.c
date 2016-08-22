/*
 * Copyright (C) 2015 David Bigagli
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

/* prm_init()
 */
int
prm_init(LIST_T *qList)
{
    if (logclass & LC_PREEMPT)
        ls_syslog(LOG_INFO, "%s: plugin initialized", __func__);

    return 0;
}

/* Elect jobs to be preempted
 */
int
prm_elect_preempt(struct qData *qPtr, link_t *rl, int numjobs)
{
    link_t *jl;
    struct jData *jPtr;
    struct jData *jPtr2;
    uint32_t numPEND;
    uint32_t numSLOTS;
    linkiter_t iter;

    if (logclass & LC_PREEMPT)
        ls_syslog(LOG_INFO, "%s: entering queue %s",
                  __func__, qPtr->queue);

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
%s: job %s queue %s can trigger preemption", __func__,
                              lsb_jobid2str(jPtr->jobId), qPtr->queue);
                }
                goto dalsi;
            }
        } else {
            /* mbatchd preempts for slots
             */
            if (! (jPtr->jStatus & JOB_STAT_PEND
                   && jPtr->newReason == 0)) {
                goto dalsi;
            }
        }

        ++numPEND;
        /* Save the candidate in jl
         */
        enqueue_link(jl, jPtr);
        if (logclass & LC_PREEMPT)
            ls_syslog(LOG_INFO, "\
%s: job %s queue %s can trigger preemption", __func__,
                      lsb_jobid2str(jPtr->jobId), qPtr->queue);
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

    /* Traverse candidate list
     */
    while ((jPtr = pop_link(jl))) {
        struct qData *qPtr2;
        int num;

        /* Initialiaze the iterator on the list
         * of preemptable queue, the list is
         * traversed in the order in which it
         * was configured.
         */
        traverse_init(jPtr->qPtr->preemptable, &iter);
        numSLOTS = jPtr->shared->jobBill.numProcessors;
        num = 0;

        while ((qPtr2 = traverse_link(&iter))) {

            if (qPtr2->numRUN == 0)
                continue;

            if (logclass & LC_PREEMPT)
                ls_syslog(LOG_INFO, "\
%s: job %s queue %s trying to canibalize %d slots in queue %s",
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

                if (jPtr2->qPtr != qPtr2)
                    continue;

                if (IS_SUSP(jPtr2->jStatus))
                    continue;

                if (jPtr2->jStatus & JOB_STAT_SIGNAL)
                    continue;

                num = num + jPtr2->shared->jobBill.numProcessors;
                push_link(rl, jPtr2);

                if (logclass & LC_PREEMPT)
                    ls_syslog(LOG_INFO, "\
%s: job %s gives up %d slots got %d want %d", __func__,
                              lsb_jobid2str(jPtr2->jobId),
                              jPtr2->shared->jobBill.numProcessors,
                              num, numSLOTS);

                if (num >= numSLOTS)
                    goto pryc;
            }
        }
    pryc:;
        if (LINK_NUM_ENTRIES(rl) >= numjobs)
            break;
    }

    fin_link(jl);

    if (logclass & LC_PREEMPT)
        ls_syslog(LOG_INFO, "%s: leaving queue %s",
                  __func__, qPtr->queue);

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
