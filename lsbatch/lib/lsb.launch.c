/*
 * Copyright (C) 2014-2016 David Bigagli
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

#include "lsb.h"

static int *tasks;
struct jRusage **jrus;
static int num_tasks;
static int jobID;
static char *hostname;

static void make_tasks(char **);
static int size_rusage(struct jRusage *);
static void send_rusage(void);
static int send2sbd(struct jRusage *);
static void free_rusage(struct jRusage *);
static struct jRusage *compact_rusage(void);

/* lsb_launch()
 *
 * Launch a job using lsfbase library. Used by blaunch command.
 * host_list and command must be NULL terminated.
 */
int
lsb_launch(char **host_list, char **command, int opt, char **env)
{
    int cc;
    int tid;
    int num_tasks;
    int task_active;
    int rest;
    char *p;

    if (host_list == NULL
        || *host_list == NULL
        || command == NULL
        || *command == NULL) {
        lsberrno = LSBE_BAD_ARG;
        return -1;
    }

    if (! (p = getenv("LSB_JOBID"))) {
        lsberrno = LSBE_NO_JOB;
        return -1;
    }
    jobID = atoi(p);

    Signal_(SIGUSR1, SIG_IGN);

    rest = 10;
    p = getenv("LSB_BLAUNCH_SLEEPTIME");
    if (p) {
        rest = atoi(p);
    }

    hostname = ls_getmyhostname();
    make_tasks(host_list);

    /* Start all jobs first
     */
    num_tasks = cc = 0;
    while (host_list[cc]) {
        /* Run the task on the host
         */
        tid = ls_rtask(host_list[cc],
                       command,
                       0);
        if (tid < 0) {
            ls_syslog(LOG_ERR, "\
%s: task %d on host %s failed %s", __func__,
                    tid, host_list[cc], ls_sysmsg());
            return -1;
        }
        /* Array of taskids
         */
        tasks[cc] = tid;
        ls_syslog(LOG_INFO, "\
%s: task id %d cc %d started on host %s", __func__, tid, cc, host_list[cc]);
        ++cc;
        ++num_tasks;
    }

znovu:
    /* Check if they are still alive
     */
    task_active = cc = 0;
    for (cc = 0; cc < num_tasks; cc++) {
        LS_WAIT_T stat;
        struct rusage ru;
        int tid;

        if (tasks[cc] < 0)
            continue;

        tid = ls_rwaittid(tasks[cc], &stat, WNOHANG, &ru);
        if (tid < 0) {
            ls_syslog(LOG_ERR, "\
%s: ls_rwaittid() failed task %d %s", __func__, tid, ls_sysmsg());
            tasks[cc] = -1;
            free_rusage(jrus[cc]);
            jrus[cc] = NULL;
            continue;
        }

        if (tid == 0) {
            /* Collect the rusage if still running
             */
            jrus[cc] = ls_getrusage(tasks[cc]);
            if (jrus[cc] == NULL) {
                ls_syslog(LOG_ERR, "\
%s: failed to get rusage from task %d %s", __func__, cc, ls_sysmsg());
                tasks[cc] = -1;
                free_rusage(jrus[cc]);
                jrus[cc] = NULL;
                continue;
            }

            ls_syslog(LOG_INFO, "\
%s: got rusage for task tid %d from host %s",
                      __func__, tasks[cc], host_list[cc]);
        }

        if (tid > 0) {
            ls_syslog(LOG_INFO, "%s: task %d done", __func__, tid);
            tasks[cc] = -1;
            free_rusage(jrus[cc]);
            jrus[cc] = NULL;
        }
    }

    for (cc = 0; cc < num_tasks; cc++) {
        if (tasks[cc] > 0) {
            ls_syslog(LOG_INFO, "\
%s: task %d still active", __func__, tasks[cc]);
            ++task_active;
        }
    }

    if (task_active > 0) {
        send_rusage();
        sleep(rest);
        goto znovu;
    }

    ls_syslog(LOG_INFO, "%s: all %d tasks gone", __func__, num_tasks);

    return 0;

}

/* make_tasks()
 */
static void
make_tasks(char **host_list)
{
    int cc;

    cc = 0;
    while (host_list[cc])
        ++cc;

    /* Total counter of tasks
     */
    num_tasks = cc;

    /* This array holds the taskids
     */
    tasks = calloc(cc, sizeof(int));
    /* This array holds the jRusage of each task
     */
    jrus = calloc(cc, sizeof(struct jRusage *));
}
static void
send_rusage(void)
{
    struct jRusage *jru;

    jru = compact_rusage();

    if (send2sbd(jru) < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed to send jRusage data to SBD on %s: %s",
                  __func__, hostname, lsb_sysmsg());
    }

    free_rusage(jru);
}

static struct jRusage *
compact_rusage(void)
{
    struct jRusage *j;
    int cc;
    int k;
    int n;

    j = calloc(1, sizeof(struct jRusage));

    for (cc = 0; cc < num_tasks; cc++) {

        if (jrus[cc] == NULL)
            continue;

        ls_syslog(LOG_INFO, "\
%s: task %d mem %d swap %d utime %d stime %d npids %d npgids %d", __func__,
                  tasks[cc], jrus[cc]->mem, jrus[cc]->swap,
                  jrus[cc]->utime, jrus[cc]->stime,
                  jrus[cc]->npids, jrus[cc]->npgids);

        j->mem = j->mem + jrus[cc]->mem;
        j->swap = j->swap + jrus[cc]->swap;
        j->utime = j->utime + jrus[cc]->utime;
        j->stime = j->stime + jrus[cc]->stime;
        j->npids = j->npids + jrus[cc]->npids;
        j->npgids = j->npgids + jrus[cc]->npgids;
    }


    j->pidInfo = calloc(j->npids, sizeof(struct pidInfo));
    j->pgid = calloc(j->npgids, sizeof(int));

    /* Now merge all the pids and pigds into the compact
     * jrusage structure. Those FORTRAN array memories.
     */
    n = k = 0;
    for (cc = 0; cc < num_tasks; cc++) {
        int i;

        if (jrus[cc] == NULL)
            continue;

        for (i = 0; i < jrus[cc]->npids; i++) {
            j->pidInfo[k].pid = jrus[cc]->pidInfo[i].pid;
            j->pidInfo[k].ppid = jrus[cc]->pidInfo[i].ppid;
            j->pidInfo[k].pgid = jrus[cc]->pidInfo[i].pgid;
            ++k;
        }

        for (i = 0; i < jrus[cc]->npgids; i++) {
            j->pgid[n] = jrus[cc]->pgid[i];
            ++n;
        }
    }

    /* Debug the global rusage
     */
    for (cc = 0; cc < j->npids; cc++)
        ls_syslog(LOG_INFO, "\
%s: pid %d ppid %d pgid %d", __func__, j->pidInfo[cc].pid,
                  j->pidInfo[cc].ppid, j->pidInfo[cc].pgid);

    for (cc = 0; cc < j->npgids; cc++)
        ls_syslog(LOG_INFO, "%s: pgid %d", __func__, j->pgid[cc]);

    return j;
}

/* send2sbd()
 */
static int
send2sbd(struct jRusage *jru)
{
    XDR xdrs;
    int len;
    int len2;
    int cc;
    struct LSFHeader hdr;
    char *req_buf;
    char *reply_buf;

    len = sizeof(LS_LONG_INT) + sizeof(struct LSFHeader);
    len = len + size_rusage(jru);
    len = len * sizeof(int);

    initLSFHeader_(&hdr);
    hdr.opCode = SBD_BLAUNCH_RUSAGE;

    req_buf = calloc(len, sizeof(char));
    xdrmem_create(&xdrs, req_buf, len, XDR_ENCODE);

    XDR_SETPOS(&xdrs, sizeof(struct LSFHeader));

    /* encode the jobID
     */
    if (! xdr_int(&xdrs, &jobID)) {
        ls_syslog(LOG_ERR, "\
%s: failed encoding jobid %d", __func__, jobID);
        _free_(req_buf);
        xdr_destroy(&xdrs);
        return -1;
    }

    /* encode the rusage
     */
    if (! xdr_jRusage(&xdrs, jru, &hdr)) {
        ls_syslog(LOG_ERR, "\
%s: failed encoding jobid %d or stepid %d", __func__, jobID);
        _free_(req_buf);
        xdr_destroy(&xdrs);
        return -1;
    }

    len2 = XDR_GETPOS(&xdrs);
    hdr.length =  len2 - sizeof(struct LSFHeader);
    XDR_SETPOS(&xdrs, 0);

    if (!xdr_LSFHeader(&xdrs, &hdr)) {
        ls_syslog(LOG_ERR, "\
%s: failed encoding jobid %d or stepid %d", __func__, jobID);
        _free_(req_buf);
        xdr_destroy(&xdrs);
        return -1;
    }

    XDR_SETPOS(&xdrs, len2);

    /* send 2 sbatchd
     */
    reply_buf = NULL;
    cc = cmdCallSBD_(hostname, req_buf, len, &reply_buf, &hdr, NULL);
    if (cc < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed calling SBD on %s: %s", __func__, hostname, lsb_sysmsg());
        _free_(req_buf);
        xdr_destroy(&xdrs);
        return -1;
    }

    if (hdr.opCode != LSBE_NO_ERROR) {
        /* Here we assuem sbatchd is replying us with LSBE
         * number rather than hist sbdReplyType.
         */
        lsberrno = hdr.opCode;
        ls_syslog(LOG_ERR, "\
%s: SBD on %s returned: %s", __func__, hostname, lsb_sysmsg());
        _free_(req_buf);
        xdr_destroy(&xdrs);
        return -1;
    }

    _free_(req_buf);
    xdr_destroy(&xdrs);

    return 0;
}

/* size_rusage()
 */
static int
size_rusage(struct jRusage *jru)
{
    int cc;

    cc = 0;
    cc = 5 * sizeof(int);
    cc = cc + jru->npids * sizeof(struct pidInfo);
    cc = cc + jru->npgids * sizeof(int *);

    return cc;
}

static void
free_rusage(struct jRusage *jru)
{
    if (!jru)
        return;

    _free_(jru->pidInfo);
    _free_(jru->pgid);
    _free_(jru);
}
