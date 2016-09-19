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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 *
 */

#include <unistd.h>

#include "cmd.h"

static void printLong(struct parameterInfo *);
static void printShort(struct parameterInfo *);
void
usage(char *cmd)
{
    fprintf(stderr, "usage: %s [-h] [-V] [-l]\n", cmd);
}

int
main (int argc, char **argv)
{
    int cc;
    struct parameterInfo  *paramInfo;
    int longFormat;

    if (lsb_init(argv[0]) < 0) {
        lsb_perror("lsb_init");
        return -1;
    }

    longFormat = FALSE;

    while ((cc = getopt(argc, argv, "Vhl")) != EOF) {
        switch (cc) {
            case 'l':
                longFormat = TRUE;
                break;
            case 'V':
                fputs(_LS_VERSION_, stderr);
                return 0;
            case 'h':
                usage(argv[0]);
                return 0;
            default :
                usage(argv[0]);
                return -1;
        }
    }

    if (!(paramInfo = lsb_parameterinfo(NULL, NULL, 0))) {
        lsb_perror(NULL);
        return -1;
    }
    if (longFormat)
        printLong(paramInfo);
    else
        printShort(paramInfo);

    return 0;
}

static void
printShort(struct parameterInfo *reply)
{
    printf("Default Queues:  %s\n", reply->defaultQueues);
    if (reply->defaultHostSpec[0] != '\0')
        printf ("Default Host Specification:  %s\n", reply->defaultHostSpec);
    printf("\
Job Dispatch Interval:  %d seconds\n", reply->mbatchdInterval);
    printf("\
Job Checking Interval:  %d seconds\n", reply->sbatchdInterval);
    printf("\
Job Accepting Interval:  %d seconds\n",
           reply->jobAcceptInterval * reply->mbatchdInterval);
}

static void
printLong(struct parameterInfo *reply)
{
    printf("System default queues for automatic queue selection:\n");
    printf(" %16.16s = %s\n\n",  "DEFAULT_QUEUE", reply->defaultQueues);

    if (reply->defaultHostSpec[0] != '\0') {
        printf("System default host or host model for adjusting CPU time limit");
        printf(" %20.20s = %s\n\n",  "DEFAULT_HOST_SPEC",
               reply->defaultHostSpec);
    }

    printf("The interval for dispatching jobs by master batch daemon:\n");
    printf("    MBD_SLEEP_TIME = %d (seconds)\n\n", reply->mbatchdInterval);

    printf("The interval for checking jobs by slave batch daemon:\n");
    printf("    SBD_SLEEP_TIME = %d (seconds)\n\n", reply->sbatchdInterval);

    printf("The interval for a host to accept two batch jobs:\n");
    printf("    JOB_ACCEPT_INTERVAL = %d (* MBD_SLEEP_TIME)\n\n",
           reply->jobAcceptInterval);

    printf("The idle time of a host for resuming pg suspended jobs:\n");
    printf("    PG_SUSP_IT = %d (seconds)\n\n", reply->pgSuspendIt);

    printf("The amount of time during which finished jobs are kept in core:\n");
    printf("    CLEAN_PERIOD = %d (seconds)\n\n", reply->cleanPeriod);

    printf("\
The maximum number of finished jobs that can be stored in current events file:\n");
    printf("    MAX_JOB_NUM = %d\n\n", reply->maxNumJobs);

    printf("The maximum number of retries for reaching a slave batch daemon:\n");
    printf("    MAX_SBD_FAIL = %d\n\n", reply->maxSbdRetries);

    printf("The default project assigned to jobs:\n");
    printf("    %15s = %s\n\n", "DEFAULT_PROJECT", reply->defaultProject);

    printf("The interval to terminate a job:\n");
    printf("    JOB_TERMINATE_INTERVAL = %d \n\n",
           reply->jobTerminateInterval);

    printf("The maximum number of jobs in a job array:\n");
    printf("    MAX_JOB_ARRAY_SIZE = %d\n\n", reply->maxJobArraySize);
    printf("User level account mapping for remote jobs is %s.\n",
           (reply->disableUAcctMap == TRUE) ?
           "disabled" : "permitted");

    if (strlen(reply->pjobSpoolDir) > 0) {
        printf("The batch jobs' temporary output directory:\n");
        printf("    JOB_SPOOL_DIR = %s\n\n", reply->pjobSpoolDir);
    }

    if ( reply->maxUserPriority > 0 ) {
        printf("Maximal job priority defined for all users:\n");
        printf("    MAX_USER_PRIORITY = %d\n", reply->maxUserPriority);
        printf("    DEFAULT_USER_PRIORITY = %d\n\n", reply->maxUserPriority/2);
    }

    if (reply->jobPriorityValue > 0) {
        printf("Job priority is increased by the system dynamically based on waiting time:\n");
        printf("    JOB_PRIORITY_OVER_TIME = %d/%d (minutes)\n\n",
               reply->jobPriorityValue, reply->jobPriorityTime);
    }

    if (reply->sharedResourceUpdFactor > 0){
        printf("Static shared resource update interval for the cluster:\n");
        printf("    SHARED_RESOURCE_UPDATE_FACTOR = %d \n\n",
               reply->sharedResourceUpdFactor);
    }

    if (reply->jobDepLastSub == 1) {
        printf("Used with job dependency scheduling:\n");
        printf("    JOB_DEP_LAST_SUB = %d \n\n", reply->jobDepLastSub);
    }

    printf("The Maximum JobId defined in the system:\n");
    printf("    MAX_JOBID = %d\n\n", reply->maxJobId);

    if (reply->maxAcctArchiveNum > 0) {
        printf("Max number of Acct files:\n");
        printf(" %24s = %d\n\n", "MAX_ACCT_ARCHIVE_FILE", reply->maxAcctArchiveNum);
    }

    if (reply->acctArchiveInDays > 0) {
        printf("Mbatchd Archive Interval:\n");
        printf(" %19s = %d days\n\n", "ACCT_ARCHIVE_AGE", reply->acctArchiveInDays);
    }

    if (reply->acctArchiveInSize > 0) {
        printf("Mbatchd Archive threshold:\n");
        printf(" %20s = %d kB\n\n", "ACCT_ARCHIVE_SIZE", reply->acctArchiveInSize);
    }

    printf("Maximum number of preempted jobs per cycle:\n");
    printf("    MAX_PREEMPT_JOBS = %d\n\n", reply->maxPreemptJobs);

    if (reply->maxStreamRecords > 0) {
        printf("Maximum number of records in lsb.stream file:\n");
        printf("    MAX_STREAM_RECORDS = %d\n\n", reply->maxStreamRecords);
    }

    /* 15 is the default load update interval defined
     * in mbd.h as DEF_FRESH_PERIOD
     */
    if (reply->freshPeriod != 15) {
        printf("MBD load update interval is:\n");
        printf("     LOAD_UPDATE_INTVL = %d\n\n", reply->freshPeriod);
    }

    printf("Maximum open connections with SBD\n");
    printf("    MAX_SBD_CONNS = %d\n\n", reply->maxSbdConnections);

    printf("History minutes\n");
    printf("    HIST_MINUTES = %d\n\n", reply->hist_mins);

    if (reply->run_abs_limit == true) {
        printf("Absolute Runtime\n");
        printf("    ABS_RUNTIME is enabled.\n\n");
    }

    /* If enabled then mbatchd will preempt jobs based on queue
     * priority and resource usage rather then slots.
     */
    if (reply->preemptableResources[0] != 0) {
        printf("Preemptable resources\n");
        printf("    PREEMPTABLE_RESOURCES = %s\n\n", reply->preemptableResources);
    }

    printf("Suspend job when preempting for slots:\n");
    if (reply->preempt_slot_suspend)
        printf("    PREEMPT_SLOT_SUSPEND = yes\n\n");
    else
        printf("    PREEMPT_SLOT_SUSPEND = no\n\n");

    if (reply->slot_decay_factor != DEF_SLOT_DECAY_FACTOR) {
        printf("Slot decay factor:\n");
        printf("    SLOT_DECAY_FACTOR = %d\n\n", reply->slot_decay_factor);
    }
}
