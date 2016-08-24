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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA
 *
 */

#include "cmd.h"
#include "../../lsf/intlib/sshare.h"

static void prtQueuesLong(int, struct queueInfoEnt *);
static void prtQueuesShort(int, struct queueInfoEnt *);
static void print_slot_shares(struct queueInfoEnt *);
static void print_slot_owned(struct queueInfoEnt *);

static char wflag = FALSE;
extern int terminateWhen_(int *, char *);

#define QUEUE_NAME_LENGTH    15
#define QUEUE_PRIO_LENGTH    4
#define QUEUE_STATUS_LENGTH  14
#define QUEUE_JL_U_LENGTH    4
#define QUEUE_JL_P_LENGTH    4
#define QUEUE_JL_H_LENGTH    4
#define QUEUE_MAX_LENGTH     4
#define QUEUE_NJOBS_LENGTH   5
#define QUEUE_PEND_LENGTH    5
#define QUEUE_RUN_LENGTH     5
#define QUEUE_SUSP_LENGTH    5
#define QUEUE_SSUSP_LENGTH   5
#define QUEUE_USUSP_LENGTH   5
#define QUEUE_NICE_LENGTH    4
#define QUEUE_RSV_LENGTH     4

void
usage(const char *cmd)
{
    fprintf(stderr, ": %s [-h] [-V] [-w | -l] [-m host_name | -m cluster_name]\n", cmd);
    fprintf(stderr, " [-u user_name]");
    fprintf(stderr, " [queue_name ...]\n");
}

int
main(int argc, char **argv)
{
    int numQueues;
    char **queues;
    struct queueInfoEnt *queueInfo;
    char lflag = FALSE;
    int cc;
    char *host;
    char *user;

    numQueues = 0;
    user = host = NULL;

    if (lsb_init(argv[0]) < 0) {
        lsb_perror("lsb_init");
        return -1;
    }

    while ((cc = getopt(argc, argv, "Vhlwm:u:")) != EOF) {
        switch (cc) {
            case 'l':
                lflag = TRUE;
                if (wflag) {
                    usage(argv[0]);
                    return -1;
                }
                break;
            case 'w':
                wflag = TRUE;
                if (lflag) {
                    usage(argv[0]);
                    return -1;
                }
                break;
            case 'm':
                if (host != NULL || *optarg == '\0')
                    usage(argv[0]);
                host = optarg;
                break;
            case 'u':
                if (user != NULL || *optarg == '\0')
                    usage(argv[0]);
                user = optarg;
                break;
            case 'V':
                fputs(_LS_VERSION_, stderr);
                return 0;
            case 'h':
            default:
                usage(argv[0]);
            return -1;
        }
    }

    queues = NULL;
    numQueues = 0;

    if (optind < argc) {
        numQueues = argc - optind;
        queues = calloc(argc - optind, sizeof(char *));
        for (cc = 0; cc < argc - optind; cc++)
            queues[cc] = argv[optind + cc];
    }

    TIMEIT(0, (queueInfo = lsb_queueinfo(queues,
                                         &numQueues,
                                         host,
                                         user,
                                         0)), "lsb_queueinfo");

    if (!queueInfo) {
        if (lsberrno == LSBE_BAD_QUEUE && queues)
            lsb_perror(queues[numQueues]);
        else {
            switch (lsberrno) {
                case LSBE_BAD_HOST   :
                case LSBE_QUEUE_HOST :
                    lsb_perror(host);
                    break;
                case LSBE_BAD_USER   :
                case LSBE_QUEUE_USE  :
                    lsb_perror(user);
                    break;
                default :
                    lsb_perror(NULL);
            }
        }
        _free_(queues);
        return -1;
    }

    if (lflag)
        prtQueuesLong(numQueues, queueInfo);
    else
        prtQueuesShort(numQueues, queueInfo);

    _free_(queues);

    return 0;
}

/* prtQueuesLong()
 */
static void
prtQueuesLong(int numQueues, struct queueInfoEnt *queueInfo)
{
    struct queueInfoEnt *qp;
    char *status;
    char userJobLimit[MAX_CHARLEN];
    char procJobLimit[MAX_CHARLEN];
    char hostJobLimit[MAX_CHARLEN];
    char maxJobs[MAX_CHARLEN];
    int i;
    int numDefaults = 0;
    struct lsInfo *lsInfo;
    int printFlag = 0;
    int printFlag1 = 0;
    int printFlag2 = 0;
    int printFlag3 = 0;
    int procLimits[3];

    if ((lsInfo = ls_info()) == NULL) {
        ls_perror("ls_info");
        exit(-1);
    }

    printf("\n");

    for (i = 0; i < numQueues; i++ )  {
        qp = &(queueInfo[i]);
        if (qp->qAttrib & Q_ATTRIB_DEFAULT)
            numDefaults++;
    }

    for (i = 0; i < numQueues; i++ ) {

        qp = &(queueInfo[i]);

        if (qp->qStatus & QUEUE_STAT_OPEN) {
            if ((qp->qStatus & QUEUE_STAT_ACTIVE)
                && (qp->qStatus & QUEUE_STAT_RUN))
                status = "   Open:Active  ";
            if ((qp->qStatus & QUEUE_STAT_ACTIVE)
                && !(qp->qStatus & QUEUE_STAT_RUN))
                status = " Open:Inact_Win "; /* windows are closed */
            if (!(qp->qStatus & QUEUE_STAT_ACTIVE))
                status = " Open:Inact_Adm "; /* By user (manager) */
        } else {  /* queue is disabled */
            if ((qp->qStatus & QUEUE_STAT_ACTIVE)
                && (qp->qStatus & QUEUE_STAT_RUN))
                status = "  Closed:Active ";
            if ((qp->qStatus & QUEUE_STAT_ACTIVE)
                && !(qp->qStatus & QUEUE_STAT_RUN))
                status = "Closed:Inact_Win"; /* by time window */
            if (!(qp->qStatus & QUEUE_STAT_ACTIVE))
                status = "Closed:Inact_Adm"; /* by user (manager) */
        }

        if (qp->maxJobs < INFINIT_INT)
            sprintf (maxJobs, "%5d", qp->maxJobs);
        else
            sprintf (maxJobs, "   - ");
        if (qp->userJobLimit < INFINIT_INT)
            sprintf (userJobLimit, "%4d", qp->userJobLimit);
        else
            sprintf (userJobLimit, "  - ");
        if (qp->procJobLimit < INFINIT_FLOAT)
            sprintf (procJobLimit, "%4.1f", qp->procJobLimit);
        else
            sprintf (procJobLimit, "  - ");
        if (qp->hostJobLimit < INFINIT_INT)
            sprintf (hostJobLimit, "%4d", qp->hostJobLimit);
        else
            sprintf (hostJobLimit, "  - ");

        if (i > 0)
            printf("-------------------------------------------------------------------------------\n\n");
        printf("QUEUE: %s\n", qp->queue);
        printf("  -- %s", qp->description);
        if (qp->qAttrib & Q_ATTRIB_DEFAULT) {
            if (numDefaults == 1)
                printf("  This is the default queue.\n\n");
            else
                printf("  This is one of the default queues.\n\n");
        } else
            printf("\n\n");

        printf("PARAMETERS/STATISTICS\n");
        printf("PRIO");
        printf(" NICE");
        printf("     STATUS       ");
        printf("MAX JL/U JL/P JL/H ");
        printf("NJOBS  PEND  RUN  SSUSP USUSP  RSV\n");
        printf("%4d", qp->priority);
        printf(" %3d", qp->nice);
        printf(" %-16.16s", status);

        printf("%s %s %s %s",
               maxJobs, userJobLimit, procJobLimit,hostJobLimit);

        printf("%5d %5d %5d %5d %5d %4d\n\n",
               qp->numJobs, qp->numPEND, qp->numRUN,
               qp->numSSUSP, qp->numUSUSP, qp->numRESERVE);

        if (qp->mig < INFINIT_INT)
            printf("Migration threshold is %d minutes\n", qp->mig);

        if (qp->schedDelay < INFINIT_INT)
            printf("Schedule delay for a new job is %d seconds\n",
                   qp->schedDelay);

        if (qp->acceptIntvl < INFINIT_INT)
            printf("Interval for a host to accept two jobs is %d seconds\n",
                   qp->acceptIntvl);


        if (((qp->defLimits[LSF_RLIMIT_CPU] != INFINIT_INT) &&
             (qp->defLimits[LSF_RLIMIT_CPU] > 0 )) ||
            ((qp->defLimits[LSF_RLIMIT_RUN] != INFINIT_INT) &&
             (qp->defLimits[LSF_RLIMIT_RUN] > 0 )) ||
            ((qp->defLimits[LSF_RLIMIT_DATA] != INFINIT_INT) &&
             (qp->defLimits[LSF_RLIMIT_DATA] > 0 )) ||
            ((qp->defLimits[LSF_RLIMIT_RSS] != INFINIT_INT) &&
             (qp->defLimits[LSF_RLIMIT_RSS] > 0 )) ||
            ((qp->defLimits[LSF_RLIMIT_PROCESS] != INFINIT_INT) &&
             (qp->defLimits[LSF_RLIMIT_PROCESS] > 0 ))) {


            printf("\n");
            printf("DEFAULT LIMITS:");
            prtResourceLimit (qp->defLimits, qp->hostSpec, 1.0, 0);
            printf("\n");
            printf("MAXIMUM LIMITS:");
        }

        procLimits[0] = qp->minProcLimit;
        procLimits[1] = qp->defProcLimit;
        procLimits[2] = qp->procLimit;
        prtResourceLimit (qp->rLimits, qp->hostSpec, 1.0, procLimits);

        printf("\nSCHEDULING PARAMETERS\n");
        if (printThresholds(qp->loadSched,  qp->loadStop, NULL, NULL,
                            MIN(lsInfo->numIndx, qp->nIdx), lsInfo) < 0)
            exit(-1);

        if ((qp->qAttrib & Q_ATTRIB_EXCLUSIVE)
            || (qp->qAttrib & Q_ATTRIB_BACKFILL)
            || (qp->qAttrib & Q_ATTRIB_IGNORE_DEADLINE)
            || (qp->qAttrib & Q_ATTRIB_ONLY_INTERACTIVE)
            || (qp->qAttrib & Q_ATTRIB_NO_INTERACTIVE)
            || (qp->qAttrib & Q_ATTRIB_FAIRSHARE)) {
            printf("\nSCHEDULING POLICIES:");
            if (qp->qAttrib & Q_ATTRIB_FAIRSHARE)
                printf("  FAIRSHARE");
            if (qp->qAttrib & Q_ATTRIB_BACKFILL)
                printf("  BACKFILL");
            if (qp->qAttrib & Q_ATTRIB_IGNORE_DEADLINE)
                printf("  IGNORE_DEADLINE");
            if (qp->qAttrib & Q_ATTRIB_EXCLUSIVE)
                printf("  EXCLUSIVE");
            if (qp->qAttrib & Q_ATTRIB_NO_INTERACTIVE)
                printf("  NO_INTERACTIVE");
            if (qp->qAttrib & Q_ATTRIB_ONLY_INTERACTIVE)
                printf("  ONLY_INTERACTIVE");
            if (qp->qAttrib & Q_ATTRIB_OWNERSHIP)
                printf("  OWNED_SLOTS");
            if (qp->qAttrib & Q_ATTRIB_ROUND_ROBIN)
                printf("ROUND_ROBIN_SCHEDULING:  yes\n");
            printf("\n");
        }

        /* If the queue has the FAIRSHARE policy on, print out
         * shareAcctInforEnt data structure.
         */
        if (qp->qAttrib & Q_ATTRIB_FAIRSHARE) {
            print_slot_shares(qp);
        }

        if (qp->qAttrib & Q_ATTRIB_OWNERSHIP) {
            print_slot_owned(qp);
        }

        if (qp->qAttrib & Q_ATTRIB_PREEMPTIVE)
            printf("\nPREEMPTION = %s", qp->preemption);

        if (strcmp (qp->defaultHostSpec, " ") !=  0)
            printf("\nDEFAULT HOST SPECIFICATION:  %s\n", qp->defaultHostSpec);

        if (qp->windows && strcmp (qp->windows, " " ) !=0)
            printf("\nRUN_WINDOWS:  %s\n", qp->windows);
        if (strcmp (qp->windowsD, " ")  !=  0)
            printf("\nDISPATCH_WINDOW:  %s\n", qp->windowsD);

        if ( strcmp(qp->userList, " ") == 0) {
            printf("\nUSERS:  all users\n");
        } else {
            if (strcmp(qp->userList, " ") != 0 && qp->userList[0] != 0)
                printf("\nUSERS:  %s\n", qp->userList);
        }

        if (strcmp(qp->hostList, " ") == 0) {
            printf("HOSTS:  all hosts used by the OpenLava system\n");
        } else {
            if (strcmp(qp->hostList, " ") != 0 && qp->hostList[0])
                printf("HOSTS:  %s\n", qp->hostList);
        }
        if (strcmp (qp->admins, " ") != 0)
            printf("ADMINISTRATORS:  %s\n", qp->admins);
        if (strcmp (qp->preCmd, " ") != 0)
            printf("PRE_EXEC:  %s\n", qp->preCmd);
        if (strcmp (qp->postCmd, " ") != 0)
            printf("POST_EXEC:  %s\n", qp->postCmd);
        if (strcmp (qp->requeueEValues, " ") != 0)
            printf("REQUEUE_EXIT_VALUES:  %s\n", qp->requeueEValues);
        if (strcmp (qp->resReq, " ") != 0)
            printf("RES_REQ:  %s\n", qp->resReq);
        if (qp->slotHoldTime > 0
            && !(qp->qAttrib & Q_ATTRIB_MEM_RESERVE))
            printf("Maximum slot reservation time: %d seconds\n", qp->slotHoldTime);
        if (qp->slotHoldTime > 0
            && (qp->qAttrib & Q_ATTRIB_MEM_RESERVE))
            printf("Maximum slot and memory reservation time: %d seconds\n",
                   qp->slotHoldTime);
        if (strcmp (qp->resumeCond, " ") != 0)
            printf("RESUME_COND:  %s\n", qp->resumeCond);
        if (strcmp (qp->stopCond, " ") != 0)
            printf("STOP_COND:  %s\n", qp->stopCond);
        if (strcmp (qp->jobStarter, " ") != 0)
            printf("JOB_STARTER:  %s\n", qp->jobStarter);

        /* CONF_SIG_ACT */

        printf("\n");
        printFlag = 0;
        if  ((qp->suspendActCmd != NULL)
             && (qp->suspendActCmd[0] != ' '))
            printFlag = 1;

        printFlag1 = 0;
        if  ((qp->resumeActCmd != NULL)
             && (qp->resumeActCmd[0] != ' '))
            printFlag1 = 1;

        printFlag2 = 0;
        if  ((qp->terminateActCmd != NULL)
             && (qp->terminateActCmd[0] != ' '))
            printFlag2 = 1;

        if (printFlag || printFlag1 || printFlag2)
            printf("JOB_CONTROLS:\n");


        if (printFlag) {
            printf("    SUSPEND:  ");
            if (strcmp (qp->suspendActCmd, " ") != 0)
                printf("    [%s]\n", qp->suspendActCmd);
        }

        if (printFlag1) {
            printf("    RESUME:   ");
            if (strcmp (qp->resumeActCmd, " ") != 0)
                printf("    [%s]\n", qp->resumeActCmd);
        }

        if (printFlag2) {
            printf("    TERMINATE:");
            if (strcmp (qp->terminateActCmd, " ") != 0)
                printf("    [%s]\n", qp->terminateActCmd);
        }

        if (printFlag || printFlag1 || printFlag2)
            printf("\n");

        printFlag = terminateWhen_(qp->sigMap, "USER");
        printFlag1 = terminateWhen_(qp->sigMap, "PREEMPT");
        printFlag2 = terminateWhen_(qp->sigMap, "WINDOW");
        printFlag3 = terminateWhen_(qp->sigMap, "LOAD");

        if (printFlag | printFlag1 | printFlag2 | printFlag3) {
            printf("TERMINATE_WHEN = ");
            if (printFlag) printf("USER ");
            if (printFlag1) printf("PREEMPT ");
            if (printFlag2) printf("WINDOW ");
            if (printFlag3) printf("LOAD");
            printf("\n");
        }
    }
}

/* prtQueuesSort()
 */
static void
prtQueuesShort(int numQueues, struct queueInfoEnt *queueInfo)
{
    struct queueInfoEnt *qp;
    char *status;
    char first = FALSE;
    int i;
    char userJobLimit[MAX_CHARLEN];
    char procJobLimit[MAX_CHARLEN];
    char hostJobLimit[MAX_CHARLEN];
    char maxJobs[MAX_CHARLEN];

    if(!first) {
        printf("QUEUE_NAME     PRIO      STATUS      ");

        printf("MAX  JL/U JL/P JL/H ");

        printf("NJOBS  PEND  RUN  SUSP\n");
        first = TRUE;
    }
    for (i = 0; i < numQueues; i++) {

        qp = &(queueInfo[i]);

        if (qp->qStatus & QUEUE_STAT_OPEN) {
            if ((qp->qStatus & QUEUE_STAT_ACTIVE)
                && (qp->qStatus & QUEUE_STAT_RUN))
                status = "  Open:Active  ";
            if ((qp->qStatus & QUEUE_STAT_ACTIVE)
                && !(qp->qStatus & QUEUE_STAT_RUN))
                status = " Open:Inactive ";
            if (!(qp->qStatus & QUEUE_STAT_ACTIVE))
                status = " Open:Inactive "; /* By user (manager) */
        } else {  /* queue is closed */
            if ((qp->qStatus & QUEUE_STAT_ACTIVE)
                && (qp->qStatus & QUEUE_STAT_RUN))
                status = " Closed:Active ";
            if ((qp->qStatus & QUEUE_STAT_ACTIVE)
                && !(qp->qStatus & QUEUE_STAT_RUN))
                status = "Closed:Inactive"; /* by time window */
            if (!(qp->qStatus & QUEUE_STAT_ACTIVE))
                status = "Closed:Inactive"; /* by user (manager) */
        }
        if (qp->maxJobs < INFINIT_INT)
            sprintf (maxJobs, "%5d", qp->maxJobs);
        else
            sprintf (maxJobs, "    -");
        if (qp->userJobLimit < INFINIT_INT)
            sprintf (userJobLimit, "%4d", qp->userJobLimit);
        else
            sprintf (userJobLimit, "   -");
        if (qp->procJobLimit < INFINIT_FLOAT)
            sprintf (procJobLimit, "%4.0f", qp->procJobLimit);
        else
            sprintf (procJobLimit, "   -");
        if (qp->hostJobLimit < INFINIT_INT)
            sprintf (hostJobLimit, "%4d", qp->hostJobLimit);
        else
            sprintf (hostJobLimit, "   -");

        if (wflag) {
            printf("%-15s %2d  %-15s", qp->queue, qp->priority, status);
            printf("%s %s %s %s", maxJobs,
                   userJobLimit, procJobLimit, hostJobLimit);

            printf(" %5d %5d %5d %5d \n",
                   qp->numJobs, qp->numPEND, qp->numRUN,
                   qp->numSSUSP + qp->numUSUSP);
        } else {

            printf("%-15.15s %2d  %-15.15s",
                   qp->queue, qp->priority, status);

            printf("%5s %4s %4s %4s", maxJobs,
                   userJobLimit, procJobLimit, hostJobLimit);

            printf(" %5d %5d %5d %5d \n",
                   qp->numJobs, qp->numPEND, qp->numRUN,
                   qp->numSSUSP + qp->numUSUSP);
        }
    }
}

/* print_slot_shares()
 */
static void
print_slot_shares(struct queueInfoEnt *qp)
{
    int i;
    uint32_t numRUN;

    numRUN = 0;
    for (i = 0; i < qp->numAccts; i++) {
        if (qp->saccts[i]->options & SACCT_USER)
            numRUN = numRUN + qp->saccts[i]->numRUN;
    }

    printf("\
\nTOTAL_SLOTS: %u FREE_SLOTS: %u\n", qp->numFairSlots,
           qp->numFairSlots - numRUN);

    printf("\
%9s   %6s   %8s   %6s   %6s   %6s\n",
           "USER/GROUP", "SHARES", "PRIORITY", "PEND", "RUN", "RAN");
    for (i = 0; i < qp->numAccts; i++) {
        char buf[MAXLSFNAMELEN];

        if (qp->saccts[i]->options & SACCT_GROUP)
            sprintf(buf, "%s/", qp->saccts[i]->name);
        else
            sprintf(buf, "%s", qp->saccts[i]->name);

        printf("%-9s   %6d    %8.3f",
               buf,
               qp->saccts[i]->shares,
               qp->saccts[i]->dshares);
        printf("   %6d   %6d   %6d\n",
               qp->saccts[i]->numPEND,
               qp->saccts[i]->numRUN,
               qp->saccts[i]->numRAN);
    }
}

/* print_slot_owned()
 */
static void
print_slot_owned(struct queueInfoEnt *qp)
{
    int i;
    uint32_t numRUN;

    numRUN = 0;
    for (i = 0; i < qp->numAccts; i++) {
        if (qp->saccts[i]->options & SACCT_USER)
            numRUN = numRUN + qp->saccts[i]->numRUN;
    }

    if (qp->loan_duration > 0) {
        printf("\
\nTOTAL_SLOTS: %u FREE_SLOTS: %u LOAN_DURATION %d\n", qp->num_owned_slots,
               qp->num_owned_slots - numRUN, qp->loan_duration);
    } else {
        printf("\
\nTOTAL_SLOTS: %u FREE_SLOTS: %u\n", qp->num_owned_slots,
               qp->num_owned_slots - numRUN);
    }

    printf("\
%9s   %6s   %6s   %6s   %8s\n",
           "USER/GROUP", "OWNED", "PEND", "RUN", "BORROWED");
    for (i = 0; i < qp->numAccts; i++) {
        char buf[MAXLSFNAMELEN];

        if (qp->saccts[i]->options & SACCT_GROUP)
            sprintf(buf, "%s/", qp->saccts[i]->name);
        else
            sprintf(buf, "%s", qp->saccts[i]->name);

        printf("%-9s   %6d",
               buf,
               qp->saccts[i]->shares);
        printf("   %6d   %6d   %8d\n",
               qp->saccts[i]->numPEND,
               qp->saccts[i]->numRUN,
               qp->saccts[i]->numBORROWED);
    }
}
