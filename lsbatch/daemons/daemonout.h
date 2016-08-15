/*
 * Copyright (C) 2015 David Bigagli
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

#ifndef MBDOUT_H
#define MBDOUT_H

#include "../lsbatch.h"
#include "../../lsf/lib/lproto.h"
#include "../../lsf/intlib/intlibout.h"
#include "../../lsf/lib/lib.table.h"
#include "../../lsf/lib/lib.queue.h"

#define BATCH_MASTER_PORT   40000
#define ALL_HOSTS      "all"

#define  PUT_LOW(word, s)  (word = (s | (word & ~0x0000ffff)))
#define  PUT_HIGH(word, s) (word = ((s << 16) | (word & 0x0000ffff)))
#define  GET_LOW(s, word)  (s = word & 0x0000ffff)
#define  GET_HIGH(s, word) (s = (word >> 16) & 0x0000ffff)

#define RSCHED_LISTSEARCH_BY_EXECJID       0
#define RSCHED_LISTSEARCH_BY_EXECLUSNAME   1

typedef enum {
    BATCH_JOB_SUB       = 1,
    BATCH_JOB_INFO      = 2,
    BATCH_JOB_PEEK      = 3,
    BATCH_JOB_SIG       = 4,
    BATCH_HOST_INFO     = 5,
    BATCH_QUE_INFO      = 6,
    BATCH_GRP_INFO      = 7,
    BATCH_QUE_CTRL      = 8,
    BATCH_RECONFIG      = 9,
    BATCH_HOST_CTRL     = 10,
    BATCH_JOB_SWITCH    = 11,
    BATCH_JOB_MOVE      = 12,
    BATCH_JOB_MIG       = 13,
    BATCH_STATUS_JOB    = 15,
    BATCH_SLAVE_RESTART = 16,
    BATCH_USER_INFO     = 17,
    BATCH_PARAM_INFO    = 20,
    BATCH_JOB_MODIFY    = 22,
    BATCH_JOB_EXECED    = 25,
    BATCH_JOB_MSG       = 27,
    BATCH_STATUS_MSG_ACK = 28,
    BATCH_DEBUG         = 29,
    BATCH_RESOURCE_INFO = 30,
    BATCH_RUSAGE_JOB    = 32,
    BATCH_JOB_FORCE      = 37,
    BATCH_UNUSED_38      = 38,
    BATCH_UNUSED_39      = 39,
    BATCH_STATUS_CHUNK   = 40,
    BATCH_JOBMSG_INFO,
    BATCH_RESLIMIT_INFO,
    BATCH_SET_JOB_ATTR   = 90,
    BATCH_JOBDEP_INFO,
    BATCH_JGRP_ADD,
    BATCH_JGRP_DEL,
    BATCH_JGRP_INFO,
    BATCH_JGRP_MOD,
    READY_FOR_OP         = 1023,
    PREPARE_FOR_OP       = 1024
} mbdReqType;

#define SUB_RLIMIT_UNIT_IS_KB 0x80000000

struct submitReq {
    int     options;
    int     options2;
    char    *jobName;
    char    *queue;
    int     numAskedHosts;
    char    **askedHosts;
    char    *resReq;
    int     rLimits[LSF_RLIM_NLIMITS];
    char    *hostSpec;
    int     numProcessors;
    char    *dependCond;
    time_t  beginTime;
    time_t  termTime;
    int     sigValue;
    char    *subHomeDir;
    char    *inFile;
    char    *outFile;
    char    *errFile;
    char    *command;
    char    *inFileSpool;
    char    *commandSpool;
    time_t  chkpntPeriod;
    char    *chkpntDir;
    int     restartPid;
    int     nxf;
    struct  xFile *xf;
    char    *jobFile;
    char    *fromHost;
    time_t  submitTime;
    int     umask;
    char    *cwd;
    char    *preExecCmd;
    char    *mailUser;
    char    *projectName;
    int     niosPort;
    int     maxNumProcessors;
    char    *loginShell;
    char    *schedHostType;
    char    *userGroup;
    int     userPriority;
    char    *job_group;
    char    *job_description;
};

#define SHELLLINE "#! /bin/sh\n\n"
#define CMDSTART "# LSBATCH: User input\n"
#define CMDEND "# LSBATCH: End user input\n"
#define ENVSSTART "# LSBATCH: Environments\n"
#define LSBNUMENV "#LSB_NUM_ENV="
#define EDATASTART "# LSBATCH: edata\n"
#define AUXAUTHSTART "# LSBATCH: aux_auth_data\n"
#define EXITCMD "exit `expr $? \"|\" $ExitStat`\n"
#define WAITCLEANCMD "\nExitStat=$?\nwait\n# LSBATCH: End user input\ntrue\n"
#define TAILCMD "'; export "
#define TRAPSIGCMD "$LSB_TRAPSIGS\n$LSB_RCP1\n$LSB_RCP2\n$LSB_RCP3\n"
#define JOB_STARTER_KEYWORD "%USRCMD"
#define SCRIPT_WORD "_USER_\\SCRIPT_"
#define SCRIPT_WORD_END "_USER_SCRIPT_"

struct submitMbdReply {
    LS_LONG_INT jobId;
    char    *queue;
    int     badReqIndx;
    char    *badJobName;
};

struct jgrpReq{
    char *groupSpec;
    char *destSpec; /* used only for bgmodify */
    time_t submitTime;
    int  options;
};

struct modifyReq {
    LS_LONG_INT jobId;
    char * jobIdStr;
    int    delOptions;
    int    delOptions2;
    struct submitReq submitReq;
};

struct jobInfoReq {
    int    options;
    char   *userName;
    LS_LONG_INT jobId;
    char   *jobName;
    char   *queue;
    char   *host;
};

struct jobInfoReply {
    LS_LONG_INT jobId;
    int       status;
    int       *reasonTb;
    int       numReasons;
    int       reasons;
    int       subreasons;
    time_t    startTime;
    time_t    predictedStartTime;
    time_t    endTime;
    float     cpuTime;
    int       numToHosts;
    char      **toHosts;
    int       nIdx;
    float     *loadSched;
    float     *loadStop;
    int       userId;
    char      *userName;
    int       execUid;
    int       exitStatus;
    char      *execHome;
    char      *execCwd;
    char      *execUsername;
    struct    submitReq *jobBill;
    time_t    reserveTime;
    int       jobPid;
    time_t    jRusageUpdateTime;
    struct    jRusage runRusage;
    int       jType;
    char      *parentGroup;
    char      *jName;
    int       counter[NUM_JGRP_COUNTERS];
    u_short   port;
    int       jobPriority;
};

struct infoReq {
    int options;
    int numNames;
    char **names;
    char  *resReq;
};


struct userInfoReply {
    int   badUser;
    int   numUsers;
    struct  userInfoEnt *users;
};

struct queueInfoReply {
    int    badQueue;
    int    numQueues;
    int    nIdx;
    struct queueInfoEnt *queues;
};

struct hostDataReply {
    int  badHost;
    int  numHosts;
    int  nIdx;
    int  flag;
#define LOAD_REPLY_SHARED_RESOURCE 0x1
    struct hostInfoEnt  *hosts;
};

struct groupInfoReply {
    int  numGroups;
    struct groupInfoEnt *groups;
};

struct jobPeekReq {
    LS_LONG_INT   jobId;
};

struct jobPeekReply {
    char *outFile;
    char *pSpoolDir;
};

struct signalReq {
    int    sigValue;
    LS_LONG_INT jobId;
    time_t chkPeriod;
    int    actFlags;
};


struct jobMoveReq {
    int         opCode;
    LS_LONG_INT  jobId;
    int         position;
};

struct jobSwitchReq {
    LS_LONG_INT jobId;
    char   queue[MAX_LSB_NAME_LEN];
};

struct controlReq {
    int         opCode;
    char        *name;
    char        *message;
};


struct migReq {
    LS_LONG_INT jobId;
    int options;
    int numAskedHosts;
    char **askedHosts;
};

struct resLimitReply {
    int    numLimits;
    struct resLimit *limits;
    int    numUsage;
    struct resLimitUsage *usage;
};

struct resLimitUsage {
    char    *limitName;
    char    *project;
    char    *user;
    char    *queue;
    char    *host;
    float   slots;
    float   maxSlots;
    float   jobs;
    float   maxJobs;
};

typedef enum {
    MBD_NEW_JOB_KEEP_CHAN = 0,
    MBD_NEW_JOB     = 1,
    MBD_SIG_JOB     = 2,
    MBD_SWIT_JOB    = 3,
    MBD_PROBE       = 4,
    MBD_REBOOT      = 5,
    MBD_SHUTDOWN    = 6,
    CMD_SBD_DEBUG   = 7,
    UNUSED_8        = 8,
    MBD_MODIFY_JOB  = 9,
    SBD_JOB_SETUP   = 100,
    SBD_SYSLOG      = 101,
    CMD_SBD_REBOOT   = 300,
    CMD_SBD_SHUTDOWN = 301
    /* In lsbatch.h see  sbd_lib_opcode_t for library
     * to SBD protocol code.
     */
} sbdReqType;


struct lenDataList {
    int   numJf;
    struct lenData *jf;
};

extern void initTab(struct hTab *);
extern hEnt *addMemb(struct hTab *, LS_LONG_INT);
extern char remvMemb(struct hTab *, LS_LONG_INT);
extern hEnt *chekMemb(struct hTab *, LS_LONG_INT);
extern hEnt *addMembStr(struct hTab *, char *);
extern char remvMembStr(struct hTab *, char *);
extern hEnt *chekMembStr(struct hTab *, char *);
extern void convertRLimit(int *, int);
extern int limitIsOk_(int *);
extern int handShake_(int ,char, int);

#define CALL_SERVER_NO_WAIT_REPLY 0x1
#define CALL_SERVER_USE_SOCKET    0x2
#define CALL_SERVER_NO_HANDSHAKE  0x4
#define CALL_SERVER_ENQUEUE_ONLY  0x8
extern int call_server(char *,
                       ushort,
                       char *,
                       int,
                       char **,
                       struct LSFHeader *,
                       int,
                       int,
                       int *,
                       int (*)(),
                       int *,
                       int);
/* To do: find out why this header file is included
 * in here.
 */
#include "../lib/lsb.xdr.h"
extern int sndJobFile_(int, struct lenData *);
extern struct group *mygetgrnam(const char *);
extern void freeUnixGrp(struct group *);
extern struct group *copyUnixGrp(struct group *);
extern void freeGroupInfoReply(struct groupInfoReply *);
extern void appendEData(struct lenData *, struct lenData *);

#endif
