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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA
 *
 */

#include "mbd.h"
#include "../../lsf/lib/lsi18n.h"
#include "fairshare.h"
#include "preempt.h"

extern void resetStaticSchedVariables(void);
extern void cleanSbdNode(struct jData *);
extern void uDataPtrTbInitialize(void);

extern struct mbd_func_type mbd_func;

extern void freeUConf(struct userConf *, int);
extern void freeHConf(struct hostConf *, int);
extern void freeQConf(struct queueConf *, int);
extern void freeWorkUser(int);
extern void freeWorkHost(int);
extern void freeWorkQueue(int);
extern void cleanCandHosts (struct jData *);
static uint32_t getQueueSlots(struct qData *);

#define setString(string, specString) {         \
        FREEUP(string);                         \
        if(specString != NULL)                  \
            string = safeSave(specString);}

#define setValue(value, specValue) {                                    \
        if(specValue != INFINIT_INT && specValue != INFINIT_FLOAT)      \
            value = specValue;                                          \
    }

static char defUser = FALSE;
static int numofclusters = 0;
static struct clusterInfo *clusterInfo = NULL;
struct tclLsInfo *tclLsInfo = NULL;
static struct clusterConf clusterConf;
static struct lsConf *paramFileConf = NULL;
static struct lsConf *userFileConf = NULL;
static struct lsConf *hostFileConf = NULL;
static struct lsConf *queueFileConf = NULL;
static struct lsConf *resFileConf = NULL;
struct userConf *userConf = NULL;
static struct hostConf *hostConf = NULL;
static struct queueConf *queueConf = NULL;
static struct paramConf *paramConf = NULL;
struct resLimitConf *limitConf = NULL;
static struct gData *tempUGData[MAX_GROUPS];
static struct gData *tempHGData[MAX_GROUPS];
static int nTempUGroups;
static int nTempHGroups;

static char batchName[MAX_LSB_NAME_LEN] = "root";
#define PARAM_FILE    0x01
#define USER_FILE     0x02
#define HOST_FILE     0x04
#define QUEUE_FILE    0x08
#define RESOURCE_FILE 0x16

#define HDATA         1
#define UDATA         2

extern int rusageUpdateRate;
extern int rusageUpdatePercent;

extern void initTab(struct hTab *);
static void readParamConf(int);
static int  readHostConf(int);
static void readUserConf(int);
static void readQueueConf(int);
static void readResConf(int);

static int isHostAlias (char *);
static int searchAll(char *);
static void initThresholds(float *, float *);
static void parseGroups(int, struct gData **, char *, char *);
static void addMember(struct gData *, char *, int, char *,
                      struct gData *[], int *);
static struct gData *addGroup (struct gData **, char *, int *);
static struct  qData *initQData (void);
static int isInGrp(char *, struct gData *);
static struct gData *addUnixGrp(struct group *, char *, char *,
                                struct gData *[], int*);
static void parseAUids(struct qData *, char *);

static void getClusterData(void);
static void setManagers(struct clusterInfo);
static void setAllusers(struct qData *, struct admins *);

static void createTmpGData(struct groupInfoEnt *, int, int,
                           struct gData *[], int *);
static void addHostData(int, struct hostInfoEnt *);
static void setParams(struct paramConf *);
static void addUData(struct userConf *);
static void setDefaultParams(void);
static void addQData(struct queueConf *, int);
static int updCondData(struct lsConf *, int);
static struct condData *initConfData(void);
static void createCondNodes(int, char **, char *, int);
static struct lsConf * getFileConf(char *, int);
static void copyQData(struct qData *, struct qData *);
static void copyGroups(int);
static void addDefaultHost (void);
static void removeFlags(struct hTab *, int, int);
static int needPollQHost(struct qData *, struct qData *);
static void updQueueList(void);
static void updUserList(int);

static void addUGDataValues(struct uData *, struct gData *);
static void addUGDataValues1(struct uData *, struct uData *);
static int parseQHosts(struct qData *, char *);
static void fillClusterConf(struct clusterConf *);
static void fillSharedConf(struct sharedConf *);
static void createDefQueue(void);
static void freeGrp(struct gData *);
static int validHostSpec(char *);
static void getMaxCpufactor(void);
static int parseFirstHostErr(int,
                             char *,
                             char *,
                             struct qData *,
                             struct askedHost *,
                             int);
static struct hData *mkLostAndFoundHost(void);
static int init_fairshare_scheduler(void);
static struct fair_sched *load_fair_plugin(struct qData *,
                                           const char *);
static int parse_preemption(struct qData *);
static int sort_queues(void);
static int queue_cmp(const void *, const void *);
static void check_same_priority_queues(void);
static int init_preemption_scheduler(void);
static int load_preempt_plugin(struct qData *);
static int init_ownership_scheduler(void);
static bool_t is_fairplugin_ok(void);
static bool_t is_preemptplugin_ok(void);
static bool_t is_ownplugin_ok(void);
static int parse_host_shares(const char *, struct qData *qp);
static void make_hsacct(struct hData *, char *, int);
static bool_t check_ownership(struct qData *);
static bool_t has_slot_preemption_;
static void add_host_dedicated_res(struct hData *, struct hostInfo *);

int
minit(int mbdInitFlags)
{
    struct hData *hPtr;
    int list;
    int i;
    char *master;

    ls_syslog(LOG_DEBUG, "%s: Entering this routine...", __func__);


    Signal_(SIGTERM, (SIGFUNCTYPE) terminate_handler);
    Signal_(SIGINT,  (SIGFUNCTYPE) terminate_handler);
    Signal_(SIGCHLD, (SIGFUNCTYPE) mbd_child_handler);
    Signal_(SIGALRM, SIG_IGN);
    Signal_(SIGHUP,  SIG_IGN);
    Signal_(SIGPIPE, SIG_IGN);

    if (!(debug || lsb_CheckMode)) {
        Signal_(SIGTTOU, SIG_IGN);
        Signal_(SIGTTIN, SIG_IGN);
        Signal_(SIGTSTP, SIG_IGN);
    }

    clientList = (struct clientNode *)listCreate("client list");

    for (list = 0; list < ALLJLIST; list++) {
        char name[128];
        sprintf(name, "Job Data List <%d>", list);
        jDataList[list] = (struct jData *)listCreate(name);
    }

    initTab(&jobIdHT);
    initTab(&jgrpIdHT);

    qDataList = (struct qData *)listCreate("Queue List");

    /* who am I...
     */
    batchId = getuid();

    TIMEIT(0, masterHost = ls_getmyhostname(), "minit_ls_getmyhostname");
    if (masterHost == NULL) {

        ls_syslog(LOG_ERR, "\
%s: Ohmygosh failed to get my own name, %M", __func__);
        if (! lsb_CheckMode)
            mbdDie(MASTER_RESIGN);
        else {
            lsb_CheckError = FATAL_ERR;
            return 0;
        }
    }

    master = ls_getmastername();
    if (master == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ls_getmastername(): Failed to get master name, %M", __func__);
        if (!lsb_CheckMode) {
            mbdDie(MASTER_RESIGN);
        } else {
            lsb_CheckError = FATAL_ERR;
            return 0;
        }
    }

    if (strcmp(master, masterHost) != 0) {
        ls_syslog(LOG_ERR, "\
%s: Current host %s is not master %s", __func__, masterHost, master);
        if (!lsb_CheckMode) {
            mbdDie(MASTER_RESIGN);
        } else {
            lsb_CheckError = FATAL_ERR;
            return 0;
        }
    }

    if (lsb_CheckMode)
        ls_syslog(LOG_INFO, "\
%s: Calling LIM to get cluster name ...", __func__);

    getClusterData();

    TIMEIT(0, allLsInfo = ls_info(), "minit_ls_info");
    if (allLsInfo == NULL) {
        ls_syslog(LOG_ERR, "\
%s: Ohmygosh ls_info() failed just now... %M", __func__);
        if (lsb_CheckMode) {
            lsb_CheckError = FATAL_ERR;
            return 0;
        }
        mbdDie(MASTER_FATAL);
    }

    for (i = allLsInfo->nModels; i < MAXMODELS; i++)
        allLsInfo->cpuFactor[i] = 1.0;

    TIMEIT(0, getLsfHostInfo(true), "minit_getLsfHostInfo");
    if (LIMhosts == NULL) {
        ls_syslog(LOG_ERR, "\
%s: Ohmygosh ls_gethostinfo() failed just now... %M", __func__);
        if (lsb_CheckMode) {
            lsb_CheckError = FATAL_ERR;
            return 0;
        } else
            mbdDie(MASTER_FATAL);
    }

    initParse(allLsInfo);
    tclLsInfo = getTclLsInfo();
    initTcl(tclLsInfo);

    TIMEIT(0, readParamConf(mbdInitFlags), "minit_readParamConf");
    TIMEIT(0, readHostConf(mbdInitFlags), "minit_readHostConf");
    getLsbResourceInfo();

    /* Call LIM and update the load information
     * about the batch hosts we just built.
     */
    getLsbHostLoad();
    updHostList();
    copyGroups(true);

    if ((hPtr = getHostData(masterHost)) == NULL) {
        ls_syslog(LOG_ERR, "\
%s: Master host %s is not defined in the lsb.hosts file",
                  __func__, masterHost);
        mbdDie(MASTER_FATAL);
    }
    if (defaultHostSpec != NULL && !validHostSpec(defaultHostSpec)) {
        ls_syslog(LOG_ERR, "\
%s: Invalid system defined DEFAULT_HOST_SPEC %s; ignored",
                  __func__, defaultHostSpec);
        FREEUP(defaultHostSpec);
        FREEUP(paramConf->param->defaultHostSpec);
        lsb_CheckError = WARNING_ERR;
    }

    TIMEIT(0, readUserConf(mbdInitFlags), "minit_readUserConf");
    TIMEIT(0, readQueueConf(mbdInitFlags), "minit_readQueueConf");
    TIMEIT(0, readResConf(mbdInitFlags), "minit_readResConf");
    copyGroups(FALSE);

    updUserList(mbdInitFlags);
    updQueueList();

    if (chanInit_() < 0) {
        ls_syslog(LOG_ERR, "\
%s: Ohmygosh chanInit() failed... %M", __func__);
        mbdDie(MASTER_FATAL);
    }

    treeInit();

    if (get_ports() < 0) {
        if (!lsb_CheckMode)
            mbdDie(MASTER_FATAL);
        else
            lsb_CheckError = FATAL_ERR;
    }

    if (getenv("RECONFIG_CHECK") == NULL) {
        batchSock = init_ServSock(mbd_port);
        if (batchSock < 0) {
            ls_syslog(LOG_ERR, "\
%s: Cannot get batch server socket... %M", __func__);
            if (! lsb_CheckMode)
                mbdDie(MASTER_FATAL);
            else
                lsb_CheckError = FATAL_ERR;
        }
    }

    if (!(debug || lsb_CheckMode)) {
        nice(NICE_LEAST);
        nice(NICE_MIDDLE);
        nice(0);
    }

    if (!(debug || lsb_CheckMode)) {

        if (chdir(LSTMPDIR) < 0) {
            ls_syslog(LOG_ERR, "\
%s: Ohmygosh chdir(%s) failed %m", __func__, LSTMPDIR);
            if (!lsb_CheckMode)
                mbdDie(MASTER_FATAL);
            else
                lsb_CheckError = FATAL_ERR;
        }
    }

    /* Initialize the plugin before loading
     * the lsb.events.
     */
    init_fairshare_scheduler();
    init_preemption_scheduler();
    init_ownership_scheduler();

    if (!lsb_CheckMode) {
        TIMEIT(0, init_log(), "init_log()");
    }

    getMaxCpufactor();

    return 0;
}

static int
readHostConf(int mbdInitFlags)
{
    char file[PATH_MAX];

    if (mbdInitFlags == FIRST_START
        || mbdInitFlags == RECONFIG_CONF)  {

        sprintf(file, "%s/lsb.hosts", daemonParams[LSB_CONFDIR].paramValue);

        hostFileConf = getFileConf(file, HOST_FILE);
        if (hostFileConf == NULL && lserrno == LSE_NO_FILE) {
            ls_syslog(LOG_ERR, "\
%s: lsb.hosts not found %M, all hosts known by LSF will be used",
                      __FUNCTION__);
            addDefaultHost();
            return 0;
        }
    } else {
        if (hostFileConf == NULL) {
            ls_syslog(LOG_ERR, "\
%s: lsb.hosts not found, all hosts known by LSF will be used",
                      __FUNCTION__);
            addDefaultHost();
            return 0;
        }
    }

    fillClusterConf(&clusterConf);
    /* Invoke the library lsb.conf.c to read the
     * the lsb.hosts file.
     */
    if ((hostConf = lsb_readhost(hostFileConf,
                                 allLsInfo,
                                 CONF_CHECK,
                                 &clusterConf)) == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL_MM, __FUNCTION__, "lsb_readhost");
        if (lsb_CheckMode) {
            lsb_CheckError = FATAL_ERR;
            return -1;
        } else {
            mbdDie(MASTER_FATAL);
        }
    }
    addHostData(hostConf->numHosts, hostConf->hosts);
    createTmpGData(hostConf->hgroups,
                   hostConf->numHgroups,
                   HOST_GRP,
                   tempHGData,
                   &nTempHGroups);
    if (lsberrno == LSBE_CONF_WARNING)
        lsb_CheckError = WARNING_ERR;

    /* make it...
     */
    mkLostAndFoundHost();

    return 0;
}

struct hData *
initHData(struct hData *hData)
{
    int   i;

    if (hData == NULL) {
        /* Allocate memory only when creating the host data.
         * When hData in input is not NULL than we are copying
         * the host data from a temporary storage.
         */
        hData = my_calloc(1, sizeof(struct hData), "initHData");
        hData->dres_tab = calloc(1, sizeof(hTab));
        h_initTab_(hData->dres_tab, 11);
    }

    hData->host = NULL;
    hData->hostEnt.h_name = NULL;
    hData->hostEnt.h_aliases = NULL;
    hData->pollTime = 0;
    hData->acceptTime = 0;
    hData->numDispJobs = 0;
    hData->cpuFactor = 1.0;
    hData->numCPUs  = 1;
    hData->hostType = NULL;
    hData->sbdFail = 0;
    hData->hStatus  = HOST_STAT_OK;
    hData->uJobLimit = INFINIT_INT;
    hData->uAcct     = NULL;
    hData->maxJobs   = INFINIT_INT;
    hData->numJobs   = 0;
    hData->numRUN    = 0;
    hData->numSSUSP  = 0;
    hData->numUSUSP  = 0;
    hData->numRESERVE  = 0;
    hData->mig = INFINIT_INT;
    hData->chkSig = SIG_CHKPNT;
    hData->hostModel = NULL;
    hData->maxMem    = INFINIT_INT;
    hData->maxSwap    = INFINIT_INT;
    hData->maxTmp    = INFINIT_INT;
    hData->nDisks    = INFINIT_INT;
    hData->limStatus = NULL;
    hData->resBitMaps  = NULL;
    hData->lsfLoad  = NULL;
    hData->lsbLoad  = NULL;
    hData->busyStop  = NULL;
    hData->busySched  = NULL;
    hData->loadSched = NULL;
    hData->loadStop =  NULL;
    hData->windows = NULL;
    hData->windEdge = 0;
    for (i = 0; i < 8; i++)
        hData->week[i] = NULL;
    for (i = 0; i < 3; i++)
        hData->msgq[i] = NULL;
    hData->flags = 0;
    hData->numInstances = 0;
    hData->instances = NULL;
    hData->pxySJL = NULL;
    hData->pxyRsvJL = NULL;
    hData->leftRusageMem = INFINIT_LOAD;
    hData->affinity = FALSE;

    return hData;
}

static void
initThresholds(float loadSched[], float loadStop[])
{
    int   i;

    for (i = 0; i < allLsInfo->numIndx; i++) {
        if (allLsInfo->resTable[i].orderType == INCR) {
            loadSched[i] = INFINIT_LOAD;
            loadStop[i]  = INFINIT_LOAD;
        } else {
            loadSched[i] = -INFINIT_LOAD;
            loadStop[i]  = -INFINIT_LOAD;
        }
    }
}

static void
readUserConf(int mbdInitFlags)
{
    char file[PATH_MAX];
    struct sharedConf sharedConf;
    hEnt *ent;

    memset(&sharedConf, 0, sizeof(struct sharedConf));

    if (mbdInitFlags == FIRST_START
        || mbdInitFlags == RECONFIG_CONF) {

        sprintf(file, "%s/lsb.users", daemonParams[LSB_CONFDIR].paramValue);
        userFileConf = getFileConf(file, USER_FILE);
        if (userFileConf == NULL && lserrno == LSE_NO_FILE) {

            ls_syslog(LOG_ERR, "\
%s: lsb.users not found %M, default user will be used", __FUNCTION__);
            userConf = my_calloc(1,
                                 sizeof(struct userConf),
                                 __FUNCTION__);
            goto defaultUser;
        }

    } else if (userFileConf == NULL) {

        ls_syslog(LOG_ERR, "\
%s: lsb.users not found %M, default user will be used",
                  __FUNCTION__);

        userConf = my_calloc(1,
                             sizeof(struct userConf),
                             __FUNCTION__);
        goto defaultUser;
    }

    fillClusterConf (&clusterConf);
    fillSharedConf( &sharedConf);

    if ((userConf = lsb_readuser_ex(userFileConf,
                                    CONF_CHECK,
                                    &clusterConf,
                                    &sharedConf)) == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, __FUNCTION__, "lsb_readuser_ex");
        if (lsb_CheckMode) {
            lsb_CheckError = FATAL_ERR;
            FREEUP(sharedConf.clusterName);
            return;
        } else {
            mbdDie(MASTER_FATAL);
        }
    }

    createTmpGData(userConf->ugroups,
                   userConf->numUgroups,
                   USER_GRP,
                   tempUGData,
                   &nTempUGroups);

    if (lsberrno == LSBE_CONF_WARNING)
        lsb_CheckError = WARNING_ERR;

    addUData(userConf);

defaultUser:

    if (! defUser) {
        ent = h_getEnt_(&uDataList, "default");
        if (ent == NULL) {
            addUserData ("default",
                         INFINIT_INT,
                         INFINIT_FLOAT,
                         "readUserConf",
                         FALSE,
                         TRUE);
        }
        defUser = TRUE;
    }

    FREEUP(sharedConf.clusterName);
}

/* readQueueConf()
 * Copy the queue configuration as returned from
 * the library that processed the batch configuration.
 */
static void
readQueueConf(int mbdInitFlags)
{
    char file[PATH_MAX];
    char *cp;
    char *word;
    struct qData *qp;
    int numDefQue;
    int numQueues;
    struct sharedConf sharedConf;

    if (mbdInitFlags == FIRST_START
        || mbdInitFlags == RECONFIG_CONF) {

        sprintf(file, "%s/lsb.queues", daemonParams[LSB_CONFDIR].paramValue);
        queueFileConf = getFileConf(file, QUEUE_FILE);
        if (queueFileConf == NULL && lserrno == LSE_NO_FILE) {
            ls_syslog(LOG_ERR, "\
%s: lsb.queues not found %M, using default queue",
                      __FUNCTION__);
            createDefQueue ();
            return;
        }
    } else if (queueFileConf == NULL) {
        ls_syslog(LOG_ERR, "\
%s: lsb.queues not be found, using default queue", __FUNCTION__);
        createDefQueue ();
        return;
    }

    fillSharedConf(&sharedConf);
    if ((queueConf = lsb_readqueue(queueFileConf,
                                   allLsInfo,
                                   CONF_CHECK| CONF_RETURN_HOSTSPEC,
                                   &sharedConf)) == NULL) {
        if (lsberrno == LSBE_CONF_FATAL) {
            ls_syslog(LOG_ERR, I18N_FUNC_FAIL, __FUNCTION__, "lsb_readqueue");
            if (lsb_CheckMode) {
                lsb_CheckError = FATAL_ERR;
                return;
            } else
                mbdDie(MASTER_FATAL);
        } else {
            lsb_CheckError = WARNING_ERR;
            ls_syslog(LOG_ERR, "\
%s: lsb_readqueue() failed %M, using default queue", __FUNCTION__);
            createDefQueue ();
            return;
        }
    }
    FREEUP(sharedConf.clusterName);
    if (lsberrno == LSBE_CONF_WARNING)
        lsb_CheckError = WARNING_ERR;

    addQData(queueConf, mbdInitFlags);

    numQueues = 0;
    for (qp = qDataList->forw; qp != qDataList; qp = qp->forw)
        if (qp->flags & QUEUE_UPDATE)
            numQueues++;

    if (!numQueues) {
        ls_syslog(LOG_WARNING, "\
%s: No valid queue defined", __FUNCTION__);
        lsb_CheckError = WARNING_ERR;
    }

    numDefQue = 0;
    if (numQueues && defaultQueues) {

        cp = defaultQueues;

        while ((word = getNextWord_(&cp))) {
            if ((qp = getQueueData(word)) == NULL
                || !(qp->flags & QUEUE_UPDATE)) {
                ls_syslog(LOG_WARNING, "\
%s: Invalid queue name specified by parameter DEFAULT_QUEUE; ignoring <%s>",
                          __FUNCTION__, word);
                lsb_CheckError = WARNING_ERR;
            } else {
                qp->qAttrib |= Q_ATTRIB_DEFAULT;
                numDefQue++;
            }
        }
    }

    if (numDefQue)
        return;

    ls_syslog(LOG_WARNING, "\
%s: lsb.queues: No valid default queue defined", __FUNCTION__);

    lsb_CheckError = WARNING_ERR;

    if ((qp = getQueueData("default")) != NULL) {
        ls_syslog(LOG_WARNING, "\
%s: Using the default queue <default> provided by lsb.queues ",
                  __FUNCTION__);

        qp->qAttrib |= Q_ATTRIB_DEFAULT;
        FREEUP (defaultQueues);
        defaultQueues = safeSave ("default");
        qp->flags |= QUEUE_UPDATE;
        return;
    }

    createDefQueue();

}
static void
createDefQueue(void)
{
    struct qData *qp;

    ls_syslog(LOG_WARNING, "\
%s: Using the default queue <default> provided by the batch system", __func__);

    FREEUP(defaultQueues);
    defaultQueues = safeSave("default");
    qp = initQData();
    qp->qAttrib |= Q_ATTRIB_DEFAULT;
    qp->description = safeSave("This is the default queue provided by the batch system.");
    qp->queue = safeSave("default");
    qp->hostSpec = safeSave (masterHost);
    qp->flags |=  QUEUE_UPDATE;
    qp->numProcessors = numofprocs;
    qp->askedOthPrio = 0;
    qp->acceptIntvl = accept_intvl;
    inQueueList(qp);
}

static void
readResConf(int mbdInitFlags)
{
    char file[PATH_MAX];
    struct stat st;

    if (mbdInitFlags == FIRST_START
        || mbdInitFlags == RECONFIG_CONF) {
        sprintf(file, "%s/lsb.resources", daemonParams[LSB_CONFDIR].paramValue);

        /* lsb.resources is optional */
        if (stat(file, &st) != 0) {
            limitConf = my_calloc(1, sizeof (struct resLimitConf), __FUNCTION__);
            return;
        }

        resFileConf = getFileConf(file, RESOURCE_FILE);
        if (resFileConf == NULL && lserrno == LSE_NO_FILE) {
            ls_syslog(LOG_ERR, "\
%s: lsb.resources not found %M, no resource limit", __FUNCTION__);
            limitConf = my_calloc(1, sizeof (struct resLimitConf), __FUNCTION__);
            return;
        }
    } else if (resFileConf == NULL) {
        ls_syslog(LOG_DEBUG, "\
%s: lsb.resources not found, no resource limit", __FUNCTION__);
        limitConf = my_calloc(1, sizeof (struct resLimitConf), __FUNCTION__);
        return;
    }

    if ((limitConf = lsb_readres(resFileConf)) == NULL) {
        if (lsberrno == LSBE_CONF_FATAL) {
            ls_syslog(LOG_ERR, I18N_FUNC_FAIL, __FUNCTION__, "lsb_readres");
            if (lsb_CheckMode) {
                lsb_CheckError = FATAL_ERR;
                return;
            }
            mbdDie(MASTER_FATAL);
        }

        if (lsberrno == LSBE_CONF_WARNING)
            lsb_CheckError = WARNING_ERR;
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL_MM, __FUNCTION__, "lsb_readres");
        limitConf = my_calloc(1, sizeof (struct resLimitConf), __FUNCTION__);
        return;
    }

    if (lsberrno == LSBE_CONF_WARNING)
        lsb_CheckError = WARNING_ERR;
}

/* addHost()
 * Configure the host to be part of MBD. Merge the
 * lsf base hostInfo information with the batch
 * configuration information, create a new host entry
 * in the host hash table.
 */
void
addHost(struct hostInfo *lsf,
        struct hData *thPtr)
{
    static char first = TRUE;
    hEnt *ent;
    struct hData *hPtr;
    int new;
    int i;
    char *word;

    if (first) {
        h_initTab_(&hostTab, 101);
        first = FALSE;
    }

    ent = h_addEnt_(&hostTab, thPtr->host, &new);
    if (!new) {
        assert(new);
        /* this cannot happen since lsb.conf.c
         * should have taken care of all checking.
         */
        ls_syslog(LOG_WARNING, "\
%s: Host %s appears to be multiply defined nth instance ignored",
                  __func__, thPtr->host);
        return;
    }
    /* This is a new host, hop la'
     * in the hostTab hash table.
     */
    hPtr = initHData(NULL);
    hPtr->flags |= HOST_NEEDPOLL;
    ent->hData = hPtr;

    /* In the case of lost and found host
     * we don't have lsf host data
     */
    if (lsf == NULL) {
        hPtr->host = safeSave(thPtr->host);
    }

    /* Let's inherit the lsf host base data.
     */
    if (lsf) {
        hPtr->host = safeSave (lsf->hostName);
        hPtr->cpuFactor = lsf->cpuFactor;
        hPtr->hostType = safeSave (lsf->hostType);

        /* Save the number of CPUs later on we will
         * overwrite it if MXJ is set for this host.
         */
        if (lsf->maxCpus > 0) {
            hPtr->numCPUs = lsf->maxCpus;
        } else {
            ls_syslog(LOG_DEBUG, "\
%s: numCPUs <%d> of host <%s> is not greater than 0; assuming as 1",
                      __func__, lsf->maxCpus, lsf->hostName);
            hPtr->numCPUs = 1;
        }

        hPtr->hostModel = safeSave (lsf->hostModel);
        hPtr->maxMem    = lsf->maxMem;

        if (lsf->maxMem != 0)
            hPtr->leftRusageMem = (float) lsf->maxMem;

        hPtr->maxSwap    = lsf->maxSwap;
        hPtr->maxTmp    = lsf->maxTmp;
        hPtr->nDisks    = lsf->nDisks;
        hPtr->resBitMaps  = getResMaps(lsf->nRes, lsf->resources);
        add_host_dedicated_res(hPtr, lsf);

        /* Fill up the hostent structure that is not used
         * anywhere anyway...
         */
        hPtr->hostEnt.h_name = putstr_(hPtr->host);
        hPtr->hostEnt.h_aliases = NULL;
    }

    hPtr->uJobLimit = thPtr->uJobLimit;
    hPtr->maxJobs   = thPtr->maxJobs;
    hPtr->affinity  = thPtr->affinity;
    if (thPtr->maxJobs == -1) {
        /* The MXJ was set as ! in lsb.hosts
         */
        hPtr->maxJobs =  hPtr->numCPUs;
        hPtr->flags   |= HOST_AUTOCONF_MXJ;
    }

    hPtr->mig = thPtr->mig;
    hPtr->chkSig = thPtr->chkSig;

    hPtr->loadSched = my_calloc(allLsInfo->numIndx,
                                sizeof(float), __func__);
    hPtr->loadStop = my_calloc(allLsInfo->numIndx,
                               sizeof(float), __func__);
    hPtr->lsfLoad = my_calloc(allLsInfo->numIndx,
                              sizeof(float), __func__);
    hPtr->lsbLoad = my_calloc(allLsInfo->numIndx,
                              sizeof(float), __func__);
    hPtr->busySched = my_calloc(GET_INTNUM(allLsInfo->numIndx),
                                sizeof (int), __func__);
    hPtr->busyStop = my_calloc(GET_INTNUM(allLsInfo->numIndx),
                               sizeof (int), __func__);

    initThresholds(hPtr->loadSched, hPtr->loadStop);

    for (i = 0; i < allLsInfo->numIndx; i++) {

        if (thPtr->loadSched != NULL
            && thPtr->loadSched[i] != INFINIT_FLOAT)
            hPtr->loadSched[i] = thPtr->loadSched[i];

        if (thPtr->loadStop != NULL
            && thPtr->loadStop[i] != INFINIT_FLOAT)
            hPtr->loadStop[i] = thPtr->loadStop[i];
    }

    hPtr->flags |= HOST_UPDATE;

    if (thPtr->windows) {
        char *sp = thPtr->windows;

        hPtr->windows = safeSave(thPtr->windows);
        *(hPtr->windows) = '\0';
        while ((word = getNextWord_(&sp)) != NULL) {
            char *save;
            save = safeSave(word);
            if (addWindow(word, hPtr->week, thPtr->host) <0) {
                ls_syslog(LOG_ERR, "\
%s: Bad time expression <%s>; ignored.", __func__, word);
                lsb_CheckError = WARNING_ERR;
                freeWeek (hPtr->week);
                free (save);
                continue;
            }
            hPtr->windEdge = now;
            if (*(hPtr->windows) != '\0')
                strcat (hPtr->windows, " ");
            strcat (hPtr->windows, save);
            free (save);
        }
    } else {
        hPtr->windEdge = 0 ;
        hPtr->hStatus = HOST_STAT_OK;
    }

    hPtr->limStatus = my_calloc
        ((1 + GET_INTNUM(allLsInfo->numIndx)), sizeof (int), __func__);

    if (new) {
        /* openlava
         * If user did configure MXJ in their lsb.hosts
         * we do set the number of CPUs of this batch
         * host to be MXJ. This is for parallel job scheduling
         * as we believe that users know how many job slots
         * they have. However if MXJ is not set then we
         * use the physical number of CPUs we got from
         * the lsf base.
         */
        if (hPtr->maxJobs > 0 && hPtr->maxJobs < INFINIT_INT )
            hPtr->numCPUs = hPtr->maxJobs;

        if (hPtr->numCPUs > 0)
            numofprocs += hPtr->numCPUs;
        else
            numofprocs++;
    }

    QUEUE_INIT(hPtr->msgq[MSG_STAT_QUEUED]);
    QUEUE_INIT(hPtr->msgq[MSG_STAT_SENT]);
    QUEUE_INIT(hPtr->msgq[MSG_STAT_RCVD]);

} /* addHost() */

void
freeHData(struct hData *hPtr)
{
    hEnt *ent;
    int  i;

    ent = h_getEnt_(&hostTab, hPtr->host);
    assert(ent);

    /* Remove from the hostlist
     */
    listRemoveEntry(hostList, (LIST_ENTRY_T *)hPtr);

    FREEUP(hPtr->host);
    FREEUP(hPtr->hostType);
    FREEUP(hPtr->hostModel);
    FREEUP(hPtr->hostEnt.h_name);

    if (hPtr->hostEnt.h_aliases) {
        for (i = 0; hPtr->hostEnt.h_aliases[i]; i++)
            free(hPtr->hostEnt.h_aliases[i]);
        free(hPtr->hostEnt.h_aliases);
    }
    FREEUP(hPtr->loadSched);
    FREEUP(hPtr->loadStop);
    FREEUP(hPtr->windows);
    freeWeek(hPtr->week);

    if (hPtr->uAcct) {
        if ( hPtr->uAcct->slotPtr)
            h_delTab_(hPtr->uAcct);
        else
            FREEUP(hPtr->uAcct);
    }

    FREEUP(hPtr->lsfLoad);
    FREEUP(hPtr->lsbLoad);
    FREEUP(hPtr->resBitMaps);

    QUEUE_DESTROY(hPtr->msgq[MSG_STAT_QUEUED]);
    QUEUE_DESTROY(hPtr->msgq[MSG_STAT_SENT]);
    QUEUE_DESTROY(hPtr->msgq[MSG_STAT_RCVD]);

    FREEUP(hPtr->instances);
    FREEUP(hPtr->limStatus);
    FREEUP(hPtr->busySched);
    FREEUP(hPtr->busyStop);

    h_rmEnt_(&hostTab, ent);
    FREEUP(hPtr);
}

static struct gData *
addGroup(struct gData **groups, char *gname, int *ngroups)
{

    groups[*ngroups] = my_calloc(1,
                                 sizeof (struct gData), "addGroup");
    groups[*ngroups]->group = safeSave(gname);
    h_initTab_(&groups[*ngroups]->memberTab, 0);
    groups[*ngroups]->numGroups = 0;
    (*ngroups)++;

    return (groups[*ngroups - 1]);

}

static void
addMember(struct gData *groupPtr,
          char *word,
          int grouptype,
          char *filename,
          struct gData *groups[],
          int *ngroups)
{
    static char fname[] = "addMember";
    struct passwd *pw = NULL;
    char isgrp = FALSE;
    struct gData *subgrpPtr = NULL;
    char name[MAXHOSTNAMELEN];
    struct hostent *hp;
    struct hEnt *ent;

    if (grouptype == USER_GRP) {
        subgrpPtr = getGrpData (tempUGData, word, nTempUGroups);
        if (!subgrpPtr)
            TIMEIT(0, pw = getpwnam(word), "addMemeber_getpwnam");

        isgrp = TRUE;
        if (pw != NULL) {
            strcpy(name, word);
            isgrp = FALSE;
        }
        else if (!subgrpPtr) {
            char *grpSl = NULL;
            struct group *unixGrp = NULL;
            int lastChar = strlen (word) - 1;
            if (lastChar > 0 && word[lastChar] == '/') {
                grpSl = safeSave (word);
                grpSl[lastChar] = '\0';
                TIMEIT(0, unixGrp = mygetgrnam(grpSl),"do_Users_getgrnam");
                FREEUP (grpSl);
            }
            else
                TIMEIT(0, unixGrp = mygetgrnam(word), "do_Users_getgrnam");

            if (unixGrp) {


                subgrpPtr = addUnixGrp (unixGrp, word, filename,
                                        groups, ngroups);
                if (!subgrpPtr) {
                    ls_syslog(LOG_WARNING, _i18n_msg_get(ls_catd , NL_SETN, 6149,
                                                         "%s: No valid users defined in Unix group <%s>; ignoring"), fname, word); /* catgets 6149 */
                    lsb_CheckError = WARNING_ERR;
                    return;
                }

            } else {

                strcpy(name, word);
                isgrp = FALSE;
            }
        }
    }

    if (grouptype == HOST_GRP) {
        if ((subgrpPtr = getGrpData (tempHGData, word, nTempHGroups)) != NULL) {
            isgrp = TRUE;
        } else {
            hp = Gethostbyname_(word);
            if (hp == NULL) {
                ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6151,
                                                 "%s: Bad host/group name <%s> in group <%s>; ignored"), fname, word, groupPtr->group); /* catgets 6151 */
                lsb_CheckError = WARNING_ERR;
                return;
            }
            strcpy(name, hp->h_name);
            if (findHost(name) == NULL && numofhosts() != 0) {
                ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6152,
                                                 "%s: Host <%s> is not used by the batch system; ignored"), fname, name); /* catgets 6152 */
                lsb_CheckError = WARNING_ERR;
                return;
            }
            isgrp = FALSE;
        }
    }

    if (isInGrp (word, groupPtr)) {
        if (isgrp)
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6153,
                                             "%s: Group <%s> is multiply defined in group <%s>; ignored"), fname, word, groupPtr->group); /* catgets 6153 */
        else
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6154,
                                             "%s: Member <%s> is multiply defined in group <%s>; ignored"), fname, word, groupPtr->group); /* catgets 6154 */
        lsb_CheckError = WARNING_ERR;
        return;
    }

    if (isgrp) {
        groupPtr->gPtr[groupPtr->numGroups] = subgrpPtr;
        groupPtr->numGroups++;
    } else {
        ent = h_addEnt_(&groupPtr->memberTab, name, NULL);
        ent->hData = strdup(name);
    }

    return;

}

static void
parseAUids (struct qData *qp, char *line)
{
    static char fname[] = "parseAUids";
    int i, numAds = 0, callFail = FALSE;
    char *sp, *word, *tempStr = NULL, *member;
    struct passwd *pw;
    struct  group *unixGrp;
    struct  gData *uGrp;
    struct admins admins;
    char forWhat[MAXLINELEN];

    sprintf (forWhat, "for queue <%s> administrator", qp->queue);

    sp = line;

    while ((word=getNextWord_(&sp)) != NULL)
        numAds++;
    if (numAds) {
        admins.adminIds = (int *) my_malloc ((numAds) * sizeof(int) , fname);
        admins.adminGIds = (int *) my_malloc ((numAds) * sizeof(int) , fname);
        admins.adminNames =
            (char **) my_malloc (numAds * sizeof(char *), fname);
        admins.nAdmins = 0;
    } else {

        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6155,
                                         "%s: No queue's administrators defined; ignored"), fname); /* catgets 6155 */
        lsb_CheckError = WARNING_ERR;
        return;
    }
    sp = line;
    while ((word=getNextWord_(&sp)) != NULL) {
        if (strcmp (word, "all") == 0) {
            setAllusers (qp, &admins);
            return;
        } else if ((pw = getpwnam(word))) {
            if (putInLists (word, &admins, &numAds, forWhat) < 0) {
                callFail = TRUE;
                break;
            }
        } else if ((uGrp = getGrpData (tempUGData, word, nTempUGroups)) &&
                   (tempStr = getGroupMembers (uGrp, TRUE))) {
            char *sp = tempStr;
            while ((member = getNextWord_(&sp)) != NULL) {
                if (strcmp (word, "all") == 0) {
                    setAllusers (qp, &admins);
                    return;
                }
                if (putInLists (member, &admins, &numAds, forWhat) <0) {
                    callFail = TRUE;
                    break;
                }
            }
            FREEUP (tempStr);
        } else if ((unixGrp = mygetgrnam(word)) != NULL) {
            i = 0;
            while (unixGrp->gr_mem[i] != NULL) {
                if (putInLists (unixGrp->gr_mem[i++], &admins, &numAds, forWhat) < 0) {
                    callFail = TRUE;
                    break;
                }
            }
        } else {

            if (putInLists (word, &admins, &numAds, forWhat) < 0) {
                callFail = TRUE;
                break;
            }
        }
        if (callFail == TRUE)
            break;
    }
    if (callFail == TRUE && ! lsb_CheckMode)
        mbdDie(MASTER_RESIGN);

    if (!admins.nAdmins) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6157,
                                         "%s: No valid queue's administrators defined in <%s> for queue <%s>; ignored"), fname, line, qp->queue); /* catgets 6157 */
        lsb_CheckError = WARNING_ERR;
        FREEUP(admins.adminIds);
        FREEUP(admins.adminGIds);
        FREEUP(admins.adminNames);
        return;
    }
    qp->nAdmins = admins.nAdmins;
    qp->adminIds = admins.adminIds;
    qp->admins = (char *) my_malloc (admins.nAdmins * MAX_LSB_NAME_LEN, fname);
    qp->admins[0] = '\0';
    for (i = 0; i < admins.nAdmins; i++) {
        strcat(qp->admins,  admins.adminNames[i]);
        strcat(qp->admins,  " ");
        FREEUP (admins.adminNames[i]);
    }
    FREEUP (admins.adminNames);
    FREEUP (admins.adminGIds);
}

static void
setAllusers (struct qData *qp, struct admins *admins)
{
    int i;

    qp->nAdmins = 1;
    qp->adminIds = (int *) my_malloc (sizeof (int), "setAllusers");
    qp->adminIds[0] = ALL_USERS_ADMINS;
    qp->admins = (char *) my_malloc (MAX_LSB_NAME_LEN, "setAllusers");
    qp->admins[0] = '\0';
    strcat (qp->admins, "all users");

    FREEUP (admins->adminIds);
    FREEUP (admins->adminGIds);
    for (i = 0; i < admins->nAdmins; i++)
        FREEUP (admins->adminNames[i]);
    FREEUP (admins->adminNames);

}
void
freeQData(struct qData *qp, int delete)
{
    FREEUP(qp->queue);
    FREEUP(qp->description);
    if (qp->uGPtr) {
        freeGrp (qp->uGPtr);
    }

    FREEUP(qp->loadSched);
    FREEUP(qp->loadStop);
    FREEUP(qp->windows);
    FREEUP(qp->windowsD);
    freeWeek(qp->weekR);
    freeWeek(qp->week);
    FREEUP(qp->hostSpec);
    FREEUP(qp->defaultHostSpec);
    if (qp->nAdmins > 0) {
        FREEUP(qp->adminIds);
        FREEUP(qp->admins);
    }
    FREEUP(qp->preCmd);
    FREEUP(qp->postCmd);
    FREEUP(qp->prepostUsername);
    if (qp->requeueEValues) {
        clean_requeue(qp);
    }
    FREEUP(qp->requeueEValues);
    FREEUP(qp->resReq);
    if (qp->resValPtr) {
        FREEUP(qp->resValPtr->rusage_bit_map);
        FREEUP(qp->resValPtr);
    }
    if (qp->uAcct)
        h_delTab_(qp->uAcct);
    if (qp->hAcct)
        h_delTab_(qp->hAcct);
    FREEUP(qp->resumeCond);
    lsbFreeResVal(&qp->resumeCondVal);
    FREEUP(qp->stopCond);
    FREEUP(qp->jobStarter);

    FREEUP(qp->suspendActCmd);
    FREEUP(qp->resumeActCmd);
    FREEUP(qp->terminateActCmd);

    FREEUP(qp->askedPtr);
    FREEUP(qp->hostList);

    if (delete == TRUE) {
        offList((struct listEntry *)qp);
        numofqueues--;
    }
    if (qp->reasonTb) {
        FREEUP(qp->reasonTb[0]);
        FREEUP(qp->reasonTb[1]);
        FREEUP(qp->reasonTb);
    }

    if (qp->hostInQueue) {
        setDestroy(qp->hostInQueue);
        qp->hostInQueue = NULL;
    }

    FREEUP(qp);
}

static void
parseGroups (int groupType, struct gData **group, char *line, char *filename)
{
    static char fname[] = "parseGroups";
    char *word, *groupName, *grpSl = NULL;
    int lastChar, i;
    struct group *unixGrp;
    struct gData *gp, *mygp = NULL;
    struct passwd *pw;
    struct hostent *hp;

    mygp = my_malloc(sizeof (struct gData), fname);
    *group = mygp;
    mygp->group = "";
    h_initTab_(&mygp->memberTab, 16);
    mygp->numGroups = 0;
    for (i = 0; i < MAX_GROUPS; i++)
        mygp->gPtr[i] = NULL;

    if (groupType == USER_GRP)
        groupName = "User/User";
    else
        groupName = "Host/Host";

    while ((word = getNextWord_(&line)) != NULL) {
        if (isInGrp (word, mygp)) {
            ls_syslog(LOG_ERR, "\
%s: Member <%s> is multiply defined %s; ignored", __func__, word, groupName);
            lsb_CheckError = WARNING_ERR;
            continue;
        }
        if (groupType == USER_GRP) {
            TIMEIT(0, pw = getpwnam(word), "parseGroups_getpwnam");
            if (pw != NULL) {
                h_addEnt_(&mygp->memberTab, word, NULL);
                continue;
            }
            FREEUP(grpSl);
            lastChar = strlen (word) - 1;
            if (lastChar > 0 && word[lastChar] == '/') {
                grpSl = safeSave (word);
                grpSl[lastChar] = '\0';
            }


            gp = getGrpData (tempUGData, word, nTempUGroups);
            if (gp != NULL) {
                mygp->gPtr[mygp->numGroups] = gp;
                mygp->numGroups++;
                continue;
            }

            if (grpSl) {
                TIMEIT(0, unixGrp = mygetgrnam(grpSl), "parseGroups_getgrnam");

                grpSl[lastChar] = '/';
            } else
                TIMEIT(0, unixGrp = mygetgrnam(word), "parseGroups_getgrnam");
            if (unixGrp != NULL) {

                gp = addUnixGrp(unixGrp, grpSl, filename, tempUGData, &nTempUGroups);
                if (gp == NULL) {
                    ls_syslog(LOG_WARNING, _i18n_msg_get(ls_catd , NL_SETN, 6165,
                                                         "%s/%s: No valid users in Unix group <%s>; ignoring"), filename, fname, word); /* catgets 6165 */
                    lsb_CheckError = WARNING_ERR;
                    continue;
                }
                mygp->gPtr[mygp->numGroups] = gp;
                mygp->numGroups++;
            } else {

                h_addEnt_(&mygp->memberTab, word, NULL);
                continue;
            }
            continue;
        }
        else {


            gp = getGrpData (tempHGData, word, nTempHGroups);
            if (gp != NULL) {
                mygp->gPtr[mygp->numGroups] = gp;
                mygp->numGroups++;
                continue;
            }

            hp = Gethostbyname_(word);
            if (hp != NULL) {
                word = hp->h_name;
                if (numofhosts()) {
                    if (!h_getEnt_(&hostTab, word)) {
                        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6170,
                                                         "%s/%s: Host <%s> is not used by the batch system; ignored"), filename, fname, word); /* catgets 6170 */
                        lsb_CheckError = WARNING_ERR;
                        continue;
                    }
                }
                if (isInGrp (word, mygp)) {
                    ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6171,
                                                     "%s/%s: Host name <%s> is multiply defined; ignored"), filename, fname, word); /* catgets 6171 */
                    lsb_CheckError = WARNING_ERR;
                    continue;
                }
                h_addEnt_(&mygp->memberTab, word, NULL);
                continue;
            } else {
                ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6172,
                                                 "%s/%s: Unknown host or host group <%s>; ignored"), filename, fname, word); /* catgets 6172 */
                lsb_CheckError = WARNING_ERR;
                continue;
            }
        }
    }
    FREEUP (grpSl);

    return;
}

/* readParamConf()
 */
static void
readParamConf(int mbdInitFlags)
{
    char file[PATH_MAX];

    setDefaultParams();

    if (mbdInitFlags == FIRST_START
        || mbdInitFlags == RECONFIG_CONF) {

        sprintf(file, "%s/lsb.params", daemonParams[LSB_CONFDIR].paramValue);

        paramFileConf = getFileConf(file, PARAM_FILE);
        if (paramFileConf == NULL
            && lserrno == LSE_NO_FILE) {
            ls_syslog(LOG_ERR, "\
%s: lsb.params can not be found %M, using default parameters",
                      __FUNCTION__);
            return;
        }
    }

    if ((paramConf = lsb_readparam(paramFileConf)) == NULL) {

        if (lsberrno == LSBE_CONF_FATAL) {
            ls_syslog(LOG_ERR, I18N_FUNC_FAIL, __FUNCTION__, "lsb_readparam");
            if (lsb_CheckMode) {
                lsb_CheckError = FATAL_ERR;
                return;
            }

            mbdDie(MASTER_FATAL);
        }

        if (lsberrno == LSBE_CONF_WARNING)
            lsb_CheckError = WARNING_ERR;
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL_MM, __FUNCTION__, "lsb_readparam");

        return;
    }

    if (lsberrno == LSBE_CONF_WARNING)
        lsb_CheckError = WARNING_ERR;

    setParams(paramConf);
}

int
my_atoi (char *arg, int upBound, int botBound)
{
    int num;

    if (!isint_ (arg)) {
        return (INFINIT_INT);
    }
    num = atoi (arg);
    if (num >= upBound || num <= botBound)
        return (INFINIT_INT);
    return (num);

}

/* initQData()
 */
static struct qData *
initQData (void)
{
    struct qData *qPtr;
    int i;

    qPtr = my_calloc(1, sizeof (struct qData), "initQData");

    qPtr->queue = NULL;
    qPtr->queueId = 0;
    qPtr->description = NULL;
    qPtr->numProcessors      = 0;
    qPtr->priority           = DEF_PRIO;
    qPtr->nice               = DEF_NICE;
    qPtr->uJobLimit          = INFINIT_INT;
    qPtr->uAcct              = NULL;
    qPtr->pJobLimit          = INFINIT_FLOAT;
    qPtr->hAcct              = NULL;
    qPtr->windows            = NULL;
    qPtr->windowsD           = NULL;
    qPtr->windEdge           = 0;
    qPtr->runWinCloseTime    = 0;
    qPtr->numHUnAvail = 0 ;
    for (i = 0; i < 8; i++) {
        qPtr->weekR[i] = NULL;
        qPtr->week[i] = NULL;
    }
    for (i = 0; i < LSF_RLIM_NLIMITS; i++) {
        qPtr->rLimits[i] = -1;
        qPtr->defLimits[i] = -1;
    }
    qPtr->hostSpec           = NULL;
    qPtr->defaultHostSpec    = NULL;
    qPtr->qAttrib            = 0;
    qPtr->qStatus = (QUEUE_STAT_OPEN | QUEUE_STAT_ACTIVE | QUEUE_STAT_RUN);
    qPtr->maxJobs           = INFINIT_INT;
    qPtr->numJobs           = 0;
    qPtr->numPEND            = 0;
    qPtr->numRUN             = 0;
    qPtr->numSSUSP           = 0;
    qPtr->numUSUSP           = 0;
    qPtr->mig                = INFINIT_INT;
    qPtr->schedDelay         = INFINIT_INT;
    qPtr->acceptIntvl        = INFINIT_INT;

    qPtr->loadSched = my_calloc(allLsInfo->numIndx,
                                sizeof(float), __func__);
    qPtr->loadStop = my_calloc(allLsInfo->numIndx,
                               sizeof(float), __func__);
    initThresholds (qPtr->loadSched, qPtr->loadStop);
    qPtr->procLimit = -1;
    qPtr->minProcLimit = -1;
    qPtr->defProcLimit = -1;
    qPtr->adminIds = NULL;
    qPtr->admins   = NULL;
    qPtr->nAdmins  = 0;
    qPtr->preCmd   = NULL;
    qPtr->postCmd = NULL;
    qPtr->prepostUsername = NULL;
    qPtr->requeueEValues = NULL;
    qPtr->requeEStruct = NULL;
    qPtr->hJobLimit = INFINIT_INT;
    qPtr->resValPtr = NULL;
    qPtr->resReq = NULL;
    qPtr->reasonTb = NULL;
    qPtr->numReasons = 0;
    qPtr->numUsable = 0;
    qPtr->numRESERVE = 0;
    qPtr->slotHoldTime = 0;
    qPtr->resumeCond = NULL;
    qPtr->resumeCondVal = NULL;
    qPtr->stopCond = NULL;
    qPtr->jobStarter = NULL;
    qPtr->suspendActCmd = NULL;
    qPtr->resumeActCmd = NULL;
    qPtr->terminateActCmd = NULL;
    for (i = 0; i < LSB_SIG_NUM; i++)
        qPtr->sigMap[i] = 0;

    qPtr->flags = 0;
    qPtr->uGPtr    = NULL;
    qPtr->hostList = NULL;
    qPtr->hostInQueue = NULL;
    qPtr->askedPtr = NULL;
    qPtr->numAskedPtr = 0;
    qPtr->askedOthPrio = -1;
    FREEUP(qPtr->reasonTb);
    qPtr->reasonTb = my_calloc(2, sizeof(int *), __func__);
    qPtr->reasonTb[0] = my_calloc(numofhosts() + 1,
                                  sizeof(int), __func__);
    qPtr->reasonTb[1] = my_calloc(numofhosts() + 1,
                                  sizeof(int), __func__);
    qPtr->schedStage = 0;
    qPtr->chkpntPeriod = -1;
    qPtr->chkpntDir = NULL;
    qPtr->fairshare = NULL;
    qPtr->ownership = NULL;

    return qPtr;
}

static int
searchAll (char *word)
{
    char *sp, *cp;

    if (!word)
        return false;
    cp = word;
    while ((sp = getNextWord_(&cp))) {
        if (strcmp (sp, "all") == 0)
            return  true;
    }
    return false;

}

void
freeKeyVal(struct keymap keylist[])
{
    int cc;
    for (cc = 0; keylist[cc].key != NULL; cc++) {
        if (keylist[cc].val != NULL) {
            free(keylist[cc].val);
            keylist[cc].val = NULL;
        }
    }
}

static int
isInGrp (char *word, struct gData *gp)
{
    int i;

    if (h_getEnt_(&gp->memberTab, word))
        return TRUE;

    for (i = 0; i < gp->numGroups; i++) {
        if (strcmp (gp->gPtr[i]->group, word) == 0)
            return TRUE;
        if (isInGrp(word, gp->gPtr[i]))
            return TRUE;
    }
    return FALSE;

}

struct qData *
lostFoundQueue(void)
{
    static struct qData *qPtr;

    if (qPtr != NULL)
        return qPtr;

    qPtr = initQData();
    qPtr->description = safeSave ("This queue is created by the system to hold the jobs that were in the queues being removed from the configuration by the LSF Administrator.  Jobs in this queue will not be started unless they are switched to other queues.");
    qPtr->queue = safeSave (LOST_AND_FOUND);
    qPtr->uJobLimit = 0;
    qPtr->pJobLimit = 0.0;
    qPtr->acceptIntvl = DEF_ACCEPT_INTVL;
    qPtr->qStatus = (!QUEUE_STAT_OPEN | !QUEUE_STAT_ACTIVE);
    qPtr->uGPtr = (struct gData *) my_malloc
        (sizeof (struct gData), "lostFoundQueue");
    qPtr->uGPtr->group = "";
    h_initTab_(&qPtr->uGPtr->memberTab, 16);
    qPtr->uGPtr->numGroups = 0;
    h_addEnt_(&qPtr->uGPtr->memberTab, "nobody", 0);


    qPtr->hostSpec = safeSave (masterHost);
    inQueueList (qPtr);
    checkQWindow();

    return qPtr;

}

/* mkLostAndFoundHost()
 * Always make this entry, it won't hurt and having
 * migrant hosts we will always need it.
 */
static struct hData *
mkLostAndFoundHost(void)
{
    struct hData *lost;
    struct hData host;

    memset(&host, 0, sizeof(struct hData));

    initHData(&host);
    host.host = LOST_AND_FOUND;
    host.cpuFactor = 1.0;
    host.uJobLimit = 0;
    host.maxJobs =  0;

    addHost(NULL, &host);
    checkHWindow();

    lost = getHostData(LOST_AND_FOUND);
    lost->flags = HOST_LOST_FOUND;

    return lost;
}

void
queueHostsPF(struct qData *qPtr, int *allPoll)
{
    int j;
    struct hData *hData;

    qPtr->numProcessors = 0;

    if (qPtr->hostList == NULL ||  qPtr->askedOthPrio > 0) {

        qPtr->numProcessors = numofprocs;

        if ((qPtr->flags & QUEUE_NEEDPOLL) && *allPoll == FALSE) {
            sTab hashSearchPtr;
            hEnt *hashEntryPtr;
            hashEntryPtr = h_firstEnt_(&hostTab, &hashSearchPtr);
            while (hashEntryPtr) {
                hData = (struct hData *)hashEntryPtr->hData;
                hashEntryPtr = h_nextEnt_(&hashSearchPtr);
                if (hData->hStatus & HOST_STAT_REMOTE)
                    continue;
                if (hData->flags & HOST_LOST_FOUND)
                    continue;
                hData->flags |= HOST_NEEDPOLL;
                hData->pollTime = 0;
            }
            *allPoll = TRUE;
        }

    } else {
        for (j = 0; j < qPtr->numAskedPtr; j++) {
            if (qPtr->askedPtr[j].hData== NULL)
                continue;
            qPtr->numProcessors += qPtr->askedPtr[j].hData->numCPUs;
            hData = qPtr->askedPtr[j].hData;
            if ((qPtr->flags & QUEUE_NEEDPOLL) && *allPoll == FALSE) {
                hData->flags |= HOST_NEEDPOLL;
                hData->pollTime = 0;
            }
        }
    }

}

static struct gData *
addUnixGrp (struct group *unixGrp, char *grpName,
            char *filename, struct gData *groups[], int *ngroups)
{
    int i = -1;
    struct gData *gp;
    struct passwd *pw = NULL;
    char *grpTemp;

    if (grpName == NULL) {
        grpTemp = unixGrp->gr_name;
    } else {
        grpTemp = grpName;
    }
    gp = addGroup (groups, grpTemp, ngroups);
    while (unixGrp->gr_mem[++i] != NULL)  {
        if ((pw = getpwnam(unixGrp->gr_mem[i])))
            addMember (gp, unixGrp->gr_mem[i], USER_GRP, filename,
                       groups, ngroups);
        else {
            ls_syslog(LOG_WARNING, _i18n_msg_get(ls_catd, NL_SETN, 6179,
                                                 "%s: Unknown user <%s> in group <%s>"), /* catgets 6179 */
                      filename, unixGrp->gr_mem[i], grpTemp);
            lsb_CheckError = WARNING_ERR;
        }
    }
    if (gp->memberTab.numEnts == 0 && gp->numGroups == 0) {
        freeGrp (gp);
        *ngroups -= 1;
        return NULL;
    }
    return (gp);

}

static void
getClusterData(void)
{
    int i;
    int num;

    TIMEIT(0, clusterName = ls_getclustername(), "minit_ls_getclustername");
    if (clusterName == NULL) {
        ls_syslog(LOG_ERR, "%s: ls_getclustername() failed %M", __func__);
        if (! lsb_CheckMode)
            mbdDie(MASTER_RESIGN);
        else {
            lsb_CheckError = FATAL_ERR;
            return ;
        }
    }

    if (debug) {
        FREEUP(managerIds);
        if (lsbManagers)
            FREEUP (lsbManagers[0]);
        FREEUP (lsbManagers);
        nManagers = 1;
        lsbManagers = my_malloc(sizeof(char *), __func__);
        lsbManagers[0] = my_malloc(MAX_LSB_NAME_LEN, __func__);

        if (getUser(lsbManagers[0], MAX_LSB_NAME_LEN) == 0) {

            managerIds = my_malloc(sizeof (uid_t), __func__);
            if (getUid(lsbManagers[0], &managerIds[0]) != 0) {
                ls_syslog(LOG_ERR, "\
%s: getUid() failed: %M", __func__);
                if (! lsb_CheckMode)
                    mbdDie(MASTER_RESIGN);
                else {
                    lsb_CheckError = FATAL_ERR;
                    return ;
                }
                managerIds[0] = -1;
            }
        } else {
            ls_syslog(LOG_ERR, "%s: getUser() failed %M", __func__);
            mbdDie(MASTER_RESIGN);
        }

        if (!lsb_CheckMode)
            ls_syslog(LOG_NOTICE, \
                      "%s: The LSF administrator is the invoker in debug mode", __func__);
        lsbManager = lsbManagers[0];
        managerId  = managerIds[0];
    }

    num = 0;
    clusterInfo = ls_clusterinfo(NULL, &num, NULL, 0, 0);
    if (clusterInfo == NULL) {
        ls_syslog(LOG_ERR, "%s: ls_clusterinfo() failed %M", __func__);
        if (!lsb_CheckMode)
            mbdDie(MASTER_RESIGN);
        else {
            lsb_CheckError = FATAL_ERR;
            return ;
        }
    } else {
        if (!debug) {
            if (nManagers > 0) {
                for (i = 0; i < nManagers; i++)
                    FREEUP (lsbManagers[i]);
                FREEUP (lsbManagers);
                FREEUP (managerIds);
            }
        }

        for (i = 0; i < num; i++) {
            if (!debug &&
                (strcmp(clusterName, clusterInfo[i].clusterName) == 0))
                setManagers(clusterInfo[i]);
        }

        if (!nManagers && !debug) {
            ls_syslog(LOG_ERR, "\
%s: Local cluster %s not returned by LIM", __func__, clusterName);
            mbdDie(MASTER_FATAL);
        }

        numofclusters = num;
    }
}

static void
setManagers(struct clusterInfo clusterInfo)
{
    struct passwd *pw;
    int i;
    int num;
    int gid;
    int temNum = 0;
    int tempId;
    char *sp;
    char managerIdStr[MAX_LSB_NAME_LEN];
    char managerStr[MAX_LSB_NAME_LEN];

    temNum = (clusterInfo.nAdmins) ? clusterInfo.nAdmins : 1;
    managerIds = my_malloc(sizeof(uid_t) * temNum, __func__);
    lsbManagers = my_malloc(sizeof (char *) * temNum, __func__);

    num = 0;
    for (i = 0; i < temNum; i++) {

        sp = (clusterInfo.nAdmins) ?
            clusterInfo.admins[i] : clusterInfo.managerName;
        tempId = (clusterInfo.nAdmins) ?
            clusterInfo.adminIds[i] : clusterInfo.managerId;

        lsbManagers[i] = safeSave (sp);

        if ((pw = getpwnam (sp)) == NULL) {
            ls_syslog(LOG_ERR, "\
%s: Invalid LSF administrator name <%s> and userId <%d>",
                      __func__, sp, tempId);
            managerIds[i] = -1;
            continue;
        }
        managerIds[i] = pw->pw_uid;
        if (num == 0) {
            gid = pw->pw_gid;
            lsbManager = lsbManagers[i];
            managerId  = managerIds[i];
        }
        num++;
    }

    if (num == 0) {
        ls_syslog(LOG_ERR, "%s: No valid LSF administrators", __func__);
        mbdDie(MASTER_FATAL);
    }

    nManagers = temNum;

    setgid(gid);

    if (getenv("LSB_MANAGERID") == NULL) {
        sprintf(managerIdStr, "%d", managerId);
        putEnv("LSB_MANAGERID", managerIdStr);
    }

    if (getenv("LSB_MANAGER") == NULL) {
        sprintf(managerStr, "%s", lsbManager);
        putEnv("LSB_MANAGER", managerStr);
    }

    /* This message is most likely going to syslog.
     */
    if (ls_logchown(managerId) < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed to chown() logfile to %d %m", __func__, managerId);
    }

    if (setreuid(managerId, managerId) < 0) {
        ls_syslog(LOG_ERR, "\
%s: failed to set managerId for mbatchd running as uid %d: %m",
                  __func__, getuid());
        /* Don't die keep running as root.
         */
    } else {
        ls_syslog(LOG_INFO, "\
%s: mbatchd running as managerId %d", __func__, managerId);
    }
}

static void
setParams(struct paramConf *paramConf)
{
    struct parameterInfo *params;

    if (paramConf == NULL || paramConf->param == NULL) {
        ls_syslog(LOG_ERR, "\
%s: paramConf or param in paramConf is NULL; default parameters will be used",
                  __func__);
        return;
    }

    params = paramConf->param;
    setString(defaultQueues, params->defaultQueues);
    setString(defaultHostSpec, params->defaultHostSpec);
    setString(lsfDefaultProject, params->defaultProject);

    setValue(msleeptime, params->mbatchdInterval);
    setValue(sbdSleepTime, params->sbatchdInterval);
    setValue(accept_intvl, params->jobAcceptInterval);
    setValue(max_retry, params->maxDispRetries);
    setValue(max_sbdFail, params->maxSbdRetries);
    setValue(clean_period, params->cleanPeriod);
    setValue(maxjobnum, params->maxNumJobs);
    setValue(pgSuspIdleT, params->pgSuspendIt);
    setValue(retryIntvl, params->retryIntvl);
    setValue(rusageUpdateRate, params->rusageUpdateRate);
    setValue(rusageUpdatePercent, params->rusageUpdatePercent);
    setValue(condCheckTime, params->condCheckTime);
    setValue(maxJobArraySize, params->maxJobArraySize);
    setValue(jobRunTimes, params->jobRunTimes);
    setValue(jobDepLastSub, params->jobDepLastSub);

    setValue(maxSbdConnections, params->maxSbdConnections);

    setValue(maxSchedStay, params->maxSchedStay);
    setValue(freshPeriod, params->freshPeriod);
    setValue(jobTerminateInterval, params->jobTerminateInterval);
    setString(pjobSpoolDir, params->pjobSpoolDir);

    setValue(maxUserPriority, params->maxUserPriority);
    setValue(jobPriorityValue, params->jobPriorityValue);
    setValue(jobPriorityTime, params->jobPriorityTime);
    setJobPriUpdIntvl();

    setValue(sharedResourceUpdFactor, params->sharedResourceUpdFactor);

    setValue(preExecDelay, params->preExecDelay);
    setValue(slotResourceReserve, params->slotResourceReserve);

    setValue(maxJobId, params->maxJobId);

    setValue(maxAcctArchiveNum, params->maxAcctArchiveNum);
    setValue(acctArchiveInDays, params->acctArchiveInDays);
    setValue(acctArchiveInSize, params->acctArchiveInSize);

    /* Set all the globals in one data strcuture.
     */
    mbdParams = calloc(1, sizeof(struct parameterInfo));

    /* Copy the library values to the mbd data strcutures
     */
    memcpy(mbdParams, params, sizeof(struct parameterInfo));
    if (params->defaultQueues)
        mbdParams->defaultQueues = strdup(params->defaultQueues);
    if (params->defaultHostSpec)
        mbdParams->defaultHostSpec = strdup(params->defaultHostSpec);
    if (params->pjobSpoolDir)
        mbdParams->pjobSpoolDir = strdup(params->pjobSpoolDir);

    /* Set default and convert to minutes
     */
    if (mbdParams->hist_mins == -1)
        mbdParams->hist_mins = DEF_HIST_MINUTES;
    mbdParams->hist_mins = mbdParams->hist_mins * 60;
    /* By default the value is false and we scale the
     * runtime limit by the cpu factor, if set to true
     * we don't.
     */
    mbdParams->run_abs_limit = params->run_abs_limit;

    /* If defined in lsb.params then mbatchd preempts based on this
     * specified resources instead of slots.
     */
    if (params->preemptableResources)
        mbdParams->preemptableResources = strdup(params->preemptableResources);
}

static void
addUData(struct userConf *userConf)
{
    struct userInfoEnt *uPtr;
    int i;

    removeFlags(&uDataList, USER_UPDATE, UDATA);
    for (i = 0; i < userConf->numUsers; i++) {
        uPtr = &(userConf->users[i]);
        addUserData(uPtr->user, uPtr->maxJobs, uPtr->procJobLimit,
                    (char *)__func__, TRUE, TRUE);
        if (strcmp ("default", uPtr->user) == 0)
            defUser = TRUE;
    }
}

static void
createTmpGData(struct groupInfoEnt *groups,
               int num,
               int groupType,
               struct gData *tempGData[],
               int *nTempGroups)
{
    struct groupInfoEnt *gPtr;
    char *HUgroups;
    char *sp;
    char *wp;
    int i;
    struct gData *grpPtr;

    if (groupType == USER_GRP)
        HUgroups = "usergroup";
    else
        HUgroups = "hostgroup";

    for (i = 0; i < num; i++) {

        gPtr = &(groups[i]);

        if (groupType == HOST_GRP && gPtr && gPtr->group
            && isHostAlias(gPtr->group)) {
            ls_syslog(LOG_WARNING, "\
%s: Host group name <%s> conflicts with host name; ignoring", __func__,
                      gPtr->group);
            lsb_CheckError = WARNING_ERR;
            continue;
        }

        if (getGrpData (tempGData, gPtr->group, *nTempGroups)) {
            ls_syslog(LOG_WARNING, "\
%s: Group <%s> is multiply defined in %s; ignoring", __func__,
                      gPtr->group, HUgroups);
            lsb_CheckError = WARNING_ERR;
            continue;
        }

        grpPtr = addGroup(tempGData, gPtr->group, nTempGroups);
        sp = gPtr->memberList;

        while (strcmp(gPtr->memberList, "all") != 0 &&
               (wp = getNextWord_(&sp)) != NULL) {
            addMember(grpPtr,
                      wp,
                      groupType,
                      (char *)__func__,
                      tempGData,
                      nTempGroups);
        }

        /* Copy the group slots built in resource
         */
        if (gPtr->group_slots) {
            grpPtr->group_slots = strdup(gPtr->group_slots);
            grpPtr->max_slots = gPtr->max_slots;
        }

        if (grpPtr->memberTab.numEnts == 0
            && grpPtr->numGroups == 0
            && strcmp(gPtr->memberList, "all") != 0) {
            ls_syslog(LOG_WARNING, "\
%s: No valid member in group <%s>; ignoring the group", __func__, grpPtr->group);
            lsb_CheckError = WARNING_ERR;
            freeGrp(grpPtr);
            *nTempGroups -= 1;
        }
    }

}

static int
isHostAlias (char *grpName)
{
    int i;
    hEnt *hashEntryPtr;
    sTab hashSearchPtr;
    struct hData *hData;

    hashEntryPtr = h_firstEnt_(&hostTab, &hashSearchPtr);
    while (hashEntryPtr) {
        hData = (struct hData *) hashEntryPtr->hData;
        hashEntryPtr = h_nextEnt_(&hashSearchPtr);
        if (hData->hostEnt.h_aliases) {
            for (i = 0; hData->hostEnt.h_aliases[i]; i++) {
                if (equalHost_(grpName, hData->hostEnt.h_aliases[i]))
                    return TRUE;
            }
        }
    }
    return FALSE;

}

/* addHostData()
 * Add a host into MBD internal data structures.
 */
static void
addHostData(int numHosts, struct hostInfoEnt *hosts)
{
    int i;
    int j;
    struct hData hPtr;

    removeFlags (&hostTab, HOST_UPDATE | HOST_AUTOCONF_MXJ, HDATA);
    if (numHosts == 0 || hosts == NULL) {
        ls_syslog(LOG_ERR, "\
%s: No hosts specified in lsb.hosts; all hosts known by LSF will be used",
                  __func__);
        addDefaultHost();
        return;
    }

    for (i = 0; i < numHosts; i++) {

        /* Find the batch host among LIM hosts
         */
        for (j = 0; j < numLIMhosts; j++) {
            if (equalHost_(hosts[i].host, LIMhosts[j].hostName))
                break;
        }
        if (j == numLIMhosts) {
            ls_syslog(LOG_ERR, "\
%s: Host <%s> is not used by the batch system; ignored",
                      __func__, hosts[i].host);
            continue;
        }
        if (LIMhosts[j].isServer != TRUE) {
            ls_syslog(LOG_ERR, "\
%s: Host <%s> is not a server; ignoring", __func__, hosts[i].host);
            continue;
        }

        if (!Gethostbyname_(LIMhosts[j].hostName)) {
            ls_syslog(LOG_ERR, "\
%s: Host <%s> is not a valid host; ignoring", __func__, hosts[i].host);
            continue;
        }

        /* Initialize the temporary hData structure.
         */
        initHData(&hPtr);

        /* Copy the host batch host configuration
         * into the temporary host data.
         */
        hPtr.host = hostConf->hosts[i].host;
        hPtr.uJobLimit = hostConf->hosts[i].userJobLimit;

        /* Migrant host uses rexPriority to indicate
         * the MXJ and maxCPU the migrant host has.
         */
        if (LIMhosts[j].rexPriority > 0) {
            hPtr.maxJobs = LIMhosts[j].rexPriority;
            LIMhosts[j].maxCpus = LIMhosts[j].maxCpus;
        } else {
            hPtr.maxJobs = hostConf->hosts[i].maxJobs;
        }

        if (hostConf->hosts[i].chkSig != INFINIT_INT)
            hPtr.chkSig = hostConf->hosts[i].chkSig;
        if (hostConf->hosts[i].mig == INFINIT_INT)
            hPtr.mig = hostConf->hosts[i].mig;
        else
            hPtr.mig = hostConf->hosts[i].mig * 60;

        hPtr.loadSched = hostConf->hosts[i].loadSched;
        hPtr.loadStop = hostConf->hosts[i].loadStop;
        hPtr.windows = hostConf->hosts[i].windows;
        hPtr.affinity = hostConf->hosts[i].affinity;

        /* Add the host by merging the lsf base
         * host information with the batch configuration.
         */
        addHost(&LIMhosts[j], &hPtr);

    } /* for (i = 0; i < numHosts; i++) */
}

static void
setDefaultParams(void)
{

    FREEUP(defaultQueues);
    FREEUP(defaultHostSpec);
    FREEUP(lsfDefaultProject);
    FREEUP(pjobSpoolDir);

    msleeptime = DEF_MSLEEPTIME;
    sbdSleepTime = DEF_SSLEEPTIME;
    accept_intvl = DEF_ACCEPT_INTVL;
    max_retry = DEF_MAX_RETRY;
    max_sbdFail = DEF_MAXSBD_FAIL;
    preemPeriod = DEF_PREEM_PERIOD;
    clean_period = DEF_CLEAN_PERIOD;
    maxjobnum = DEF_MAX_JOB_NUM;
    pgSuspIdleT = DEF_PG_SUSP_IT;
    retryIntvl = DEF_RETRY_INTVL;
    rusageUpdateRate = DEF_RUSAGE_UPDATE_RATE;
    rusageUpdatePercent = DEF_RUSAGE_UPDATE_PERCENT;
    condCheckTime = DEF_COND_CHECK_TIME;
    maxSbdConnections = DEF_MAX_SBD_CONNS;
    maxSchedStay = DEF_SCHED_STAY;
    freshPeriod = DEF_FRESH_PERIOD;
    maxJobArraySize = DEF_JOB_ARRAY_SIZE;
    jobTerminateInterval = DEF_JTERMINATE_INTERVAL;
    jobRunTimes = INFINIT_INT;
    jobDepLastSub = 0;
    maxUserPriority = -1;
    jobPriorityValue = -1;
    jobPriorityTime = -1;
    preExecDelay = DEF_PRE_EXEC_DELAY;
    slotResourceReserve = FALSE;
    maxJobId = DEF_MAX_JOBID;
    maxAcctArchiveNum = -1;
    acctArchiveInDays = -1;
    acctArchiveInSize = -1;
}

static void
addQData(struct queueConf *queueConf, int mbdInitFlags )
{
    int i;
    int badqueue;
    int j;
    struct qData *qPtr;
    struct qData *oldQPtr;
    struct queueInfoEnt *queue;
    char *sp;
    char *word;
    int queueIndex = 0;

    for (qPtr = qDataList->forw; qPtr != qDataList; qPtr = qPtr->forw) {
        qPtr->flags &= ~QUEUE_UPDATE;
    }

    if (queueConf == NULL || queueConf->numQueues <= 0
        || queueConf->queues == NULL) {
        ls_syslog(LOG_ERR, "\
%s: No valid queue in queueConf structure", __func__);
        lsb_CheckError = WARNING_ERR;
        return;
    }

    for (i = 0; i < queueConf->numQueues; i++) {

        queue = &(queueConf->queues[i]);
        qPtr = initQData();

        qPtr->queue = safeSave (queue->queue);
        if (queue->description)
            qPtr->description = safeSave(queue->description);
        else
            qPtr->description = safeSave("No description provided.");

        setValue(qPtr->priority, queue->priority);

        if (queue->userList)
            parseGroups(USER_GRP,
                        &qPtr->uGPtr,
                        queue->userList,
                        (char *)__func__);

        if (queue->hostList) {

            if (strcmp(queue->hostList, "none") == 0) {
                qPtr->hostList = safeSave(queue->hostList);

            } else {
                if (searchAll(queue->hostList) == FALSE) {
                    if (parseQHosts(qPtr, queue->hostList) != 0 ) {
                        qPtr->hostList = safeSave(queue->hostList);
                    }
                }
            }
        }

        badqueue = FALSE;
        if (qPtr->uGPtr) {
            if (qPtr->uGPtr->memberTab.numEnts == 0
                && qPtr->uGPtr->numGroups == 0) {

                ls_syslog(LOG_ERR, "\
%s: No valid value for key USERS in the queue <%s>; ignoring the queue",
                          __func__, qPtr->queue);
                lsb_CheckError = WARNING_ERR;
                badqueue = TRUE;
            }
        }

        if (qPtr->hostList != NULL
            && strcmp(qPtr->hostList, "none")
            && qPtr->numAskedPtr <= 0
            && qPtr->askedOthPrio <= 0) {
            ls_syslog(LOG_ERR, "\
%s: No valid value for key HOSTS in the queue <%s>; ignoring the queue",
                      __func__, qPtr->queue);
            lsb_CheckError = WARNING_ERR;
            badqueue = TRUE;
        }

        if (queue->hostshare) {
            if (parse_host_shares(queue->hostshare, qPtr) < 0) {
                ls_syslog(LOG_ERR, "\
%s: No valid value for key HOSTS_SHARES in the queue <%s>; ignoring the queue",
                          __func__, qPtr->queue);
                lsb_CheckError = WARNING_ERR;
                badqueue = TRUE;
            }
        }

        if (badqueue) {
            freeQData(qPtr, FALSE);
            continue;
        }

        ++queueIndex;
        qPtr->queueId = queueIndex;

        if ((oldQPtr = getQueueData(qPtr->queue)) == NULL) {
            inQueueList (qPtr);
            qPtr->flags |= QUEUE_UPDATE;
        } else {
            copyQData (qPtr, oldQPtr);
            oldQPtr->flags |= QUEUE_UPDATE;
            FREEUP(qPtr->queue);
            FREEUP(qPtr->reasonTb[0]);
            FREEUP(qPtr->reasonTb[1]);
            FREEUP(qPtr->reasonTb);
            FREEUP(qPtr);

            if (mbdInitFlags == RECONFIG_CONF
                || mbdInitFlags == WINDOW_CONF) {

                FREEUP(oldQPtr->reasonTb[0]);
                FREEUP(oldQPtr->reasonTb[1]);
                oldQPtr->reasonTb[0] = my_calloc(numLIMhosts + 2,
                                                 sizeof(int),
                                                 __func__);
                oldQPtr->reasonTb[1] = my_calloc(numLIMhosts + 2,
                                                 sizeof(int),
                                                 __func__);

                for(j = 0; j < numLIMhosts+2; j++) {
                    oldQPtr->reasonTb[0][j] = 0;
                    oldQPtr->reasonTb[1][j] = 0;
                }
            }
        }

        if (queue->fairshare) {
            qPtr->fairshare = strdup(queue->fairshare);
            qPtr->qAttrib |= Q_ATTRIB_FAIRSHARE;
        }

        if (queue->preemption) {
            qPtr->preemption = strdup(queue->preemption);
            qPtr->qAttrib |= Q_ATTRIB_PREEMPTIVE;
        }

        if (queue->ownership) {
            /* Keep using the qAttrib since the
             * API library needs it
             */
            qPtr->ownership = strdup(queue->ownership);
            qPtr->qAttrib |= Q_ATTRIB_OWNERSHIP;
            qPtr->loan_duration = queue->loan_duration;
        }
    }

    for (i = 0; i < queueConf->numQueues; i++) {

        queue = &(queueConf->queues[i]);
        qPtr = getQueueData(queue->queue);

        if (queue->nice != INFINIT_SHORT)
            qPtr->nice = queue->nice;

        setValue(qPtr->uJobLimit, queue->userJobLimit);
        setValue(qPtr->pJobLimit, queue->procJobLimit);
        setValue(qPtr->maxJobs, queue->maxJobs);
        setValue(qPtr->hJobLimit, queue->hostJobLimit);
        setValue(qPtr->procLimit, queue->procLimit);
        setValue(qPtr->minProcLimit, queue->minProcLimit);
        setValue(qPtr->defProcLimit, queue->defProcLimit);
        qPtr->windEdge = 0 ;

        if (queue->windows != NULL) {

            qPtr->windows = safeSave(queue->windows);
            *(qPtr->windows) = '\0';
            sp = queue->windows;
            while ((word = getNextWord_(&sp)) != NULL) {
                char *save;
                save = safeSave(word);
                if (addWindow(word, qPtr->weekR, qPtr->queue) < 0) {
                    ls_syslog(LOG_ERR, "\
%s: Bad time expression %s/%s in queue window %s; ignored",
                              __func__, queue->windows, save, qPtr->queue);
                    lsb_CheckError = WARNING_ERR;
                    freeWeek(qPtr->weekR);
                    free(save);
                    continue;
                }
                qPtr->windEdge = now;
                if (*(qPtr->windows) != '\0')
                    strcat(qPtr->windows, " ");
                strcat(qPtr->windows, save);
                free(save);
            }
        }

        if (queue->windowsD) {

            qPtr->windowsD = safeSave(queue->windowsD);
            *(qPtr->windowsD) = '\0';
            sp = queue->windowsD;

            while ((word = getNextWord_(&sp)) != NULL) {
                char *save;
                save = safeSave(word);
                if (addWindow(word, qPtr->week, qPtr->queue) <0) {
                    ls_syslog(LOG_ERR, "\
%s: Bad time expression %s/%s in queue dispatch windows %s; ignored",
                              __func__, queue->windowsD,
                              save, qPtr->queue);
                    lsb_CheckError = WARNING_ERR;
                    freeWeek(qPtr->week);
                    free(save);
                    continue;
                }
                qPtr->windEdge = now;
                if (*(qPtr->windowsD) != '\0')
                    strcat(qPtr->windowsD, " ");

                strcat(qPtr->windowsD, save);
                free(save);
            }
        }

        if (!qPtr->windows && (qPtr->qStatus & QUEUE_STAT_RUNWIN_CLOSE)) {
            qPtr->qStatus &= ~QUEUE_STAT_RUNWIN_CLOSE;
        }

        if (!qPtr->windows && !qPtr->windowsD
            && !(qPtr->qStatus & QUEUE_STAT_RUN)) {
            qPtr->qStatus |= QUEUE_STAT_RUN;
        }

        if (queue->defaultHostSpec) {
            float *cpuFactor;

            cpuFactor = getModelFactor(queue->defaultHostSpec);
            if (cpuFactor == NULL)
                cpuFactor = getHostFactor (queue->defaultHostSpec);
            if (cpuFactor == NULL) {
                ls_syslog(LOG_ERR, "\
%s: Invalid hostspec %s for %s in queue <%s>; ignored",
                          __func__,
                          queue->defaultHostSpec,
                          "DEFAULT_HOST_SPEC",
                          qPtr->queue);
                lsb_CheckError = WARNING_ERR;
            }
            if (cpuFactor != NULL)
                qPtr->defaultHostSpec = safeSave(queue->defaultHostSpec);
        }

        if (queue->hostSpec)
            qPtr->hostSpec = safeSave (queue->hostSpec);

        for (j = 0; j < LSF_RLIM_NLIMITS; j++) {
            if (queue->rLimits[j] == INFINIT_INT)
                qPtr->rLimits[j] = -1;
            else
                qPtr->rLimits[j] = queue->rLimits[j];

            if (queue->defLimits[j] == INFINIT_INT)
                qPtr->defLimits[j] = -1;
            else
                qPtr->defLimits[j] = queue->defLimits[j];
        }

        qPtr->qAttrib = (qPtr->qAttrib | queue->qAttrib);

        if (queue->mig != INFINIT_INT) {
            setValue(qPtr->mig, queue->mig);
            qPtr->mig *= 60;
        }

        if (queue->schedDelay != INFINIT_INT)
            qPtr->schedDelay = queue->schedDelay;

        if (queue->acceptIntvl != INFINIT_INT)
            qPtr->acceptIntvl = queue->acceptIntvl;
        else
            qPtr->acceptIntvl = accept_intvl;

        if (queue->admins)
            parseAUids (qPtr, queue->admins);

        if (queue->preCmd)
            qPtr->preCmd = safeSave (queue->preCmd);

        if (queue->prepostUsername)
            qPtr->prepostUsername = safeSave (queue->prepostUsername);

        if (queue->postCmd)
            qPtr->postCmd = safeSave (queue->postCmd);

        if (queue->requeueEValues) {
            qPtr->requeueEValues = safeSave (queue->requeueEValues);

            if (qPtr->requeEStruct) {
                clean_requeue(qPtr);
            }
            requeueEParse (&qPtr->requeEStruct, queue->requeueEValues, &j);

        }

        if (queue->resReq) {
            qPtr->resValPtr = checkResReq(queue->resReq,
                                          USE_LOCAL
                                          | PARSE_XOR
                                          | CHK_TCL_SYNTAX);
            if (qPtr->resValPtr == NULL) {
                ls_syslog(LOG_ERR, "\
%s: invalid RES_REQ %s in queues %s; ignoring",
                          __func__, queue->resReq, qPtr->queue);
                lsb_CheckError = WARNING_ERR;
            } else
                qPtr->resReq = safeSave (queue->resReq);
        }

        if (queue->slotHoldTime != INFINIT_INT)
            qPtr->slotHoldTime = queue->slotHoldTime * msleeptime;

        if (queue->resumeCond) {
            struct resVal *resValPtr;
            resValPtr = checkResReq(queue->resumeCond,
                                    USE_LOCAL | CHK_TCL_SYNTAX);
            if (resValPtr == NULL) {
                ls_syslog(LOG_ERR, "\
%s: invalid RESUME_COND %s in queue %s; ignoring",
                          __func__, queue->resumeCond, qPtr->queue);
                lsb_CheckError = WARNING_ERR;
            } else {
                qPtr->resumeCondVal = resValPtr;
                qPtr->resumeCond = safeSave (queue->resumeCond);
            }
        }

        if (queue->stopCond) {
            struct resVal *resValPtr;
            resValPtr = checkResReq(queue->stopCond,
                                    USE_LOCAL | CHK_TCL_SYNTAX);
            if (resValPtr == NULL) {
                ls_syslog(LOG_ERR, "\
%s: invalid STOP_COND %s in queue %s; ignoring",
                          __func__, queue->stopCond, qPtr->queue);
                lsb_CheckError = WARNING_ERR;
            } else {
                lsbFreeResVal (&resValPtr);
                qPtr->stopCond = safeSave (queue->stopCond);
            }
        }

        if (queue->jobStarter)
            qPtr->jobStarter = safeSave(queue->jobStarter);

        if (queue->suspendActCmd != NULL)
            qPtr->suspendActCmd = safeSave(queue->suspendActCmd);

        if (queue->resumeActCmd != NULL)
            qPtr->resumeActCmd = safeSave(queue->resumeActCmd);

        if (queue->terminateActCmd != NULL)
            qPtr->terminateActCmd = safeSave(queue->terminateActCmd);

        for (j = 0; j < LSB_SIG_NUM; j++)
            qPtr->sigMap[j] = queue->sigMap[j];

        initThresholds (qPtr->loadSched, qPtr->loadStop);
        for (j = 0; j < queue->nIdx; j++) {
            if (queue->loadSched[j] != INFINIT_FLOAT)
                qPtr->loadSched[j] = queue->loadSched[j];
            if (queue->loadStop[j] != INFINIT_FLOAT)
                qPtr->loadStop[j] = queue->loadStop[j];
        }

        setValue(qPtr->chkpntPeriod, queue->chkpntPeriod);
        if (queue->chkpntDir) {
            if (qPtr->chkpntDir)
                FREEUP(qPtr->chkpntDir);
            qPtr->chkpntDir = safeSave (queue->chkpntDir);
        }

        if (queue->qAttrib & Q_ATTRIB_CHKPNT)
            qPtr->qAttrib |= Q_ATTRIB_CHKPNT;

        if (queue->qAttrib & Q_ATTRIB_RERUNNABLE)
            qPtr->qAttrib |= Q_ATTRIB_RERUNNABLE;

        if (qPtr->qAttrib & Q_ATTRIB_BACKFILL)
            qAttributes |= Q_ATTRIB_BACKFILL;
    }
}

/* parse_host_shares()
 *
 * [[host1, 1] [grp2,5]] or [all, 10]
 */
static int
parse_host_shares(const char *host_shares,
                  struct qData *qp)
{
    char *p;
    char *u;
    int cc;
    int n;

    u = strdup(host_shares);
    assert(u);

    p = strchr(u, '[');
    *p++ = 0;

    tokenize(p);

    while (1) {
        char name[128];
        int shares;
        int i;
        struct hData *hPtr = NULL;

        cc = sscanf(p, "%s%u%n", name, &shares, &n);
        if (cc == EOF)
            break;
        if (cc != 2)
            goto error;
        p = p + n;

        /* configure shares for all hosts in the queue */
        if (searchAll(name)) {
            if (qp->hostList && qp->numAskedPtr > 0) {
                for (i = 0; i < qp->numAskedPtr; i++)
                    make_hsacct(qp->askedPtr[i].hData, qp->queue, shares);
            } else {
                struct hData *hPtr;
                for (hPtr = (struct hData *)hostList->back;
                     hPtr != (void *)hostList;
                     hPtr = hPtr->back) {
                    make_hsacct(hPtr, qp->queue, shares);
                }
            }
            /* configure shares for a specific host */
        } else if (hostQMember(name, qp)
                   && (hPtr = getHostData(name)) != NULL) {
            make_hsacct(hPtr, qp->queue, shares);
        } else {
            struct gData *gp = NULL;
            int num = 0;
            char** grpMembers = NULL;

            /* invalid shares */
            if ((gp = getHGrpData(name)) == NULL)
                goto error;

            /* configure shares for a host group */
            grpMembers = expandGrp(gp, name, &num);
            for (i = 0; i < num; i++) {
                if (hostQMember(grpMembers[i], qp)
                    && (hPtr = getHostData(grpMembers[i])) != NULL)
                    make_hsacct(hPtr, qp->queue, shares);
            }
            FREEUP(grpMembers);
        }
    }

    FREEUP(u);
    return true;

error:

    FREEUP(u);
    return false;
}

/*
 * Make host share account of queues.
 */
static void
make_hsacct(struct hData *hPtr, char *queue, int shares)
{
    struct hShareData *hData = NULL;
    struct qShareData *qData = NULL;
    struct qShareData *qPtr = NULL;
    hEnt *he = NULL;
    hEnt *qe = NULL;
    hEnt *hashEntryPtr = NULL;
    sTab hashSearchPtr;
    int  new;

    if (!hPtr->affinity
        && !daemonParams[SBD_BIND_CPU].paramValue) {
        ls_syslog(LOG_WARNING, "\
%s: HOSTS_SHARES requires SBD_BIND_CPU=y in lsb.conf or AFFINITY=y for host <%s> in lsb.hosts; ignoring HOSTS_SHARES of host <%s> for queue <%s>",
                  __func__, hPtr->host, hPtr->host, queue);
        return;
    }

    if (hShareTab.numEnts == 0)
        h_initTab_(&hShareTab, numofhosts());

    he = h_addEnt_(&hShareTab, hPtr->host, &new);
    if (new) {
        hData = my_calloc(1,
                          sizeof(struct hShareData),
                          "make_hsacct");
        hData->host = safeSave(hPtr->host);
        hData->total = 0;
        hData->qAcct= my_calloc(1,
                                sizeof(struct hTab),
                                "make_hsacct");
        h_initTab_(hData->qAcct, numofqueues);
        he->hData = hData;
    } else {
        hData = (struct hShareData *)he->hData;
    }

    qe = h_addEnt_(hData->qAcct, queue, &new);
    if (new) {
        qData = my_calloc(1,
                          sizeof(struct qShareData),
                          "make_hsacct");
        qData->queue = safeSave(queue);
        qData->shares = shares;
        hData->total += shares;
        qe->hData = qData;

        /* re-build share ratio */
        hashEntryPtr = h_firstEnt_(hData->qAcct, &hashSearchPtr);
        while (hashEntryPtr) {
            qPtr = (struct qShareData *) hashEntryPtr->hData;
            qPtr->dshares = ((double)qPtr->shares)/hData->total;
            hashEntryPtr = h_nextEnt_(&hashSearchPtr);
        }
    }
}

/* copyGroups()
 */
static void
copyGroups(int copyHGroups)
{
    int i;

    if (copyHGroups == FALSE) {
        for (i = 0; i < numofugroups; i++) {
            if (usergroups[i] == NULL)
                continue;
            freeGrp (usergroups[i]);
        }
        for (i = 0; i < nTempUGroups; i++) {
            usergroups[i] = tempUGData[i];
            tempUGData[i] = NULL;
        }
        numofugroups = nTempUGroups;
        nTempUGroups = 0;
        return;
    }
    for (i = 0; i < numofhgroups; i++) {
        if (hostgroups[i] == NULL)
            continue;
        freeGrp (hostgroups[i]);
    }

    for (i = 0; i < nTempHGroups; i++)
        hostgroups[i] = tempHGData[i];
    numofhgroups = nTempHGroups;
    nTempHGroups = 0;

} /* copyGroups */


static void
createCondNodes (int numConds, char **conds, char *fileName, int flags)
{
    static char fname[] = "createCondNodes";
    static int first = TRUE;
    int i, errcode = 0, jFlags =0, new;
    struct lsfAuth auth;
    struct condData *condNode;
    struct hEnt *ent;

    if (numConds <= 0 || conds == NULL)
        return;

    if (first == TRUE) {
        h_initTab_(&condDataList, 20);
        first = FALSE;
    }

    auth.uid = batchId;
    strncpy(auth.lsfUserName, batchName, MAXLSFNAMELEN);
    auth.lsfUserName[MAXLSFNAMELEN-1] = '\0';

    for (i = 0; i < numConds; i++) {
        if (conds[i] == NULL || conds[i][0] == '\0')
            continue;
        if (h_getEnt_(&condDataList, conds[i])) {
            ls_syslog(LOG_DEBUG, "%s: Condition <%s> in file <%s> is multiply specified;retaining old definition", fname, conds[i], fileName);
            continue;
        }
        condNode = initConfData();
        condNode->name = safeSave (conds[i]);
        condNode->flags = flags;
        if ((condNode->rootNode = parseDepCond (conds[i], &auth, &errcode,
                                                NULL, &jFlags, 0)) == NULL)
        {
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6207,
                                             "%s: Bad condition <%s> in file <%s> :%M; ignored"), fname, conds[i], fileName); /* catgets 6207 */
            lsb_CheckError = WARNING_ERR;
        }
        ent = h_addEnt_(&condDataList, condNode->name, &new);
        ent->hData = condNode;
        continue;
    }
}

int
updAllConfCond (void)
{
    int needReadAgain = FALSE;

    if (hostFileConf != NULL && updCondData (hostFileConf, HOST_FILE) == TRUE)
        needReadAgain = TRUE;
    if (userFileConf != NULL && updCondData (userFileConf, USER_FILE) == TRUE)
        needReadAgain = TRUE;
    if (queueFileConf != NULL && updCondData (queueFileConf, QUEUE_FILE) == TRUE)
        needReadAgain = TRUE;
    if (paramFileConf != NULL && updCondData (paramFileConf, PARAM_FILE) == TRUE)
        needReadAgain = TRUE;

    return (needReadAgain);

}

static int
updCondData (struct lsConf *conf, int fileType)
{
    static char fname[] = "updCondData";
    hEnt *hashEntryPtr;
    struct condData *condition;
    int status, i, needReadAgain = FALSE;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG2, "%s: Entering this routine...", fname);

    if (conf == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6208,
                                         "%s: NULL Conf pointer for fileType <%x>"), fname, fileType); /* catgets 6208 */
        return FALSE;
    }
    if (conf->numConds <= 0 || conf->conds == NULL)
        return FALSE;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG2, "%s: numConds <%d>, fileType <%x>", fname, conf->numConds, fileType);

    for (i = 0; i < conf->numConds; i++) {
        if ((hashEntryPtr = h_getEnt_(&condDataList, conf->conds[i])) == NULL)
            continue;
        condition = (struct condData *) hashEntryPtr->hData;
        if (condition->rootNode == NULL)
            continue;
        status = evalDepCond (condition->rootNode, NULL, NULL);
        if (status == DP_TRUE)
            status = TRUE;
        else
            status = FALSE;
        if (status == TRUE) {
            conf->values[i] = 1;
        } else {
            conf->values[i] = 0;
        }
        if (status != condition->status) {
            condition->lastStatus = condition->status;
            condition->status = status;
            condition->lastTime = now;
            needReadAgain = TRUE;
        }
        if (logclass & LC_TRACE)
            ls_syslog(LOG_DEBUG3, "%s: Condition name <%s> for %dth condition status <%d> lastStatus <%d>", fname, conf->conds[i], i+1, condition->status, condition->lastStatus);
    }
    return (needReadAgain);

}

static struct condData *
initConfData (void)
{
    struct condData *cData;

    cData = (struct condData *)my_malloc (sizeof (struct condData), "initConfData");
    cData->name = NULL;
    cData->status = FALSE;
    cData->lastStatus = FALSE;
    cData->lastTime = now;
    cData->flags = 0;
    cData->rootNode = NULL;
    return (cData);

}

static struct lsConf *
getFileConf (char *fileName, int fileType)
{
    static char fname[] = "getFileConf";
    struct lsConf *confPtr = NULL;

    confPtr = ls_getconf (fileName);
    if (confPtr == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL_M, fname, "ls_getconf");
        if (lserrno != LSE_NO_FILE) {
            if (lsb_CheckMode) {
                lsb_CheckError = FATAL_ERR;
                return NULL;
            } else
                mbdDie(MASTER_FATAL);
        }

    }
    if (confPtr != NULL) {
        createCondNodes (confPtr->numConds, confPtr->conds, fileName, fileType);
        updCondData (confPtr, fileType);
    }
    return (confPtr);

}

static void
copyQData (struct qData *fromQp, struct qData *toQp)
{
    int i;

#define copyString(string, newPointer) {        \
        FREEUP(string);                         \
        string = newPointer;}

    if (needPollQHost(fromQp, toQp))
        toQp->flags |= QUEUE_NEEDPOLL;


    copyString(toQp->description, fromQp->description);
    copyString(toQp->resReq, fromQp->resReq);
    copyString(toQp->preCmd, fromQp->preCmd);
    copyString(toQp->postCmd, fromQp->postCmd);
    copyString(toQp->prepostUsername, fromQp->prepostUsername);
    copyString(toQp->requeueEValues, fromQp->requeueEValues);
    copyString(toQp->windowsD, fromQp->windowsD);
    copyString(toQp->windows, fromQp->windows);
    copyString(toQp->hostSpec, fromQp->hostSpec);
    copyString(toQp->defaultHostSpec, fromQp->defaultHostSpec);
    copyString(toQp->resumeCond, fromQp->resumeCond);
    copyString(toQp->stopCond, fromQp->stopCond);
    copyString(toQp->jobStarter, fromQp->jobStarter);


    copyString(toQp->suspendActCmd, fromQp->suspendActCmd);
    copyString(toQp->resumeActCmd, fromQp->resumeActCmd);
    copyString(toQp->terminateActCmd, fromQp->terminateActCmd);

    copyString(toQp->hostList, fromQp->hostList);


    toQp->numProcessors = fromQp->numProcessors;
    toQp->mig = fromQp->mig;
    toQp->priority = fromQp->priority;
    toQp->nice = fromQp->nice;
    toQp->procLimit = fromQp->procLimit;
    toQp->minProcLimit = fromQp->minProcLimit;
    toQp->defProcLimit = fromQp->defProcLimit;
    toQp->hJobLimit = fromQp->hJobLimit;
    toQp->pJobLimit = fromQp->pJobLimit;
    toQp->uJobLimit = fromQp->uJobLimit;
    toQp->maxJobs = fromQp->maxJobs;
    toQp->slotHoldTime = fromQp->slotHoldTime;
    toQp->askedOthPrio = fromQp->askedOthPrio;
    toQp->numAskedPtr = fromQp->numAskedPtr;
    toQp->queueId = fromQp->queueId;


    if (toQp->uGPtr) {
        freeGrp (toQp->uGPtr);
    }
    toQp->uGPtr = fromQp->uGPtr;

    FREEUP (toQp->askedPtr);
    toQp->askedPtr = fromQp->askedPtr;

    FREEUP(toQp->loadSched);
    toQp->loadSched = fromQp->loadSched;

    FREEUP(toQp->loadStop);
    toQp->loadStop = fromQp->loadStop;

    lsbFreeResVal (&toQp->resValPtr);
    toQp->resValPtr = fromQp->resValPtr;

    lsbFreeResVal (&toQp->resumeCondVal);
    toQp->resumeCondVal = fromQp->resumeCondVal;

    for (i = 0; i < LSF_RLIM_NLIMITS; i++) {
        toQp->rLimits[i] = fromQp->rLimits[i];
        toQp->defLimits[i] = fromQp->defLimits[i];
    }

    if (toQp->nAdmins > 0) {
        FREEUP (toQp->adminIds);
        FREEUP (toQp->admins);
    }
    toQp->nAdmins = fromQp->nAdmins;
    toQp->adminIds = fromQp->adminIds;
    toQp->admins = fromQp->admins;

    if (toQp->weekR)
        freeWeek (toQp->weekR);

    if (toQp->week)
        freeWeek (toQp->week);
    for (i = 0; i < 8; i++) {
        toQp->weekR[i] = fromQp->weekR[i];
        toQp->week[i] = fromQp->week[i];
    }

    toQp->qAttrib = fromQp->qAttrib;


}

static void
addDefaultHost(void)
{
    int i;
    struct hData hData;

    for (i = 0; i < numLIMhosts; i++) {
        if (LIMhosts[i].isServer != TRUE)
            continue;
        initHData(&hData);
        hData.host = LIMhosts[i].hostName;
        addHost(&LIMhosts[i], &hData);
    }
}

static void
removeFlags (struct hTab *dataList, int flags, int listType)
{
    static char    fname[] = "removeFlags()";
    sTab           sTabPtr;
    hEnt           *hEntPtr;
    struct hData   *hData;
    struct uData   *uData;

    hEntPtr = h_firstEnt_(dataList, &sTabPtr);
    while (hEntPtr) {
        switch (listType) {
            case HDATA:
                hData = (struct hData *) hEntPtr->hData;
                hData->flags &= ~(flags);
                break;
            case UDATA:
                uData = (struct uData  *)hEntPtr->hData;
                uData->flags &= ~(flags);
                break;
            default:
                ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6210,
                                                 "%s: Bad data list type <%d>"), /* catgets 6210 */
                          fname,
                          listType);
                return;
        }
        hEntPtr = h_nextEnt_(&sTabPtr);
    }

}
static int
needPollQHost (struct qData *newQp, struct qData *oldQp)
{

    if ((newQp->resumeCond == NULL && oldQp->resumeCond != NULL) ||
        (newQp->resumeCond != NULL && oldQp->resumeCond == NULL) ||
        (newQp->resumeCond != NULL && oldQp->resumeCond != NULL &&
         strcmp (newQp->resumeCond, oldQp->resumeCond)))
        return true;
    if ((newQp->stopCond == NULL && oldQp->stopCond != NULL) ||
        (newQp->stopCond != NULL && oldQp->stopCond == NULL) ||
        (newQp->stopCond != NULL && oldQp->stopCond != NULL &&
         strcmp (newQp->stopCond, oldQp->stopCond)))
        return true;

    if ((newQp->windows == NULL && oldQp->windows != NULL) ||
        (newQp->windows != NULL && oldQp->windows == NULL) ||
        (newQp->windows != NULL && oldQp->windows != NULL &&
         strcmp (newQp->windows, oldQp->windows)))
        return true;

    if (newQp->nice != oldQp->nice ||
        newQp->mig != oldQp->mig)
        return TRUE;

    if (newQp->rLimits[LSF_RLIMIT_RUN] != oldQp->rLimits[LSF_RLIMIT_RUN] ||
        newQp->rLimits[LSF_RLIMIT_CPU] != oldQp->rLimits[LSF_RLIMIT_CPU])
        return TRUE;

    return FALSE;

}

void
updHostList(void)
{
    struct qData *qPtr;
    struct uData *uPtr;
    struct hData *hPtr;
    sTab stab;
    hEnt *e;
    int cc;

    ls_syslog(LOG_DEBUG, "\
%s: Entering this routine...", __func__);

    if (hostList) {
        FREEUP(hostList->name);
        FREEUP(hostList);
    }

    hostList = listCreate("Host List");

    cc = 1;
    for (e = h_firstEnt_(&hostTab, &stab);
         e != NULL;
         e = h_nextEnt_(&stab)) {

        hPtr = (struct hData *)e->hData;

        /* These two type of hosts arent' used
         * for scheduling so don't include them in
         * the working list.
         */
        if (hPtr->hStatus & HOST_STAT_REMOTE)
            continue;

        if (hPtr->flags & HOST_LOST_FOUND)
            continue;

        hPtr->flags &=~HOST_UPDATE;
        hPtr->hStatus &= ~HOST_STAT_EXCLUSIVE;

        hPtr->hostId = cc;
        ++cc;

        /* Hopsa in da lista... To traverse the host list
         * in increasing hostId order, as bjobs expects,
         * go via the forward pointer.
         */
        listInsertEntryAtBack(hostList, (LIST_ENTRY_T *)hPtr);

        if (hPtr->maxJobs > 0 && hPtr->maxJobs < INFINIT_INT)
            hPtr->numCPUs = hPtr->maxJobs;

        if (hPtr->numCPUs > 0)
            numofprocs += hPtr->numCPUs;
        else
            numofprocs++;
    }

    if (hReasonTb != NULL) {
        FREEUP(hReasonTb[0]);
        FREEUP(hReasonTb[1]);
        FREEUP(hReasonTb);
    }
    /* Resize the reason table...
     */
    hReasonTb = my_calloc(2, sizeof(int *), __func__);
    hReasonTb[0] = my_calloc(numofhosts() + 1, sizeof(int), __func__);
    hReasonTb[1] = my_calloc(numofhosts() + 1, sizeof(int), __func__);

    /* Resize the other reason tables...
     */
    for (qPtr = qDataList->forw; qPtr != qDataList; qPtr = qPtr->forw) {

        FREEUP(qPtr->reasonTb);
        qPtr->reasonTb = my_calloc(2, sizeof(int *), __func__);
        qPtr->reasonTb[0] = my_calloc(numofhosts() + 1,
                                      sizeof(int), __func__);
        qPtr->reasonTb[1] = my_calloc(numofhosts() + 1,
                                      sizeof(int), __func__);
    }

    e = h_firstEnt_(&uDataList, &stab);
    while (e) {

        uPtr = e->hData;
        FREEUP(uPtr->reasonTb);
        uPtr->reasonTb = my_calloc(2, sizeof(int *), __func__);
        uPtr->reasonTb[0] = my_calloc(numofhosts() + 1,
                                      sizeof(int), __func__);
        uPtr->reasonTb[1] = my_calloc(numofhosts() + 1,
                                      sizeof(int), __func__);
        e = h_nextEnt_(&stab);
    }

    checkHWindow();
}

static void
updQueueList(void)
{
    int list, allHosts = FALSE;
    struct qData *qp;
    struct qData *lost_and_found = NULL;
    struct jData *jpbw;

    /* Sort by increasing priority
     */
    sort_queues();

    for (qp = qDataList->forw; qp != qDataList; qp = qp->forw) {

        if (strcmp (qp->queue, LOST_AND_FOUND) == 0)
            continue;

        if (qp->flags & QUEUE_UPDATE) {
            qp->flags &= ~QUEUE_UPDATE;
            continue;
        }

        if (lost_and_found == NULL
            && (lost_and_found = getQueueData(LOST_AND_FOUND)) == NULL)
            lost_and_found = lostFoundQueue();

        for (list = SJL; list < FJL; list++) {

            for (jpbw = jDataList[list]->back;
                 jpbw != jDataList[list];
                 jpbw = jpbw->back) {

                if (jpbw->qPtr != qp)
                    continue;

                updSwitchJob(jpbw,
                             qp,
                             lost_and_found,
                             jpbw->shared->jobBill.maxNumProcessors);
            }
        }
    }

    numofqueues = 0;
    for (qp = qDataList->forw; qp != qDataList; qp = qp->forw) {
        parse_preemption(qp);
        queueHostsPF(qp, &allHosts);
        createQueueHostSet(qp);
        numofqueues++;
    }

    checkQWindow();

    check_same_priority_queues();
}

static void
updUserList(int mbdInitFlags)
{
    struct uData *defUser;
    struct gData *gPtr;
    int i;
    hEnt *ent;
    sTab next;

    defUser = getUserData ("default");

    if (defUser && !(defUser->flags & USER_UPDATE))
        defUser = NULL;

    ent = h_firstEnt_(&uDataList, &next);
    while (ent) {
        struct uData *uData;

        uData = (struct uData *)ent->hData;

        if (mbdInitFlags == RECONFIG_CONF
            || mbdInitFlags == WINDOW_CONF) {
            int j;
            FREEUP( uData->reasonTb[0]);
            FREEUP( uData->reasonTb[1]);
            uData->reasonTb[0] = my_calloc(numLIMhosts + 2,
                                           sizeof(int), __func__);
            uData->reasonTb[1] = my_calloc(numLIMhosts + 2,
                                           sizeof(int), __func__);

            for(j = 0; j < numLIMhosts+2; j++) {
                uData->reasonTb[0][j] = 0;
                uData->reasonTb[1][j] = 0;
            }
        }

        if (uData->flags & USER_UPDATE) {

            if ((gPtr= getUGrpData (uData->user)) != NULL) {
                if (mbdInitFlags != FIRST_START &&
                    uData->numJobs == 0 &&
                    uData->numPEND   == 0 &&
                    uData->numRUN  == 0 &&
                    uData->numSSUSP == 0 &&
                    uData->numUSUSP  == 0 &&
                    uData->numRESERVE  == 0)

                    addUGDataValues (uData, gPtr);
            }

            if (uData != defUser)
                uData->flags &= ~USER_UPDATE;
            ent = h_nextEnt_(&next);
            continue;

        } else if (getpwnam (uData->user) != NULL) {

            if (defUser != NULL) {

                uData->pJobLimit = defUser->pJobLimit;
                uData->maxJobs   = defUser->maxJobs;
            } else {
                uData->pJobLimit = INFINIT_FLOAT;
                uData->maxJobs   = INFINIT_INT;
            }

        } else {

            if (mbdInitFlags != RECONFIG_CONF && mbdInitFlags != WINDOW_CONF) {

                FREEUP (uData->user);

                FREEUP(uData->reasonTb[0]);
                FREEUP(uData->reasonTb[1]);
                FREEUP(uData->reasonTb);
                setDestroy(uData->ancestors);
                uData->ancestors = NULL;
                setDestroy(uData->parents);
                uData->parents = NULL;
                setDestroy(uData->children);
                uData->children = NULL;
                setDestroy(uData->descendants);
                uData->descendants = NULL;

                if (uData->hAcct)
                    h_delTab_(uData->hAcct);
                h_delEnt_(&uDataList, ent);
            }
        }
        ent = h_nextEnt_(&next);
    }

    uDataGroupCreate();

    if (mbdInitFlags == RECONFIG_CONF || mbdInitFlags == WINDOW_CONF) {
        struct uData*  uData;

        for (i = 0; i < numofugroups; i++) {

            uData = getUserData (usergroups[i]->group);

            if (defUser != NULL) {

                if(uData->maxJobs == INFINIT_INT){
                    uData->maxJobs = defUser->maxJobs;
                }
                if(uData->pJobLimit == INFINIT_FLOAT){
                    uData->pJobLimit = defUser->pJobLimit;
                }

            }
        }
    }
}


static void
addUGDataValues (struct uData *gUData, struct gData *gPtr)
{
    static char fname[] = "addUGDataValues()";
    int i, numMembers;
    char **groupMembers;
    struct uData *uPtr;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG, "addUGDataValues: Enter the routine...");

    groupMembers = expandGrp(gPtr, gUData->user, &numMembers);

    if (numMembers == 1
        && strcmp(gUData->user, groupMembers[0]) == 0) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6215,
                                         "%s: User group <%s> with no members is ignored"),/* catgets 6215 */
                  fname, gUData->user);
    } else {
        for (i = 0; i < numMembers; i++) {
            if ((uPtr = getUserData (groupMembers[i])) == NULL)
                continue;
            addUGDataValues1 (gUData, uPtr);
        }
        FREEUP(groupMembers);
    }
}
static void
addUGDataValues1 (struct uData *gUData, struct uData *uData)
{
    sTab hashSearchPtr;
    hEnt *hashEntryPtr;
    struct hostAcct *hAcct;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG, "addUGDataValues1: Enter the routine...");

    gUData->numJobs += uData->numJobs;
    gUData->numPEND += uData->numPEND;
    gUData->numRUN += uData->numRUN;
    gUData->numSSUSP += uData->numSSUSP;
    gUData->numUSUSP += uData->numUSUSP;
    gUData->numRESERVE += uData->numRESERVE;

    if (uData->hAcct == NULL)
        return;

    hashEntryPtr = h_firstEnt_(uData->hAcct, &hashSearchPtr);
    while (hashEntryPtr) {
        hAcct = (struct hostAcct *)hashEntryPtr->hData;
        hashEntryPtr = h_nextEnt_(&hashSearchPtr);
        addHAcct (&gUData->hAcct, hAcct->hPtr, hAcct->numRUN, hAcct->numSSUSP,
                  hAcct->numUSUSP, hAcct->numRESERVE);
    }
}

static int
parseQHosts(struct qData *qp, char *hosts)
{
    static char fname[] = "parseQHosts";
    int i;
    int  numAskedHosts = 0;
    struct askedHost *returnHosts;
    int returnErr, badReqIndx, others, numReturnHosts = 0;
    char *sp, *word, **askedHosts;

    if (qp == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6216,
                                         "%s: NULL pointer for queue"), fname); /* catgets 6216 */
        return -1;
    }
    if (hosts == NULL) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6217,
                                         "%s: NULL pointer in HOSTS for queue <%s>"), /* catgets 6217 */
                  fname, qp->queue);
        return -1;
    }

    sp = hosts;

    while ((word=getNextWord_(&sp)) != NULL)
        numAskedHosts++;
    if (numAskedHosts == 0) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6218,
                                         "%s: No host specified in HOSTS for queue <%s>"), /* catgets 6218 */
                  fname, qp->queue);
        return -1;
    }
    askedHosts = (char **) my_calloc(numAskedHosts, sizeof(char *), fname);
    sp = hosts;
    numAskedHosts = 0;
    while ((word=getNextWord_(&sp)) != NULL) {
        askedHosts[numAskedHosts++] = safeSave(word);
    }

    returnErr = chkAskedHosts(numAskedHosts, askedHosts, numofprocs,
                              &numReturnHosts, &returnHosts, &badReqIndx, &others, 0);
    if (returnErr == LSBE_NO_ERROR) {
        if (numReturnHosts > 0) {
            qp->askedPtr = (struct askedHost *) my_calloc (numReturnHosts,
                                                           sizeof(struct askedHost), fname);
            for (i = 0; i < numReturnHosts; i++) {
                qp->askedPtr[i].hData = returnHosts[i].hData;
                qp->askedPtr[i].priority = returnHosts[i].priority;
                if (qp->askedPtr[i].priority > 0)
                    qp->qAttrib |= Q_ATTRIB_HOST_PREFER;
            }

            qp->numAskedPtr = numReturnHosts;
            qp->askedOthPrio = others;
            FREEUP (returnHosts);
        }
    } else if (parseFirstHostErr(returnErr, fname, hosts, qp, returnHosts, numReturnHosts)) {
        ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd , NL_SETN, 6219,
                                         "%s: No valid host used by the batch system is specified in HOSTS <%s> for queue <%s>"), /* catgets 6219 */
                  fname, hosts, qp->queue);
    }
    for (i = 0; i <numAskedHosts; i++)
        FREEUP (askedHosts[i]);
    FREEUP (askedHosts);

    if( returnErr == LSBE_NO_ERROR ) {
        return numReturnHosts;
    } else {
        return -1;
    }
}

static void
fillClusterConf (struct clusterConf *clusterConf)
{

    clusterConf->clinfo = clusterInfo;
    clusterConf->numHosts = numLIMhosts;
    clusterConf->hosts = LIMhosts;

}

static void
fillSharedConf(struct sharedConf *sConf)
{
    static char     fname[] = "fillSharedConf";

    sConf->lsinfo = allLsInfo;
    sConf->servers = NULL;

    sConf->clusterName = my_malloc(sizeof (char *), fname);
}

static void
freeGrp(struct gData *grpPtr)
{
    if (grpPtr == NULL)
        return;

    h_delTab_(&grpPtr->memberTab);
    if (grpPtr->group && grpPtr->group[0] != '\0')
        FREEUP(grpPtr->group);
    FREEUP(grpPtr->group_slots);
    FREEUP(grpPtr);
}

static  int
validHostSpec(char *hostSpec)
{
    if (hostSpec == NULL)
        return FALSE;

    if (getModelFactor (hostSpec) == NULL) {
        if (getHostFactor (hostSpec) == NULL)
            return FALSE;
    }
    return TRUE;
}

static void
getMaxCpufactor(void)
{
    struct hData *hPtr;

    for (hPtr = (struct hData *)hostList->back;
         hPtr != (void *)hostList;
         hPtr = (struct hData *)hPtr->back) {

        if (hPtr->cpuFactor > maxCpuFactor)
            maxCpuFactor = hPtr->cpuFactor;
    }
}

static int
parseFirstHostErr(int returnErr,
                  char *fname,
                  char *hosts,
                  struct qData *qp,
                  struct askedHost *returnHosts,
                  int numReturnHosts)
{
    int i;

    if (returnErr == LSBE_MULTI_FIRST_HOST
        ||returnErr == LSBE_HG_FIRST_HOST
        ||returnErr == LSBE_HP_FIRST_HOST
        ||returnErr == LSBE_OTHERS_FIRST_HOST) {

        if (returnErr == LSBE_MULTI_FIRST_HOST )
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd, NL_SETN, 6242, "%s: Multiple first execution hosts specified <%s> in HOSTS section for queue <%s>. Ignored."),/*catgets 6242 */
                      fname, hosts, qp->queue);
        else if ( returnErr == LSBE_HG_FIRST_HOST )
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd, NL_SETN, 6243, "%s: host group <%s> specified as first execution host in HOSTS section for queue <%s>. Ignored."), /* catgets 6243 */
                      fname, hosts, qp->queue);
        else if ( returnErr == LSBE_HP_FIRST_HOST )
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd, NL_SETN, 6244, "%s: host partition <%s> specified as first execution hosts specified <%s> in HOSTS section for queue <%s>. Ignored."), /* catgets 6244 */
                      fname, hosts, qp->queue);
        else if ( returnErr == LSBE_OTHERS_FIRST_HOST )
            ls_syslog(LOG_ERR, _i18n_msg_get(ls_catd, NL_SETN, 6245, "%s: \"others\" specified as first execution host specified in HOSTS section for queue <%s>. Ignored."), /* catgets 6245 */
                      fname, qp->queue);

        if (returnHosts) FREEUP (returnHosts);
        qp->numAskedPtr = numReturnHosts;
        if (numReturnHosts > 0 ) {
            qp->askedPtr = my_calloc (numReturnHosts,
                                      sizeof(struct askedHost), fname);
            for (i = 0; i < numReturnHosts; i++) {
                qp->askedPtr[i].hData = returnHosts[i].hData;
                qp->askedPtr[i].priority = returnHosts[i].priority;
                if (qp->askedPtr[i].priority > 0)
                    qp->qAttrib |= Q_ATTRIB_HOST_PREFER;
            }
        } else {
            qp->askedPtr = NULL;
            qp->askedOthPrio = 0;
            (*hosts) = '\0';
        }
        return 0;
    }
    return 1;
}

/* init_fairshare_scheduler()
 */
static int
init_fairshare_scheduler(void)
{
    char buf[PATH_MAX];
    struct qData *qPtr;
    int cc;

    if (! is_fairplugin_ok())
        return -1;

    sprintf(buf, "\
%s/libfairshare.so", daemonParams[LSF_LIBDIR].paramValue);

    for (qPtr = qDataList->forw;
         qPtr != (void *)qDataList;
         qPtr = qPtr->forw) {

        if (qPtr->fairshare == NULL)
            continue;

        qPtr->fsSched = load_fair_plugin(qPtr, buf);
        if (qPtr->fsSched == NULL) {
            ls_syslog(LOG_ERR, "\
%s: failed loading fairshare plugin, fall back to fcfs", __func__);
            FREEUP(qPtr->fairshare);
            qPtr->qAttrib &= ~Q_ATTRIB_FAIRSHARE;
            continue;
        }
        /* invoke the plugin initializer
         */
        cc = (*qPtr->fsSched->fs_init)(qPtr, userConf);
        if (cc < 0) {
            ls_syslog(LOG_ERR, "\
%s: failed initializing fairshare plugin, fall back to fcfs", __func__);
            tree_free(qPtr->fsSched->tree, free_sacct);
            dlclose(qPtr->fsSched->handle);
            _free_(qPtr->fsSched->name);
            _free_(qPtr->fsSched);
            _free_(qPtr->fairshare);
            qPtr->qAttrib &= ~Q_ATTRIB_FAIRSHARE;
            return -1;
        }

        qPtr->numFairSlots = getQueueSlots(qPtr);
        (*qPtr->fsSched->fs_init_sched_session)(qPtr);
    }

    return 0;
}

/* load_fair_plugin()
 */
static struct fair_sched *
load_fair_plugin(struct qData *qPtr,
                 const char *file)
{
    struct fair_sched *f;

    f = calloc(1, sizeof(struct fair_sched));

    f->handle = dlopen(file, RTLD_NOW);
    if (f->handle == NULL) {
        ls_syslog(LOG_ERR, "\
%s: gudness cannot open %s: %s", __func__, file, dlerror());
        return NULL;
    }

    /* now check for scheduler symbols.
     */
    f->fs_init = dlsym(f->handle, "fs_init");
    if (f->fs_init == NULL) {
        ls_syslog(LOG_ERR, "%s: ohmygosh missing fs_init() symbol in %s: %s",
                  __func__, file, dlerror());
        dlclose(f->handle);
        free(f);
        return NULL;
    }

    f->fs_update_sacct = dlsym(f->handle, "fs_update_sacct");
    if (f->fs_update_sacct == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_update_sacct() symbol in %s: %s", __func__,
                  file, dlerror());
        dlclose(f->handle);
        free(f);
        return NULL;
    }

    f->fs_init_sched_session = dlsym(f->handle, "fs_init_sched_session");
    if (f->fs_init_sched_session == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_init_sched_session() symbol in %s: %s", __func__,
                  file, dlerror());
        dlclose(f->handle);
        free(f);
        return NULL;
    }

    f->fs_elect_job = dlsym(f->handle, "fs_elect_job");
    if (f->fs_elect_job == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_elect_job() symbol in %s: %s", __func__,
                  file, dlerror());
        dlclose(f->handle);
        free(f);
        return NULL;
    }

    f->fs_fin_sched_session = dlsym(f->handle, "fs_fin_sched_session");
    if (f->fs_fin_sched_session == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_fin_sched_session() symbol in %s: %s", __func__,
                  file, dlerror());
        dlclose(f->handle);
        free(f);
        return NULL;
    }

    f->fs_get_saccts = dlsym(f->handle, "fs_get_saccts");
    if (f->fs_get_saccts == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_get_saccts() symbol in %s: %s", __func__,
                  file, dlerror());
        dlclose(f->handle);
        free(f);
        return NULL;
    }

    f->fs_own_init = dlsym(f->handle, "fs_own_init");
    if (f->fs_own_init == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_own_init() symbol in %s: %s", __func__,
                  file, dlerror());
        dlclose(f->handle);
        free(f);
        return NULL;
    }

    f->fs_init_own_sched_session = dlsym(f->handle, "fs_init_own_sched_session");
    if (f->fs_init_own_sched_session == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_init_own_sched_session() symbol in %s: %s", __func__,
                  file, dlerror());
        dlclose(f->handle);
        free(f);
        return NULL;
    }

    f->fs_decay_ran_time = dlsym(f->handle, "fs_decay_ran_time");
    if (f->fs_decay_ran_time == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_decay_ran_time() symbol in %s: %s", __func__,
                  file, dlerror());
        dlclose(f->handle);
        free(f);
        return NULL;
    }

    f->name = strdup(qPtr->queue);

    return f;
}

/* getQueueSlots()
 *
 * Get the global number of slots to be used
 * in computing the fairshare values.
 *
 */
static uint32_t
getQueueSlots(struct qData *qPtr)
{
    struct hData *hPtr;
    uint32_t numSlots;

    numSlots = 0;
    for (hPtr = (struct hData *)hostList->back;
         hPtr != (void *)hostList;
         hPtr = hPtr->back) {

        if (!isHostQMember(hPtr, qPtr))
            continue;

        numSlots = numSlots + hPtr->maxJobs;
    }

    return numSlots;
}

/* parse_preemption()
 *
 * This function expects the queue list
 * to be sorted.
 */
static int
parse_preemption(struct qData *qPtr)
{
    char *p;
    char *p2;
    char *p0;
    int i;
    struct qData *qPtr2;

    if (! (qPtr->qAttrib & Q_ATTRIB_PREEMPTIVE))
        return 0;

    i = 0;
    p0 = p = strdup(qPtr->preemption);
    p = strcasestr(p0, "PREEMPTIVE");
    if (p == NULL) {
        ls_syslog(LOG_ERR, "\
%s: Ohmygosh keyword PREEMPTIVE missing in %s queue %s", __func__,
                  qPtr->preemption, qPtr->queue);
        _free_(qPtr->preemption);
        _free_(p0);
        qPtr->qAttrib &= ~Q_ATTRIB_PREEMPTIVE;
        return -1;
    }

    p = p + strlen("PREEMPTIVE");
    while (p[i]) {
        if (p[i] == '['
            || p[i] == ']')
            p[i] = ' ';
        ++i;
    }

    qPtr->preemptable = make_link();
    while ((p2 = getNextWord_(&p))) {

        for (qPtr2 = qDataList->forw;
             qPtr2 != (void *)qDataList;
             qPtr2 = qPtr2->forw) {

            if (strcasecmp(qPtr2->queue, p2) == 0)
                break;
        }

        if (qPtr2 == qDataList) {
            ls_syslog(LOG_ERR, "\
%s: queue %s not found preemption for queue %s disbled",
                      __func__, p2, qPtr->queue);
            fin_link(qPtr->preemptable);
            qPtr->preemptable = NULL;
            qPtr->qAttrib &= ~Q_ATTRIB_PREEMPTIVE;
            FREEUP(qPtr->preemption);
            FREEUP(p0);
            return -1;
        }
        enqueue_link(qPtr->preemptable, qPtr2);
    }

    _free_(p0);
    ls_syslog(LOG_DEBUG, "\
%s: preeption for queue %s built %s", __func__,
              qPtr->queue, qPtr->preemption);

    return 0;
}

/* sort_queues()
 */
static int
sort_queues(void)
{
    struct qData *qPtr;
    struct qData **v;
    int n;
    int i;
    LIST_T *l;

    n = 0;
    for (qPtr = qDataList->back;
         qPtr != (void *)qDataList;
         qPtr = qPtr->back)
        ++n;

    v = calloc(n, sizeof(struct qData *));
    l = (LIST_T *)qDataList;

    i = 0;
    while ((qPtr = (struct qData *)listPop(l))) {
        v[i] = qPtr;
        ++i;
    }

    qsort(v, n, sizeof(struct qData *), queue_cmp);

    for (i = 0; i < n; i++) {
        qPtr = v[i];
        listInsertEntryAtFront(l, (LIST_ENTRY_T *)qPtr);
    }

    _free_(v);

    return 0;
}

/* queue_cmp()
 */
static int
queue_cmp(const void *x, const void *y)
{
    struct qData *q1;
    struct qData *q2;

    q1 = *(struct qData **)x;
    q2 = *(struct qData **)y;

    if (q1->priority > q2->priority)
        return 1;
    if (q1->priority < q2->priority)
        return -1;
    return 0;
}

/* check_same_priority_queues()
 *
 * Disable preemption for queues with
 * the same priority.
 */
static void
check_same_priority_queues(void)
{
    struct qData *qPtr;
    struct qData *qPtr2;

    qPtr = qDataList->forw;
    while (qPtr != qDataList->forw) {

        qPtr2 = qPtr->forw;

        if (qPtr->priority == qPtr2->priority) {

            if (qPtr->qAttrib & Q_ATTRIB_PREEMPTIVE) {
                qPtr->qAttrib &= ~Q_ATTRIB_PREEMPTIVE;
                FREEUP(qPtr->preemption);
                fin_link(qPtr->preemptable);
                ls_syslog(LOG_ERR, "\
%s: queue %s %d same priority as queue %s %d cannot use preemtion",
                          __func__, qPtr->queue, qPtr->priority,
                          qPtr2->queue, qPtr2->priority);
            }
            if (qPtr2->qAttrib & Q_ATTRIB_PREEMPTIVE) {
                qPtr2->qAttrib &= ~Q_ATTRIB_PREEMPTIVE;
                FREEUP(qPtr2->preemption);
                fin_link(qPtr2->preemptable);
                ls_syslog(LOG_ERR, "\
%s: queue %s same priority %d as queue %s %d cannot use preemtion",
                          __func__, qPtr->queue, qPtr->priority,
                          qPtr2->queue, qPtr2->priority);
            }
        }
        qPtr = qPtr2;
    }
}

/* init_preemption_scheduler()
 */
static int
init_preemption_scheduler(void)
{
    struct qData *qPtr;
    bool_t has_preemption = false;

    has_slot_preemption_ = false;

    if (! is_preemptplugin_ok())
        return -1;

    for (qPtr = qDataList->forw;
         qPtr != qDataList;
         qPtr = qPtr->forw) {

        if (! (qPtr->qAttrib & Q_ATTRIB_PREEMPTIVE))
            continue;

        if (load_preempt_plugin(qPtr) < 0) {
            ls_syslog(LOG_ERR, "\
%s: load_preemption_plugin() failed, no preemption in the system",
                      __func__);
        }
        has_preemption = true;
    }

    if (has_preemption
        && mbdParams->preemptableResources) {
        has_slot_preemption_ = false;
        ls_syslog(LOG_INFO, "\
%s: system has preemption, preemtable reesources %s, no slot preemption",
                 __func__, mbdParams->preemptableResources);
    }

    if (has_preemption
        && mbdParams->preemptableResources == NULL) {
        has_slot_preemption_ = true;
        ls_syslog(LOG_INFO, "\
%s: system has slots based preemption", __func__);
    }

    return 0;
}

/* has_slot_preemption()
 *
 * Do we have preemption defined in the scheduler.
 */
bool_t
has_slot_preemption(void)
{
    return has_slot_preemption_;
}

/* load_preempt_plugin()
 */
static int
load_preempt_plugin(struct qData *qPtr)
{
    char buf[PATH_MAX];
    struct prm_sched *p;

    p = qPtr->prmSched = calloc(1, sizeof(struct fair_sched));
    qPtr->prmSched->name = strdup(qPtr->queue);

    sprintf(buf, "\
%s/libpreempt.so", daemonParams[LSF_LIBDIR].paramValue);

    p->handle = dlopen(buf, RTLD_NOW);
    if (p->handle == NULL) {
        ls_syslog(LOG_ERR, "\
%s: gudness cannot open %s: %s", __func__, buf, dlerror());
        _free_(qPtr->prmSched->name);
        _free_(qPtr->prmSched);
        qPtr->qAttrib &= ~Q_ATTRIB_PREEMPTIVE;
        return -1;
    }

    p->prm_init = dlsym(p->handle, "prm_init");
    if (p->prm_init == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_init() symbol in %s: %s",
                  __func__, buf, dlerror());
        dlclose(p->handle);
        _free_(qPtr->prmSched->name);
        free(qPtr->prmSched);
        qPtr->qAttrib &= ~Q_ATTRIB_PREEMPTIVE;
        return -1;
    }

    p->prm_elect_preempt = dlsym(p->handle, "prm_elect_preempt");
    if (p->prm_elect_preempt == NULL) {
        ls_syslog(LOG_ERR, "\
%s: ohmygosh missing fs_init() symbol in %s: %s",
                  __func__, buf, dlerror());
        dlclose(p->handle);
        _free_(qPtr->prmSched->name);
        free(qPtr->prmSched);
        qPtr->qAttrib &= ~Q_ATTRIB_PREEMPTIVE;
        return -1;
    }

    return 0;
}

/* init_ownersip_scheduler()
 */
static int
init_ownership_scheduler(void)
{
    char buf[PATH_MAX];
    struct qData *qPtr;
    int cc;

    if (! is_ownplugin_ok())
        return -1;

    sprintf(buf, "\
%s/libfairshare.so", daemonParams[LSF_LIBDIR].paramValue);

    for (qPtr = qDataList->forw;
         qPtr != (void *)qDataList;
         qPtr = qPtr->forw) {

        if (qPtr->ownership == NULL)
            continue;

        qPtr->own_sched = load_fair_plugin(qPtr, buf);
        if (qPtr->own_sched == NULL) {
            ls_syslog(LOG_ERR, "\
%s: failed loading ownership plugin, fall back to fcfs", __func__);
            _free_(qPtr->ownership);
            qPtr->qAttrib &= ~Q_ATTRIB_OWNERSHIP;
            continue;
        }

        cc = (*qPtr->own_sched->fs_own_init)(qPtr, userConf);
        if (cc < 0) {
            ls_syslog(LOG_ERR, "\
%s: failed initializing fairshare plugin, fall back to fcfs", __func__);
            tree_free(qPtr->own_sched->tree, free_sacct);
            dlclose(qPtr->own_sched->handle);
            _free_(qPtr->own_sched->name);
            _free_(qPtr->own_sched);
            _free_(qPtr->ownership);
            qPtr->qAttrib &= ~Q_ATTRIB_OWNERSHIP;
            return -1;
        }

        /* Keep both fairshare slots and owned slots as they
         * are used in the 2 different algorithms we run
         * in the queue.
         */
        qPtr->numFairSlots = qPtr->num_owned_slots = getQueueSlots(qPtr);
        if (! check_ownership(qPtr)) {
            ls_syslog(LOG_ERR, "\
%s: check_ownership() failed, fall back to fcfs", __func__);
            /* dlclose(qPtr->own_sched->handle);
             */
            tree_free(qPtr->own_sched->tree, free_sacct);
            dlclose(qPtr->own_sched->handle);
            _free_(qPtr->own_sched->name);
            _free_(qPtr->own_sched);
            _free_(qPtr->ownership);
            qPtr->qAttrib &= ~Q_ATTRIB_OWNERSHIP;
            return -1;
        }
        (*qPtr->own_sched->fs_init_own_sched_session)(qPtr);
    }

    return 0;
}

/* is_fairplugin_ok()
 */
static bool_t
is_fairplugin_ok(void)
{
    struct qData *qPtr;

    if (daemonParams[LSF_LIBDIR].paramValue)
        return true;

    ls_syslog(LOG_ERR, "\
%s: LSF_LIBDIR not defined in lsf.conf, no fairshare possible",
              __func__);

    for (qPtr = qDataList->forw;
         qPtr != (void *)qDataList;
         qPtr = qPtr->forw) {

        if (qPtr->fairshare) {
            _free_(qPtr->fairshare);
            qPtr->qAttrib &= ~Q_ATTRIB_FAIRSHARE;
            ls_syslog(LOG_ERR, "\
%s: queue %s fairshare disabled", __func__, qPtr->queue);
        }
    }

    return false;
}

static bool_t
is_preemptplugin_ok(void)
{
    struct qData *qPtr;

    if (daemonParams[LSF_LIBDIR].paramValue)
        return true;

    ls_syslog(LOG_ERR, "\
%s: LSF_LIBDIR not defined in lsf.conf, no preemption possible",
              __func__);

    for (qPtr = qDataList->forw;
         qPtr != (void *)qDataList;
         qPtr = qPtr->forw) {

        if (qPtr->qAttrib & Q_ATTRIB_PREEMPTIVE) {
            qPtr->qAttrib &= ~Q_ATTRIB_PREEMPTIVE;
            ls_syslog(LOG_ERR, "\
%s: queue %s preemption disabled", __func__, qPtr->queue);
        }
    }

    return false;
}

static bool_t
is_ownplugin_ok(void)
{
    struct qData *qPtr;

    if (daemonParams[LSF_LIBDIR].paramValue)
        return true;

    ls_syslog(LOG_ERR, "\
%s: LSF_LIBDIR not defined in lsf.conf, no ownership possible",
              __func__);

    for (qPtr = qDataList->forw;
         qPtr != (void *)qDataList;
         qPtr = qPtr->forw) {

        if (qPtr->ownership) {
            _free_(qPtr->ownership);
            qPtr->qAttrib &= ~Q_ATTRIB_OWNERSHIP;
            ls_syslog(LOG_ERR, "\
%s: queue %s ownership disabled", __func__, qPtr->queue);
        }
    }

    return false;
}

/* check_ownership()
 *
 * Sum up all the slots in the root
 * group and make sure they do not exceed
 * the total number of configured ownership.
 */

static bool_t
check_ownership(struct qData *qPtr)
{
    struct tree_node_ *n;
    struct share_acct *sacct;
    int sum;

    sum = 0;
    n = qPtr->own_sched->tree->root->child;
    while (n) {
        sacct = n->data;
        if (logclass & LC_TRACE) {
            ls_syslog(LOG_INFO, "\
%s: group %s num owned slots %d sum %d", __func__, n->name,
                      sacct->shares, sum);
        }
        sum = sum + sacct->shares;
        n = n->right;
    }

    if (sum > qPtr->num_owned_slots) {
        ls_syslog(LOG_ERR, "\
%s: the sum of owned slots by root groups %d > total slots %d, not allowed",
                  __func__, sum, qPtr->num_owned_slots);
        return false;
    }

    return true;
}

/* add_host_dedicated_res()
 */
static void
add_host_dedicated_res(struct hData *hPtr, struct hostInfo *lsf)
{
    int cc;
    hEnt *ent;

    if (! daemonParams[MBD_DEDICATED_RESOURCES].paramValue)
        return;

    for (cc = 0; cc < lsf->nRes; cc++) {
        /* This resource giving us from openlava base
         * for this host, is also configured as dedicated
         * resources in lsf.conf. Remember if in this host.
         */
        if (! (ent = h_getEnt_(d_res_tab, lsf->resources[cc])))
            continue;

        if (logclass & LC_TRACE) {
            ls_syslog(LOG_INFO, "\
%s: resource %s is dedicated on host %s", __func__, lsf->resources[cc],
                      hPtr->host);
        }

        ent = h_addEnt_(hPtr->dres_tab, strdup(lsf->resources[cc]), NULL);
        ent->hData = strdup(lsf->resources[cc]);
    }
}
