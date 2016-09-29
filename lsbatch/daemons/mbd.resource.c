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

#include <stdlib.h>
#include <sys/types.h>
#include <math.h>
#include "mbd.h"

#include "../../lsf/lib/lsi18n.h"
#define NL_SETN         10

static void addSharedResource (struct lsSharedResourceInfo *);
static void addInstances (struct lsSharedResourceInfo *, struct sharedResource *);
static void freeHostInstances (void);
static void initHostInstances (int);
static int copyResource (struct lsbShareResourceInfoReply *,
                         struct sharedResource *, char *);

struct objPRMO *pRMOPtr = NULL;

static void initQPRValues(struct qPRValues *, struct qData *);
static void freePreemptResourceInstance(struct preemptResourceInstance *);
static void freePreemptResource(struct preemptResource *);
static int addPreemptableResource(int);
static struct preemptResourceInstance * findPRInstance(int, struct hData *);
static struct qPRValues * findQPRValues(int, struct hData *, struct qData *);
static struct qPRValues * addQPRValues(int, struct hData *, struct qData *);
static float roundFloatValue (float);

static float
checkOrTakeAvailableByPreemptPRHQValue(int index,
                                       float value,
                                       struct hData *hPtr,
                                       struct qData *qPtr,
                                       int update);
static void compute_group_slots(int, struct lsSharedResourceInfo *);
static int get_group_slots(struct gData *);
/* num_tokens and the tokens array are global variable the
 * glb functions work with.
 */
static int num_tokens;
static struct mbd_token *tokens;
static void get_glb_tokens(void);
static void add_tokens(struct lsSharedResourceInfo *, int);
static float get_job_tokens(struct resVal *, struct mbd_token *, struct jData *);
static int compute_used_tokens(struct mbd_token *);
static int glb_get_more_tokens(const char *);
static int glb_release_tokens(struct mbd_token *, int);
static int preempt_jobs_for_tokens(struct mbd_token *);
static struct mbd_token *is_a_token(const char *);
static char old_path[PATH_MAX];
static char new_path[PATH_MAX];
static int save_glb_allocation_state(void);
static void copy_glb_tokens(struct glb_token *, int);
static void free_mbd_tokens(void);

void
getLsbResourceInfo(void)
{
    int i;
    int numRes;
    struct lsSharedResourceInfo *resourceInfo;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG3, "%s: Entering ..." , __func__);

    numRes = 0;
    resourceInfo = ls_sharedresourceinfo(NULL, &numRes, NULL, 0);
    if (resourceInfo == NULL) {
        return;
    }

    /* tokens is a global data structure
     * manage by those routines.
     */
    get_glb_tokens();

    save_glb_allocation_state();

    if (numResources > 0)
        freeSharedResource();

    initHostInstances(numRes);

    add_tokens(resourceInfo, numRes);

    compute_group_slots(numRes, resourceInfo);

    for (i = 0; i < numRes; i++)
        addSharedResource(&resourceInfo[i]);
}


static void
addSharedResource(struct lsSharedResourceInfo *lsResourceInfo)
{
    struct sharedResource *resource;
    struct sharedResource **temp;

    if (lsResourceInfo == NULL)
        return;

    if (getResource(lsResourceInfo->resourceName) != NULL) {
        if (logclass & LC_TRACE)
            ls_syslog (LOG_DEBUG3, "\
%s: More than one resource %s found in lsResourceInfo from lim; ignoring later",
                       __func__, lsResourceInfo->resourceName);
        return;
    }

    resource =  my_malloc(sizeof (struct sharedResource), __func__);
    resource->numInstances = 0;
    resource->instances = NULL;
    resource->resourceName = safeSave (lsResourceInfo->resourceName);

    addInstances(lsResourceInfo, resource);

    if (numResources == 0)
        temp =  my_malloc (sizeof (struct sharedResource *), __func__);
    else
        temp = realloc(sharedResources,
                       (numResources + 1) *sizeof (struct sharedResource *));
    if (temp == NULL) {
        ls_syslog(LOG_ERR, "%s: realloc() failed: %m", __func__);
        mbdDie(MASTER_MEM);
    }

    sharedResources = temp;
    sharedResources[numResources] = resource;
    numResources++;
}

static void
addInstances(struct lsSharedResourceInfo *lsResourceInfo,
             struct sharedResource *resource)
{
    int i;
    int numHosts;
    int j;
    int numInst = 0;
    struct resourceInstance *instance;
    struct hData *hPtr;

    if (lsResourceInfo->nInstances <= 0)
        return;

    resource->instances = my_malloc(lsResourceInfo->nInstances
                                    * sizeof (struct resourceInstance *),
                                    __func__);

    for (i = 0; i < lsResourceInfo->nInstances; i++) {

        instance = my_malloc(sizeof (struct resourceInstance), __func__);
        instance->nHosts = 0;
        instance->lsfValue = NULL;
        instance->value = NULL;
        instance->resName = resource->resourceName;

        if (lsResourceInfo->instances[i].nHosts > 0) {
            instance->hosts = my_malloc(lsResourceInfo->instances[i].nHosts
                                        * sizeof (struct hData *), __func__);
        } else
            instance->hosts = NULL;

        numHosts = 0;
        for (j = 0; j < lsResourceInfo->instances[i].nHosts; j++) {
            hPtr = getHostData(lsResourceInfo->instances[i].hostList[j]);
            if (hPtr == NULL) {
                if (logclass & LC_TRACE)
                    ls_syslog (LOG_DEBUG3, "\
%s: Host <%s> is not used by the batch system;ignoring", __func__,
                               lsResourceInfo->instances[i].hostList[j]);
                continue;
            }

            if ((hPtr->hStatus & HOST_STAT_REMOTE)
                || (hPtr->flags & HOST_LOST_FOUND)) {
                continue;
            }

            hPtr->instances[hPtr->numInstances] = instance;
            hPtr->numInstances++;
            instance->hosts[numHosts] = hPtr;
            numHosts++;
        }

        instance->nHosts = numHosts;
        instance->lsfValue = safeSave(lsResourceInfo->instances[i].value);
        instance->value = safeSave(lsResourceInfo->instances[i].value);
        resource->instances[numInst++] = instance;
    }
    resource->numInstances = numInst;
}

void
freeSharedResource(void)
{
    int i;
    int j;

    if (numResources <= 0)
        return;

    for (i = 0; i < numResources; i++) {
        for (j = 0; j < sharedResources[i]->numInstances; j++) {
            FREEUP(sharedResources[i]->instances[j]->hosts);
            FREEUP(sharedResources[i]->instances[j]->lsfValue);
            FREEUP(sharedResources[i]->instances[j]->value);
            FREEUP(sharedResources[i]->instances[j]);
        }
        FREEUP(sharedResources[i]->instances);
        FREEUP(sharedResources[i]->resourceName);
        FREEUP(sharedResources[i]);
    }
    FREEUP (sharedResources);
    numResources = 0;

    freeHostInstances();
}

static void
freeHostInstances(void)
{
    sTab hashSearchPtr;
    hEnt *hashEntryPtr;
    struct hData *hData;

    hashEntryPtr = h_firstEnt_(&hostTab, &hashSearchPtr);
    while (hashEntryPtr) {
        hData = (struct hData *) hashEntryPtr->hData;
        hashEntryPtr = h_nextEnt_(&hashSearchPtr);

        if (hData->flags & HOST_LOST_FOUND)
            continue;

        if (hData->hStatus & HOST_STAT_REMOTE)
            continue;

        FREEUP(hData->instances);
        hData->numInstances = 0;
    }

}

static void
initHostInstances(int num)
{
    sTab hashSearchPtr;
    hEnt *hashEntryPtr;
    struct hData *hData;

    hashEntryPtr = h_firstEnt_(&hostTab, &hashSearchPtr);

    while (hashEntryPtr) {

        hData = (struct hData *)hashEntryPtr->hData;
        hashEntryPtr = h_nextEnt_(&hashSearchPtr);

        if (hData->flags & HOST_LOST_FOUND)
            continue;

        if (hData->hStatus & HOST_STAT_REMOTE)
            continue;

        hData->instances = my_calloc(num,
                                     sizeof (struct resourceInstance *),
                                     __func__);
        hData->numInstances = 0;
    }

}

struct sharedResource *
getResource(char *resourceName)
{
    int i;

    if (resourceName == NULL || numResources <= 0)
        return NULL;

    for (i = 0; i < numResources; i++) {
        if (!strcmp (sharedResources[i]->resourceName, resourceName))
            return sharedResources[i];
    }
    return NULL;

}

int
checkResources(struct resourceInfoReq *resourceInfoReq,
               struct lsbShareResourceInfoReply *reply)
{
    static char fname[] = "checkResources";
    int i, j, allResources = FALSE, found = FALSE, replyCode;
    struct hData *hPtr;
    struct hostent *hp;

    if (resourceInfoReq->numResourceNames == 1
        && !strcmp (resourceInfoReq->resourceNames[0], "")) {
        allResources = TRUE;
    }
    reply->numResources = 0;

    if (numResources == 0)
        return LSBE_NO_RESOURCE;

    if (resourceInfoReq->hostName == NULL
        || (resourceInfoReq->hostName
            && strcmp (resourceInfoReq->hostName, " ")== 0))
        hPtr = NULL;
    else {

        if ((hp = Gethostbyname_(resourceInfoReq->hostName)) == NULL) {
            return LSBE_BAD_HOST;
        }

        if ((hPtr = getHostData (hp->h_name)) == NULL) {
            ls_syslog(LOG_ERR, "\
%s: Host <%s>  is not used by the batch system",
                      fname, resourceInfoReq->hostName);
            return LSBE_BAD_HOST;
        }
    }

    reply->numResources = 0;
    reply->resources = my_malloc(numResources
                                  * sizeof (struct lsbSharedResourceInfo), fname);

    for (i = 0; i < resourceInfoReq->numResourceNames; i++) {
        found = FALSE;
        for (j = 0; j < numResources; j++) {
            if (allResources == FALSE
                 && (strcmp (resourceInfoReq->resourceNames[i],
                            sharedResources[j]->resourceName)))
                continue;
            found = TRUE;
            if ((replyCode = copyResource (reply, sharedResources[j],
                     (hPtr != NULL)?hPtr->host:NULL)) != LSBE_NO_ERROR) {
                return replyCode;
            }
            reply->numResources++;
            if (allResources == FALSE)
                break;
        }
        if (allResources == FALSE && found == FALSE) {

            reply->badResource = i;
            reply->numResources = 0;
            FREEUP(reply->resources);
            return LSBE_BAD_RESOURCE;
        }
        found = FALSE;
        if (allResources == TRUE)
            break;
    }
    return LSBE_NO_ERROR;

}

static int
copyResource (struct lsbShareResourceInfoReply *reply,
              struct sharedResource *resource, char *hostName)
{
    static char fname[] = "copyResource";
    int i, j, num, found = FALSE, numInstances;
    float rsvValue;
    char stringValue[12];

    num = reply->numResources;
    reply->resources[num].resourceName = resource->resourceName;


    reply->resources[num].instances
        = my_malloc(resource->numInstances
                    * sizeof (struct lsbSharedResourceInstance), fname);
    reply->resources[num].nInstances = 0;
    numInstances = 0;

    for (i = 0; i < resource->numInstances; i++) {
        if (hostName) {
            for (j = 0; j < resource->instances[i]->nHosts; j++) {
                if (strcmp (hostName, resource->instances[i]->hosts[j]->host))
                    continue;
                found = TRUE;
                break;
            }
        }
        if (hostName && found == FALSE)
            continue;

        found = FALSE;
        reply->resources[num].instances[numInstances].totalValue =
                     safeSave (resource->instances[i]->value);

        if ((rsvValue = atoi(resource->instances[i]->value)
             - atoi(resource->instances[i]->lsfValue)) < 0)
            rsvValue = -rsvValue;

        sprintf (stringValue, "%-10.1f", rsvValue);
        reply->resources[num].instances[numInstances].rsvValue =
                     safeSave (stringValue);

        reply->resources[num].instances[numInstances].hostList =
          (char **) my_malloc (resource->instances[i]->nHosts * sizeof (char *),                               fname);
        for (j = 0; j < resource->instances[i]->nHosts; j++) {
            reply->resources[num].instances[numInstances].hostList[j] =
                       safeSave(resource->instances[i]->hosts[j]->host);
        }
        reply->resources[num].instances[numInstances].nHosts =
                                resource->instances[i]->nHosts;
        numInstances++;
    }
    reply->resources[num].nInstances = numInstances;
    return LSBE_NO_ERROR;

}

float
getHRValue(char *resName,
           struct hData *hPtr,
           struct resourceInstance **instance)
{
    int i;

    for (i = 0; i < hPtr->numInstances; i++) {

        if (strcmp(hPtr->instances[i]->resName, resName) != 0)
            continue;

        *instance = hPtr->instances[i];
        if (strcmp(hPtr->instances[i]->value, "-") == 0) {
            return -INFINIT_LOAD;
        }
        return atof(hPtr->instances[i]->value);
    }

    return -INFINIT_LOAD;

}

void
resetSharedResource(void)
{
    int i;
    int j;

    for (i = 0; i < numResources; i++) {

        for (j = 0; j < sharedResources[i]->numInstances; j++) {

            FREEUP(sharedResources[i]->instances[j]->value);

            sharedResources[i]->instances[j]->value =
                safeSave(sharedResources[i]->instances[j]->lsfValue);
        }
    }
}
void
updSharedResourceByRUNJob(const struct jData* jp)
{
    int i;
    int ldx;
    int resAssign = 0;
    float jackValue;
    float originalLoad;
    float duration;
    float decay;
    struct resVal *resValPtr;
    struct resourceInstance *instance;
    static int *rusage_bit_map;
    int adjForPreemptableResource = FALSE;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG, "\
%s: Updating shared resource by job=%s", __func__, lsb_jobid2str(jp->jobId));

    if (rusage_bit_map == NULL) {
        rusage_bit_map = my_calloc(GET_INTNUM(allLsInfo->nRes),
                                   sizeof (int), __func__);
    }

    if (!jp->numHostPtr || jp->hPtr == NULL) {
        return;
    }

    if ((resValPtr = getReserveValues(jp->shared->resValPtr,
                                      jp->qPtr->resValPtr)) == NULL) {
        return;
    }

    for (i = 0; i < GET_INTNUM(allLsInfo->nRes); i++) {
        resAssign += resValPtr->rusage_bit_map[i];
        rusage_bit_map[i] = 0;
    }

    if (resAssign == 0)
        return;

    duration = (float) resValPtr->duration;
    decay = resValPtr->decay;
    if (resValPtr->duration != INFINIT_INT && (duration - jp->runTime <= 0)){

        if ((duration - runTimeSinceResume(jp) <= 0)
            || !isReservePreemptResource(resValPtr)) {
            return;
        }
        adjForPreemptableResource = TRUE;
    }

    for (i = 0; i < jp->numHostPtr; i++) {
        float load;
        char  loadString[MAXLSFNAMELEN];

        if (jp->hPtr[i]->hStatus & HOST_STAT_UNAVAIL)
            continue;

        for (ldx = 0; ldx < allLsInfo->nRes; ldx++) {
            float factor;
            int isSet;


            if (ldx < allLsInfo->numIndx)
                continue;

            if (NOT_NUMERIC(allLsInfo->resTable[ldx]))
                continue;

            TEST_BIT(ldx, resValPtr->rusage_bit_map, isSet);
            if (isSet == 0)
                continue;


            if (adjForPreemptableResource && (!isItPreemptResourceIndex(ldx))) {
                continue;
            }


            if (jp->jStatus & JOB_STAT_RUN) {

                goto adjustLoadValue;


            } else if (IS_SUSP(jp->jStatus)
                       &&
                       ! (allLsInfo->resTable[ldx].flags & RESF_RELEASE)) {

                goto adjustLoadValue;


            } else if (IS_SUSP(jp->jStatus)
                       &&
                       (jp->jStatus & JOB_STAT_RESERVE)) {

                goto adjustLoadValue;

            } else {



                continue;

            }

adjustLoadValue:

            jackValue = resValPtr->val[ldx];
            if (jackValue >= INFINIT_LOAD || jackValue <= -INFINIT_LOAD)
                continue;

            if ( (load = getHRValue(allLsInfo->resTable[ldx].name,
                                    jp->hPtr[i],
                                    &instance) ) < 0.0) {
                continue;
            } else {

                TEST_BIT (ldx, rusage_bit_map, isSet)
                    if (isSet == TRUE)
                        continue;
            }

            if (logclass & LC_SCHED)
                ls_syslog(LOG_DEBUG,"\
%s: jobId=<%s>, hostName=<%s>, resource name=<%s>, the specified rusage of the load or instance <%f>, current lsbload or instance value <%f>, duration <%f>, decay <%f>",
                          __func__,
                          lsb_jobid2str(jp->jobId),
                          jp->hPtr[i]->host,
                          allLsInfo->resTable[ldx].name,
                          jackValue,
                          load,
                          duration,
                          decay);


            factor = 1.0;
            if (resValPtr->duration != INFINIT_INT) {
                if (resValPtr->decay != INFINIT_FLOAT) {
                    float du;

                    if ( isItPreemptResourceIndex(ldx) ) {
                        du = duration - runTimeSinceResume(jp);
                    } else {
                        du = duration - jp->runTime;
                    }
                    if (du > 0) {
                        factor = du/duration;
                        factor = pow (factor, decay);
                    }
                }
                jackValue *= factor;
            }

            if (allLsInfo->resTable[ldx].orderType == DECR)
                jackValue = -jackValue;

            originalLoad = atof (instance->value);
            load = originalLoad + jackValue;

            if (load < 0.0)
                load = 0.0;

            sprintf (loadString, "%-10.1f", load);

            FREEUP (instance->value);
            instance->value = safeSave (loadString);

            SET_BIT (ldx, rusage_bit_map);

            if (logclass & LC_SCHED)
                ls_syslog(LOG_DEBUG,"\
%s: JobId=<%s>, hostname=<%s>, resource name=<%s>, the amount by which the load or the instance has been adjusted <%f>, original load or instance value <%f>, runTime=<%d>, value of the load or the instance after the adjustment <%f>, factor <%f>",
                          __func__,
                          lsb_jobid2str(jp->jobId),
                          jp->hPtr[i]->host,
                          allLsInfo->resTable[ldx].name,
                          jackValue,
                          originalLoad,
                          jp->runTime,
                          load,
                          factor);
        }
    }
}

/* compute_group_slots()
 */
static void
compute_group_slots(int num_res, struct lsSharedResourceInfo *res)
{
    int cc;
    int i;
    char buf[128];

    /* For all shared resources returned by lim
     */
    for (cc = 0; cc < num_res; cc++) {

        /* For all host groups.
         * Here we can optimize and have a hash table
         * of host groups having group_slots != NULL.
         */
        for (i = 0; i < numofhgroups; i++) {
            int k;
            int slots;

            /* if the host group has a slot resource
             * which name is equal to the shared resource,
             * its value is the sum of usable slots in the
             * entire group. If no host is down it is the
             * sum(MXJ)
             */
            if (hostgroups[i]->group_slots == NULL)
                continue;
            if (strcmp(hostgroups[i]->group_slots, res[cc].resourceName) != 0)
                continue;

            slots = get_group_slots(hostgroups[i]);
            for (k = 0; k < res->nInstances; k++) {
                _free_(res[cc].instances[k].value);
                sprintf(buf, "%d", slots);
                res[cc].instances[k].value = strdup(buf);
            }
            break;
        } /* for (i = 0; i < numofhostgroup; i++) */
    }
}

/* get_group_slots()
 */
static int
get_group_slots(struct gData *gPtr)
{
    hEnt *ent;
    sTab stab;
    char *host;
    struct hData *hPtr;
    int slots;

    /* A group with no members is a group whose
     * members are all hosts.
     */
    slots = 0;
    if (HTAB_NUM_ELEMENTS(&gPtr->memberTab) == 0) {

        for (hPtr = (struct hData *)hostList->forw;
             hPtr != (void *)hostList;
             hPtr = hPtr->forw) {
            if (LSB_HOST_UNUSABLE(hPtr->hStatus)) {
                ls_syslog(LOG_DEBUG, "\
%s: group %s host %s status 0x%x unusable", __func__, gPtr->group,
                          hPtr->host,
                          hPtr->hStatus);
                continue;
            }
            if (slots + hPtr->maxJobs >= gPtr->max_slots) {
                slots = gPtr->max_slots;
                goto via;
            }
            slots = slots + hPtr->maxJobs;
        }
    via:
        ls_syslog(LOG_DEBUG, "\
%s: group %s resource %s slots %d", __func__, gPtr->group,
                  gPtr->group_slots, slots);
        return slots;
    }

    slots = 0;
    ent = h_firstEnt_(&gPtr->memberTab, &stab);
    while (ent) {

        host = ent->hData;
        hPtr = getHostData(host);
        if (LSB_HOST_UNUSABLE(hPtr->hStatus)) {
                ls_syslog(LOG_DEBUG, "\
%s: group %s host %s status 0x%x unusable", __func__, gPtr->group,
                          hPtr->host,
                          hPtr->hStatus);
            ent = h_nextEnt_(&stab);
            continue;
        }
        if (slots + hPtr->maxJobs >= gPtr->max_slots) {
            slots = gPtr->max_slots;
            goto pryc;
        }
        slots = slots + hPtr->maxJobs;
        ent = h_nextEnt_(&stab);
    }
pryc:
    ls_syslog(LOG_DEBUG, "\
%s: 2 group %s resource %s slots %d", __func__, gPtr->group,
              gPtr->group_slots, slots);

    return slots;
}

/* get_glb_tokens()
 */
static void
get_glb_tokens(void)
{
    struct glb_token *t;
    static int first = 1;

    if (first) {
        char *p;

        p = getenv("LSF_ENVDIR");
        setenv("GLB_ENVDIR", p, 0);

        if (glb_init() < 0) {
            ls_syslog(LOG_ERR, "\
%s: gudness glb_init() failed cannot get even hold of GLB %d",
                      __func__, errno);
            return;
        }
        first = 0;
    }

    free_mbd_tokens();

    t = glb_gettokens(clusterName, &num_tokens);
    if (t == NULL) {
        ls_syslog(LOG_ERR, "\
%s: Ohmygosh cannot get tokens from GLB %d", __func__, errno);
        tokens = recover_glb_allocation_state();
        if (tokens == NULL) {
            num_tokens = 0;
            return;
        }
        return;
    }

    /* turn the glb tokens into mbd tokens
     */
    copy_glb_tokens(t, num_tokens);

    free_glb_tokens(t, num_tokens);
}

/* copy_glb_tokens()
 */
static void
copy_glb_tokens(struct glb_token *t, int num)
{
    int cc;

    tokens = calloc(num, sizeof(struct mbd_token));

    for (cc = 0; cc < num; cc++) {

        tokens[cc].name = strdup(t[cc].name);
        tokens[cc].allocated = t[cc].allocated;
        tokens[cc].ideal = t[cc].ideal;
        tokens[cc].recalled = t[cc].recalled;
    }
}

/* free_mbd_tokens()
 */
static void
free_mbd_tokens(void)
{
    int cc;

    if (num_tokens == 0
        || tokens == NULL)
        return;

    for (cc = 0; cc < num_tokens; cc++)
        __free__(tokens[cc].name);

    __free__(tokens);
}

/* add_tokens()
 */
static void
add_tokens(struct lsSharedResourceInfo *res, int num_res)
{
    int cc;
    int i;
    int n;
    char buf[64];

    if (num_tokens == 0
        || tokens == NULL)
        return;

    for (i = 0; i < num_tokens; i++) {

        for (cc = 0; cc < num_res; cc++) {

            if (strcmp(tokens[i].name, res[cc].resourceName) != 0)
                continue;

            for (n = 0; n < res[cc].nInstances; n++) {
                _free_(res[cc].instances[n].value);
                sprintf(buf, "%d", tokens[i].allocated);
                res[cc].instances[n].value = strdup(buf);
            }
            break;
        }
    }
}

/* glb_token_policy()
 */
void
glb_token_policy(void)
{
    int cc;
    int used;

    ls_syslog(LOG_INFO, "%s: Entering...", __func__);

    for (cc = 0; cc < num_tokens; cc++) {

        /* Compute the usage for each token
         */
        used = compute_used_tokens(&tokens[cc]);

        if (used > 0 && tokens[cc].allocated <= 0) {
            ls_syslog(LOG_INFO, "\
%s: tokens %s has %d allocated tokens but %d used GLB and MBD out of sync?",
                      __func__, tokens[cc].name,
                      tokens[cc].allocated, used);
            tokens[cc].allocated = used;
            continue;
        }

        ls_syslog(LOG_INFO, "\
%s: token %s used %d alloc %d ideal %d recalled %d need_more %d",
                  __func__, tokens[cc].name, used,
                  tokens[cc].allocated, tokens[cc].ideal,
                  tokens[cc].recalled, tokens[cc].need_more);

        /* GLB is recalling tokens.
         */
        if (tokens[cc].recalled > 0) {
            int num_preempted;

            /* We may need to preempt to release some
             * of the requested tokens.
             */
            ls_syslog(LOG_INFO, "\
%s: GLB recalling %d tokens used %d %d ideal", __func__,
                      tokens[cc].recalled, used, tokens[cc].ideal);

            num_preempted = preempt_jobs_for_tokens(&tokens[cc]);

            ls_syslog(LOG_INFO, "\
%s: num_preempted %d", __func__, num_preempted);

            if (num_preempted > 0) {
                glb_release_tokens(&tokens[cc], num_preempted);
                ls_syslog(LOG_INFO, "\
%s: 1 releasing %d tokens", __func__, num_preempted);
                continue;
            }

        } /* under recall */

        /* mbatchd is using less tokens than allocated
         * release the extra to glb
         */
        if (used < tokens[cc].allocated
            && tokens[cc].need_more == 0) {

            ls_syslog(LOG_INFO, "\
%s: used %d allocated %d releasing %d to GLB", __func__,
                      tokens[cc].allocated, tokens[cc].allocated,
                      tokens[cc].allocated - used);

            glb_release_tokens(&tokens[cc], tokens[cc].allocated - used);
            continue;
        }

        /* Full use and not recalled ask for more tokens
         */
       if (tokens[cc].need_more > 0
           && tokens[cc].recalled <= 0) {
           if (glb_get_more_tokens(tokens[cc].name) < 0) {
               ls_syslog(LOG_ERR, "\
%s: failed to get_more_tokens() glb down?", __func__);
               return;
           }
       }

    } /* for (cc = 0; cc < num_tokens; cc++) */
}

static struct mbd_token *
is_a_token(const char *name)
{
    int cc;

    for (cc = 0; cc < num_tokens; cc++) {
        if (strcmp(name, tokens[cc].name) == 0)
            return &tokens[cc];
    }

    return NULL;
}

bool_t
is_token_under_recall(const char *name)
{
    struct mbd_token *t;

    t = is_a_token(name);
    if (!t)
        return false;

    if (t->recalled > 0)
        return true;

    return false;
}

void
need_tokens(const char *name)
{
    struct mbd_token *t;

    t = is_a_token(name);
    if (!t)
        return;

    t->need_more++;
}

void
handle_finished_tokens(struct jData *jPtr)
{
    int cc;
    float x;
    struct resVal *r;

    /* Try the host and then the queue
     */
    r = jPtr->shared->resValPtr;
    if (r == NULL)
        r = jPtr->qPtr->resValPtr;
    if (r == NULL)
        return;

    x = 0.0;
    for (cc = 0; cc < num_tokens; cc++) {

        if (tokens[cc].recalled <= 0)
            continue;

        x = get_job_tokens(r, &tokens[cc], jPtr);
        if (x > 0) {
            int recalled;

            if (x >= tokens[cc].recalled) {
                recalled = tokens[cc].recalled;
                tokens[cc].recalled = 0;
            } else {
                recalled = x;
                tokens[cc].recalled = tokens[cc].recalled - x;
            }

            ls_syslog(LOG_INFO, "\
%s: job %s finished using token %s num %d", __func__,
                      lsb_jobid2str(jPtr->jobId), tokens[cc]. name, x);
            glb_release_tokens(&tokens[cc], recalled);
        }
    }
}

static int
compute_used_tokens(struct mbd_token *t)
{
    struct jData *jPtr;
    float used;
    float x;
    struct resVal *r;
    int num;

    used = 0.0;
    x = 0.0;
    num = 0;

    for (jPtr = jDataList[SJL]->back;
         jPtr != jDataList[SJL];
         jPtr = jPtr->back) {

        /* Try the host and then the queue
         */
        r = jPtr->shared->resValPtr;
        if (r == NULL)
            r = jPtr->qPtr->resValPtr;
        if (r == NULL)
            continue;

        /* Check if this running jobs is using
         * the token t and also if has not
         * been preempted because of GLB recall.
         */
        x = get_job_tokens(r, t, jPtr);
        if (x > 0) {
            used = used + x;
            ++num;
        }
    }

    ls_syslog(LOG_INFO, "\
%s: there are %d jobs on SJL using token %s", __func__, num, t->name);

    return (int)used;
}

static float
get_job_tokens(struct resVal *resPtr, struct mbd_token *t, struct jData *jPtr)
{
    int cc;
    int rusage;
    int is_set;
    linkiter_t iter;
    struct _rusage_ *r;
    struct resVal r2;

    rusage = 0;
    for (cc = 0; cc < GET_INTNUM(allLsInfo->nRes); cc++)
        rusage += resPtr->rusage_bit_map[cc];

    if (rusage == 0)
        return 0.0;

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

            if (strcmp(allLsInfo->resTable[cc].name, t->name) != 0)
                continue;

            if (IS_FINISH(jPtr->jStatus))
                return r2.val[cc];

            /* Ok the job was started on a GLB token but it
             * has been suspended by MBD because of token recall
             */
            if (jPtr->jStatus & JOB_STAT_USUSP
                && ((jPtr->jFlags & JFLAG_PREEMPT_GLB)
                    || (jPtr->jFlags & JFLAG_JOB_PREEMPTED))) {
                return 0.0;
            }

            /* We already transition the job from USUSP to
             * SSUSP so it is waiting to be resumed, in this
             * case set the need_more parameter for GLB.
             */
            if (jPtr->jStatus & JOB_STAT_SSUSP
                && ((jPtr->jFlags & JFLAG_PREEMPT_GLB)
                    || (jPtr->jFlags & JFLAG_JOB_PREEMPTED))) {
                t->need_more++;
                return 0;
            }

            return r2.val[cc];
        }
    }

    return 0.0;
}

static int
glb_get_more_tokens(const char *name)
{
    int cc;

    ls_syslog(LOG_INFO, "\
%s calling GLB to get more %s tokens", __func__, name);

    /* glb_init() called in get_glb_tokens()
     */
    cc = glb_moretokens(clusterName, name);
    if (cc != GLBE_NOERR) {
        ls_syslog(LOG_ERR, "\
%s: failed in glb_moretokens() %d", __func__, errno);
        return -1;
    }

    return 0;
}

static int
glb_release_tokens(struct mbd_token *t, int how_many)
{
    int cc;

    ls_syslog(LOG_INFO, "\
%s: token %s releasing %d of tokens to GLB", __func__,
              t->name, how_many);

    cc = glb_releasetokens(clusterName, t->name, how_many);
    if (cc != GLBE_NOERR) {
        ls_syslog(LOG_ERR, "\
%s: failed talking to GLB to release token %s number %d",
                  __func__, t->name, how_many);
        return -1;
    }

    /* Upate your state since the allocation has
     * changed because of the release.
     */
    t->allocated = t->allocated - how_many;
    assert(t->allocated >= 0);
    save_glb_allocation_state();

    return 0;
}

static int
preempt_jobs_for_tokens(struct mbd_token *t)
{
    int num_preempt;
    int x;
    int got;
    struct resVal *r;
    struct jData *jPtr;

    got = 0;
    num_preempt = t->recalled;

    for (jPtr = jDataList[SJL]->forw;
         jPtr != jDataList[SJL];
         jPtr = jPtr->forw) {

        if ((jPtr->jStatus & JOB_STAT_SSUSP)
            || (jPtr->jStatus & JOB_STAT_USUSP))
            continue;
        if (! (jPtr->qPtr->qAttrib & Q_ATTRIB_PREEMPTABLE))
            continue;

        r = jPtr->shared->resValPtr;
        if (r == NULL)
            r = jPtr->qPtr->resValPtr;
        if (r == NULL)
            continue;
        /* we use got as the jobs can return different
         * numbers of tokens eventually greater
         * then num_preempt
         */
        x = get_job_tokens(r, t, jPtr);
        if (x >= num_preempt) {
            stop_job(jPtr, JFLAG_PREEMPT_GLB);
            got = got + x;
            break;
        }
        stop_job(jPtr, JFLAG_PREEMPT_GLB);
        num_preempt = num_preempt - x;
        got = got + x;
    }

    return got;
}

int
stop_job(struct jData *jPtr, int flag)
{
    struct signalReq s;
    struct lsfAuth auth;
    int cc;

    s.sigValue = SIGSTOP;
    s.jobId = jPtr->jobId;
    s.chkPeriod = 0;
    s.actFlags = 0;

    memset(&auth, 0, sizeof(struct lsfAuth));
    strcpy(auth.lsfUserName, lsbManager);
    auth.gid = auth.uid = managerId;

    cc = signalJob(&s, &auth);
    if (cc != LSBE_NO_ERROR) {
        ls_syslog(LOG_ERR, "\
%s: error while suspending job %s state 0x%x", __func__,
                  lsb_jobid2str(jPtr->jobId), jPtr->jStatus);
        return -1;
    }

    jPtr->jFlags |= flag;

    ls_syslog(LOG_INFO, "\
%s: stopping job %s jFlag %s", __func__, lsb_jobid2str(jPtr->jobId),
              str_flags(jPtr->jFlags));

    return 0;
}

/* get_glb_token_size()
 */
int
get_glb_tokens_size(int *n)
{
    int cc;
    int size;

    /* from the mbd_token take only the part
     * that glb understands.
     */
    size = 0;
    for (cc = 0; cc < num_tokens; cc++) {
        size = size + ALIGNWORD_(strlen(tokens[cc].name) + 1)
            + 3 * sizeof(int);
    }
    *n = num_tokens;

    return size;
}

int
encode_glb_tokens(XDR *xdrs, struct LSFHeader *hdr)
{
    int cc;
    struct glb_token t;

    for (cc = 0; cc < num_tokens; cc++) {
        /* Copy only what you need
         */
        memcpy(&t, &tokens[cc], sizeof(struct glb_token));
        if (! xdr_glb_token(xdrs, &t, hdr))
            return -1;
    }

    return 0;
}

static int
save_glb_allocation_state(void)
{
    int i;
    FILE *fp;

    if (num_tokens == 0) {
        assert(tokens == NULL);
        return -1;
    }

    sprintf(old_path, "%s/logdir/glb.tokens.0",
            daemonParams[LSB_SHAREDIR].paramValue);
    sprintf(new_path, "%s/logdir/glb.tokens",
            daemonParams[LSB_SHAREDIR].paramValue);

    fp = fopen(old_path, "w");
    if (fp == NULL) {
        ls_syslog(LOG_ERR, "\
%s: failed fopen %s: %m cannot save GLB alloc state", __func__, old_path);
        return -1;
    }

    for (i = 0; i < num_tokens; i++) {
        fprintf(fp, "%s %d\n", tokens[i].name, tokens[i].allocated);
    }

    fclose(fp);

    if (rename(old_path, new_path) < 0) {
        ls_syslog(LOG_ERR, "\
%s: rename() failed from %s to %s %m, cannot save state",
                  __func__, old_path, new_path);
        return -1;
    }

    return 0;
}

struct mbd_token *
recover_glb_allocation_state(void)
{
    FILE *fp;
    struct mbd_token *t;
    int cc;

    sprintf(new_path, "%s/logdir/glb.tokens",
            daemonParams[LSB_SHAREDIR].paramValue);

    /* count the number of lines
     */
    num_tokens = count_stream(new_path);
    if (num_tokens == 0) {
        ls_syslog(LOG_WARNING, "\
%s: file %s has %d num_tokens GLB state not saved yet.",
                  __func__, new_path, num_tokens);
        return NULL;
    }

    fp = fopen(new_path, "r");
    if (fp == NULL) {
        ls_syslog(LOG_ERR, "\
%s: failed fopen %s: %m cannot save GLB alloc state", __func__, new_path);
        num_tokens = 0;
        return NULL;
    }

    t = calloc(num_tokens, sizeof(struct mbd_token));

    for (cc = 0; cc < num_tokens; cc++) {

        t[cc].name = calloc(MAXLSFNAMELEN, sizeof(char));

        fscanf(fp, "%s %d", t[cc].name, &t[cc].allocated);
        ls_syslog(LOG_INFO, "\
%s: token %s allocated %d", __func__, t[cc].name, t[cc].allocated);

    }

    fclose(fp);

    return t;
}

static void
initQPRValues(struct qPRValues *qPRValuesPtr, struct qData *qPtr)
{
    qPRValuesPtr->qData                 = qPtr;
    qPRValuesPtr->usedByRunJob          = 0.0;
    qPRValuesPtr->reservedByPreemptWait = 0.0;
    qPRValuesPtr->availableByPreempt    = 0.0;
    qPRValuesPtr->preemptingRunJob      = 0.0;

    return;

}


static void
freePreemptResourceInstance(struct preemptResourceInstance *pRIPtr)
{

    pRIPtr->instancePtr = NULL;
    pRIPtr->nQPRValues = 0;
    FREEUP(pRIPtr->qPRValues);

    return;

}

static void
freePreemptResource(struct preemptResource *pRPtr)
{
    int i;

    pRPtr->index = -1;
    for (i=0; i<pRPtr->numInstances; i++) {
        freePreemptResourceInstance(&pRPtr->pRInstance[i]);
    }
    pRPtr->numInstances = 0;
    FREEUP(pRPtr->pRInstance);

    return;

}

void
newPRMO(char *nameList)
{
    struct nameList *nameListPtr;
    int i, index, addedNum = 0;

    if (logclass & LC_TRACE)
        ls_syslog(LOG_DEBUG3, "%s: Entering ..." , __func__);

    if (pRMOPtr != NULL) {
        delPRMO();
    }

    if ((nameList == NULL) || (nameList[0] == '\0')) {
        return;
    }

    if ((nameListPtr = lsb_parseLongStr(nameList)) == NULL) {
        return;
    }

    nameList[0]='\0';


    if (nameListPtr->listSize <= 0) {
        return;
    }
    pRMOPtr = (struct objPRMO *) my_malloc(sizeof (struct objPRMO), __func__);
    pRMOPtr->numPreemptResources = 0;
    pRMOPtr->pResources = (struct preemptResource *)
        my_malloc(nameListPtr->listSize * sizeof (struct preemptResource), __func__);


    for (i=0; i<nameListPtr->listSize; i++) {
        if (nameListPtr->counter[i] > 1) {
            ls_syslog(LOG_WARNING, I18N(7707,
                "Duplicate preemptable resource names (%s) defined, take one only."),/* catgets 7707*/
                nameListPtr->names[i]);
            lsberrno = LSBE_CONF_WARNING;

        }
        if ((index = resName2resIndex(nameListPtr->names[i])) < 0) {
            ls_syslog(LOG_ERR, I18N(7708,
                "Unknown preemptable resource name (%s) defined, ignored."),/* catgets 7708*/
                nameListPtr->names[i]);
            lsberrno = LSBE_CONF_WARNING;
            continue;
        }


        if (index < allLsInfo->numIndx) {
            if (allLsInfo->resTable[index].flags & RESF_BUILTIN) {
                ls_syslog(LOG_ERR, I18N(7709,
                    "Built-in load index (%s) can't be defined as a preemptable resource, ignored."),/* catgets 7709*/
                    nameListPtr->names[i]);
            } else {
                ls_syslog(LOG_ERR, I18N(7714,
                    "Host load index (%s) can't be defined as a preemptable resource, ignored."),/* catgets 7714*/
                    nameListPtr->names[i]);
            }
            lsberrno = LSBE_CONF_WARNING;
            continue;
        }


        if (NOT_NUMERIC(allLsInfo->resTable[index])) {
            ls_syslog(LOG_ERR, I18N(7710,
                "Non-numeric resource (%s) can't be defined as a preemptable resource, ignored."),/* catgets 7710*/
                nameListPtr->names[i]);
            lsberrno = LSBE_CONF_WARNING;
            continue;
        }


        if (!(allLsInfo->resTable[index].flags & RESF_RELEASE)) {
            ls_syslog(LOG_ERR, I18N(7711,
                "Non-releasable resource (%s) can't be defined as a preemptable resource, ignored."),/* catgets 7711*/
                nameListPtr->names[i]);
            lsberrno = LSBE_CONF_WARNING;
            continue;
        }


        if (allLsInfo->resTable[index].orderType != DECR) {
            ls_syslog(LOG_ERR, I18N(7712,
                "Increasing resource (%s) can't be defined as a preemptable resource, ignored."),/* catgets 7712*/
                nameListPtr->names[i]);
            lsberrno = LSBE_CONF_WARNING;
            continue;
        }


        if (addPreemptableResource(index) == 0) {

            if (addedNum > 0) {
                strcat(nameList, " ");
            }
            strcat(nameList, nameListPtr->names[i]);
            addedNum++;
        }
    }

    return;

}

void delPRMO()
{
    int i;

    if (pRMOPtr == NULL) {
        return;
    }

    for (i=0; i<pRMOPtr->numPreemptResources; i++) {
        freePreemptResource(&pRMOPtr->pResources[i]);
    }
    FREEUP(pRMOPtr->pResources);
    FREEUP(pRMOPtr);

    return;

}

void mbdReconfPRMO(void)
{
}

void resetPRMOValues(int valueSelectFlag)
{
    int i, j, k;

    if (pRMOPtr == NULL) {
        return;
    }

    for (i = 0; i < pRMOPtr->numPreemptResources; i++) {
        struct preemptResource *pRPtr;

        pRPtr = &(pRMOPtr->pResources[i]);

        for (j = 0; j < pRPtr->numInstances; j++) {
            struct preemptResourceInstance *pRIPtr;

            pRIPtr = &(pRPtr->pRInstance[j]);

            for (k = 0; k < pRIPtr->nQPRValues; k++) {
                if ((valueSelectFlag == PRMO_ALLVALUES) ||
                    (valueSelectFlag & PRMO_USEDBYRUNJOB)) {
                    pRIPtr->qPRValues[k].usedByRunJob = 0.0;
                }
                if ((valueSelectFlag == PRMO_ALLVALUES) ||
                    (valueSelectFlag & PRMO_RESERVEDBYPREEMPTWAIT)) {
                    pRIPtr->qPRValues[k].reservedByPreemptWait = 0.0;
                }
                if ((valueSelectFlag == PRMO_ALLVALUES) ||
                    (valueSelectFlag & PRMO_AVAILABLEBYPREEMPT)) {
                    pRIPtr->qPRValues[k].availableByPreempt = 0.0;
                }
                if ((valueSelectFlag == PRMO_ALLVALUES) ||
                    (valueSelectFlag & PRMO_PREEMPTINGRUNJOB)) {
                    pRIPtr->qPRValues[k].preemptingRunJob = 0.0;
                }
            }
        }
    }

    return;

}

float
getUsablePRHQValue(int index, struct hData *hPtr, struct qData *qPtr,
struct resourceInstance **instance)
{
    float currentValue;
    float reservedValue=0.0;
    struct preemptResourceInstance *pRInstancePtr;

    currentValue = getHRValue(allLsInfo->resTable[index].name, hPtr, instance);


    if (currentValue < 0.0 || currentValue == INFINIT_LOAD) {
        return(currentValue);
    }

    if ((pRInstancePtr = findPRInstance(index, hPtr)) == NULL) {
        return(currentValue);
    }

    return(roundFloatValue(currentValue - reservedValue));

}

float
takeAvailableByPreemptPRHQValue(int index, float value, struct hData *hPtr,
                                struct qData *qPtr)
{
    return checkOrTakeAvailableByPreemptPRHQValue(index, value, hPtr, qPtr, 1);
}

float
checkOrTakeAvailableByPreemptPRHQValue(int index, float value,
    struct hData *hPtr, struct qData *qPtr, int update)
{
    struct preemptResourceInstance *pRInstancePtr;
    float remainValue = value;

    if ((pRInstancePtr = findPRInstance(index, hPtr)) == NULL) {
        return(remainValue);
    }

    return(remainValue);

}

void
addRunJobUsedPRHQValue(int index, float value, struct hData *hPtr,
struct qData *qPtr)
{
    struct qPRValues *qPRVPtr;

    if ((qPRVPtr = addQPRValues(index, hPtr, qPtr)) == NULL) {
        return;
    }
    qPRVPtr->usedByRunJob = roundFloatValue(qPRVPtr->usedByRunJob+value);

    return;

}

void
removeRunJobUsedPRHQValue(int index, float value, struct hData *hPtr,
struct qData *qPtr)
{
    struct qPRValues *qPRVPtr;

    if ((qPRVPtr = findQPRValues(index, hPtr, qPtr)) == NULL) {
        return;
    }
    qPRVPtr->usedByRunJob = roundFloatValue(qPRVPtr->usedByRunJob-value);
    if (qPRVPtr->usedByRunJob < 0.1) {
        qPRVPtr->usedByRunJob = 0.0;
    }

    return;

}

void
addReservedByWaitPRHQValue(int index, float value, struct hData *hPtr,
struct qData *qPtr)
{
    struct qPRValues *qPRVPtr;

    if ((qPRVPtr = addQPRValues(index, hPtr, qPtr)) == NULL) {
        return;
    }
    qPRVPtr->reservedByPreemptWait =
        roundFloatValue(qPRVPtr->reservedByPreemptWait+value);

    return;
}

void
removeReservedByWaitPRHQValue(int index, float value, struct hData *hPtr,
struct qData *qPtr)
{
    struct qPRValues *qPRVPtr;

    if ((qPRVPtr = findQPRValues(index, hPtr, qPtr)) == NULL) {
        return;
    }
    qPRVPtr->reservedByPreemptWait =
        roundFloatValue(qPRVPtr->reservedByPreemptWait-value);
    if (qPRVPtr->reservedByPreemptWait < 0.1) {
        qPRVPtr->reservedByPreemptWait = 0.0;
    }

    return;

}

float
getReservedByWaitPRHQValue(int index, struct hData *hPtr,
struct qData *qPtr)
{
    struct qPRValues *qPRVPtr;

    if ((qPRVPtr = findQPRValues(index, hPtr, qPtr)) == NULL) {
        return(0.0);
    }
    return(qPRVPtr->reservedByPreemptWait);

}

void
addAvailableByPreemptPRHQValue(int index, float value, struct hData *hPtr,
struct qData *qPtr)
{
    struct qPRValues *qPRVPtr;

    if ((qPRVPtr = addQPRValues(index, hPtr, qPtr)) == NULL) {
        return;
    }
    qPRVPtr->availableByPreempt =
        roundFloatValue(qPRVPtr->availableByPreempt+value);

    return;

}

void
removeAvailableByPreemptPRHQValue(int index, float value, struct hData *hPtr,
struct qData *qPtr)
{
    struct qPRValues *qPRVPtr;

    if ((qPRVPtr = addQPRValues(index, hPtr, qPtr)) == NULL) {
        return;
    }
    qPRVPtr->availableByPreempt =
        roundFloatValue(qPRVPtr->availableByPreempt-value);

    return;
}

float
getAvailableByPreemptPRHQValue(int index, struct hData *hPtr,
struct qData *qPtr)
{
    struct qPRValues *qPRVPtr;

    if ((qPRVPtr = findQPRValues(index, hPtr, qPtr)) == NULL) {
        return(0.0);
    }
    return(qPRVPtr->availableByPreempt);

}

int
resName2resIndex(char *resName)
{
    int i;

    for (i = 0; i < allLsInfo->nRes; i++) {
        if (!strcmp(allLsInfo->resTable[i].name, resName)) {
            return(i);
        }
    }

    return -1;

}

int
isItPreemptResourceName(char *resName)
{

    return(isItPreemptResourceIndex(resName2resIndex(resName)));

}

int
isItPreemptResourceIndex(int index)
{
    int i;

    if (pRMOPtr == NULL) {
        return 0;
    }

    for (i = 0; i < pRMOPtr->numPreemptResources; i++) {
        if (pRMOPtr->pResources[i].index == index) {

            return(1);
        }
    }

    return 0;
}

int
isReservePreemptResource(struct  resVal *resValPtr)
{
    int resn;

    if (!resValPtr) {
        return 0;
    }

    FORALL_PRMPT_RSRCS(resn) {
        int valSet = FALSE;
        TEST_BIT(resn, resValPtr->rusage_bit_map, valSet);
        if ((valSet != 0) && (resValPtr->val[resn] > 0.0)) {
            return(1);
        }
    } ENDFORALL_PRMPT_RSRCS;

    return 0;

}

static int
addPreemptableResource(int index)
{
    static char fname[] = "addPreemptableResource";
    int i;
    int loc;
    int sharedIndex = -1;

    if (pRMOPtr == NULL) {
        return -1;
    }


    for (i=0; i<pRMOPtr->numPreemptResources; i++) {
        if (pRMOPtr->pResources[i].index == index) {
            ls_syslog(LOG_INFO, I18N(7707,
                "Duplicated preemptable resource name (%s) defined, ignored."),/* catgets 7707*/
                allLsInfo->resTable[index].name);
            lsberrno = LSBE_CONF_WARNING;
            return -1;
        }
    }


    for (i=0; i<numResources; i++) {
        if (!strcmp(sharedResources[i]->resourceName,
            allLsInfo->resTable[index].name)) {
            sharedIndex = i;
            break;
        }

    }

    if (sharedIndex < 0) {
        ls_syslog(LOG_ERR, I18N(7713,
            "Preemptable resource name (%s) is not a shared resource, ignored."),/* catgets 7713*/
            allLsInfo->resTable[index].name);
        lsberrno = LSBE_CONF_WARNING;
        return -1;
    }

    loc = pRMOPtr->numPreemptResources;
    pRMOPtr->pResources[loc].index = index;
    pRMOPtr->pResources[loc].numInstances =
        sharedResources[sharedIndex]->numInstances;
    pRMOPtr->pResources[loc].pRInstance =
        (struct preemptResourceInstance *)
        my_malloc ( sharedResources[sharedIndex]->numInstances *
            sizeof(struct preemptResourceInstance), fname);

    for (i=0; i<pRMOPtr->pResources[loc].numInstances; i++) {
        pRMOPtr->pResources[loc].pRInstance[i].instancePtr =
            sharedResources[sharedIndex]->instances[i];
        pRMOPtr->pResources[loc].pRInstance[i].nQPRValues  = 0;
        pRMOPtr->pResources[loc].pRInstance[i].qPRValues   = NULL;
    }
    pRMOPtr->numPreemptResources++;

    return 0;

}

static struct preemptResourceInstance *
findPRInstance(int index, struct hData *hPtr)
{
    int i, j;
    struct preemptResource *pRPtr=NULL;

    if (pRMOPtr == NULL || pRMOPtr->numPreemptResources <= 0) {
        return NULL;
    }

    for (i = 0; i < pRMOPtr->numPreemptResources; i++) {

        if (pRMOPtr->pResources[i].index == index) {
            pRPtr = &(pRMOPtr->pResources[i]);
            break;
        }
    }

    if (pRPtr == NULL) {
        return NULL;
    }

    for (i = 0; i < pRPtr->numInstances; i++) {
        for (j = 0; j < pRPtr->pRInstance[i].instancePtr->nHosts; j++) {
            if (pRPtr->pRInstance[i].instancePtr->hosts[j] == hPtr) {
                return(&(pRPtr->pRInstance[i]));
            }
        }
    }

    return NULL;

}

static struct qPRValues *
findQPRValues(int index, struct hData *hPtr, struct qData *qPtr)
{
    struct preemptResourceInstance *pRIPtr;
    int i;

    if ((pRIPtr = findPRInstance(index, hPtr)) == NULL) {
        return NULL;
    }

    for (i = 0; i < pRIPtr->nQPRValues; i++) {
        if (pRIPtr->qPRValues[i].qData->priority > qPtr->priority) {
          break;
        }
        if (pRIPtr->qPRValues[i].qData == qPtr) {
            return(&(pRIPtr->qPRValues[i]));
        }
    }
    return NULL;

}

static struct qPRValues *
addQPRValues(int index, struct hData *hPtr, struct qData *qPtr)
{
    struct preemptResourceInstance *pRIPtr;
    struct qPRValues *temp;
    int i, pos;

    if ((pRIPtr = findPRInstance(index, hPtr)) == NULL) {
        return NULL;
    }


    pos = pRIPtr->nQPRValues;
    for (i = 0; i < pRIPtr->nQPRValues; i++) {
        if (pRIPtr->qPRValues[i].qData->priority > qPtr->priority) {
          pos = i;
          break;
        }
        if (pRIPtr->qPRValues[i].qData == qPtr) {
            return(&(pRIPtr->qPRValues[i]));
        }
    }


    if (pRIPtr->nQPRValues == 0)
        temp = (struct qPRValues *)
                     my_malloc (sizeof (struct qPRValues), __func__);
    else
        temp = (struct qPRValues *) realloc (pRIPtr->qPRValues,
                     (pRIPtr->nQPRValues + 1) *sizeof (struct qPRValues));
    if (temp == NULL) {
        ls_syslog(LOG_ERR, I18N_FUNC_FAIL, __func__, "realloc");
        mbdDie(MASTER_MEM);
    }
    pRIPtr->qPRValues = (struct qPRValues *) temp;
    pRIPtr->nQPRValues++;


    for (i=pRIPtr->nQPRValues-1; i > pos; i--) {
      pRIPtr->qPRValues[i] = pRIPtr->qPRValues[i-1];
    }
    initQPRValues(&(pRIPtr->qPRValues[pos]), qPtr);

    return(&(pRIPtr->qPRValues[pos]));

}

static float
roundFloatValue (float old)
{
    static char stringValue[12];

    sprintf (stringValue, "%-10.1f", old);

    return (atof (stringValue));

}

void printPRMOValues()
{
    static char fname[] = "printPRMOValues";
    int i, j, k;

    if (!(logclass & LC_SCHED)) {
        return;
    }

    if (pRMOPtr == NULL) {
        return;
    }

    for (i = 0; i < pRMOPtr->numPreemptResources; i++) {
        struct preemptResource *pRPtr;

        pRPtr = &(pRMOPtr->pResources[i]);
        ls_syslog(LOG_DEBUG2,
            "%s: preemptable resource(%s, %d) has %d instances:",
            fname,
            allLsInfo->resTable[pRPtr->index].name,
            pRPtr->index,
            pRPtr->numInstances);

        for (j = 0; j < pRPtr->numInstances; j++) {
            struct preemptResourceInstance *pRIPtr;

            pRIPtr = &(pRPtr->pRInstance[j]);
            ls_syslog(LOG_DEBUG2,
                "%s: instance(%d) has %d queues:",
                fname,
                j,
                pRIPtr->nQPRValues);

            for (k = 0; k < pRIPtr->nQPRValues; k++) {
                ls_syslog(LOG_DEBUG2,
                    "%s: queue(%s) has UBRJ-%f, RBPW-%f, ABP-%f, PRJ-%f.",
                    fname,
                    pRIPtr->qPRValues[k].qData->queue,
                    pRIPtr->qPRValues[k].usedByRunJob,
                    pRIPtr->qPRValues[k].reservedByPreemptWait,
                    pRIPtr->qPRValues[k].availableByPreempt,
                    pRIPtr->qPRValues[k].preemptingRunJob);
            }
        }
    }

    return;

}

int
isHostsInSameInstance(int index, struct hData *hPtr1, struct hData *hPtr2)
{
    int         i;
    int         j;

    if (hPtr1 == hPtr2) {
        return true;
    }

    for (i = 0; i < numResources; i++) {
        if (strcmp(allLsInfo->resTable[index].name,
            sharedResources[i]->resourceName)) {
            continue;
        }
        for (j = 0; j < sharedResources[i]->numInstances; j++) {
            int found = 0;
            int k;

            for (k = 0; k < sharedResources[i]->instances[j]->nHosts; k++) {
                if (sharedResources[i]->instances[j]->hosts[k] == hPtr1) {
                    found++;
                }
                if (sharedResources[i]->instances[j]->hosts[k] == hPtr2) {
                    found++;
                }
            }
            if (found >= 2) {
                return true;
            }
            if (found == 1) {
                return false;
            }
        }
        return false;
    }
    return false;
}
