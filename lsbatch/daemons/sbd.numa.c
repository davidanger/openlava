/*
 * Copyright (C) 2016 Teraproc
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
#include <hwloc.h>
#include <numa.h>

#include "sbd.h"

#define NL_SETN         11

/* NUMA topology
 *
 * numaTopology  ---------------->   host
 *                          /                \
 *                        node--------------- node
 *                     /        \          /        \
 *                   sock-----sock       sock-----sock
 *                   /  \     /   \     /   \     /   \
 * numaCores --> core-core->core-core->core-core->core-core
 */
static numa_obj_t *numaTopology;
static numa_obj_t **numaCores;
static hwloc_topology_t hwlocTopology;
int num_numa_cores;

/* alloc_numa_obj()
 */
numa_obj_t*
alloc_numa_obj(hwloc_obj_t obj, numa_obj_t* child)
{
    numa_obj_t* numaObj = NULL;
    numa_obj_t* ent = NULL;
    int numCores;

    numaObj = calloc(1, sizeof(numa_obj_t));
    numaObj->forw = numaObj->back = numaObj;
    numaObj->index = obj->logical_index;

    switch (obj->type) {
        case HWLOC_OBJ_MACHINE:
            numaObj->type = NUMA_HOST;
            break;
        case HWLOC_OBJ_NODE:
            numaObj->type = NUMA_NODE;
            break;
        case HWLOC_OBJ_SOCKET:
            numaObj->type = NUMA_SOCKET;
            break;
        case HWLOC_OBJ_CORE:
            numaObj->type = NUMA_CORE;
            numaObj->total = 1;
            num_numa_cores++;
            break;
        default:
            /* don't care this type
             * let grandparent adopt my children
             */
            FREEUP(numaObj);
            return child;
    }

    /* I am the lowest level numa object */
    if (!child)
        return numaObj;

    /* compuate number of cores of my children */
    numCores = 0;
    for (ent = child->forw; ent != child; ent = ent->forw) {
        numCores += ent->total;
        ent->parent = numaObj;
    }
    numaObj->total = numCores;
    numaObj->child = child;
    return numaObj;
}

/*
 * Walk the topology with a tree style.
 *
 * depth 0:         Machine  (HWLOC_OBJ_MACHINE)
 *  depth 1:        NUMANode (HWLOC_OBJ_NODE)
 *   depth 2:       Package  (HWLOC_OBJ_SOCKET)
 *    depth 3:      L3Cache  (HWLOC_OBJ_L3CACHE)
 *     depth 4:     L2Cache  (HWLOC_OBJ_L2CACHE)
 *      depth 5:    L1dCache (HWLOC_OBJ_L1CACHE)
 *       depth 6:   L1iCache (HWLOC_OBJ_L1ICACHE)
 *        depth 7:  Core     (HWLOC_OBJ_CORE)
 *         depth 8: PU       (HWLOC_OBJ_PU)
 *
 * so far we only consider Machine, NUMANode, Package and
 * Core to build NUMA topology
 */
numa_obj_t*
walk_numa_topoloy(hwloc_obj_t obj)
{
    int i;
    numa_obj_t* child = NULL;
    numa_obj_t* ent = NULL;

    /* come to the lowest level */
    if (obj->type == HWLOC_OBJ_PU)
        return NULL;

    /* walk children array below this object */
    for (i = 0; i < obj->arity; i++) {
        ent = walk_numa_topoloy(obj->children[i]);
        if (!ent)
            continue;

        /* build relationship of siblings */
        if (ent->forw != ent) {  /* ent is a list */
            if (!child) {
                child = ent;
            } else {  /* merge two list */
                ent->forw->back = child;
                ent->back->forw = child->forw;
                child->forw->back = ent->back;
                child->forw = ent->forw;
                FREEUP(ent);
            }
        } else {       /* ent is a numa object */
            if (!child)
                child = (numa_obj_t *)mkListHeader();
            ent->back = child->back;
            ent->forw = child;
            child->back->forw = ent;
            child->back = ent;
        }
    }

    /* alloc numa object with children below this object*/
    return alloc_numa_obj(obj, child);
}

/* init_numa_cores()
 */
void
init_numa_cores(void) {
    numa_obj_t* node;
    numa_obj_t* socket;
    numa_obj_t* core;

    /* all numa cores of the machine */
    numaCores = calloc(num_numa_cores+1, sizeof(numa_obj_t*));
    for (node = numaTopology->child->forw;
         node != numaTopology->child;
         node = node->forw) {
        for (socket = node->child->forw;
             socket != node->child;
             socket = socket->forw) {
            for (core = socket->child->forw;
                 core != socket->child;
                 core = core->forw) {
                numaCores[core->index] = core;
            }
        }
    }
}

/* init_numa_topology()
 */
int
init_numa_topology(void)
{
    hwloc_obj_t root;

    /* allocate and initialize topology object. */
    hwloc_topology_init(&hwlocTopology);

    /* perform the topology detection. */
    hwloc_topology_load(hwlocTopology);

    /* get 0 node means OS is NUMA-unaware
     * get 1 node means OS is non-NUMA but NUMA-aware; everything is under the same node
     */
    if (hwloc_get_nbobjs_by_type(hwlocTopology, HWLOC_OBJ_NODE) < 1) {
        /* Destroy topology object. */
        hwloc_topology_destroy(hwlocTopology);
        return 0;
    }

    /* walk numa topology */
    root = hwloc_get_root_obj(hwlocTopology);
    numaTopology = walk_numa_topoloy(root);

    /* core array */
    init_numa_cores();
    return 1;
}


/* bind_to_numa_core()
 */
int
bind_to_numa_core(pid_t pid, int num, int* cores_idx)
{
    hwloc_obj_t obj;
    hwloc_bitmap_t cpuset;
    int i, idx;

    /* compute the cpuset of cores where process pid can run */
    cpuset = hwloc_bitmap_alloc();
    for (i = 0; i < num; i++) {
        obj = hwloc_get_obj_by_type(hwlocTopology, HWLOC_OBJ_CORE, cores_idx[i]);
        if (!obj) {
            hwloc_bitmap_free(cpuset);
            return -1;
        }
        hwloc_bitmap_or(cpuset, cpuset, obj->cpuset);
    }

    /* bind process pid on cores specified by cpuset */
    if (hwloc_set_proc_cpubind(hwlocTopology,
                               pid,
                               cpuset,
                               HWLOC_CPUBIND_PROCESS)) {
        ls_syslog(LOG_ERR, "\
%s: hwloc_set_proc_cpubind() failed binding process %d to numa cores [%s]: %m",
                 __func__, pid, covert_cores_to_str(num, cores_idx));
        hwloc_bitmap_free(cpuset);
        return -1;
    }
    hwloc_bitmap_free(cpuset);

    for (i = 0; i < num; i++) {
        idx = cores_idx[i];
        numaCores[idx]->bound++;

        /* this core is shared by multiple processes,
         * don't overcount used of the core and its parents.
         */
        if (numaCores[idx]->bound > 1)
            continue;
        numaCores[idx]->used++;
        numaCores[idx]->parent->used++;   /* socket */
        numaCores[idx]->parent->parent->used++; /* node */
        numaCores[idx]->parent->parent->parent->used++;  /* host */
    }
    return 0;
}

/* free_numa_core()
 */
void
free_numa_core(int num, int* core_idx, int reset)
{
    int i, idx;

    for(i = 0; i < num; i++) {
        idx = core_idx[i];
        if (idx < 0 || idx > num_numa_cores)
            continue;

        if (reset)
            numaCores[idx]->bound = 0;
        else
            numaCores[idx]->bound--;

        if (numaCores[idx]->bound == 0) {
            numaCores[idx]->used--;
            numaCores[idx]->parent->used--;   /* socket */
            numaCores[idx]->parent->parent->used--; /* node */
            numaCores[idx]->parent->parent->parent->used--;  /* host */
        }
    }
}

/* find_numa_bound_core()
 */
int*
find_numa_bound_core(pid_t pid, int *num)
{
    hwloc_bitmap_t cpubind_set;
    hwloc_obj_t obj, pre;
    int i, idx;
    int* cores_idx;

    /* get the current physical binding of process pid */
    cpubind_set = hwloc_bitmap_alloc();
    if (hwloc_get_proc_cpubind(hwlocTopology, pid, cpubind_set, HWLOC_CPUBIND_PROCESS)) {
        ls_syslog(LOG_ERR, "\
%s: hwloc_get_proc_cpubind() failed for pid %d %m", __func__, pid);
        hwloc_bitmap_free(cpubind_set);
        return NULL;
    }

    *num = hwloc_get_nbobjs_inside_cpuset_by_type(hwlocTopology, cpubind_set, HWLOC_OBJ_CORE);
    /* no object for cpu binding exists inside cpubind_set */
    if (*num == 0) {
        hwloc_bitmap_free(cpubind_set);
        return NULL;
    }
    cores_idx = calloc(*num, sizeof(int));

    /* get the cores where a process ran */
    pre = NULL;
    i = 0;
    while ((obj = hwloc_get_next_obj_inside_cpuset_by_type(hwlocTopology, cpubind_set, HWLOC_OBJ_CORE, pre))) {
        idx = obj->logical_index;
        cores_idx[i++] = idx;
        numaCores[idx]->bound++;
        if (numaCores[idx]->bound == 1) {
            numaCores[idx]->used++;
            numaCores[idx]->parent->used++;   /* socket */
            numaCores[idx]->parent->parent->used++; /* node */
            numaCores[idx]->parent->parent->parent->used++;  /* host */
        }
        pre = obj;
    }
    hwloc_bitmap_free(cpubind_set);
    return cores_idx;
}

/* bind_to_num_mem()
 */
void
bind_to_numa_mem(int* cores, int localonly)
{
    int node;
    struct bitmask *nodemask;
    char buf[64];

    if (!numaTopology)
        return;

    if (!cores || cores[0] < 0 || cores[0] > num_numa_cores)
        return;

    /* allows to allocate memory on other nodes when
     * there isn't enough free on local node
     */
    if (!localonly) {
        numa_set_localalloc();
        return;
    }

    node = numaCores[cores[0]]->parent->parent->index;

    /* set strict policy which means the allocation will fail if
     * the memory cannot be allocated on target node
     */
    sprintf(buf, "%d", node);
    nodemask = numa_parse_nodestring(buf);
    /* 1 strict, 0 preferred */
    numa_set_bind_policy(1);
    /* only allocate memory from the nodes set */
    numa_set_membind(nodemask);
    numa_bitmask_free(nodemask);
}

/* find_free_numa_core()
 *
 * pack all cores as close as possible on a single socket or node
 *
 */
int* find_free_numa_core(int request)
{
    numa_obj_t* ent;
    numa_obj_t* selectedNode;
    numa_obj_t* selectedSocket;
    int *selectedCores = NULL;
    int num = 0;

    /* machine does not have enough free core */
    if(numaTopology->total - numaTopology->used < request) {
        return NULL;
    }

    /* try to pack all cores on a single socket if possible */
    for (selectedNode = numaTopology->child->back;
         selectedNode != numaTopology->child;
         selectedNode = selectedNode->back) {
        if (selectedNode->total - selectedNode->used < request)
            continue;

        for (selectedSocket = selectedNode->child->back;
             selectedSocket != selectedNode->child;
             selectedSocket = selectedSocket->back) {
            if (selectedSocket->total - selectedSocket->used < request)
                continue;

            selectedCores = calloc(request, sizeof(int));
            for (ent = selectedSocket->child->back;
                 ent != selectedSocket->child;
                 ent = ent->back) {
                if (ent->bound == 0) {
                    selectedCores[num++] = ent->index;
                    if (num == request)
                        return selectedCores;
                }
            }
        }
    }

    /* try to pack all cores on a single node if possible */
    for (selectedNode = numaTopology->child->back;
         selectedNode != numaTopology->child;
         selectedNode = selectedNode->back) {
        if (selectedNode->total - selectedNode->used < request)
            continue;

        selectedCores = calloc(request, sizeof(int));
        for (selectedSocket = selectedNode->child->back;
             selectedSocket != selectedNode->child;
             selectedSocket = selectedSocket->back) {
            for (ent = selectedSocket->child->back;
                 ent != selectedSocket->child;
                 ent = ent->back) {
                if (ent->bound == 0) {
                    selectedCores[num++] = ent->index;
                    if (num == request)
                        return selectedCores;
                }
            }
        }
    }

    /* select cores on any node */
    selectedCores = calloc(request, sizeof(int));
    for (selectedNode = numaTopology->child->back;
         selectedNode != numaTopology->child;
         selectedNode = selectedNode->back) {
        for (selectedSocket = selectedNode->child->back;
             selectedSocket != selectedNode->child;
             selectedSocket = selectedSocket->back) {
            for (ent = selectedSocket->child->back;
                 ent != selectedSocket->child;
                 ent = ent->back) {
                if (ent->bound == 0) {
                    selectedCores[num++] = ent->index;
                    if (num == request)
                        return selectedCores;
                }
            }
        }
    }
    return NULL;
}

