/*
 * Copyright (C) 2016 David Bigagli
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA
 *
 */

#include "../lsf.h"
#include "../lib/lib.h"
#include "../lib/lproto.h"
#include "../intlib/intlibout.h"
#include "../intlib/hash.h"
#include "../intlib/link.h"

/* Static char buf for all possible uses
 */
static char buf[PATH_MAX];
/* no buggy verbose
 */
static int verbose = false;
static void
usage(void)
{
    fprintf(stderr, "lsgetpid: [-h] [-V] [-v verbose] pid\n");
}

static struct jRusage *make_prusage(link_t *, link_t *, struct hash_tab *);
static int get_proc_info(const char *,
                         int *,
                         int *,
                         int *,
                         uint32_t *,
                         uint32_t *,
                         uint32_t *,
                         int32_t *);
static void print_pru(pid_t, struct jRusage *);
static void debug_proc(const char *, struct proc_info *);
static void free_prusage(struct jRusage *);

static pid_t global_pid;

int
main(int argc, char **argv)
{
    int cc;
    pid_t pid;
    struct jRusage *pru;

    while ((cc = getopt(argc, argv, "hVv")) != EOF) {
        switch (cc) {
            case 'V':
                fputs(_LS_VERSION_, stderr);
                return 0;
            case 'v':
                verbose = true;
                break;
            case 'h':
            case '?':
                usage();
            return -1;
        }
    }

    if (argv[optind] == NULL) {
        fprintf(stderr, "process id not specified.\n");
        return -1;
    }

    if (! isint_(argv[optind])) {
        fprintf(stderr, "process id not an integer?\n");
        return -1;
    }

    pid = atoi(argv[optind]);
    global_pid = pid;
    if (verbose)
        printf("\
%s: looking for pid %d its children and groups\n", __func__, pid);

    pru = ls_getprocusage(pid);

    print_pru(pid, pru);

    free_prusage(pru);

    return 0;
}

struct jRusage *
ls_getprocusage(pid_t pid)
{
    struct jRusage *pru;
    DIR *dir;
    struct dirent *d;
    link_t *all_pids;
    link_t *wanted_pids;
    link_t *ppgid_pids;
    linkiter_t iter;
    linkiter_t iter2;
    struct hash_tab *ppgid_tab;
    struct proc_info *pinfo;
    struct proc_info *pinfo2;

    dir = opendir(PROC_DIR);
    if (dir == NULL)
        return NULL;

    all_pids = make_link();
    wanted_pids = make_link();

    pru = NULL;
    while ((d = readdir(dir))) {
        int spid;
        int ppid;
        int prgd;
        uint32_t utime;
        uint32_t stime;
        uint32_t vsize;
        int32_t rss;

        if (! isint_(d->d_name))
            continue;

        sprintf(buf, "%s/%s/stat", PROC_DIR, d->d_name);
        get_proc_info(buf, &spid, &ppid, &prgd, &utime, &stime, &vsize, &rss);

        pinfo = calloc(1, sizeof(struct proc_info));
        pinfo->swap = vsize;
        pinfo->mem = rss;
        pinfo->pid = spid;
        pinfo->ppid = ppid;
        pinfo->pgid = prgd;
        pinfo->utime = utime;
        pinfo->stime = stime;
        /* Save in the link of processes
         */
        push_link(all_pids, pinfo);
    }

    /* traverse all pids
     */
    traverse_init(all_pids, &iter);
    while ((pinfo = traverse_link(&iter))) {

        /* skip myself
         */
        if (getpid() == pinfo->pid)
            continue;

        /* See if this is the pid we want
         * or if this pid parent is the pid we want
         */
        if (pid == pinfo->pid
            || pid == pinfo->ppid) {
            /* this list represents the all the processes
             * we want, either the processes with pid specified
             * in input or its direct children.
             */
            push_link(wanted_pids, pinfo);
            if (verbose) {
                debug_proc("wanted pid", pinfo);
            }
        }
    }

    /* traverse wanted pids by gids
     */
    ppgid_tab = hash_make(277);
    ppgid_pids =make_link();

    traverse_init(wanted_pids, &iter);
    while ((pinfo = traverse_link(&iter))) {
        int dup;

        if (verbose)
            debug_proc("searching detached pgids", pinfo);

        /* Search for the pgid only once as per each
         * ppgid we make a while pass through the process
         * list.
         */
        dup = 0;
        sprintf(buf, "%d", pinfo->pgid);
        hash_install(ppgid_tab, buf, pinfo, &dup);
        if (dup == 1)
            continue;

        traverse_init(all_pids, &iter2);
        while ((pinfo2 = traverse_link(&iter2))) {

            /* You are not the gid I want
             */
            if (pinfo->pgid != pinfo2->pgid)
                continue;
            /* This pidinfo is already in the list
             * of wanted pids
             */
            if (pinfo2->pid == pid
                || pinfo2->ppid == pid)
                continue;

            if (getpid() == pinfo2->pid)
                continue;

            /* this ppgid has a pid and ppid different
             * from intput pid but belongs to one of the
             * groups generated by children of x
             */
            push_link(ppgid_pids, pinfo2);
            if (verbose)
                debug_proc("found detached pgids", pinfo);
        }
    }

    /* Now merger the wanted pid records and the orphan pids
     * which we found based on the process group id.
     */
    pru = make_prusage(wanted_pids, ppgid_pids, ppgid_tab);

    fin_link(all_pids);
    fin_link(wanted_pids);
    fin_link(ppgid_pids);
    hash_free(ppgid_tab, NULL);

    return pru;
 }

 /* get_proc_info()
  *
  * +1 pid %d
  * 2 comm %s
  * 3 state %s
  * +4 ppid %d
  * +5 pgrp %d
  * 6 session %d
  * 7 tty_nr %d
  * 8 tpgid %d
  * 9 flags %u
  * 10 minflt %lu
  * 11 cminflt %lu
  * 12 majflt %lu
  * 13 cmajflt %lu
  * 14 +utime %lu
  * 15 +stime %lu
  * 16 cutime %ld
  * 17 cstime %ld
  * 18 priority %ld
  * 19 nice %ld
  * 20 threads %ld
  * 21 itrealvalue %ld
  * 22 starttime %llu
  * 23 +vsize %lu
  * 24 +rss %ld
  * The rest is ignore for now
  * 25 rsslim %lu
  * 26 startcode %lu
  * 27 endcode %lu
  * 28 startstack %lu
  * 29 kstkesp %lu
  * 30 kstkeip %lu
  * 31 signal %lu
  * 32 blocked %lu
  * 33 sigignore %lu
  * 34 sigcatch %lu
  * 35 wchan %lu
  * 36 nswap %lu
  * 37 cnswap %lu
  * 38 exit_signal %d
  * 39 processor %d
  * 40 rt_priority %u
  * 41 policy %u
  * 42 delayacct_blkio_ticks %llu
  * 43 guest_time %lu
  * 44 cguest_time %ld
  */
 static int
 get_proc_info(const char *buf,
               int *spid,
               int *ppid,
               int *prgd,
               uint32_t *utime,
               uint32_t *stime,
               uint32_t *vsize,
               int32_t *rss)
 {
     FILE *fp;
     long jifs;
     long page;

     fp = fopen(buf, "r");
     if (fp == NULL)
         return -1;

     jifs = sysconf(_SC_CLK_TCK);
     page = sysconf(_SC_PAGESIZE);

     fscanf(fp, "\
 %d%*s%*s%d%d%*d%*d%*d%*u%*u%*u%*u%*u%u%u %*d%*d%*d%*d%*d%*d%*u%u%d",
            spid, ppid, prgd, utime, stime, vsize, rss);
    fclose(fp);

    if (verbose
        && (*spid == global_pid
            || *ppid == global_pid))  {
        printf("%s: pid: %d ppid: %d prgd:%d utime: %d stime: %d vsize: %u rss: %d\n",
               __func__, *spid, *ppid, *prgd, *utime, *stime, *vsize, *rss);
    }

    /* Virtual memory is reported in MB
     */
    *vsize = (float)(*vsize)/(1024.0 * 1024.0);
    /* Resident set size is reported in KB
     */
    *rss = ceil(((float)(*rss)*(float)page)/1024.0);
    *utime = (*utime) / jifs;
    *stime = (*stime) / jifs;

    return 0;
}

static struct jRusage *
make_prusage(link_t *x, link_t *y, struct hash_tab *t)
{
    struct proc_info *p;
    struct jRusage *pru;
    struct hash_walk hwalk;
    int cc;
    int i;
    link_t *v[2];

    v[0] = x;
    v[1] = y;

    pru = calloc(1, sizeof(struct jRusage));

    pru->npids = LINK_NUM_ENTRIES(x) + LINK_NUM_ENTRIES(y);
    pru->pidInfo = calloc(pru->npids, sizeof(struct pidInfo));

    cc = 0;
    for (i = 0; i < sizeof(v)/sizeof(v[0]); i++) {
        while ((p = pop_link(v[i]))) {
            pru->mem = pru->mem + p->mem;
            pru->swap = pru->swap + p->swap;
            pru->utime = pru->utime + p->utime;
            pru->stime = pru->stime + p->stime;
            pru->pidInfo[cc].pid = p->pid;
            pru->pidInfo[cc].ppid = p->ppid;
            pru->pidInfo[cc].pgid = p->pgid;
            ++cc;
        }
    }

    pru->npgids = HASH_NUM_HASHED(t);
    pru->pgid = calloc(pru->npgids, sizeof(int));

    cc = 0;
    hash_walk_start(t, &hwalk);
    while ((p = hash_walk(&hwalk))) {
        pru->pgid[cc] = p->pgid;
        ++cc;
    }

    return pru;
}

static void
print_pru(pid_t pid, struct jRusage *pru)
{
    int cc;

    printf("Process %d its children and groups\n", pid);

    printf(" Resource usage:\n");
    printf("\
 mem: %d swap: %d utime: %d stime: %d\n", pru->mem, pru->swap,
           pru->utime, pru->stime);

    printf(" Number of pids %d\n", pru->npids);
    for (cc = 0; cc < pru->npids; cc++) {
        printf("\
 pid: %d ppid: %d pgid: %d\n", pru->pidInfo[cc].pid, pru->pidInfo[cc].ppid,
               pru->pidInfo[cc].pgid);
    }

    printf(" Number of pgids %d\n", pru->npgids);
    for (cc = 0; cc < pru->npgids; cc++)
        printf("pgid: %d\n", pru->pgid[cc]);
}

static void
debug_proc(const char *s, struct proc_info *p)
{
    printf("%s\n", s);
    printf("\
pid: %d ppid: %d pgid: %d mem: %d swap: %d utime: %d stime: %d\n",
           p->pid, p->ppid, p->pgid, p->mem, p->swap, p->utime, p->stime);
}

static void
free_prusage(struct jRusage *pru)
{
    if (pru == NULL)
        return;

    _free_(pru->pidInfo);
    _free_(pru->pgid);
    _free_(pru);
}
