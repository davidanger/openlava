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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 *
 */


#include "cmd.h"
#include "../lib/lsb.h"

static void
usage(void)
{
    fprintf(stderr, "blaunch: [-h] [-V] [-n] [-z hosts...]\n");
}

/* Global variables we revolve around, the program works
 * with parallel arrays.
 */
static int arg_count;
static char **arg_char;
static char **host_list;
static char **command;
extern char **environ;
static char buf[BUFSIZ];
static int jobID;
static char *hostname;
static int verbose;

/* Functions using global variables
 */
static int redirect_stdin(void);
static int get_host_list(const char *);
static int get_host_list_from_file(const char *);
static int count_lines(FILE *);
static int get_hosts_from_arg(void);
static void build_command(void);
static void free_hosts(void);
static void free_command(void);
static void print_hosts(void);
static int get_job_id(void);

int
main(int argc, char **argv)
{
    int cc;
    char z;
    char u;

    /* If this is set, then option processing stops as soon as a
     * non-option argument is encountered.
     */
    setenv("POSIXLY_CORRECT", "Y", 1);

    arg_count = argc;
    arg_char = argv;

    if (argc <= 1) {
        usage();
        return -1;
    }

    u = z = 0;
    while ((cc = getopt(argc, argv, "hVvnz:u:t:")) != EOF) {
        switch (cc) {
            case 'v':
                ++verbose;
                break;
            case 'n':
                redirect_stdin();
                break;
            case 'z':
                if (u == 0)
                    get_host_list(optarg);
                ++z;
                break;
            case 'u':
                if (z == 0)
                    get_host_list_from_file(optarg);
                ++u;
                break;
            case 't':
                setenv("LSB_BLAUNCH_SLEEPTIME", optarg, 1);
                break;
            case 'h':
            case '?':
                usage();
                return(-1);
        };
    }


    /* initialize the remote execution
     * library
     */
    if (ls_initrex(1, 0) < 0) {
        fprintf(stderr, "blaunch: ls_initrex() failed %s", ls_sysmsg());
        return -1;
    }

    /* Since we call SBD initialize the batch library as well.
     */
    if (lsb_init("blaunch") < 0) {
        fprintf(stderr, "blaunch: lsb_init() failed %s", lsb_sysmsg());
        return -1;
    }

    /* Open log to tell user what's going on
     */
    if (verbose) {
        /* Use stderr
         */
        ls_openlog("blaunch", NULL, true, "LOG_INFO");
    } else {
        ls_openlog("blaunch",
                   genParams_[LSF_LOGDIR].paramValue,
                   false,
                   genParams_[LSF_LOG_MASK].paramValue);
    }

    if (get_job_id()) {
        ls_syslog(LOG_ERR, "%s: cannot run without jobid", __func__);
        return -1;
    }

    if (z > 0
        && u > 0) {
        ls_syslog(LOG_ERR, "blaunch: -u and -z are mutually exclusive");
        usage();
        return -1;
    }

    hostname = ls_getmyhostname();

    if (!host_list)
        get_hosts_from_arg();

    if (!host_list) {
        usage();
        return -1;
    }

    print_hosts();

    build_command();
    if (! command) {
        ls_syslog(LOG_ERR, "blaunch: no command to run?");
        usage();
        return -1;
    }

    if (lsb_launch(host_list, command, 0, environ) < 0) {
        ls_syslog(LOG_ERR, "%s: lsb_launch() failed %m", "blaunch");
        free_hosts();
        free_command();
        return -1;
    }

    free_hosts();
    free_command();

    return 0;
}

static int
redirect_stdin(void)
{
    int cc;

    cc = open("/dev/null", O_RDONLY);
    if (cc < 0)
        return -1;

    dup2(cc, STDIN_FILENO);

    return 0;
}

/* get_host_list()
 *
 * Get host list from -z option
 */
static int
get_host_list(const char *hosts)
{
    char *p;
    char *p0;
    char *h;
    int n;

    p0 = p = strdup(hosts);
    n = 0;
    while (getNextWord_(&p))
        ++n;

    host_list = calloc(n + 1, sizeof(char *));
    _free_(p0);

    n = 0;
    p0 = p = strdup(hosts);
    while ((h = getNextWord_(&p))) {
        host_list[n] = strdup(h);
        ++n;
    }

    _free_(p0);

    return 0;
}

/* get_host_list_from_file()
 *
 * Get host list from the host file
 */
static int
get_host_list_from_file(const char *file)
{
    char s[MAXHOSTNAMELEN];
    FILE *fp;
    int n;

    fp = fopen(file, "r");
    if (fp == NULL) {
        return -1;
    }

    n = count_lines(fp);
    host_list = calloc(n + 1, sizeof(char *));

    rewind(fp);
    n = 0;
    while ((fgets(s, sizeof(s), fp))) {
        s[strlen(s) - 1] = 0;
        host_list[n] = strdup(s);
        ++n;
    }

    return 0;
}

/* count_lines()
 */
static int
count_lines(FILE *fp)
{
    int n;
    int cc;

    n = 0;
    while ((cc = fgetc(fp)) != EOF) {
        if (cc == '\n')
            ++n;
    }

    return n;
}

/* get_hosts_from_args()
 *
 * Get host list from commmand line arguments
 */
static int
get_hosts_from_arg(void)
{
    char *host;
    struct hostent *hp;

    /* We have no -u nor -z so optind
     * points to the first non option
     * parameter which must be a host.
     */

    host = arg_char[optind];
    hp = gethostbyname(host);
    if (hp == NULL) {
        ls_syslog(LOG_ERR, "\
%s: gethostbyname(%s) failed %s", __func__, host, hstrerror(h_errno));
        return -1;
    }

    host_list = calloc(2, sizeof(char *));
    host_list[0] = strdup(host);
    /* increase so we skip over the next argv[] since
     * it is the host we get from the command line on which
     * we are going to run
     */
    ++optind;

    return 0;
}

static void
print_hosts(void)
{
    char *h;
    int cc;

    sprintf(buf, "host(s): ");
    cc = 0;
    while ((h = host_list[cc])) {
        sprintf(buf + strlen(buf), "%s ", h);
        ++cc;
    }

    ls_syslog(LOG_INFO, "%s: host list: %s", __func__, buf);

}

static void
build_command(void)
{
    int cc;
    int n;
    int l;

    /* no command to build, optind now points
     * to the first non option argument of blaunch
     */
    if (arg_count - optind <= 0)
        return;

    /* the command array is NULL terminated as UNIX wants
     */
    command = calloc((arg_count - optind) + 1, sizeof(char *));

    n = 0;
    for (cc = optind; cc < arg_count; cc++) {
        command[n] = strdup(arg_char[cc]);
        l = strlen(command[n]);
        while (command[n][0] == ' ' || command[n][0] == '"') {
            memmove(command[n], command[n] + 1, l);
            l--;
        }
        if (command[n][l-1] == '"')
            command[n][l-1] = '\0';
        ++n;
    }

    sprintf(buf, "command: ");
    cc = 0;
    while (command[cc]){
        sprintf(buf + strlen(buf), " %s ", command[cc]);
        ++cc;
    }
    ls_syslog(LOG_INFO, "%s: user command: %s", __func__, buf);
}

static void
free_hosts(void)
{
    int cc;

    if (! host_list)
        return;

    cc = 0;
    while (host_list[cc]) {
        _free_(host_list[cc]);
        ++cc;
    }

    _free_(host_list);
}

static void
free_command(void)
{
    int cc;

    if (! command)
        return;

    cc = 0;
    while (command[cc]) {
        _free_(command[cc]);
        ++cc;
    }

    _free_(command);
}


/* get_job_id()
 */
static int
get_job_id(void)
{
    char *j;

    if (! (j = getenv("LSB_JOBID")))
        return -1;

    jobID = atoi(j);

    return 0;
}
