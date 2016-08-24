/*
 * Copyright (C) 2014 - 2016 David Bigagli
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
 */

/* sshare = support share
 * Routines and data structure to support hierarchical
 * fairshare in MBD. Implemented in lsf/intlib for eventual
 * reuse somewhere, someday, somehow.
 */

#include "sshare.h"
#include <assert.h>

extern void ls_syslog(int, const char *, ...);

static link_t *parse_user_shares(const char *);
static link_t *parse_group_member(const char *,
                                  uint32_t,
                                  struct group_acct *);
static struct share_acct *get_sacct(const char *,
                                    const char *);
static uint32_t compute_slots(struct tree_node_ *, uint32_t, uint32_t);
static void sort_siblings(struct tree_node_ *,
                          int (*cmp)(const void *, const void *));
static char *get_next_word(char **);
static int node_cmp(const void *, const void *);
static int node_cmp2(const void *, const void *);
static int print_node(struct tree_node_ *, struct tree_ *);
static void sort_tree_by_deviate(struct tree_ *);
static void sort_tree_by_shares(struct tree_ *);
static void zero_out_sent(struct tree_ *);
static uint32_t compute_deviate(struct tree_node_ *, uint32_t, uint32_t);
static void make_leafs(struct tree_ *);

/* sshare_make_tree()
 * In this function we match the user_shares as configured
 * in the queue file with the groups configured in lsb.users.
 * The fairshare configuration in the queues can be such
 * that they don't match for example:
 *
 * FAIRSHARE = USER_SHARES[[crock ,2] [zebra, 1]]
 * FAIRSHARE = USER_SHARES[[all, 1]]
 *
 * in these cases the shares are specified for users
 * directly and no groups are involved.
 */
struct tree_ *sshare_make_tree(const char *user_shares,
                               uint32_t num_grp,
                               struct group_acct *grp)
{
    struct tree_ *t;
    link_t *l;
    link_t *stack;
    linkiter_t iter;
    struct share_acct *sacct;
    struct tree_node_ *n;
    struct tree_node_ *root;

    n = NULL;
    stack = make_link();
    t = tree_init("");
    /* root summarizes all the tree counters
     */
    t->root->data = make_sacct("/", 1);
    root = NULL;

    /* First parse the user shares and make
     * the first level of the tree
     */
    l = parse_user_shares(user_shares);

z:
    if (root)
        l = parse_group_member(n->name, num_grp, grp);
    else
        root = t->root;

    if (l == NULL) {
        free_sacct(t->root->data);
        fin_link(stack);
        tree_free(t, free_sacct);
        return NULL;
    }

    traverse_init(l, &iter);
    while ((sacct = traverse_link(&iter))) {

        n = calloc(1, sizeof(struct tree_node_));
        /* dup() the name as later we free both
         * the tree node and the share account
         */
        n->name = strdup(sacct->name);

        n = tree_insert_node(root, n);
        enqueue_link(stack, n);
        n->data = sacct;
    }

    /* Sort by shares so the tree
     * is always sorted by share
     * priority
     */
    sort_siblings(root, node_cmp);

    fin_link(l);

    n = pop_link(stack);
    if (n) {
        root = n;
        goto z;
    }

    fin_link(stack);

    n = t->root;
    while ((n = tree_next_node(n))) {
        char buf[BUFSIZ];
        /* Create the hash table of nodes and their
         * immediate parent.
         */
        sacct = n->data;
        if (n->child == NULL) {
            /* all and default are synonyms as they
             * both indicate a user that is not explicitly
             * defined.
             */
            if (strcasecmp(sacct->name, "all") == 0
                || strcasecmp(sacct->name, "default") == 0) {
                sacct->options |= SACCT_USER_ALL;
            }
            sacct->options |= SACCT_USER;
            sprintf(buf, "%s/%s", n->parent->name, n->name);
            hash_install(t->node_tab, buf, n, NULL);
        } else {
            sacct->options |= SACCT_GROUP;
        }
        sprintf(buf, "%s", n->name);
        hash_install(t->node_tab, buf, n, NULL);
        print_node(n, t);
    }

    traverse_init(t->leafs, &iter);
    while ((n = traverse_link(&iter)))
        print_node(n, t);

    /* Fairshare tree is built and sorted
     * by decreasing shares, the scheduler
     * algorithm will fill it up with
     * slots from now on.
     */
    tree_walk2(t, print_node);
    sort_tree_by_shares(t);

    return t;
}

void
sshare_sort_tree_by_ran_job(struct tree_ *t)
{
    /* This must be emptied after every scheduling
     * cycle. There could be still some leafs
     * if not all jobs got dispatched.
     */
    while (pop_link(t->leafs))
        ;
    sort_tree_by_deviate(t);
}

/* sshare_distribute_slots()
 */
int
sshare_distribute_slots(struct tree_ *t,
                        uint32_t slots)
{
    struct tree_node_ *n;
    link_t *stack;
    struct share_acct *sacct;
    uint32_t avail;
    int tried;

    stack = make_link();
    /* This must be emptied after every scheduling
     * cycle. There could be still some leafs
     * if not all jobs got dispatched.
     */
    while (pop_link(t->leafs))
        ;
    avail = slots;

    sort_tree_by_deviate(t);
    zero_out_sent(t);
    tried = 0;
    /* only after sort get the first child
     */
    n = t->root->child;
znovu:

    /* Iterate at each tree level but
     * don't traverse branches without
     * tokens.
     */
    while (n) {

        sacct = n->data;
        /* all is a dummy share account the
         * tree is populated with real ones.
         */
        if (sacct->options & SACCT_USER_ALL) {
            n = n->right;
            continue;
        }

        /* enqueue as we want to traverse
         * the tree by priority
         */
        if (n->child)
            enqueue_link(stack, n);

        avail = avail - compute_slots(n, slots, avail);

        assert(avail >= 0);
        /* As we traverse in priority order
         * the leafs are also sorted
         */
        if (n->child == NULL)
            enqueue_link(t->leafs, n);

        n = n->right;
    }

    n = pop_link(stack);
    if (n) {
        /* tokens come from the parent
         */
        sacct = n->data;
        avail = slots = sacct->sent;
        n = n->child;
        goto znovu;
    }

    if (avail > 0
        && tried == 0) {
        ++tried;
        n = t->root->child;
        goto znovu;
    }

    fin_link(stack);

    return 0;
}

/* sshare_distribute_own_slots()
 */
int
sshare_distribute_own_slots(struct tree_ *t,
                            uint32_t slots)
{
    struct tree_node_ *n;
    link_t *stack;
    struct share_acct *sacct;
    uint32_t avail;
    int tried;
    int first;

    stack = make_link();
    /* This must be emptied after every scheduling
     * cycle. There could be still some leafs
     * if not all jobs got dispatched.
     */
    while (pop_link(t->leafs))
        ;
    avail = slots;

    sort_tree_by_deviate(t);
    zero_out_sent(t);
    tried = 0;
    /* only after sort get the first child
     */
    n = t->root->child;
    first = 0;

znovu:
    /* Iterate at each tree level but
     * don't traverse branches without
     * tokens.
     */
    while (n) {

        sacct = n->data;
        /* all is a dummy share account the
         * tree is populated with real ones.
         */
        if (sacct->options & SACCT_USER_ALL) {
            n = n->right;
            continue;
        }

        /* enqueue as we want to traverse
         * the tree by priority
         */
        if (n->child)
            enqueue_link(stack, n);

        /* Enforce the ownership/guarantee at the
         * first level of the tree.
         */
        if (first == 0) {
            if (sacct->numRUN > sacct->shares)
                sacct->sent = 0;
            else
                sacct->sent = sacct->shares - sacct->numRUN;
        } else {
            avail = avail - compute_slots(n, slots, avail);
        }

        assert(avail >= 0);
        /* As we traverse in priority order
         * the leafs are also sorted
         */
        if (n->child == NULL)
            enqueue_link(t->leafs, n);

        n = n->right;
    }

    ++first;
    n = pop_link(stack);
    if (n) {
        /* tokens come from the parent
         */
        sacct = n->data;
        avail = slots = sacct->sent;
        n = n->child;
        goto znovu;
    }

    if (avail > 0
        && tried == 0) {
        ++tried;
        n = t->root->child;
        goto znovu;
    }

    fin_link(stack);

    return 0;
}

/* sort_tree_by_shares()
 */
static void
sort_tree_by_shares(struct tree_ *t)
{
    link_t *stack;
    struct tree_node_ *root;
    struct tree_node_ *n;

    stack = make_link();
    root = t->root;
    n = root->child;

znovu:
    while (n) {

        if (n->child)
            enqueue_link(stack, n);

        n = n->right;
    }

    sort_siblings(root, node_cmp);

    n = pop_link(stack);
    if (n) {
        root = n;
        n = root->child;
        goto znovu;
    }

    fin_link(stack);
    make_leafs(t);
}

/* zero_out_sent()
 */
static void
zero_out_sent(struct tree_ *t)
{
    link_t *stack;
    struct tree_node_ *root;
    struct tree_node_ *n;
    struct share_acct *s;

    stack = make_link();
    root = t->root;
    n = root->child;

znovu:
    while (n) {

        if (n->child)
            enqueue_link(stack, n);

        s = n->data;
        s->sent = 0;
        n = n->right;
    }

    n = pop_link(stack);
    if (n) {
        root = n;
        n = root->child;
        goto znovu;
    }

    fin_link(stack);
}

/* make_sacct()
 */
struct share_acct *
make_sacct(const char *name, uint32_t shares)
{
    struct share_acct *s;

    s = calloc(1, sizeof(struct share_acct));
    s->name = strdup(name);
    s->shares = shares;

    return s;
}

/* free_sacct()
 */
void
free_sacct(void *s)
{
    struct share_acct *sacct;

    sacct = s;

    _free_(sacct->name);
    _free_(sacct);
}

/* compute_slots()
 */
static uint32_t
compute_slots(struct tree_node_ *n, uint32_t total, uint32_t avail)
{
    struct share_acct *s;
    double q;
    uint32_t u;
    uint32_t x;

    s = n->data;

    q = s->dshares * (double)total;
    u = (uint32_t)ceil(q);
    x = MIN(u, avail);
    s->sent = s->sent + MIN(s->numPEND, x);

    return s->sent;
}

/* sort_tree_by_deviate()
 *
 */
static void
sort_tree_by_deviate(struct tree_ *t)
{
    struct tree_node_ *n;
    struct tree_node_ *n2;
    struct tree_node_ *root;
    link_t *stack;
    struct share_acct *s;
    uint64_t sum;
    uint64_t avail;

    stack = make_link();
    root = t->root;
    n = root->child;

znovu:
    n2 = n;
    sum = 0;
    while (n) {

        if (n->child)
            enqueue_link(stack, n);

        s = n->data;
        s->dsrv2 = 0;
        /* sum up the historical.
         */
        sum = sum + s->numRAN;

        if (logclass & LC_FAIR) {
            ls_syslog(LOG_INFO, "\
%s: updating %s pend %d run %d ran %d", __func__,
                      s->name, s->numPEND, s->numRAN, s->numRUN);
        }

        n = n->right;
    }

    avail = sum;
    n = n2;
    while (n) {

        avail = avail - compute_deviate(n, sum, avail);
        assert(avail >= 0);
        n = n->right;
    }

    sort_siblings(root, node_cmp2);

    if (logclass & LC_FAIR) {
        /* Verify the sorting order which should be from
         * less numRAN to higher ones.
         */
        n = root->child;
        while (n) {
            s = n->data;
            ls_syslog(LOG_INFO, "\
%s: sorted %s pend %d ran %d run %d", __func__,
                      s->name, s->numPEND, s->numRAN, s->numRUN);
            n = n->right;
        }
    }

    n = pop_link(stack);
    if (n) {
        root = n;
        n = n->child;
        goto znovu;
    }

    fin_link(stack);
    make_leafs(t);
}


/* compute_distance()
 */
static uint32_t
compute_deviate(struct tree_node_ *n,
                uint32_t sum,
                uint32_t avail)
{
    double q;
    struct share_acct *s;
    uint32_t u;

    s = n->data;

    q = s->dshares * (double)sum;
    u = (int32_t)ceil(q);
    /* How much you deviate from what
     * you should have used.
     */
    s->dsrv2 = u - s->numRAN;

    if (logclass & LC_FAIR) {
        ls_syslog(LOG_INFO, "\
%s: shares %s ideal %f ceil(ideal) %d numRAN %d deserve %d", __func__,
                  s->name, q, u, s->numRAN, (u - s->numRAN));
    }

    return s->dsrv2;
}

/* sort_siblings()
 *
 * Sort siblings under root.
 */
static void
sort_siblings(struct tree_node_ *root,
              int (*cmp)(const void *, const void *))

{
    struct tree_node_ *n;
    int num;
    int i;
    struct tree_node_ **v;

    if (root->child == NULL)
        return;

    num = 0;
    n = root->child;
    while (n) {
        ++num;
        n = n->right;
    }

    if (num == 1)
        return;

    v = calloc(num, sizeof(struct tree_node_ *));
    n = root->child;
    num = 0;
    while (n) {
        tree_rm_node(n);
        v[num] = n;
        ++num;
        n = n->right;
    }
    root->child = NULL;

    /* We want to sort in ascending order as we use
     * tree_insert_node() which always inserts
     * node in the left most position.
     */
    qsort(v, num, sizeof(struct tree_node_ *), cmp);

    for (i = 0; i < num; i++) {
        v[i]->parent = v[i]->right = v[i]->left = NULL;
        tree_insert_node(root, v[i]);
    }

    free(v);
}

/* make_leafs()
 */
static void
make_leafs(struct tree_ *t)
{
    struct tree_node_ *n;

    while(pop_link(t->leafs))
        ;

    n = t->root;
    while ((n = tree_next_node(n))) {

        if (n->child == NULL) {
            enqueue_link(t->leafs, n);
        }
    }
}

/* parse_user_shares()
 *
 * parse user_shares[[g, 1] [e,1]]
 */
static link_t *
parse_user_shares(const char *user_shares)
{
    link_t *l;
    char *p;
    char *u;
    int cc;
    int n;
    struct share_acct *sacct;
    uint32_t sum_shares;
    linkiter_t iter;

    u = strdup(user_shares);
    assert(u);

    p = strchr(u, '[');
    *p++ = 0;

    tokenize(p);

    l = make_link();
    sum_shares = 0;

    while (1 ) {
        char name[128];
        uint32_t shares;

        cc = sscanf(p, "%s%u%n", name, &shares, &n);
        if (cc == EOF)
            break;
        if (cc != 2)
            goto bail;
        p = p + n;

        sacct = make_sacct(name, shares);
        assert(sacct);

        sum_shares = sum_shares + sacct->shares;

        enqueue_link(l, sacct);
    }

    traverse_init(l, &iter);
    while ((sacct = traverse_link(&iter))) {
        sacct->dshares = (double)sacct->shares/(double)sum_shares;
    }

    _free_(u);
    return l;

bail:

    _free_(u);
    traverse_init(l, &iter);
    while ((sacct = traverse_link(&iter)))
        free_sacct(sacct);
    fin_link(l);

    return NULL;
}

/* parse_group_member()
 */
static link_t *
parse_group_member(const char *gname,
                   uint32_t num,
                   struct group_acct *grps)
{
    link_t *l;
    linkiter_t iter;
    struct group_acct *g;
    int cc;
    char *w;
    char *p;
    uint32_t sum;
    struct share_acct *sacct;

    l = make_link();
    g = NULL;
    for (cc = 0; cc < num; cc++) {

        /* Match the group name and the group
         * must have shares.
         */
        if (strcmp(gname, grps[cc].group) == 0
            && grps[cc].user_shares) {
            g = calloc(1, sizeof(struct group_acct));
            assert(g);
            g->group = strdup(grps[cc].group);
            g->memberList = strdup(grps[cc].memberList);
            g->user_shares = strdup(grps[cc].user_shares);
            tokenize(g->user_shares);
            break;
        }
    }

    /* gudness leaf member
     * caller will free the link
     */
    if (g == NULL)
        return l;

    p = g->memberList;
    sum = 0;
    while ((w = get_next_word(&p))) {

        sacct = get_sacct(w, g->user_shares);
        if (sacct == NULL) {
            while ((sacct = pop_link(l)))
                free_sacct(sacct);
            fin_link(l);
            return NULL;
        }
        sum = sum + sacct->shares;
        enqueue_link(l, sacct);
    }

    traverse_init(l, &iter);
    while ((sacct = traverse_link(&iter))) {
        sacct->dshares = (double)sacct->shares/(double)sum;
    }

    _free_(g->group);
    _free_(g->memberList);
    _free_(g->user_shares);
    _free_(g);

    return l;
}

static struct share_acct *
get_sacct(const char *acct_name, const char *user_list)
{
    char name[128];
    uint32_t shares;
    int cc;
    int n;
    struct share_acct *sacct;
    char *p;
    char *p0;

    p0 = p = strdup(user_list);
    /* If we don't find account name
     * in the user list make sure we
     * return NULL and not random memory.
     */
    sacct = NULL;

    cc = sscanf(p, "%s%u%n", name, &shares, &n);
    if (cc == EOF) {
        _free_(p);
        return NULL;
    }

    /* default can have users all
     * or explicitly named users
     */
    if (strcmp(name, "default") == 0) {
        sacct = make_sacct(acct_name, shares);
        goto pryc;
    }

    /* Match the user in the user_list
     */
    while (1) {

        cc = sscanf(p, "%s%u%n", name, &shares, &n);
        if (cc == EOF)
            break;
        if (strcmp(name, acct_name) != 0) {
            p = p + n;
            continue;
        }
        sacct = make_sacct(name, shares);
        break;
    }

pryc:
    _free_(p0);
    return sacct;
}

void
tokenize(char *p)
{
    int cc;
    int l;

    l = strlen(p);

    for (cc = 0; cc < l; cc++) {
        if (p[cc] == '['
            || p[cc] == ']'
            || p[cc] == ',')
            p[cc] = ' ';
    }
}

static char *
get_next_word(char **line)
{
    static char word[BUFSIZ];
    char *wordp = word;

    while (isspace(**line))
        (*line)++;

    while (**line && !isspace(**line))
        *wordp++ = *(*line)++;

    if (wordp == word)
        return NULL;

    *wordp = '\0';
    return word;
}

/* node_cmp()
 *
 * Function for qsort(), sort nodes by
 * shares
 */
static int
node_cmp(const void *x, const void *y)
{
    struct tree_node_ *n1;
    struct tree_node_ *n2;
    struct share_acct *s1;
    struct share_acct *s2;

    n1 = *(struct tree_node_ **)x;
    n2 = *(struct tree_node_ **)y;

    s1 = n1->data;
    s2 = n2->data;

    /* We want to sort in ascending order as we use
     * tree_inser_node() which always inserts
     * node in the left most position.
     */
    if (s1->shares > s2->shares)
        return 1;
    if (s1->shares < s2->shares)
        return -1;

    return 0;
}

/* node_cmp2()
 *
 * Function for qsort() sort nodes by
 * dsrv2.
 */
static int
node_cmp2(const void *x, const void *y)
{
    struct tree_node_ *n1;
    struct tree_node_ *n2;
    struct share_acct *s1;
    struct share_acct *s2;
    int cc;

    n1 = *(struct tree_node_ **)x;
    n2 = *(struct tree_node_ **)y;

    s1 = n1->data;
    s2 = n2->data;

    /* Sort in descending order as the
     * tree insertion algorithm always
     * inserts right.
     * 5 2 -1 -4
     * will become
     * -4 -1 2 5
     * once on the tree
     */
    cc = 0;
    if (s1->dsrv2 > 0
        && s2->dsrv2 < 0) {
        cc = 1;
    } else if (s1->dsrv2 < 0
               && s2->dsrv2 > 0) {
        cc = -1;
    } else {
        if (s1->dsrv2 > s2->dsrv2)
            cc = 1;
        if (s1->dsrv2 < s2->dsrv2)
            cc = -1;
    }

    return cc;
}

/* print_node()
 */
static int
print_node(struct tree_node_ *n, struct tree_ *t)
{
    struct share_acct *s;

    if (t == NULL
        || n == NULL)
        return -1;

    s = n->data;

    if (0)
        printf("%s: node %s shares %d dshares %4.2f\n",
               __func__, n->name, s->shares, s->dshares);

    return -1;
}
