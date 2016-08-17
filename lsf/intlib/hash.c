
/* Chain hash tables depends on links.
 */
#include "hash.h"
#include "list2.h"

static unsigned long hash_string(const char *, int);
static void re_hash(struct hash_tab *, int);
static int primesz(int);

static int primes[] =
{
    7,
    11,
    293,
    941,
    1427,
    1619,
    2153,
    5483,
    10891,       /* 10K */
    24571,
    69857,
    111697,
    200003,
    1000003,     /* 1MB */
    2000003,
    8000099,
    16000097,
    50000063,    /* 50 MB */
    100000081,   /* 100 MB */
    150999103,
    250000103,   /* 250MB */
    500000101,   /* 500MB */
    750003379,   /* 750MB */
    1000004897,  /* 1GB */
    2002950673   /* 2GB that's one mother */
};

/* hash_string()
 */
static unsigned long
hash_string(const char *p, int size)
{
    register unsigned long h = 0;

    for(; *p; ++p ) {
        register unsigned long g;

        h <<= 4;
        h += *p;
        if ( (g = h & 0xF0000000) )
            h ^= g >> 24;
        h &= ~g;
    }

    return h % size;

}

/* hash_make()
 */
struct hash_tab *
hash_make(int size)
{
    struct hash_tab *tab;

    size = primesz(size);

    tab = calloc(1, sizeof(struct hash_tab));
    assert(tab);
    tab->size = size;
    /* the array of lists
     */
    tab->v = calloc(tab->size, sizeof(struct list_ *));
    assert(tab->v);

    return tab;

} /* hashmake() */

/* hash_lookup()
 */
void *
hash_lookup(struct hash_tab *tab,
            const char *ukey)
{
    unsigned long z;
    struct hash_ent *ent;

    z = hash_string(ukey, tab->size);

    if (tab->v[z] == NULL)
        return NULL;

    for (ent = (struct hash_ent *)tab->v[z]->back;
         ent != (void *)tab->v[z];
         ent = ent->back) {

        if (strcmp(ent->key, ukey) == 0) {
            return ent->e;
        }
    }

    return NULL;

}

/* hash_install()
 */
struct hash_ent *
hash_install(struct hash_tab *tab,
             const char *key,
             void *e,
             int  *dup)
{
    unsigned long z;
    struct hash_ent *ent;

    ent = hash_lookup(tab, key);
    if (ent) {
        if (dup)
            *dup = 1;
        return ent;
    }

    if (tab->num >= 0.9 * tab->size) {
#if 0
        fprintf(stderr, "resize num %d num*0.9 %d size %d, newsize %d\n",
                tab->num, (int)(tab->size * 0.9) , tab->size, 2*tab->size);
#endif
        re_hash(tab, 3 * tab->size);
        tab->resize++;
    }

    z = hash_string(key, tab->size);

    ent = calloc(1, sizeof(struct hash_ent));
    assert(ent);
    ent->key = strdup(key);
    assert(ent->key);
    ent->e = e;

    if (tab->v[z] == NULL)
        tab->v[z] = listmake("ulist");

    tab->num++;
    listenque(tab->v[z], (struct list_ *)ent);
    if (dup)
        *dup = 0;

    return ent;
}

/* hash_rm()
 */
void *
hash_rm(struct hash_tab *tab,
        const char *ukey)
{
    struct hash_ent *ent;
    unsigned long z;

    z = hash_string(ukey, tab->size);

    if (tab->v[z] == NULL)
        return NULL;

    for (ent = (struct hash_ent *)tab->v[z]->forw;
         ent != (void *)tab->v[z];
         ent = ent->forw) {

        if (strcmp(ent->key, ukey) == 0) {
            void   *e;

            listrm(tab->v[z], (struct list_ *)ent);
            e = ent->e;
            free(ent->key);
            free(ent);
            tab->num--;

            return e;
        }
    }

    return NULL;
}

/* re_hash()
 */
static void
re_hash(struct hash_tab *tab,
        int size)
{
    struct list_ **L;
    int c;
    struct hash_tab tab2;

    memset(&tab2, 0, sizeof(tab2));

    c = primesz(size);
    tab2.size = c;

    L = calloc(c, sizeof(struct list_ *));
    assert(L);
    tab2.v = L;

    for (c = 0; c < tab->size; c++) {
        struct hash_ent *ent;

        if (tab->v[c] == NULL)
            continue;

        while ((ent = (struct hash_ent *)listpop(tab->v[c]))) {
            int   dup;

            hash_install(&tab2, ent->key, ent->e, &dup);
            assert(dup == 0);
            free(ent->key);
            free(ent);
        }
        listfree(tab->v[c], NULL);
    }

    free(tab->v);
    tab->v = L;
    tab->size = tab2.size;
    tab->num  = tab2.num;
}

/* hash_free()
 */
void
hash_free(struct hash_tab *tab,
          void (*f)(void *))
{
    int c;

    for (c = 0; c < tab->size; c++) {
        struct hash_ent *ent;

        if (tab->v[c] == NULL)
            continue;

        while ((ent = (struct hash_ent *)listpop(tab->v[c]))) {

            if (f)
                (*f)(ent->e);

            free(ent->key);
            free(ent);
        }
        listfree(tab->v[c], NULL);

    } /* for(c = 0; ...) */

    free(tab->v);
    free(tab);
}

/* primesz()
 */
static int
primesz(int c)
{
    int   i;
    int   N;

    N = sizeof(primes)/sizeof(primes[0]);

    for (i = 0; i < N; i++) {
        if (c <= primes[i]) {
            c = primes[i];
            break;
        }
    }

    if (i == N)
        c = primes[N - 1];

    return c;

} /* primesz() */

/* hash_walk_start()
 */
void
hash_walk_start(struct hash_tab  *tab,
                struct hash_walk *w)
{
    if (!w)
        return;

    w->z = 0;
    w->ent = NULL;
    w->tab = tab;

}

/* hash_walk_end()
 */
void
hash_walk_end(struct hash_walk *w)
{
    return;
}

/* hash_walk()
 */
void *
hash_walk(struct hash_walk *w)
{
    struct list_ *L;
    struct hash_ent *ent;

    /* walk on the slots
     */
    while (w->z < w->tab->size) {

        L = w->tab->v[w->z];

        /* empty slot
         */
        if (L == NULL
            || LIST_NUM_ENTS(L) == 0) {
            w->z++;
            continue;
        }

        if (w->ent == NULL)
            w->ent = (struct hash_ent *)L->back;

        if (w->ent == (void *)L) {
            w->ent = NULL;
            w->z++;
            continue;
        }

        ent = w->ent;
        w->ent = ent->back;

        return ent->e;
    }

    return NULL;
}
