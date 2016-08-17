

#ifndef _TOOLS_HASH_
#define _TOOLS_HASH_

#include "intlibout.h"

/* Hash table is a list whose nodes are
 * the hash_ elements.
 */
struct hash_tab {
    int size;
    int num;
    int resize;
    struct list_ **v;
};

/* each hash elements has a key, an opaque data structure
 * passed in by the user and links
 */
struct hash_ent {
    struct hash_ent *forw;
    struct hash_ent *back;
    char *key;
    void *e;
};

/* iterator kinda thing, it attaches itself to the
 * hash and the function hashwalk() iterates through
 * the entire table
 */
struct hash_walk {
    int z;
    struct hash_ent *ent;
    struct hash_tab *tab;
};

/* number of hashed objects in the table
 */
#define HASH_NUM_HASHED(T) ((T)->num)

extern struct hash_tab   *hash_make(int);
extern void  *hash_lookup(struct hash_tab *,
                          const char *);
extern struct hash_ent *hash_install(struct hash_tab *,
                                     const char *,
                                     void *,
                                     int *);
extern void   *hash_rm(struct hash_tab *,
                       const char *);
extern void  hash_free(struct hash_tab *,
                       void (*f)(void *));
extern void  hash_walk_start(struct hash_tab *,
                             struct hash_walk *);
extern void  hash_walk_end(struct hash_walk *);
extern void  *hash_walk(struct hash_walk *);

#endif /* _TOOLS_HASH_ */
