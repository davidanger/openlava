/*
 * Copyright (C) 2014-2016 David Bigagli
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

#if !defined( _TREE_HEADER_)
#define _TREE_HEADER_

#include "hash.h"
#include "link.h"

struct tree_ {
    char *name;
    struct tree_node_ *root;
    struct hash_tab *node_tab;
    link_t *leafs;
};

/* Generic tree structure
 */
struct tree_node_ {
    struct tree_node_ *parent;
    struct tree_node_ *child;
    struct tree_node_ *left;
    struct tree_node_ *right;
    char *name;
    void *data;
};

extern struct tree_ *tree_init(const char *);
extern void tree_free(struct tree_ *, void(*f)(void *));
extern struct tree_node_ *tree_insert_node(struct tree_node_ *,
                                           struct tree_node_ *);
extern struct tree_node_ *tree_rm_node(struct tree_node_ *);
extern int tree_print(struct tree_node_ *, struct tree_ *);
extern int tree_walk(struct tree_ *,
                     int (*f)(struct tree_node_ *,
                              struct tree_ *));
extern int tree_walk2(struct tree_ *,
                      int (*f)(struct tree_node_ *,
                               struct tree_ *));
extern struct tree_node_ *tree_next_node(struct tree_node_ *);

#endif /* _TREE_HEADER */
