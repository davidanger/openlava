/*
 * Copyright (C) 2014 David Bigagli
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

/*
 *
 * Elementary single linked list in C.
 */
#include <stdlib.h>
#include <string.h>
#include "link.h"

/* Make a new link
 */
link_t *
make_link(void)
{
    link_t *link;

    link = calloc(1, sizeof(link_t));
    if (!link)
        return NULL;

    return link;

} /* initLink() */

/* Insert in front of the head.
 * This is the stack push operation.
 */
int
in_link(link_t *head, void *val)
{
    link_t *el;

    if (!head)
        return -1;

    el = calloc(1, sizeof(link_t));
    if (!el)
        return -1;

    el->ptr = val;
    el->next = head->next;
    head->next = el;
    head->num++;

    return 0;
}

void
fin_link(link_t * head)
{
    if (!head)
        return;

    while (pop_link(head))
        head->num--;

    free(head);
    head = NULL;
}

/* Just wrap the inLink() and call it push.
 */
int
push_link(link_t *head, void *val)
{
    int cc;

    if (!head)
        return -1;

    cc = in_link(head, val);

    return cc;
}

/* The stack pop operation.
 */
void *
pop_link(link_t *head)
{
    link_t *p;
    void *t;

    if (!head)
        return NULL;

    if (head->next != NULL) {
        p = head->next;
        head->next = p->next;
        t = p->ptr;
        free(p);
        head->num--;
        return t;
    }

    return NULL;
}

/* Queue operation. If the link is long
 * this operation is expensive.
 */
int
enqueue_link(link_t *head, void *val)
{
    link_t *p;
    link_t *p2;

    if (!head)
        return -1;

    p2 = calloc(1, sizeof(link_t));
    if (!p2)
        return -1;

    p2->ptr = val;

    /* walk till the end... if long this cost you...
     */
    for (p = head; p->next != NULL; p = p->next)
        ;

    p2->next = p->next;
    p->next = p2;
    head->num++;

    return 0;
}

/* The opposite of pop_link(), return
 * the first element in the list,
 * first in first out, if you inserted
 * the elements by pushLink()
 */
void *
dequeue_link(link_t *head)
{
    link_t *p;
    link_t *p2;
    void *t;

    if (!head
        || !head->next)
        return NULL;

    p2 = head;
    p = head->next;

    while (p->next) {
        p2 = p;
        p = p->next;
    }

    p2->next = p->next;
    t = p->ptr;
    free(p);
    head->num--;

    return t;
}

/* Insert in the linked list based on some priority defined
 * by the user function int (*cmp). (*cmp) is supposed to
 * behave like the compare function of qsort(3).
 */
int
enqueue_sort_link(link_t *head,
                  void *val,
                  void *extra,
                  int (*cmp)(const void *,
                             const void *))
{
    link_t *t;
    link_t *t2;
    link_t *p;
    int cc;

    if (! head)
        return -1;

    t = head;
    t2 = head->next;

    while (t2) {

        /* val is the new element
         */
        cc = (*cmp)(val, t2->ptr);
        if (cc <= 0)
            break;

        t = t2;
        t2 = t2->next;
    }

    p = calloc(1, sizeof(link_t));
    if (!p)
        return -1;

    p->next = t2;
    t->next = p;
    p->ptr = val;
    head->num++;

    return 0;
}

/* Return the address of the first element saved in the
 * in the linked list, the top of the stack. Unlike pop_link()
 * this routine does not remove the element from the list.
 */
void *
visit_link(link_t *head)
{
    void *p;

    if (!head || !head->next)
        return NULL;

    p = head->next->ptr;

    return p;

}

/* Remove the element val from the link.
 */
void *
rm_link(link_t *head, void *val)
{
    link_t *p;
    link_t *t;
    void *v;

    if (!head)
        return NULL;

    /* Since we have only one link
     * we need to keep track of 2
     * elements: the current and
     * the previous.
     */

    t = head;
    for (p = head->next;
         p != NULL;
         p = p->next) {
        if (p->ptr == val) {
            t->next = p->next;
            v = p->ptr;
            free(p);
            head->num--;
            return v;
        }
        t = p;
    }

    return p;
}


/* Find an element val, return it, but do not
 * remove from the list.
 */
void *
peek_link(link_t *head, void *val)
{
    link_t   *p;

    for (p = head->next;
         p != NULL;
         p = p->next) {
        if (p->ptr == val) {
            return p->ptr;
        }
    }

    return NULL;
}

/* We could this interface as
 * traverseInit(link_t *head, link_t **iter),
 * however then iter would need to be dynamically
 * allocated, while in this case it can be
 * an automatic variable defined by the caller.
 *
 * Here we still think that the STL concept of
 * iterator is useful on collections that have a bit
 * of underlying complexity....although how complex
 * is:
 *    for (p = head; p->next != NULL; p = p->next)
 *           dome(p->next);
 *
 */
void
traverse_init(const link_t *head, linkiter_t *iter)
{
    if (head == NULL) {
        memset(iter, 0, sizeof(linkiter_t));
        return;
    }

    iter->pos = head->next;
}

void *
traverse_link(linkiter_t *iter)
{
    void *p;

    if (!iter)
        return NULL;

    if (iter->pos != NULL) {
        p = iter->pos->ptr;
        iter->pos = iter->pos->next;
        return p;
    }

    return NULL;
}

#if 0
/* Dybag && test
 */
static void
printLink(link_t *h)
{
    link_t *p;

    printf("head %p\n", h);

    for (p = h->next; p != NULL; p = p->next) {
        printf("%p\n", p);
    }
}
#include<stdio.h>
#include"link.h"
#include<stdlib.h>

int
main(void)
{
    link_t   *h;
    int      i;
    char     *p;

    h = initLink();

    for (i = 0; i < 1024 * 2; i++) {

        p = malloc(128);
        enqueueLink(h, p);
        printf("%p enqueued\n", p);
    }

    printLink(h);

    while ((p = dequeueLink(h))) {
        printf("%p dequeued\n", p);
        free(p);
    }

    finLink(h);

    return 0;
}
#endif
