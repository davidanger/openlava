/*
 * Copyright (C) 2015-2016 David Bigagli
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

#include "cmd.h"

static void prtGroups(struct groupInfoEnt *, int, int);
static void print_groups(struct groupInfoEnt *, int);

void
usage(const char *cmd)
{
    if (strstr(cmd, "bugroup") != NULL)
	fprintf(stderr, "%s [-h] [-V] [-r] [group_name ...]\n", cmd);
    else
        fprintf(stderr, "%s [-h] [-V] [-r] [-s] [group_name ...]\n", cmd);
}

int
main(int argc, char **argv)
{
    int cc;
    int numGroups = 0;
    int enumGrp = 0;
    char **groups=NULL;
    char **groupPoint;
    struct groupInfoEnt *grpInfo = NULL;
    int options = GRP_ALL;
    int all;
    int slots;

    if (lsb_init(argv[0]) < 0) {
	lsb_perror("lsb_init");
	return -1;
    }

    slots = 0;
    while ((cc = getopt(argc, argv, "Vhrsw")) != EOF) {
        switch (cc) {
	    case 'r':
		options |= GRP_RECURSIVE;
		break;
	    case 'V':
		fputs(_LS_VERSION_, stderr);
		return -1;
		break;
	    case 's':
		++slots;
		break;
            case 'w':
                break;
	    case 'h':
	    default:
		usage(argv[0]);
	    return -1;
        }
    }

    if (slots) {
	if (strstr(argv[0], "bugroup")) {
	    fprintf(stderr, "bugroup: -s option applies only to bmgroup\n");
	    return -1;
	}
    }

    numGroups = getNames(argc, argv, optind, &groups, &all, "group");
    enumGrp = numGroups;

    if (numGroups) {
        options &= ~GRP_ALL;
        groupPoint = groups;
    } else
        groupPoint = NULL;

    if (strstr(argv[0], "bugroup") != NULL) {
        options |= USER_GRP;
        grpInfo = lsb_usergrpinfo(groupPoint, &enumGrp, options);
    } else if (strstr(argv[0], "bmgroup") != NULL) {
	options |= HOST_GRP;
        grpInfo = lsb_hostgrpinfo(groupPoint, &enumGrp, options);
    }

    if (grpInfo == NULL) {
        if (lsberrno == LSBE_NO_USER_GROUP || lsberrno == LSBE_NO_HOST_GROUP ) {
            if (options & HOST_GRP)
                lsb_perror("host group");
            else
                lsb_perror("user group");
	    FREEUP(groups);
            return -1;
        }
        if (lsberrno == LSBE_BAD_GROUP && groups)

            lsb_perror (groups[enumGrp]);
        else
	    lsb_perror(NULL);
        FREEUP (groups);
	return -1;
    }

    if (numGroups != enumGrp && numGroups != 0 && lsberrno == LSBE_BAD_GROUP) {
	if (groups)
            lsb_perror (groups[enumGrp]);
        else
            lsb_perror(NULL);
        FREEUP(groups);
	return -1;
    }

    FREEUP(groups);
    if (slots == 0)
	prtGroups(grpInfo, enumGrp, options);
    else
	print_groups(grpInfo, enumGrp);

    return 0;
}

static void
prtGroups (struct groupInfoEnt *grpInfo, int numReply, int options)
{
    int i;
    int j;
    int  strLen;
    char *sp;
    char *cp ;
    char *save_sp;
    char *save_sp1;
    char word[256];
    char gname[256];
    char first = true;

    for (i = 0; i < numReply; i++) {

	sp = grpInfo[i].memberList;
	strcpy(gname, grpInfo[i].group);
	sprintf(word,"%-12.12s", gname);

	/* strip the end blank space
	 */
	strLen = strlen(gname);
	for (j = 0; j < strLen ; j++)
	    if (gname[j] == ' ') {
		gname[j] = '\0';
		break;
	    }
	if (first) {
	    if (options & USER_GRP)
		printf("%-12.12s  %s\n", "GROUP_NAME", "USERS");
	    else
		printf("%-12.12s  %s\n", "GROUP_NAME", "HOSTS");
	    first = FALSE;
	}
	printf("%s ", word);              /* print the group name */

	if (strcmp (sp, "all") == 0) {
	    if(options & USER_GRP) {
		printf("all users \n");
		continue;
	    }
	    else
		printf("all hosts used by the batch system\n");
	    continue;
	}

	/* Print out the group name.
	 */
	while ((cp = getNextWord_(&sp)) != NULL) {
	    save_sp = sp;
	    strcpy(word, cp);
	    printf("%s ", word);
	    save_sp1 = sp;
	    while ((cp = getNextWord_(&sp)) != NULL) {
		if (strcmp(word, cp) == 0) {
		    save_sp1 = strchr(save_sp1, *cp);
		    for (j = 0; j < strlen(cp); j++)
			*(save_sp1 + j) = ' ';
		}
		save_sp1 = sp;
	    }
	    sp = save_sp;
	}

	putchar('\n');
    }
}

/* print_groups()
 */
static void
print_groups(struct groupInfoEnt *groups, int num)
{
    int i;

    for (i = 0; i < num; i++) {
	printf("%-12s %s\n", "GROUP_NAME", groups[i].group);
	printf("%-12s %s\n", "HOSTS", groups[i].memberList);
	if (groups[i].group_slots[0] != 0) {
	    printf("%-12s %s\n", "GROUP_SLOT", groups[i].group_slots);
	    printf("%-12s %d\n", "MAX_SLOTS", groups[i].max_slots);
	}
    }
}
