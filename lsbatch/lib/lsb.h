/*
 * Copyright (C) 2014-2015 David Bigagli
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA  02110-1301, USA
 *
 */

#ifndef _LSB_LIB_H_
#define _LSB_LIB_H_

#include "../../lsf/lsf.h"
#include "../lsbatch.h"
#include "../../lsf/lib/lib.h"
#include "../../lsf/lib/lib.table.h"
#include "../../lsf/intlib/intlibout.h"
#include "../daemons/daemonout.h"
#include "lsb.spool.h"
#include "../cmd/cmd.h"
#include "lsb.sig.h"

#ifndef MIN
#define MIN(x,y)        ((x) < (y) ? (x) : (y))
#endif

#ifndef MAX
#define MAX(x,y)        ((x) > (y) ? (x) : (y))
#endif

extern char **environ;

/* In lsb.init.c
 */
extern int lsbMode_;

extern int _lsb_conntimeout;
extern int _lsb_recvtimeout;
extern int _lsb_fakesetuid;

extern struct config_param lsbParams[];
extern int initenv_(struct config_param *, char *);
extern int sig_encode(int);

#define DEFAULT_API_CONNTIMEOUT 10
#define DEFAULT_API_RECVTIMEOUT 0

/* The config_param array is defined in lsb.init.c
 */
#define LSB_DEBUG         0
#define LSB_SHAREDIR      1
#define LSB_SBD_PORT      2
#define LSB_MBD_PORT      3
#define LSB_DEBUG_CMD     4
#define LSB_TIME_CMD      5
#define LSB_CMD_LOGDIR    6
#define LSB_CMD_LOG_MASK  7
#define LSB_API_CONNTIMEOUT 9
#define LSB_API_RECVTIMEOUT 10
#define LSB_SERVERDIR 11
#define LSB_MODE 12
#define LSB_SHORT_HOSTLIST 13
#define LSB_INTERACTIVE_STDERR 14
#define LSB_32_PAREN_ESC     15
#define LSB_API_QUOTE_CMD    16
#define LSB_DEFAULT_USER_GROUP 17

typedef struct lsbSubSpoolFile {
    char inFileSpool[MAXFILENAMELEN];
    char commandSpool[MAXFILENAMELEN];
} LSB_SUB_SPOOL_FILE_T;

extern int creat_p_socket(void);
extern int serv_connect(char *, ushort, int);
extern int getServerMsg(int, struct LSFHeader *, char **);
extern int callmbd(char *,
                   char *,
                   int,
                   char **,
                   struct LSFHeader *,
                   int *,
                   int (*)(), int *);
extern int cmdCallSBD_(char *,
                       char *,
                       int,
                       char **,
                       struct LSFHeader *,
                       int *);
extern int authTicketTokens_(struct lsfAuth *, char *);
extern int readNextPacket(char **, int, struct LSFHeader *, int);
extern void closeSession(int);
extern void upperStr(char *, char *);
extern char* getUnixSpoolDir(char *);
extern char *getMasterName(void);
extern ushort get_mbd_port (void);
extern ushort get_sbd_port (void);
extern int getAuth(struct lsfAuth *);
extern int getCommonParams (struct submit  *, struct submitReq *,
                            struct submitReply *);
extern int getOtherParams(struct submit *, struct submitReq *,
                          struct submitReply *, struct lsfAuth *,
                          LSB_SUB_SPOOL_FILE_T*);
extern int chUserRemoveSpoolFile(const char *,
                                 const char *);
extern void prtBETime_(struct submit *);
extern int runBatchEsub(struct lenData *, struct submit *);

extern void makeCleanToRunEsub();
extern char *translateString(char *);
extern void modifyJobInformation(struct submit *);
extern void compactXFReq(struct submit *);
extern char *wrapCommandLine(char *);
extern char *unwrapCommandLine(char *);
extern int  checkEmptyString(char *);
extern int  stringIsToken(char *,char *);
extern int  stringIsDigitNumber(char *);
extern int processXFReq(char *, char *, struct submit *);
extern char *extractStringValue(char *);
extern void copyJUsage(struct jRusage *, struct jRusage *);
extern void bsub_usage(int);
#endif
