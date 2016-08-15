/*
 * Copyright (C) 2007 Platform Computing Inc
 * Copyright (C) 2014-2015 David Bigagli
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

#include "lsb.h"

static int mbdTries(void);
int lsb_mbd_version = -1;

#define MAXMSGLEN  (16 * 1024 * 1024)

int
serv_connect(char *serv_host, ushort serv_port, int timeout)
{
    int chfd;
    int cc;
    struct sockaddr_in serv_addr;
    const struct hostent *hp;

    memset((char*)&serv_addr, 0, sizeof(serv_addr));

    serv_addr.sin_family = AF_INET;
    if ((hp = Gethostbyname_(serv_host)) == 0) {
        lsberrno = LSBE_BAD_HOST;
        return -1;
    }

    memcpy((char *)&serv_addr.sin_addr, (char *)hp->h_addr,
           (int)hp->h_length);
    serv_addr.sin_port = serv_port;

    chfd = chanClientSocket_(AF_INET, SOCK_STREAM, 0);
    if (chfd < 0) {
        lsberrno = LSBE_LSLIB;
        return -1;
    }

    cc = chanConnect_(chfd, &serv_addr, timeout * 1000, 0);
    if (cc < 0) {
        switch(lserrno) {
            case LSE_TIME_OUT:
                lsberrno =LSBE_CONN_TIMEOUT;
                break;
            case LSE_CONN_SYS:
                if (errno == ECONNREFUSED || errno == EHOSTUNREACH)
                    lsberrno = LSBE_CONN_REFUSED;
                else
                    lsberrno = LSBE_SYS_CALL;
                break;
            default:
                lsberrno = LSBE_SYS_CALL;
        }
        chanClose_(chfd);
        return -1;
    }

    return chfd;
}

int
call_server(char * host,
            ushort serv_port,
            char * req_buf,
            int    req_size,
            char **rep_buf,
            struct LSFHeader *replyHdr,
            int conn_timeout,
            int recv_timeout,
            int *connectedSock,
            int (*postSndFunc)(),
            int *postSndFuncArg,
            int flags)
{
    int cc;
    struct Buffer *sndBuf;
    struct Buffer reqbuf, reqbuf2, replybuf;
    struct Buffer *replyBufPtr;
    int serverSock;

    *rep_buf = NULL;
    lsberrno = LSBE_NO_ERROR;

    if (!(flags & CALL_SERVER_USE_SOCKET)) {
        if ((serverSock = serv_connect (host, serv_port, conn_timeout)) < 0)
            return -2;
    } else {
        if (connectedSock == NULL) {
            lsberrno = LSBE_BAD_ARG;
            return -2;
        }
        serverSock = *connectedSock;
    }

    if (!(flags & CALL_SERVER_NO_HANDSHAKE)) {

        if (handShake_(serverSock, TRUE, conn_timeout) < 0) {
            CLOSECD(serverSock);
            return -2;
        }
    }

    CHAN_INIT_BUF(&reqbuf);
    reqbuf.len = req_size;
    reqbuf.data = req_buf;

    if (postSndFunc) {
        CHAN_INIT_BUF(&reqbuf2);
        reqbuf2.len =  ((struct lenData *)postSndFuncArg)->len;
        reqbuf2.data = ((struct lenData *)postSndFuncArg)->data;
        reqbuf.forw = &reqbuf2;
    }

    if (flags & CALL_SERVER_NO_WAIT_REPLY)
        replyBufPtr = NULL;
    else
        replyBufPtr = &replybuf;

    if (flags & CALL_SERVER_ENQUEUE_ONLY) {
        int tsize = req_size;

        if (chanSetMode_(serverSock, CHAN_MODE_NONBLOCK) < 0) {
            CLOSECD(serverSock);
            return -2;
        }

        if (postSndFunc)
            tsize += ((struct lenData *)postSndFuncArg)->len + NET_INTSIZE_;

        if (chanAllocBuf_(&sndBuf, tsize) < 0) {
            CLOSECD(serverSock);
            return -2;
        }

        sndBuf->len = tsize;
        memcpy((char *) sndBuf->data, (char *) req_buf, req_size);

        if (postSndFunc) {
            int nlen = htonl(((struct lenData *)postSndFuncArg)->len);

            memcpy((char *) sndBuf->data + req_size,
                   (char *) NET_INTADDR_(&nlen), NET_INTSIZE_);
            memcpy((char *) sndBuf->data + req_size + NET_INTSIZE_,
                   (char *) ((struct lenData *)postSndFuncArg)->data,
                   ((struct lenData *)postSndFuncArg)->len);
        }

        if (chanEnqueue_(serverSock, sndBuf) < 0) {
            chanFreeBuf_(sndBuf);
            CLOSECD(serverSock);
            return -2;
        }

    } else {
        cc = chanRpc_(serverSock,
                      &reqbuf,
                      replyBufPtr,
                      replyHdr,
                      recv_timeout * 1000);
        if (cc < 0) {
            lsberrno = LSBE_LSLIB;
            CLOSECD(serverSock);
            return -1;
        }
    }

    if (flags & CALL_SERVER_NO_WAIT_REPLY)
        *rep_buf = NULL;
    else
        *rep_buf = replybuf.data;

    if (connectedSock) {
        *connectedSock = serverSock;
    } else {
        chanClose_(serverSock);
    }

    if (flags & CALL_SERVER_NO_WAIT_REPLY)
        return 0;
    else
        return replyHdr->length;

}

int
getServerMsg(int serverSock, struct LSFHeader *replyHdr, char **rep_buf)
{
    int len;
    struct LSFHeader hdrBuf;
    XDR  xdrs;

    xdrmem_create (&xdrs, (char *)&hdrBuf,
                   sizeof(struct LSFHeader), XDR_DECODE);

    if (readDecodeHdr_(serverSock, (char *)&hdrBuf,
                       b_read_fix, &xdrs, replyHdr) < 0) {
        if (LSE_SYSCALL(lserrno)) {
            lsberrno = LSBE_SYS_CALL;
        } else
            lsberrno = LSBE_XDR;
        closesocket(serverSock);
        xdr_destroy(&xdrs);
        return -1;
    }

    xdr_destroy(&xdrs);

    len = replyHdr->length;

    lsb_mbd_version = replyHdr->version;

    if (len > 0) {
        if (len > MAXMSGLEN ) {
            closesocket(serverSock);
            lsberrno = LSBE_PROTOCOL;
            return -1;
        }
        if ((*rep_buf = malloc(len)) == NULL) {
            closesocket(serverSock);
            lsberrno = LSBE_NO_MEM;
            return -1;
        }

        if (b_read_fix(serverSock, *rep_buf, len) == -1) {
            closesocket(serverSock);
            free(*rep_buf);
            *rep_buf = NULL;
            lsberrno = LSBE_SYS_CALL;
            return -1;
        }
    }
    return len;
}


ushort
get_mbd_port (void)
{
    struct servent *sv;
    static ushort mbd_port = 0;

    if (mbd_port != 0)
        return(mbd_port);

    if (isint_(lsbParams[LSB_MBD_PORT].paramValue)) {

        if ((mbd_port = atoi(lsbParams[LSB_MBD_PORT].paramValue)) > 0)
            return((mbd_port = htons(mbd_port)));

        mbd_port = 0;
        lsberrno = LSBE_SERVICE;

        return 0;
    }

    if (lsbParams[LSB_DEBUG].paramValue != NULL)
        return mbd_port = htons(BATCH_MASTER_PORT);

    sv = getservbyname("mbatchd", "tcp");
    if (!sv) {
        lsberrno = LSBE_SERVICE;
        return 0;
    }
    return mbd_port = sv->s_port;
}


ushort
get_sbd_port(void)
{
    struct servent *sv;
    int sbd_port;

    if (isint_(lsbParams[LSB_SBD_PORT].paramValue)) {

        if ((sbd_port = atoi(lsbParams[LSB_SBD_PORT].paramValue)) > 0)
            return((sbd_port = htons(sbd_port)));
        sbd_port = 0;
        lsberrno = LSBE_SERVICE;
        return 0;
    }

    sv = getservbyname("sbatchd", "tcp");
    if (!sv) {
        lsberrno = LSBE_SERVICE;
        return 0;
    }
    return sbd_port = sv->s_port;
}

int
callmbd(char *clusterName,
        char *request_buf,
        int requestlen,
        char **reply_buf,
        struct LSFHeader *replyHdr,
        int *serverSock,
        int (*postSndFunc)(),
        int *postSndFuncArg)
{
    char *masterHost;
    ushort mbd_port;
    int cc;
    int num = 0;
    int try = 0;
    struct clusterInfo *clusterInfo;

Retry:
    try++;

    if (clusterName == NULL) {
        if ((masterHost = getMasterName()) == NULL) {
            return -1;
        }
    } else {

        clusterInfo = ls_clusterinfo(NULL, &num, &clusterName, 1, 0);
        if (clusterInfo == NULL) {
            lsberrno = LSBE_BAD_CLUSTER;
            return -1;
        }
        if (clusterInfo[0].status & CLUST_STAT_OK)
            masterHost = clusterInfo[0].masterName;
        else {
            lsberrno = LSBE_BAD_CLUSTER;
            return -1;
        }
    }

    mbd_port = get_mbd_port();

    cc = call_server(masterHost,
                     mbd_port,
                     request_buf,
                     requestlen,
                     reply_buf,
                     replyHdr,
                     _lsb_conntimeout,
                     _lsb_recvtimeout,
                     serverSock,
                     postSndFunc,
                     postSndFuncArg,
                     CALL_SERVER_NO_HANDSHAKE);
    if (cc < 0) {
        if (cc == -2
            && (lsberrno == LSBE_CONN_TIMEOUT ||
                lsberrno == LSBE_CONN_REFUSED ||
                (lsberrno == LSBE_LSLIB &&
                 (lserrno == LSE_TIME_OUT ||
                  lserrno == LSE_LIM_DOWN ||
                  lserrno == LSE_MASTR_UNKNW ||
                  lserrno == LSE_MSG_SYS)))
            && try < mbdTries()) {
            fprintf (stderr, "\
batch system daemon not responding ... still trying\n");
            if (lsberrno == LSBE_CONN_REFUSED)
                millisleep_(_lsb_conntimeout * 1000);

            goto Retry;
        }
        return -1;
    }

    return cc;
}

/* cmdCallSBD_()
 *
 * For the library to call SBD
 */
int
cmdCallSBD_(char *sbdHost,
            char *request_buf,
            int requestlen,
            char **reply_buf,
            struct LSFHeader *replyHdr,
            int *serverSock)
{
    ushort sbdPort;
    int cc;

    if (logclass & LC_COMM)
        ls_syslog (LOG_DEBUG, "\
%s: Entering this routine... host %s", __func__, sbdHost);

    sbdPort = get_sbd_port();

    if (logclass & LC_COMM)
        ls_syslog (LOG_DEBUG, "%s: sbd_port %d", __func__, ntohs(sbdPort));

    cc = call_server(sbdHost,
                     sbdPort,
                     request_buf,
                     requestlen,
                     reply_buf,
                     replyHdr,
                     _lsb_conntimeout,
                     _lsb_recvtimeout ?  _lsb_recvtimeout : 30,
                     serverSock,
                     NULL,
                     NULL,
                     CALL_SERVER_NO_HANDSHAKE);

    if (cc < 0) {
        if (logclass & LC_COMM)
            ls_syslog(LOG_DEBUG, "\
%s: cc=%d lsberrno=%d lserrno=%d", __func__, cc, lsberrno, lserrno);
        return -1;
    }

    return cc;
}


static int
mbdTries(void)
{
    char *tries;
    static int ntries = -1;

    if (ntries >= 0)
        return ntries;

    if ((tries = getenv("LSB_NTRIES")) == NULL)
        ntries = INFINIT_INT;
    else
        ntries = atoi(tries);

    return ntries;
}



char *
getMasterName(void)
{
    char *masterHost;
    int try = 0;

Retry:
    try++;

    if ((masterHost = ls_getmastername()) == NULL) {
        if (try < mbdTries() &&
            (lserrno == LSE_TIME_OUT || lserrno == LSE_LIM_DOWN ||
             lserrno == LSE_MASTR_UNKNW)) {
            fprintf (stderr, "\
OpenLava daemon (LIM) not responding ... still trying\n");
            millisleep_(_lsb_conntimeout * 1000);
            goto Retry;
        }
        lsberrno = LSBE_LSLIB;
    }

    return masterHost;
}


int
readNextPacket(char **msgBuf, int timeout, struct LSFHeader *hdr,
               int serverSock)
{
    struct Buffer replyBuf;
    int cc;

    if (serverSock < 0) {
        lsberrno = LSBE_CONN_NONEXIST;
        return -1;
    }

    cc = chanRpc_(serverSock, NULL, &replyBuf, hdr, timeout * 1000);
    if (cc < 0) {
        lsberrno = LSBE_LSLIB;
        return -1;
    }

    if (hdr->length == 0) {
        CLOSECD(serverSock);
        lsberrno = LSBE_EOF;
        return -1;
    }
    *msgBuf = replyBuf.data;

    return hdr->reserved;
}

void
closeSession(int serverSock)
{
    chanClose_(serverSock);
}

int
handShake_(int s, char client, int timeout)
{
    struct LSFHeader  hdr, buf;
    int cc;
    XDR xdrs;
    struct Buffer reqbuf, replybuf;

    if (client) {

        memset((char *)&hdr, 0, sizeof(struct LSFHeader));
        hdr.opCode = PREPARE_FOR_OP;
        hdr.length = 0;
        xdrmem_create(&xdrs, (char *) &buf, sizeof(struct LSFHeader),
                      XDR_ENCODE);
        if (!xdr_LSFHeader(&xdrs, &hdr)) {
            lsberrno = LSBE_XDR;
            xdr_destroy(&xdrs);
            return -1;
        }
        xdr_destroy(&xdrs);

        CHAN_INIT_BUF(&reqbuf);
        reqbuf.data = (char *)&buf;
        reqbuf.len = LSF_HEADER_LEN;

        cc = chanRpc_( s, &reqbuf, &replybuf, &hdr, timeout * 1000);
        if (cc < 0) {
            lsberrno = LSBE_LSLIB;
            return -1;
        }
        if (hdr.opCode != READY_FOR_OP) {
            xdr_destroy(&xdrs);
            lsberrno =  hdr.opCode;
            return -1;
        }

    }

    return 0;
}

int
authTicketTokens_(struct lsfAuth *auth, char *toHost)
{
    if (toHost == NULL) {
        char *clusterName;
        char buf[1024];

        if ((toHost = getMasterName()) == NULL) {
            return -1;
        }
        if ((clusterName = ls_getclustername()) == NULL) {
            return -1;
        }
        sprintf(buf, "mbatchd@%s", clusterName);
        putEnv("LSF_EAUTH_SERVER", buf);
    }
    else
        putEnv("LSF_EAUTH_SERVER", "sbatchd");

    putEnv("LSF_EAUTH_CLIENT", "user");

    if (getAuth_(auth, toHost) == -1) {
        lsberrno = LSBE_LSLIB;
        return -1;
    }

    return 0;
}

float *
getCpuFactor (char *host, int name)
{
    float *tempPtr;

Retry:
    if (name == TRUE)
        tempPtr = ls_gethostfactor(host);
    else
        tempPtr = ls_getmodelfactor(host);
    if (tempPtr == NULL) {
        if (lserrno == LSE_TIME_OUT || lserrno == LSE_LIM_DOWN ||
            lserrno == LSE_MASTR_UNKNW) {
            fprintf (stderr, "\
OpenLava daemon (LIM) not responding ... still trying\n");
            millisleep_(_lsb_conntimeout * 1000);
            goto Retry;
        }
        lsberrno = LSBE_LSLIB;
    }

    return tempPtr;
}

/* ackSBDReturnCode()
 */
int
ackSBDReturnCode(struct LSFHeader *hdr)
{
    /* Translate the SBD code, perhaps we could use the LSE
     * code in the entire system.
     */
    switch (hdr->opCode) {
        case RESE_OK:
            return LSE_NO_ERR;
    }

    return -1;
}
