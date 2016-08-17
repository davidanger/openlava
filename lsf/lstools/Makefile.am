#
# Copyright (C) 2011-2016 David Bigagli
#

include $(top_srcdir)/common.mk

AM_CPPFLAGS += -I..

bin_PROGRAMS = lsacct lseligible lshosts lsid lsinfo lsloadadj \
	lsload lsplace lsrcp lsrun lsaddhost lsrmhost lsmon lsclusters \
	lsgetpid

lsclusters_SOURCES = lsclusters.c
lsclusters_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl
if SOLARIS
lsclusters_LDADD += -lnsl -lsocket
endif

lsacct_SOURCES = lsacct.c  
lsacct_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl
if SOLARIS
lsacct_LDADD += -lnsl -lsocket
endif

lseligible_SOURCES = lseligible.c  
lseligible_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl
if SOLARIS
lseligible_LDADD += -lnsl -lsocket
endif

lshosts_SOURCES = lshosts.c  
lshosts_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl
if SOLARIS
lshosts_LDADD += -lnsl -lsocket
endif

lsid_SOURCES = lsid.c  
lsid_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl
if SOLARIS
lsid_LDADD += -lnsl -lsocket
endif

lsinfo_SOURCES = lsinfo.c  
lsinfo_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl
if SOLARIS
lsinfo_LDADD += -lnsl -lsocket
endif

lsloadadj_SOURCES = lsloadadj.c  
lsloadadj_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl
if SOLARIS
lsloadadj_LDADD += -lnsl -lsocket
endif

lsload_SOURCES = lsload.c load.c
lsload_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl
if SOLARIS
lsload_LDADD += -lnsl -lsocket
endif

lsmon_SOURCES = lsmon.c  
lsmon_LDADD = load.o ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a  -lnsl
if SOLARIS
lsmon_LDADD += -lnsl -lsocket -lcurses
else
lsmon_LDADD += -lncurses
endif

lsplace_SOURCES = lsplace.c  
lsplace_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl
if SOLARIS
lsplace_LDADD += -lnsl -lsocket
endif

lsrcp_SOURCES = lsrcp.c
lsrcp_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a  -lnsl
if !CYGWIN
lsrcp_LDADD += -lnsl
endif
if SOLARIS
lsrcp_LDADD += -lsocket
endif

lsrun_SOURCES = lsrun.c
lsrun_LDADD = ../lib/.libs/liblsf.a  -lnsl
if SOLARIS
lsrun_LDADD += -lnsl -lsocket
endif

lsaddhost_SOURCES = lsaddhost.c
lsaddhost_LDADD = ../lib/.libs/liblsf.a  -lnsl
if SOLARIS
lsaddhost_LDADD += -lnsl -lsocket
endif

lsrmhost_SOURCES = lsrmhost.c
lsrmhost_LDADD = ../lib/.libs/liblsf.a  -lnsl
if SOLARIS
lsrmhost_LDADD += -lnsl -lsocket
endif

lsgetpid_SOURCES = lsgetpid.c
lsgetpid_LDADD = ../intlib/.libs/libtools.a ../intlib/.libs/liblsfint.a ../lib/.libs/liblsf.a  -lnsl
if SOLARIS
lsgetpid_LDADD += -lnsl -lsocket
endif

