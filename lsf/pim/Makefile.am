#
# Copyright (C) 2011-2014 David Bigagli
#

include $(top_srcdir)/common.mk

AM_CPPFLAGS += -I$(top_srcdir)/lsf

sbin_PROGRAMS = pim
pim_SOURCES  = pim.main.c pim.h
if SOLARIS
pim_SOURCES += pim.solaris.c
else
pim_SOURCES += pim.linux.c
endif
pim_LDADD = ../lib/.libs/liblsf.a ../intlib/.libs/liblsfint.a -lnsl

if SOLARIS
pim_LDADD += -lnsl -lsocket
endif

