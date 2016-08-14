#
# Copyright (C) 2011-2014 David Bigagli
#

include $(top_srcdir)/common.mk

AM_CPPFLAGS += -I$(top_srcdir)/lsf -I$(top_srcdir)/lsf/lib \
           -I$(top_srcdir)/lsbatch -I./

lib_LTLIBRARIES = liblsbatch.la

liblsbatch_la_SOURCES = \
lsb.comm.c lsb.groups.c lsb.jobs.c lsb.modify.c lsb.peek.c lsb.reconfig.c \
lsb.signals.c lsb.users.c lsb.conf.c lsb.hc.c lsb.log.c lsb.move.c \
lsb.qc.c lsb.resource.c lsb.spool.c lsb.xdr.c lsb.debug.c lsb.hosts.c \
lsb.mig.c lsb.msg.c lsb.queues.c lsb.launch.c \
lsb.sub.c lsb.err.c lsb.init.c lsb.misc.c lsb.params.c lsb.reason.c \
lsb.sig.c lsb.switch.c lsb.jgrp.c lsb.limit.c \
lsb.conf.h  lsb.h  lsb.log.h  lsb.sig.h  lsb.spool.h  lsb.xdr.h
liblsbatch_la_LDFLAGS =  -no-undefined -version-info 0:1
