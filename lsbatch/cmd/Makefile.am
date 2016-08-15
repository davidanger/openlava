#
# Copyright (C) 2011-2016 David Bigagli
#

include $(top_srcdir)/common.mk

AM_CPPFLAGS += -I$(top_srcdir)/lsf  -I$(top_srcdir)/lsf/lib \
           -I$(top_srcdir)/lsbatch  -I$(top_srcdir)/lsbatch/lib -I./

bin_PROGRAMS = badmin bkill bparams brestart btop bbot bmgroup \
bpeek brun busers bhosts bmig bqueues bsub bjobs bmod \
brequeue bswitch bpost bread bjdep bgadd bgdel bjgroup bgmod bresources \
blaunch

badmin_SOURCES = badmin.c cmd.bqc.c cmd.hist.c \
	cmd.bhc.c cmd.misc.c cmd.job.c cmd.prt.c \
	badmin.h cmd.h
badmin_LDADD = ../../lsf/lsadm/startup.o \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
badmin_LDADD += -lnsl
endif
if SOLARIS
badmin_LDADD += -lsocket -lnsl
endif

bkill_SOURCES = bkill.c cmd.sig.c cmd.jobid.c cmd.err.c
bkill_LDADD = \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl

if !CYGWIN
bkill_LDADD += -lnsl
endif
if SOLARIS
bkill_LDADD += -lsocket -lnsl
endif

bparams_SOURCES = bparams.c cmd.h
bparams_LDADD =   \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bparams_LDADD += -lnsl
endif
if SOLARIS
bparams_LDADD += -lsocket -lnsl
endif

brestart_SOURCES = brestart.c cmd.sub.c cmd.jobid.c \
	cmd.err.c cmd.h
brestart_LDADD =   \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
brestart_LDADD += -lnsl
endif
if SOLARIS
brestart_LDADD += -lsocket -lnsl
endif

btop_SOURCES = btop.c cmd.move.c cmd.jobid.c cmd.misc.c \
	 cmd.prt.c cmd.err.c cmd.h
btop_LDADD =  \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
btop_LDADD += -lnsl
endif
if SOLARIS
btop_LDADD += -lsocket -lnsl
endif

bbot_SOURCES = bbot.c cmd.move.c cmd.jobid.c cmd.misc.c \
	 cmd.prt.c cmd.err.c cmd.h
bbot_LDADD =   \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bbot_LDADD += -lnsl
endif
if SOLARIS
bbot_LDADD += -lsocket -lnsl
endif

bmgroup_SOURCES = bmgroup.c cmd.misc.c cmd.h
bmgroup_LDADD =   \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bmgroup_LDADD += -lnsl
endif
if SOLARIS
bmgroup_LDADD += -lsocket -lnsl
endif

bpeek_SOURCES = bpeek.c cmd.err.c cmd.jobid.c cmd.misc.c cmd.prt.c cmd.h
bpeek_LDADD =  \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bpeek_LDADD += -lnsl
endif
if SOLARIS
bpeek_LDADD += -lsocket -lnsl
endif

brun_SOURCES = brun.c cmd.jobid.c cmd.err.c cmd.h
brun_LDADD =   \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
brun_LDADD += -lnsl
endif
if SOLARIS
brun_LDADD += -lsocket -lnsl
endif

busers_SOURCES = busers.c cmd.misc.c
busers_LDADD =  \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
busers_LDADD += -lnsl
endif
if SOLARIS
busers_LDADD += -lsocket -lnsl
endif

bhosts_SOURCES = bhosts.c cmd.prt.c cmd.misc.c cmd.h
bhosts_LDADD =  \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bhosts_LDADD += -lnsl
endif
if SOLARIS
bhosts_LDADD += -lsocket -lnsl
endif

bmig_SOURCES = bmig.c cmd.jobid.c cmd.err.c cmd.h
bmig_LDADD =   \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bmig_LDADD += -lnsl
endif
if SOLARIS
bmig_LDADD += -lsocket -lnsl
endif

bqueues_SOURCES = bqueues.c cmd.prt.c cmd.misc.c cmd.h
bqueues_LDADD = \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bqueues_LDADD += -lnsl
endif
if SOLARIS
bqueues_LDADD += -lsocket -lnsl
endif

bsub_SOURCES = bsub.c cmd.sub.c cmd.jobid.c cmd.err.c cmd.h
bsub_LDADD =   \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bsub_LDADD += -lnsl
endif
if SOLARIS
bsub_LDADD += -lsocket -lnsl
endif

bjobs_SOURCES = bjobs.c cmd.prt.c cmd.err.c cmd.job.c \
	cmd.jobid.c cmd.misc.c cmd.h
bjobs_LDADD =  \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bjobs_LDADD += -lnsl
endif
if SOLARIS
bjobs_LDADD += -lsocket -lnsl
endif

bmod_SOURCES = bmod.c cmd.sub.c cmd.jobid.c cmd.err.c cmd.h
bmod_LDADD =  \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bmod_LDADD += -lnsl
endif
if SOLARIS
bmod_LDADD += -lsocket -lnsl
endif

brequeue_SOURCES = brequeue.c cmd.jobid.c cmd.err.c cmd.h
brequeue_LDADD =   \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
brequeue_LDADD += -lnsl
endif
if SOLARIS
brequeue_LDADD += -lsocket -lnsl
endif

bswitch_SOURCES = bswitch.c cmd.jobid.c cmd.err.c cmd.h
bswitch_LDADD =  \
	../lib/.libs/liblsbatch.a \
	../../lsf/lib/.libs/liblsf.a \
	../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if !CYGWIN
bswitch_LDADD += -lnsl
endif
if SOLARIS
bswitch_LDADD += -lsocket -lnsl
endif

bpost_SOURCES = bpost.c cmd.jobid.c cmd.err.c cmd.h
bpost_LDADD = \
        ../lib/.libs/liblsbatch.a \
        ../../lsf/lib/.libs/liblsf.a \
        ../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if SOLARIS
bpost_LDADD += -lsocket -lnsl
endif

bread_SOURCES = bread.c cmd.jobid.c cmd.err.c cmd.h
bread_LDADD = \
        ../lib/.libs/liblsbatch.a \
        ../../lsf/lib/.libs/liblsf.a \
        ../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if SOLARIS
bread_LDADD += -lsocket -lnsl
endif

bjdep_SOURCES = bjdep.c cmd.jobid.c cmd.err.c cmd.h
bjdep_LDADD = \
        ../lib/.libs/liblsbatch.a \
        ../../lsf/lib/.libs/liblsf.a \
        ../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if SOLARIS
bjdep_LDADD += -lsocket -lnsl
endif

bgadd_SOURCES = bgadd.c cmd.jobid.c cmd.err.c cmd.h
bgadd_LDADD = \
        ../lib/.libs/liblsbatch.a \
        ../../lsf/lib/.libs/liblsf.a \
        ../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if SOLARIS
bgadd_LDADD += -lsocket -lnsl
endif

bgdel_SOURCES = bgdel.c cmd.jobid.c cmd.err.c cmd.h
bgdel_LDADD = \
        ../lib/.libs/liblsbatch.a \
        ../../lsf/lib/.libs/liblsf.a \
        ../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if SOLARIS
bgdel_LDADD += -lsocket -lnsl
endif

bjgroup_SOURCES = bjgroup.c cmd.jobid.c cmd.err.c cmd.h
bjgroup_LDADD = \
        ../lib/.libs/liblsbatch.a \
        ../../lsf/lib/.libs/liblsf.a \
        ../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if SOLARIS
bjgroup_LDADD += -lsocket -lnsl
endif

bgmod_SOURCES = bgmod.c cmd.jobid.c cmd.err.c cmd.h
bgmod_LDADD = \
        ../lib/.libs/liblsbatch.a \
        ../../lsf/lib/.libs/liblsf.a \
        ../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if SOLARIS
bgmod_LDADD += -lsocket -lnsl
endif

bresources_SOURCES = bresources.c cmd.jobid.c cmd.err.c cmd.h
bresources_LDADD = \
        ../lib/.libs/liblsbatch.a \
        ../../lsf/lib/.libs/liblsf.a \
        ../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if SOLARIS
bresources_LDADD += -lsocket -lnsl
endif

blaunch_SOURCES = blaunch.c cmd.jobid.c cmd.err.c cmd.h
blaunch_LDADD = \
        ../lib/.libs/liblsbatch.a \
        ../../lsf/lib/.libs/liblsf.a \
        ../../lsf/intlib/.libs/liblsfint.a -lm -lnsl
if SOLARIS
blaunch_LDADD += -lsocket -lnsl
endif

install-data-local:
install-data-local:
	cd "$(DESTDIR)$(bindir)" && ln -sf bkill bstop
	cd "$(DESTDIR)$(bindir)" && ln -sf bkill bresume
	cd "$(DESTDIR)$(bindir)" && ln -sf bkill bchkpnt
	cd "$(DESTDIR)$(bindir)" && ln -sf bmgroup bugroup

