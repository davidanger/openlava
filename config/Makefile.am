#
# Copyright (C) David Bigagli
#

include $(top_srcdir)/common.mk


# Install configuration files for openlava base and batch 
# in the conf directory.

# Install the shell profile scrips and the system startup 
# script in the etc directory.
etcdir = $(prefix)/etc
noinst_DATA = openlava.sh openlava.csh \
	lsf.conf lsf.cluster.${openlavacluster} lsf.shared lsf.task \
	lsb.hosts lsb.params lsb.queues lsb.users lsb.resources limits.sh

etc_SCRIPTS = openlava

# Create the working directory where the working files are
# kept.
install-data-local:
	mkdir -p $(DESTDIR)$(prefix)/work/logdir
	mkdir -p $(DESTDIR)$(prefix)/log
	mkdir -p $(DESTDIR)$(prefix)/etc

# Tell automake to install during distcheck the
# configuration and etc files. 
#etc_DATA = gitcommit
#EXTRA_DIST = $(etc_DATA)
