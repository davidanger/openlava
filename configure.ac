#
# openlava project
#
# Copyright (C) 2011-2016 David Bigagli
#
AC_INIT(openlava, 4.0)
AC_CONFIG_HEADERS(config.h)
AC_CONFIG_SRCDIR([configure.ac])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([m4])

AC_PREFIX_DEFAULT([/opt/openlava-master])
AC_PREFIX_DEFAULT([/opt/openlava-master])
AC_DEFINE(_OPENLAVA_PROJECT_, "OpenLava master", "Set project string in binaries.")
AC_DEFINE(OPENLAVA_VERSION, 40, "Current openlava version.")
#AC_DEFINE(REL_DATE, "Nov 11 2013", "Set the release date.")

# Non Linux support
AC_CANONICAL_SYSTEM
AM_CONDITIONAL([CYGWIN], [false])
AM_CONDITIONAL([SOLARIS], [false])
case "${target_os}" in
  *cygwin*)
    AM_CONDITIONAL([CYGWIN], [true]) ;;
  *solaris*)
    AM_CONDITIONAL([SOLARIS], [true]) ;;
esac

# Initialize automake
AM_INIT_AUTOMAKE

# Experimental scheduling module
# Leave for future
#AM_CONDITIONAL([SCHED_EXPERIMENTAL], [false])

# Clean output if possible
m4_ifdef([AM_SILENT_RULES],[AM_SILENT_RULES([yes])])

# Check for programs
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_RANLIB
AC_PROG_YACC
AC_PROG_LEX
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_LIBTOOL

# --enable-debug to turn on -Werror and enable assert()
#OL_DEBUG
if test "x$ac_compiler_gnu" = xyes; then
    CFLAGS="$CFLAGS -Wall -ggdb -O0 -Wno-switch -fPIC -rdynamic"
fi

# --with-hwloc to turn on numa support
AM_CONDITIONAL([HWLOC], [false])
AC_ARG_WITH(
    [hwloc],
    [AS_HELP_STRING(
        [--with-hwloc[[=DIR]]],
        [Specify path to hwloc installation]
    )],
    [AS_IF([test "x$withval" != "xyes"], [CPPFLAGS="-I$withval/include $CPPFLAGS"
        LDFLAGS="-L$withval/lib -L$withval/lib64 $LDFLAGS"])
        AC_CHECK_HEADERS([hwloc.h], [], [AC_MSG_ERROR([cannot build openlava without hwloc])])
        AC_CHECK_LIB([hwloc], [hwloc_topology_init], [], [AC_MSG_ERROR([cannot build openlava without hwloc])])
        AC_CHECK_HEADER([numa.h], [], [AC_MSG_ERROR([cannot build openlava without numa])])
        AC_CHECK_LIB([numa], [numa_bitmask_alloc], [], [AC_MSG_ERROR([cannot build openlava without numa])])
        AM_CONDITIONAL([HWLOC], [true])
])

AC_CHECK_HEADERS(ncurses.h,[cf_cv_ncurses_header="ncurses.h"], [
 AC_MSG_ERROR([cannot build openlava without ncurses])
])

# Check for tcl, we try tcl and all 8.X versions
AC_CHECK_HEADERS([tcl.h], [], [
 AC_CHECK_HEADERS([tcl/tcl.h], [], [
  AC_CHECK_HEADERS([tcl8.6/tcl.h], [], [
  AC_CHECK_HEADERS([tcl86/tcl.h], [], [
   AC_CHECK_HEADERS([tcl8.5/tcl.h], [], [
   AC_CHECK_HEADERS([tcl85/tcl.h], [], [
    AC_CHECK_HEADERS([tcl8.4/tcl.h], [], [
    AC_CHECK_HEADERS([tcl84/tcl.h], [], [
     AC_CHECK_HEADERS([tcl8.3/tcl.h], [], [
     AC_CHECK_HEADERS([tcl83/tcl.h], [], [
      AC_CHECK_HEADERS([tcl8.2/tcl.h], [], [
      AC_CHECK_HEADERS([tcl82/tcl.h], [], [
       AC_CHECK_HEADERS([tcl8.1/tcl.h], [], [
       AC_CHECK_HEADERS([tcl81/tcl.h], [], [
        AC_CHECK_HEADERS([tcl8.0/tcl.h], [], [
        AC_CHECK_HEADERS([tcl80/tcl.h], [], [
 AC_MSG_ERROR([cannot build openlava without tcl])
])])])])])])])])])])])])])])])])

AC_CHECK_LIB([tcl], [Tcl_CreateInterp], [], [
 AC_CHECK_LIB([tcl8.6], [Tcl_CreateInterp], [], [
 AC_CHECK_LIB([tcl86], [Tcl_CreateInterp], [], [
  AC_CHECK_LIB([tcl8.5], [Tcl_CreateInterp], [], [
  AC_CHECK_LIB([tcl85], [Tcl_CreateInterp], [], [
   AC_CHECK_LIB([tcl8.4], [Tcl_CreateInterp], [], [
   AC_CHECK_LIB([tcl84], [Tcl_CreateInterp], [], [
    AC_CHECK_LIB([tcl8.3], [Tcl_CreateInterp], [], [
    AC_CHECK_LIB([tcl83], [Tcl_CreateInterp], [], [
     AC_CHECK_LIB([tcl8.2], [Tcl_CreateInterp], [], [
     AC_CHECK_LIB([tcl82], [Tcl_CreateInterp], [], [
      AC_CHECK_LIB([tcl8.1], [Tcl_CreateInterp], [], [
      AC_CHECK_LIB([tcl81], [Tcl_CreateInterp], [], [
       AC_CHECK_LIB([tcl8.0], [Tcl_CreateInterp], [], [
       AC_CHECK_LIB([tcl80], [Tcl_CreateInterp], [], [
 AC_MSG_ERROR([cannot build openlava without tcl8.*])
])])])])])])])])])])])])])])])

# Set some cluster configuration variables .
AC_ARG_VAR([openlavaadmin], [Username of the openlava admin])
AC_ARG_VAR([openlavacluster], [Name of the cluster])
# Set some defaults.
: ${openlavaadmin:=openlava}
: ${openlavacluster:=openlava}
AC_SUBST([openlavaadmin])
AC_SUBST([openlavacluster])
AC_MSG_RESULT([username for openlava admin ... ${openlavaadmin}])
AC_MSG_RESULT([name for openlava cluster ... ${openlavacluster}])
LDFLAGS="$LDFLAGS -lm -lpthread -ldl -no-install"

AC_CONFIG_FILES([
	Makefile                 \
	m4/Makefile              \
	config/Makefile          \
	lsf/Makefile             \
	lsf/intlib/Makefile      \
	lsf/lib/Makefile         \
	lsf/lim/Makefile         \
	lsf/res/Makefile         \
	lsf/pim/Makefile         \
	lsf/lstools/Makefile     \
	lsf/lsadm/Makefile       \
	lsf/man/Makefile         \
	lsf/man/man1/Makefile    \
	lsf/man/man5/Makefile    \
	lsf/man/man8/Makefile    \
	lsbatch/Makefile         \
	lsbatch/lib/Makefile     \
	lsbatch/cmd/Makefile     \
	lsbatch/bhist/Makefile   \
	lsbatch/daemons/Makefile \
	lsbatch/man1/Makefile    \
	lsbatch/man5/Makefile    \
	lsbatch/man8/Makefile    \
	eauth/Makefile           \
	scripts/Makefile         \
	chkpnt/Makefile          \
	config/lsf.conf          \
	config/lsb.hosts         \
	config/lsb.params        \
	config/lsb.queues        \
	config/lsb.users         \
	config/lsb.resources     \
	config/lsf.cluster.${openlavacluster}:config/lsf.cluster.openlava.in \
	config/lsf.shared        \
	config/lsf.task          \
	config/openlava.csh      \
	config/openlava          \
	config/openlava.setup    \
	config/openlava.sh       \
        config/limits.sh
	examples/Makefile        \
	examples/docker/Makefile \
	tests/Makefile],,
        [openlavacluster=$openlavacluster]
)

AC_OUTPUT
