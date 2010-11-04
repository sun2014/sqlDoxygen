#
# This file was generated from Makefile.in on Fri Jun 25 11:08:35 CEST 2010
#

DOXYGEN   = /Users/dimitri/doxygen
TMAKEPATH = /Users/dimitri/doxygen/tmake/lib/macosx-c++
ENV       = env TMAKEPATH=$(TMAKEPATH)
TMAKE     = /Users/dimitri/doxygen/tmake/bin/tmake
MAKE      = /opt/local/bin/gmake
PERL      = /opt/local/bin/perl
RM        = rm -f
CP        = cp
VERSION   = 1.7.1
INSTALL   = /usr/local
INSTTOOL  = /usr/bin/install
DOXYDOCS  = ..
DOCDIR    = $(INSTALL)/share/doc/packages/doxygen
QTDIR     = 
HAVE_DOT  = /usr/local/bin/dot
MKSPECS   = -spec macx-g++

all clean depend distclean: Makefile.doxyapp 
	$(MAKE) -f Makefile.doxyapp $@

distclean: clean
	$(RM) -rf Makefile doxyapp.pro Makefile.doxyapp

tmake:
	$(ENV) $(PERL) $(TMAKE) doxyapp.pro >Makefile.doxyapp

strip:
	strip doxyapp

Makefile.doxyapp: doxyapp.pro
	$(ENV) $(PERL) $(TMAKE) doxyapp.pro >Makefile.doxyapp

install:
