#
# This file was generated from doxyapp.pro.in on Fri Jun 25 11:08:35 CEST 2010
#

TEMPLATE     =	app.t
CONFIG       =	console warn_on debug
HEADERS      =	
SOURCES      =	doxyapp.cpp
LIBS          += -L../../lib -L../../lib -ldoxygen -lqtools -lmd5 -ldoxycfg -lpng
DESTDIR        = 
OBJECTS_DIR    = ../../objects
TARGET         = ../../bin/doxyapp
INCLUDEPATH   += ../../qtools ../../src
DEPENDPATH    += ../../src
TARGETDEPS     = ../../lib/libdoxygen.a

