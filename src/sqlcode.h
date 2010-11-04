/******************************************************************************
 *
 *
 */


#ifndef SQLCODE_H
#define SQLCODE_H

#include "qtbc.h"
#include <stdio.h>

class CodeOutputInterface;
class FileDef;
class MemberDef;

extern void parseSQLCode(CodeOutputInterface &,const char *,const QCString &, 
             bool ,const char *,FileDef *fd=0,
	     int startLine=-1,int endLine=-1,bool inlineFragment=FALSE,
             MemberDef *memberDef=0);
extern void resetSQLCodeParserState();

#endif
