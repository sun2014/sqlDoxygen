/******************************************************************************
 *
 * $Id: pyscanner.h,v 1.9 2001/03/19 19:27:39 root Exp $
 *
 * Copyright (C) 1997-2006 by Dimitri van Heesch.
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation under the terms of the GNU General Public License is hereby 
 * granted. No representations are made about the suitability of this software 
 * for any purpose. It is provided "as is" without express or implied warranty.
 * See the GNU General Public License for more details.
 *
 * Documents produced by Doxygen are derivative works derived from the
 * input used in their production; they are not affected by this license.
 *
 */


#ifndef SQLSCANNER_H
#define SQLSCANNER_H

#include "parserintf.h"

/** \brief SQL Language parser using state-based lexical scanning.
 *
 * This is the SQL language parser for doxygen.
 */
class SQLLanguageScanner : public ParserInterface
{
  public:
    virtual ~SQLLanguageScanner() {}
    void parseInput(const char * fileName, 
                    const char *fileBuf, 
                    Entry *root);
    bool needsPreprocessing(const QCString &extension);
    void parseCode(CodeOutputInterface &codeOutIntf,
                   const char *scopeName,
                   const QCString &input,
                   bool isExampleBlock,
                   const char *exampleName=0,
                   FileDef *fileDef=0,
                   int startLine=-1,
                   int endLine=-1,
                   bool inlineFragment=FALSE,
                   MemberDef *memberDef=0
                  );
    void resetCodeParserState();
    void parsePrototype(const char *text);
};

#endif
