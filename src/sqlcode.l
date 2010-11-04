/******************************************************************************
 *
 *
 */

%{

#include <stdio.h>
#include <qvaluestack.h>

#include "SQLcode.h"
#include "message.h"

#include "SQLscanner.h"
#include "entry.h"
#include "doxygen.h"
#include "outputlist.h"
#include "util.h"
#include "membername.h"
#include "searchindex.h"

#define YY_NEVER_INTERACTIVE 1

static ClassSDict    g_codeClassSDict(17);
static QCString      g_curClassName;
static QStrList      g_curClassBases;

static QValueStack< int > stkPrsState;

static CodeOutputInterface * g_code;
static const char *  g_inputString;     //!< the code fragment as text
static int	     g_inputPosition;   //!< read offset during parsing 
static const char *  g_currentFontClass;
static bool          g_needsTermination;
static int           g_inputLines;      //!< number of line in the code fragment
static int	     g_yyLineNr;        //!< current line number
static FileDef *     g_sourceFileDef;
static Definition *  g_currentDefinition;
static MemberDef *   g_currentMemberDef;
static bool          g_includeCodeFragment;
static QCString      g_realScope;
static bool          g_insideBody;
static int           g_bodyCurlyCount;
static bool          g_searchingForBody;
static QCString      g_classScope;
static int           g_paramParens;
//static int           g_anchorCount;

static bool          g_exampleBlock;
static QCString      g_exampleName;
static QCString      g_exampleFile;

static QCString      g_type;
static QCString      g_name;

static bool          g_doubleStringIsDoc;
//static bool          g_doubleQuote;
//static bool          g_noSuiteFound;
//static int           g_stringContext;

static QValueStack<uint> g_indents;  //!< Tracks indentation levels for scoping in python

static void endFontClass();
static void adjustScopesAndSuites(unsigned indentLength);


//----------------------------------------------------------------------------

static void SetState( int iState )
{
	//printf( "[sqlscanner] going to state: %d  [line %d]\n", iState, yyLineNr );
	BEGIN( iState );
}

static void PushState( int iState )
{
		// a new parsing state for us to go to
	stkPrsState.push( iState );
	SetState( stkPrsState.top() );

}

static void PopState()
{
		// time to go back to the previous parsing state
	stkPrsState.pop();
	SetState( stkPrsState.top() );
}

static void lineCount()
{
  for( const char* c = yytext ; *c ; ++c )
    g_yyLineNr += (*c == '\n') ;
}

//----------------------------------------------------------------------------

/*! counts the number of lines in the input */
static int countLines()
{
  const char *p=g_inputString;
  char c;
  int count=1;
  while ((c=*p)) 
  { 
    p++ ; 
    if (c=='\n') count++;  
  }
  if (p>g_inputString && *(p-1)!='\n') 
  { // last line does not end with a \n, so we add an extra
    // line and explicitly terminate the line after parsing.
    count++, 
    g_needsTermination=TRUE; 
  } 
  return count;
}

static void setCurrentDoc(const QCString &name,const QCString &base,const QCString &anchor="")
{
  static bool searchEngineEnabled=Config_getBool("SEARCHENGINE");
  if (searchEngineEnabled)
  {
    Doxygen::searchIndex->setCurrentDoc(name,base,anchor);
  }
}

static void addToSearchIndex(const char *text)
{
  static bool searchEngineEnabled=Config_getBool("SEARCHENGINE");
  if (searchEngineEnabled)
  {
    Doxygen::searchIndex->addWord(text,FALSE);
  }
}


static ClassDef *stripClassName(const char *s)
{
  int pos=0;
  QCString type = s;
  QCString className;
  QCString templSpec;
  while (extractClassNameFromType(type,pos,className,templSpec)!=-1)
  {
    QCString clName=className+templSpec;

    ClassDef *cd=0;
    if (!g_classScope.isEmpty())
    {
      cd=getResolvedClass(g_currentDefinition,g_sourceFileDef,g_classScope+"::"+clName);
    }
    if (cd==0)
    {
      cd=getResolvedClass(g_currentDefinition,g_sourceFileDef,clName);
    }
    if (cd)
    {
      return cd;
    }
  }

  return 0;
}



/*! start a new line of code, inserting a line number if g_sourceFileDef
 * is TRUE. If a definition starts at the current line, then the line
 * number is linked to the documentation of that definition.
 */
static void startCodeLine()
{
  //if (g_currentFontClass) { g_code->endFontClass(); }
  if (g_sourceFileDef)
  {
    //QCString lineNumber,lineAnchor;
    //lineNumber.sprintf("%05d",g_yyLineNr);
    //lineAnchor.sprintf("l%05d",g_yyLineNr);
   
    Definition *d   = g_sourceFileDef->getSourceDefinition(g_yyLineNr);
    //printf("startCodeLine %d d=%p\n",g_yyLineNr,d);
    //g_code->startLineNumber();
    if (!g_includeCodeFragment && d && d->isLinkableInProject())
    {
      g_currentDefinition = d;
      g_currentMemberDef = g_sourceFileDef->getSourceMember(g_yyLineNr);
      g_insideBody = FALSE;
      g_searchingForBody = TRUE;
      g_realScope = d->name().copy();
      g_classScope = d->name().copy();
      //printf("Real scope: `%s'\n",g_realScope.data());
      g_bodyCurlyCount = 0;
      QCString lineAnchor;
      lineAnchor.sprintf("l%05d",g_yyLineNr);
      if (g_currentMemberDef)
      {
        g_code->writeLineNumber(g_currentMemberDef->getReference(),
	                        g_currentMemberDef->getOutputFileBase(),
	                        g_currentMemberDef->anchor(),g_yyLineNr);
        setCurrentDoc(
                                g_currentMemberDef->qualifiedName(),
	                        g_sourceFileDef->getSourceFileBase(),
	                        lineAnchor);
      }
      else
      {
        g_code->writeLineNumber(d->getReference(),
	                        d->getOutputFileBase(),
	                        0,g_yyLineNr);
        setCurrentDoc(
                                d->qualifiedName(),
	                        g_sourceFileDef->getSourceFileBase(),
	                        lineAnchor);
      }
    }
    else
    {
      //g_code->codify(lineNumber);
      g_code->writeLineNumber(0,0,0,g_yyLineNr);
    }
    //g_code->endLineNumber();
  }
  g_code->startCodeLine(); 
  if (g_currentFontClass)
  {
    g_code->startFontClass(g_currentFontClass);
  }
}

static void codify(char* text) 
{ 
  g_code->codify(text);
}

static void endCodeLine()
{
  if (g_currentFontClass) { g_code->endFontClass(); }
  g_code->endCodeLine();
}

/*! writes a link to a fragment \a text that may span multiple lines, inserting
 * line numbers for each line. If \a text contains newlines, the link will be 
 * split into multiple links with the same destination, one for each line.
 */
static void writeMultiLineCodeLink(CodeOutputInterface &ol,
                  const char *ref,const char *file,
                  const char *anchor,const char *text)
{
  bool done=FALSE;
  char *p=(char *)text;
  while (!done)
  {
    char *sp=p;
    char c;
    while ((c=*p++) && c!='\n');
    if (c=='\n')
    {
      g_yyLineNr++;
      *(p-1)='\0';
      //printf("writeCodeLink(%s,%s,%s,%s)\n",ref,file,anchor,sp);
      ol.writeCodeLink(ref,file,anchor,sp);
      endCodeLine();
      if (g_yyLineNr<g_inputLines) 
      {
	startCodeLine();
      }
    }
    else
    {
      //printf("writeCodeLink(%s,%s,%s,%s)\n",ref,file,anchor,sp);
      ol.writeCodeLink(ref,file,anchor,sp);
      done=TRUE;
    }
  }
}


static void codifyLines(char *text)
{
	//printf("codifyLines(%d,\"%s\")\n",g_yyLineNr,text);
	char *p=text,*sp=p;
	char c;
	bool done=FALSE;
	while (!done)
	{
		sp=p;
		while ((c=*p++) && c!='\n');
		if (c=='\n')
		{
			g_yyLineNr++;
			*(p-1)='\0';
			g_code->codify(sp);
			endCodeLine();
			if (g_yyLineNr<g_inputLines) 
			{
				// Re-enable sometime
				startCodeLine();
			}
		}
		else
		{
			g_code->codify(sp);
			done=TRUE;
		}
	}
}

static void addDocCrossReference(MemberDef *src,MemberDef *dst)
{
  if (dst->isTypedef() || dst->isEnumerate()) return; // don't add types
  //printf("addDocCrossReference src=%s,dst=%s\n",src->name().data(),dst->name().data());
  if ((Config_getBool("REFERENCED_BY_RELATION") || Config_getBool("CALLER_GRAPH")) && 
      (src->isFunction() || src->isSlot()) 
     )
  {
    dst->addSourceReferencedBy(src);
  }
  if ((Config_getBool("REFERENCES_RELATION") || Config_getBool("CALL_GRAPH")) && 
      (src->isFunction() || src->isSlot())
     )
  {
    src->addSourceReferences(dst);
  }

}



static bool getLinkInScope(const QCString &c,  // scope
                           const QCString &m,  // member
			   const char *memberText, // exact text
			   CodeOutputInterface &ol,
			   const char *text
			  )
{
  MemberDef    *md;
  ClassDef     *cd;
  FileDef      *fd;
  NamespaceDef *nd;
  GroupDef     *gd;
  //printf("Trying `%s'::`%s'\n",c.data(),m.data());
  if (getDefs(c,m,"()",md,cd,fd,nd,gd,FALSE,g_sourceFileDef) && 
      md->isLinkable())
  {
    //printf("Found!\n");
    //Definition *d=0;
    //if (cd) d=cd; else if (nd) d=nd; else if (fd) d=fd; else d=gd;

    Definition *d = md->getOuterScope()==Doxygen::globalScope ?
	            md->getBodyDef() : md->getOuterScope();
    if (md->getGroupDef()) d = md->getGroupDef();
    if (d && d->isLinkable())
    {
      //g_theCallContext.setClass(stripClassName(md->typeString()));
      //printf("g_currentDefinition=%p g_currentMemberDef=%p g_insideBody=%d\n",
      //        g_currentDefinition,g_currentMemberDef,g_insideBody);

      if (g_currentDefinition && g_currentMemberDef &&
	  md!=g_currentMemberDef && g_insideBody)
      {
	addDocCrossReference(g_currentMemberDef,md);
      }
      //printf("d->getReference()=`%s' d->getOutputBase()=`%s' name=`%s' member name=`%s'\n",d->getReference().data(),d->getOutputFileBase().data(),d->name().data(),md->name().data());
     
      writeMultiLineCodeLink(ol,md->getReference(),
	                        md->getOutputFileBase(),
	                        md->anchor(),
				text ? text : memberText);
      addToSearchIndex(text ? text : memberText);
      return TRUE;
    } 
  }
  return FALSE;
}

static bool getLink(const char *className,
                    const char *memberName,
		    CodeOutputInterface &ol,
		    const char *text=0)
{
  QCString m=removeRedundantWhiteSpace(memberName);
  QCString c=className;
  if (!getLinkInScope(c,m,memberName,ol,text))
  {
    if (!g_curClassName.isEmpty())
    {
      if (!c.isEmpty()) c.prepend("::");
      c.prepend(g_curClassName);
      return getLinkInScope(c,m,memberName,ol,text);
    }
    return FALSE;
  }
  return TRUE;
}


/*
  For a given string in the source code,
  finds its class or global id and links to it.

  As of June 1, '05, this ONLY finds classes
*/
static void generateClassOrGlobalLink(CodeOutputInterface &ol,char *clName,
                                      bool /*typeOnly*/=FALSE)
{
  QCString className = clName;

  // Don't do anything for empty text
  if (className.isEmpty()) return;

  ClassDef *cd=0,*lcd=0;  /** Class def that we may find */
  MemberDef *md=0;        /** Member def that we may find */
  bool isLocal=FALSE;

  //printf("generateClassOrGlobalLink(className=%s)\n",className.data());

	cd = getResolvedClass( 0, g_sourceFileDef, className, &md );
	if (cd && cd->isLinkable()) // is it a linkable class
	{
		//printf("Class Resolved...");
		writeMultiLineCodeLink(ol,cd->getReference(),cd->getOutputFileBase(),0,clName);
		addToSearchIndex(className);
		if (md)
		{
			Definition *d = md->getOuterScope()==Doxygen::globalScope ?
								md->getBodyDef() : md->getOuterScope();
			if (md->getGroupDef()) d = md->getGroupDef();
			if (d && d->isLinkable() && md->isLinkable() && g_currentMemberDef)
			{
				addDocCrossReference(g_currentMemberDef,md);
			}
		}
	}
	else // not a class, maybe a global member
	{
		// nothing found, just write out the word
		codifyLines(clName);
		addToSearchIndex(clName);
	}
}

/*
   As of June 1, this function seems to work
   for file members, but scopes are not
   being correctly tracked for classes
   so it doesn't work for classes yet.

*/
static void generateFunctionLink(CodeOutputInterface &ol,char *funcName)
{
  //CodeClassDef *ccd=0;
  ClassDef *ccd=0;
  QCString locScope=g_classScope.copy();
  QCString locFunc=removeRedundantWhiteSpace(funcName);
  //printf("*** locScope=%s locFunc=%s\n",locScope.data(),locFunc.data());
  int i=locFunc.findRev("::");
  if (i>0)
  {
    locScope=locFunc.left(i);
    locFunc=locFunc.right(locFunc.length()-i-2).stripWhiteSpace();
  }
  
  //printf("generateFunctionLink(%s) classScope=`%s'\n",locFunc.data(),locScope.data());
  if (!locScope.isEmpty() && (ccd=g_codeClassSDict[locScope]))
  {
    //printf("using classScope %s\n",g_classScope.data());
    BaseClassListIterator bcli(*ccd->baseClasses());
    for ( ; bcli.current() ; ++bcli)
    {
      if (getLink(bcli.current()->classDef->name(),locFunc,ol,funcName)) 
      {
	return;
      }
    }
  }
  if (!getLink(locScope,locFunc,ol,funcName))
  {
    generateClassOrGlobalLink(ol,funcName);
  }
  return;
}

static void findMemberLink(CodeOutputInterface &ol,char *symName)
{
  //printf("Member reference: %s scope=%s member=%s\n",
  //    yytext,
  //    g_currentDefinition?g_currentDefinition->name().data():"<none>",
  //    g_currentMemberDef?g_currentMemberDef->name().data():"<none>"
  //    );
  if (g_currentDefinition)
  {
    DefinitionList *dl = Doxygen::symbolMap->find(symName);
    if (dl)
    {
      DefinitionListIterator dli(*dl);
      Definition *sym;
      for (dli.toFirst();(sym=dli.current());++dli)
      {
	//printf("sym %s outerScope=%s equal=%d\n",
	//    sym->name().data(),sym->getOuterScope()->name().data(),
	//    sym->getOuterScope()==g_currentDefinition);

	if (sym->getOuterScope() &&
	    sym->getOuterScope()->definitionType()==Definition::TypeClass &&
	    g_currentDefinition->definitionType()==Definition::TypeClass)
	{
	  ClassDef *cd = (ClassDef*)sym->getOuterScope();
	  ClassDef *thisCd = (ClassDef *)g_currentDefinition;
	  QCString anchor;
	  if (sym->definitionType()==Definition::TypeMember)
	  {
	    anchor=((MemberDef *)sym)->anchor();
	  }

	  // TODO: find the nearest base class in case cd is a base class of
	  // thisCd 
	  if (cd==thisCd) 
	  {
	    writeMultiLineCodeLink(ol,sym->getReference(),
		sym->getOutputFileBase(),
		anchor,
		symName);
	    return;
	  }
	}
      }
    }
  }
  //printf("sym %s not found\n",&yytext[5]);
  codify(symName);
}

static void startFontClass(const char *s)
{
  endFontClass();
  g_code->startFontClass(s);
  g_currentFontClass=s;
}

static void endFontClass()
{
  if (g_currentFontClass)
  {
    g_code->endFontClass();
    g_currentFontClass=0;
  }
}

#undef YY_INPUT
#define YY_INPUT(buf,result,max_size) result=yyread(buf,max_size);

static int yyread(char *buf,int max_size)
{
  int c=0;
  while( c < max_size && g_inputString[g_inputPosition] )
  {
    *buf = g_inputString[g_inputPosition++] ;
    c++; buf++;
  }
  return c;
}

%}


       /* start command character */
BB                [ \t]+
B                 [ \t]*
NEWLINE           \n
BN                [ \t\n]

DIGIT             [0-9]

HEXNUMBER         "0"[xX][0-9a-fA-F]+[lL]?
OCTNUMBER         "0"[0-7]+[lL]?
NUMBER            {DIGIT}+[lLjJ]?
INTNUMBER         {HEXNUMBER}|{OCTNUMBER}|{NUMBER}
FLOATNUMBER       {DIGIT}+"."{DIGIT}+([eE][+\-]?{DIGIT}+)?[jJ]?
LETTER            [A-Za-z]
NONEMPTY          [A-Za-z0-9_]
EXPCHAR           [#(){}\[\],:.%/\\=`*~|&<>!;+-]
NONEMPTYEXP       [^ \t\n:]
PARAMNONEMPTY     [^ \t\n():]

IDENTIFIER        ({LETTER}|"_")({LETTER}|{DIGIT}|"_")*  
SQLIDENTIFIER	  "@"+{IDENTIFIER}

KEYWORD           ("if"|"exists"|"select"|"from"|"as"|"where"|"drop"|"execute"|"grant"|"return")

INTBASE			  ("big"|"small"|"tiny")?"int"
CHARBASE		  "n"?("var")?"char" 
TEXTBASE		  "n"?"text"
DECIMALBASE		  "decimal"|"numeric"
SQLTYPE			  ({INTBASE}|{TEXTBASE}|"bit"|"small"?"money"|"float"|"real"|"small"?"datetime"|"var"?"binary"|"image"|"uniqueidentifier"|"sysname")

FLOWKW            ("or"|"and"|"is"|"not"|"print"|"for"|"in"|"if"|"try"|"except"|"yield"|"raise"|"break"|"continue"|"pass"|"if"|"return"|"while"|"elif"|"else"|"finally")
DASHCOMMENT      {B}"--"[^-\n][^\n]* 

STARTDOCSYMS      ^{B}"--"/[^-]

%option noyywrap
	/* found a create */
%x Definition
			/* things to create */
%x ProcDef
%x TableDef
%x ViewDef
%x FnDef
%x FnRet
		
	/* variable to be defined */	
%x GetArgs
	/* get the SQL type */
%x GetType	
%x GetRetType
	/* figure out what is next, the end or another variable */
%x NextArg		
%x FieldDefs
%x GetFldType
%x GetNextFld
%x GetBody

%x GetParens
%x GetBegin

%x LineComment
%x BlockComment

%x DoxyLineComment
%x DoxyBlockComment

%%
		/*------------------------- Comment handling -----------------------------*/
<DoxyBlockComment>{
	[^("*"/"\/")]|"*"	{  // gather every thing to the end of the block 
		lineCount();	// account for possible newlines.
	}

	"--"|"/"|"go"|")"	{		// line comment in the block
	}

	"("		{
	}
	
	[*]*"*/"({BB}*{NEWLINE})?			{	// reached the end of the block 
		lineCount();	// account for possible newlines.
		PopState();		// done with block, reset 
	}
}

<DoxyLineComment>{
	[^\n]*		{	// snag every thing to the end of the line 
	}
	
	{NEWLINE}	{		// have an empty line comment
		g_yyLineNr++;
		PopState();
	}
}

<*>"--!"			{	// begin doxy line comment 
			PushState(DoxyLineComment);		// get the rest of the line 
		}

<*>"/*!"			{	// begin a doxy block comment 
			PushState(DoxyBlockComment);	//get the rest of the block
		}

		/*----------------------------------------------------------------------*/
		/*------------------------- object identification ----------------------*/
		
(^|{BB}*)"create"{BB}		{		// an object is about to be defined !!! must be the first thing in the line NOT part of a name
		codifyLines( yytext );
		SetState(Definition);
	}

<Definition>{
	"proc""edure"?		{		// begin stored procedure definition 
		codify( yytext );
		PushState(ProcDef);		
	}

	"function"		{		// begin stored function definition 
		codify( yytext );
		PushState(FnDef);		
	}

	"table"			{		// begin table definition
		codify( yytext );
		PushState(TableDef);
	}

	"view"			{		// begin view definition 
		codify( yytext );
		PushState(ViewDef);
	}
}


<ProcDef>{
	{IDENTIFIER}		{	// have proc name 
		generateFunctionLink( *g_code, yytext );
		PopState();
	}

	"dbo."	{
		codify( yytext );
	}	// strip this 
}
	
<FnDef>{
	{IDENTIFIER}		{	// have proc name 
		generateFunctionLink( *g_code, yytext );
		PopState();
	}

	"dbo."	{
		codify( yytext );
	}	// strip this 
}

<TableDef>{
	{IDENTIFIER}		{	// have table name 
		generateClassOrGlobalLink( *g_code, yytext );
		PopState();
	}
	
	"dbo."		{
		codify( yytext );
	}	// strip this 
}

<ViewDef>{
	{IDENTIFIER}		{	// have view name 
		generateClassOrGlobalLink( *g_code, yytext );
		PopState();
	}

	"dbo."	{
		codify( yytext );
	}	// strip this
}
	
<*>{
	{NEWLINE}		{
		codifyLines( yytext );
	}
	
	.		{
		codify( yytext );
	}
	
}

%%

/*@ ----------------------------------------------------------------------------
 */

void resetSQLCodeParserState() 
{
  g_currentDefinition = 0;
  g_currentMemberDef = 0;
  g_doubleStringIsDoc = FALSE;
  g_paramParens = 0;
  g_indents.clear();
  //BEGIN( Body );
}

void parseSQLCode(CodeOutputInterface &od,const char *className,
                 const QCString &s,bool exBlock, const char *exName,
		 FileDef *fd,int startLine,int endLine,bool inlineFragment,
		 MemberDef *) 
{

  //printf("***parseCode()\n");
  
  //--- some code to eliminate warnings---
  className = "";
  exBlock = FALSE;
  exName = "";
  inlineFragment = "";
  //--------------------------------------
  if (s.isEmpty()) return;
  g_code = &od;
  g_inputString   = s;
  g_inputPosition = 0;
  g_currentFontClass = 0;
  g_needsTermination = FALSE;
  if (endLine!=-1)
    g_inputLines  = endLine+1;
  else
    g_inputLines  = countLines();
  
  if (startLine!=-1)
    g_yyLineNr    = startLine;
  else
    g_yyLineNr    = 1;
  
  g_exampleBlock  = exBlock; 
  g_exampleName   = exName;
  g_sourceFileDef = fd;


  // Starts line 1 on the output  
  startCodeLine();

  sqlcodeYYrestart( sqlcodeYYin );

   PushState( INITIAL );
  sqlcodeYYlex();

  if (!g_indents.isEmpty()) 
  {
    // printf("Exited pysourceparser in inconsistent state!\n");
  }

  if (g_needsTermination)
  {
    endFontClass();
    g_code->endCodeLine();
  }
  return;
}


#if !defined(YY_FLEX_SUBMINOR_VERSION) 
extern "C" { // some bogus code to keep the compiler happy
  void pycodeYYdummy() { yy_flex_realloc(0,0); } 
}
#elif YY_FLEX_SUBMINOR_VERSION<33
#error "You seem to be using a version of flex newer than 2.5.4. These are currently incompatible with 2.5.4, and do NOT work with doxygen! Please use version 2.5.4 or expect things to be parsed wrongly! A bug report has been submitted (#732132)."
#endif

