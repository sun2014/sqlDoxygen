/******************************************************************************
 *
 *
 */

%{

/*
 *	includes
 */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <ctype.h>

#include "qtbc.h"
#include <qarray.h>
#include <qvaluestack.h>
#include <qregexp.h>
#include <unistd.h>
#include <qfile.h>
#include <qfileinfo.h>
  
#include "sqlscanner.h"
#include "entry.h"
#include "message.h"
#include "config.h"
#include "doxygen.h"
#include "util.h"
#include "defargs.h"
#include "language.h"
#include "commentscan.h"
#include "SQLCode.h"

#define YY_NEVER_INTERACTIVE 1

/* -----------------------------------------------------------------
 *
 *	statics
 */

  
static ParserInterface *g_thisParser;
static const char *     inputString;
static int		inputPosition;
static QFile            inputFile;

static Protection	protection;

static Entry*		current_root = 0 ;
static Entry*		current      = 0 ;
static Entry*		previous     = 0 ;
static Entry*		bodyEntry    = 0 ;
static int			yyLineNr     = 1 ;
static QCString		yyFileName;
static MethodTypes 	mtype;
static bool    		gstat;
static Specifier 	virt;
static QValueStack< int > stkPrsState;
static bool			bGatherComments = false;
static int			iLastComment	= 0;

static int              docBlockContext;
static QCString         docBlock;
static QCString         docBlockName;
static bool             docBlockInBody;
static bool             docBlockJavaStyle;
static bool             docBrief;
static bool             docBlockSpecial;

static bool             g_doubleQuote;
static bool             g_specialBlock;
//static bool             g_expectModuleDocs;
static int              g_stringContext;
static QCString *       g_copyString;
static int              g_indent = 0;
static int              g_curIndent = 0;

static QDict<QCString>  g_packageNameCache(257);
static QCString         g_packageScope;

static char             g_atomStart;
static char             g_atomEnd;
static int              g_atomCount;

//static bool             g_insideConstructor;

static QCString         g_moduleScope;
static QCString         g_packageName;

static bool             g_hideClassDocs;

static QCString         g_defVal;
static int              g_braceCount;
//-----------------------------------------------------------------------------

// forward declares
static void PopState();
static void PushState( int );
static void SetState( int );

static void initParser()
{
  protection = Public;
  mtype = Method;
  gstat = FALSE;
  virt = Normal;
  previous = 0;
  g_packageNameCache.setAutoDelete(TRUE);
}

static void initEntry()
{
  //current->python = TRUE;
  current->protection = protection ;
  current->mtype      = mtype;
  current->virt       = virt;
  current->stat       = gstat;
  current->objc       = FALSE; //insideObjC;
  current->parent     = current_root;
  initGroupInfo(current);
}

static void newEntry()
{
#ifdef _DEBUG
	//current->DumpMembers();
#endif
	previous = current;
	current_root->addSubEntry(current);
	current = new Entry ;
	initEntry();
}

static void newVariable()
{
  if (!current->name.isEmpty() && current->name.at(0)=='_') // mark as private
  {
    current->protection=Private;
  }
  if (current_root->section&Entry::SCOPE_MASK) // mark as class variable
  {
    current->stat = TRUE;
  }
  newEntry();
}

static void newFunction()
{
  if (current->name.left(2)=="__" && current->name.right(2)=="__")
  {
    // special method name, see
    // http://docs.python.org/ref/specialnames.html
    current->protection=Public;
  }
  else if (current->name.at(0)=='_')
  {
    current->protection=Private;
  }
}

//-----------------------------------------------------------------------------

static void lineCount()
{
  for( const char* c = yytext ; *c ; ++c )
    yyLineNr += (*c == '\n') ;
}

//-----------------------------------------------------------------
static void startCommentBlock(bool brief)
{
  if (brief)
  {
    current->briefFile = yyFileName;
    current->briefLine = yyLineNr;
  }
  else
  {
    current->docFile = yyFileName;
    current->docLine = yyLineNr;
  }
}

static void handleCommentBlock(const QCString &doc,bool brief)
{
	//printf("handleCommentBlock(doc=[%s] brief=%d docBlockInBody=%d\n",
	//    doc.data(),brief,docBlockInBody);

	// TODO: Fix me
	docBlockInBody=FALSE;

	if (docBlockInBody && previous && !previous->doc.isEmpty())
	{
		previous->doc=previous->doc.stripWhiteSpace()+"\n\n";
	}

	int position = 0;
	bool needsEntry;
	while (parseCommentBlock(
				g_thisParser,
				(docBlockInBody && previous) ? previous : current,
				doc,     // text
				yyFileName, // file
				brief ? current->briefLine : current->docLine,   // line of block start
				docBlockInBody ? FALSE : brief, 
				docBlockJavaStyle, // javadoc style
				docBlockInBody,
				protection,
				position,
				needsEntry )
		) // need to start a new entry
	{
		if (needsEntry)
		{
			newEntry();
		}
	}
	if (needsEntry)
	{
		newEntry();
	}

}

static void endOfDef()
{
  if (bodyEntry)
  {
    bodyEntry->endBodyLine  = yyLineNr;
    bodyEntry = 0;
  }
  newEntry();
  //g_insideConstructor = FALSE;
}

static inline void addToString(const char *s)
{
  if (g_copyString) (*g_copyString)+=s;
}

static void initSpecialBlock()
{
  docBlockContext   = YY_START;
  docBlockInBody    = FALSE;
  docBlockJavaStyle = TRUE;
  docBrief = TRUE;
  docBlock.resize(0);
  startCommentBlock(TRUE);
}

//-----------------------------------------------------------------------------
/* ----------------------------------------------------------------- */
#undef	YY_INPUT
#define	YY_INPUT(buf,result,max_size) result=yyread(buf,max_size);

static int yyread(char *buf,int max_size)
{
  int c=0;
  while ( c < max_size && inputString[inputPosition] )
  {
    *buf = inputString[inputPosition++] ;
    //printf("%d (%c)\n",*buf,*buf);
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
%x IndexDef
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
%x GetStmtBlock
%x GetStmt
%x GetExpr

%x LineComment
%x BlockComment

%x DoxyLineComment
%x DoxyBlockComment

%%
		/*------------------------- Comment handling -----------------------------*/
<BlockComment>{
	[^("*"/"\/")]|"*"	{  // gather every thing to the end of the block 
		//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] Block Comment Body match\n",
		//		docBlock, YY_START, yyLineNr);

		lineCount();
		//docBlock = docBlock + yytext;
	}

	"--"|"/"|"go"|")"	{		// line comment in the block
		//docBlock = docBlock + yytext;
	}

	"("		{
		//docBlock = docBlock + yytext;
		//PushState( GetParens );
	}
	
	[*]*"*/"			{	// reached the end of the block 
			//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] Block Comment Body\n",
			//		docBlock, YY_START, yyLineNr);
			
		lineCount();
		PopState();		// done with block, reset 
	}
}

<DoxyBlockComment>{
	[^("*"/"\/")]|"*"	{  // gather every thing to the end of the block 
		//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] Block Comment Body match\n",
		//		docBlock, YY_START, yyLineNr);

		lineCount();
		docBlock = docBlock + yytext;
	}

	"--"|"/"|"go"|")"	{		// line comment in the block
		docBlock = docBlock + yytext;
	}

	"("		{
		docBlock = docBlock + yytext;
		//PushState( GetParens );
	}
	
	[*]*"*/"			{	// reached the end of the block 
			//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] DOXY Block Comment Body\n",
			//		docBlock, YY_START, yyLineNr);
			
			lineCount();
			PopState();		// done with block, reset 
		}
}

<LineComment>{
	[^\n]*			{	// snag every thing to the end of the line 
			//docBlock = docBlock + yytext;
			
			//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] Line Comment Body\n",
			//		docBlock, YY_START, yyLineNr);
			
			//PopState();		// reset, we are done with this comment 
	}

	\n	{		// have an empty line comment
		lineCount();
		PopState();
	}
}

<DoxyLineComment>{
	[^\n]*		{	// snag every thing to the end of the line 
			docBlock = docBlock + yytext;
			
			//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] DOXY Comment Body\n",
			//		docBlock, YY_START, yyLineNr);

			//PopState();		// reset, we are done with this comment 
	}
	
	\n	{		// have an empty line comment
		lineCount();
		PopState();
	}
}

<*>"--"[-]*				{	// found the beginning of a normal line comment
			//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] dash comment\n",
			//		yytext, YY_START, yyLineNr);
			
			PushState(LineComment);			// eat the line 
		}

<*>"/*"[*]*				{	// found the beginning of a normal comment block 
			//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] block comment begin\n",
			//		yytext, YY_START, yyLineNr);
			
			PushState(BlockComment);	// begin to consume the block 
		}


<*>"/*"[*]*"*/"		{	// nothing but stars in the middle
			if( bGatherComments == false )
			{
				// starting a new block of comments
				bGatherComments = true;
				docBlock.resize(0);			// clear this out 
			}
		}
		
<*>"--!"			{	// begin doxy line comment 
			//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] DOXY dash comment\n",
			//		yytext, YY_START, yyLineNr);
			
			PushState(DoxyLineComment);		// get the rest of the line 
		}

<*>"/*!"			{	// begin a doxy block comment 
			//printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] DOXY block comment begin\n",
			//		yytext, YY_START, yyLineNr);
			
			PushState(DoxyBlockComment);	//get the rest of the block
		}
		

<GetParens>{

	"("		{
		docBlock += *yytext;
		PushState( GetParens );		
	}

	")"		{
		docBlock += *yytext;
		PopState();
	}

	.		{
		docBlock += *yytext;
	}
}

<GetStmtBlock>{
	"begin"		{
		//docBlock += *yytext;
		PushState( GetStmtBlock );		
	}

	"end"		{
		//docBlock += *yytext;
		PopState();
	}

	.		{
		//docBlock += *yytext;
	}
}

		/*----------------------------------------------------------------------*/
		/*------------------------- object identification ----------------------*/
		
(^|{BB}*)"create"{BB}		{		// an object is about to be defined !!! must be the first thing in the line NOT part of a name
				 //printf("\t[sqlscanner] 'create' [ state %d ]  [line %d] begin def\n",
				 //       YY_START, yyLineNr);
		lineCount();	
		
		if( docBlock.isEmpty() == false )
		{
			handleCommentBlock( docBlock, false );
			docBlock.resize( 0 );
		}
		SetState(Definition);
	}

(^|{BB}*)"if"{BB}	{
		 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] begin IF def\n",
		 //       yytext, YY_START, yyLineNr );
		PushState( GetStmt );
		PushState( GetExpr );
	}

{B}"declare"{BB}			{		// must not be part of a larger word
		//SetState(GetArgs);		// time to get the variable name 
	}
	
<Definition>{
	"proc""edure"?		{		// begin stored procedure definition 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] begin proc def\n",
				 //       yytext, YY_START, yyLineNr );
				        
		current->fileName  = yyFileName;
		current->section = Entry::FUNCTION_SEC;
		current->protection = protection = Public;
		current->objc = FALSE;
		current->virt = Normal;
		current->stat = FALSE;
		current->mtype = mtype = Method;
		current->type = "StoredProc";
		current->name.resize(0);
		current->args.resize(0);
		current->argList->clear();
		PushState(ProcDef);		
	}

	"function"		{		// begin stored function definition 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] begin proc def\n",
				 //       yytext, YY_START, yyLineNr);
				        
		current->fileName  = yyFileName;
		current->section = Entry::FUNCTION_SEC;
		current->protection = protection = Public;
		current->objc = FALSE;
		current->virt = Normal;
		current->stat = FALSE;
		current->mtype = mtype = Method;
		current->type = "Function";
		current->name.resize(0);
		current->args.resize(0);
		current->argList->clear();
		PushState(FnDef);		
	}

	"table"			{		// begin table definition
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] begin table def\n",
				 //       yytext, YY_START, yyLineNr);
				        
		current->fileName  = yyFileName;
		current->section = Entry::CLASS_SEC;
		current->protection = protection = Public;
		current->objc = FALSE;
		current->virt = Normal;
		current->stat = FALSE;
		current->mtype = mtype = Method;
		current->type = "Table";
		current->name.resize(0);
		current->args.resize(0);
		current->argList->clear();
		PushState(TableDef);
	}

	"view"			{		// begin view definition 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] begin view def\n",
				 //       yytext, YY_START, yyLineNr);
				        
		current->fileName  = yyFileName;
		current->section = Entry::STRUCT_SEC;
		current->protection = protection = Public;
		current->objc = FALSE;
		current->virt = Normal;
		current->stat = FALSE;
		current->mtype = mtype = Method;
		current->type = "View";
		current->name.resize(0);
		current->args.resize(0);
		current->argList->clear();
		PushState(ViewDef);
	}
	
	("unique"?{BB}*("non"?"clustered"){BB})?"index"	{	// begin index def
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] begin index def\n",
				 //       yytext, YY_START, yyLineNr);
		PushState( IndexDef );
	}
}


<ProcDef>{
	{IDENTIFIER}		{	// have proc name 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] proc name\n",
				 //       yytext, YY_START, yyLineNr);
		
		current->name = yytext;
		current->startLine = yyLineNr;
		current->bodyLine  = yyLineNr;
		PopState();
		PushState(GetBody);
		PushState(GetArgs);		// get proc parameters 
	}

	"dbo."	{}	// strip this 
}
	
<FnDef>{
	{IDENTIFIER}		{	// have function name 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] function name\n",
				 //       yytext, YY_START, yyLineNr);
		
		current->name = yytext;
		current->startLine = yyLineNr;
		current->bodyLine  = yyLineNr;
		PopState();
		PushState( FnRet );		// this is where we need to go when we return from arguments gathering
		PushState( GetArgs );		// get proc parameters 
	}

	"dbo."	{}	// strip this 
}

<FnRet>{	
	"returns" {
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] function return\n",
				 //       yytext, YY_START, yyLineNr);

			// need to get the SQL RETURN type  current->type
		PushState( GetRetType );
	}
	
	"as"	{
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] function, AS before body name\n",
				 //       yytext, YY_START, yyLineNr);

			// body follows to a GO
		PopState();
		PushState( GetBody );
	}
	
	")"		{	// eat this there was a return before this 
	}
}
	
<TableDef>{
	{IDENTIFIER}		{	// have table name 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] table name\n",
				 //       yytext, YY_START, yyLineNr);
				        
		current->name = yytext;
		current->startLine = yyLineNr;
		current->bodyLine  = yyLineNr;
		endOfDef();		// flush this guy and set us up for the next Entry
		PopState();
		PushState(FieldDefs);
	}
	
	"dbo."		{}	// strip this 
}

<ViewDef>{
	{IDENTIFIER}		{	// have view name 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] view name\n",
				 //       yytext, YY_START, yyLineNr);
				        
		current->name = yytext;
		current->startLine = yyLineNr;
		current->bodyLine  = yyLineNr;
		endOfDef();		// flush this guy and set us up for the next Entry
		PopState();
		PushState( GetBody );
	}

	"dbo."	{}	// strip this
}

<IndexDef>{
	"on"	{
	}

	{IDENTIFIER}		{	// have index name 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] index name\n",
				 //       yytext, YY_START, yyLineNr);
	}
	
	"("		{		// opening parens for field names
		PopState();
		PushState( GetParens );
	}
}

<GetArgs>{

	{SQLIDENTIFIER}		{	// have variable name
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] var name\n",
				  //      yytext, YY_START, yyLineNr);
				        
		Argument *a = new Argument;
		current->argList->append(a);
		current->argList->getLast()->name = yytext;
		SetState(GetType);			// (GOTO) get the variable type 
	}	

	"("	{		// eat this, i am finding them in the proc arg list
	}	
	
	")" {		// end of the block
		endOfDef();		// flush this guy and set us up for the next Entry
		lineCount();
		PopState();		// no more variables 
	}
}
	
<GetType>{
	"as"			{		// eat this
	}
	
	{SQLTYPE}			{	// have the SQL type 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] type name\n",
				 //       yytext, YY_START, yyLineNr);
				        
		current->argList->getLast()->type = yytext;
		PushState(NextArg);			// find out if we are done or if there are more variables
	}

	"table"				{	// local table def
		current->argList->getLast()->type = "TABLE";
		PushState(NextArg);				// tells us where to go when the table def thing is done
		PushState(FieldDefs);			// dig out the columns for the local table
	}	
	
	{CHARBASE}({B}"("{B}{DIGIT}+{B}")")?			{		// snag the char defs : char ( 10 )
		current->argList->getLast()->type = yytext;
		PushState(NextArg);			// find out if we are done or if there are more variables
	}
	
	{DECIMALBASE}({B}"("{B}{DIGIT}+{B}","{B}{DIGIT}+{B}")")?		{		// snag the decimal defs : decimal ( 10 , 3 )
		current->argList->getLast()->type = yytext;
		PushState(NextArg);			// find out if we are done or if there are more variables
	}
}

<NextArg>{
	"\n"		{
		lineCount();
		endOfDef();		// flush this guy and set us up for the next Entry
		PopState();		// no more variables 
		PopState();		// no more variables 
	}
	
	"OUTPUT"			{		// output param
		current->argList->getLast()->type += " OUTPUT";
	}

	{B}"="{B}"NULL"		{		// eat default value declaration
	}
	
	")"		{		// eat these, i am finding them in the proc arg list
		endOfDef();		// flush this guy and set us up for the next Entry
		PopState();		// no more variables 
		PopState();		// no more variables 
	}	
	
	","		{
		PopState();		// there are more variables 
	}
}
	
<GetRetType>{
	"AS"			{		// eat this
	}
	
	{SQLTYPE}			{	// have the SQL type 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] type name\n",
				 //       yytext, YY_START, yyLineNr);
				        
		current->type = yytext;
		PopState();			// find out if we are done or if there are more variables
	}

	"table"				{	// local table def
		current->type = "TABLE";
		PopState();				//send us back to our caller when done
		PushState(FieldDefs);			// dig out the columns for the local table
	}	
	
	{CHARBASE}({B}"("{B}{DIGIT}+{B}")")?			{		// snag the char defs : char ( 10 )
		current->type = yytext;
		PopState();			// find out if we are done or if there are more variables
	}
	
	{DECIMALBASE}({B}"("{B}{DIGIT}+{B}","{B}{DIGIT}+{B}")")?		{		// snag the decimal defs : decimal ( 10 , 3 )
		current->type = yytext;
		PopState();			// find out if we are done or if there are more variables
	}
}
	
<FieldDefs>{
	"("			{	// the beginning
	}
	
	{IDENTIFIER}	{		// eat it
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] field name\n",
				 //       yytext, YY_START, yyLineNr);
				        
		current->fileName  = yyFileName;
		current->startLine = yyLineNr;
		current->bodyLine  = yyLineNr;
		current->section = Entry::VARIABLE_SEC;
		current->protection = protection = Public;
		current->objc = FALSE;
		current->virt = Normal;
		current->stat = FALSE;
		current->name = yytext;
		current->args.resize(0);
		current->argList->clear();
		current->parent = previous;

		SetState(GetFldType);			// (GOTO) get the variable type 
	}

	B|","			{		// separators
	}
	
	")"			{ // the end
		PopState();		// for just in case anyone DARES putting anything after a table def
	}

}

<GetFldType>{
	{SQLTYPE}			{	// have the SQL type 
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] field type name\n",
				 //       yytext, YY_START, yyLineNr);
				        
		current->type = yytext;
		PushState(GetNextFld);			// find out if we are done or if there are more variables
	}

	/*"table"				{	// local table def, best not happen in a table
		current->type = "TABLE";
		PopState();				//send us back to our caller when done
		PushState(FieldDefs);			// dig out the columns for the local table
	}	*/
	
	{CHARBASE}({B}"("{B}{DIGIT}+{B}")")?			{		// snag the char defs : char ( 10 )
		current->type = yytext;
		PushState(GetNextFld);			// find out if we are done or if there are more variables
	}
	
	{DECIMALBASE}({B}"("{B}{DIGIT}+{B}","{B}{DIGIT}+{B}")")?		{		// snag the decimal defs : decimal ( 10 , 3 )
		current->type = yytext;
		PushState(GetNextFld);			// find out if we are done or if there are more variables
	}
}

<GetNextFld>{
	","		{
		if( docBlock.isEmpty() == false )
		{
			current->brief = docBlock;
			docBlock.resize( 0 );
		}
		previous->addSubEntry(current);
		current = new Entry ;
		initEntry();

		PopState();		// there are more variables 
	}
	
	")"			{ // the end
		if( docBlock.isEmpty() == false )
		{
			current->brief = docBlock;
			docBlock.resize( 0 );
		}
		previous->addSubEntry(current);
		current = new Entry ;
		initEntry();

		PopState();		// no more variables 
		PopState();		// no more variables 
	}
	
	"("		{		// found an opening paren in the blah region
		docBlock += *yytext;
		PushState( GetParens );
	}
	
	.		{	// the rest of the BS after the type 
		docBlock += *yytext;
	}
}

	/* a body should begin with an 'as' and end with a 'go' */
<GetBody>{
	{BN}+"go"{BN}		{
				 //printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] end of body \n",
				 //       yytext, YY_START, yyLineNr);
			// end of the body
			// this is very specific for now ( go being the only thing on a line at the beginning of the line)
			lineCount();
			PopState();
	}
	
	.	{
			// eat the rest of this stuff for now.
	}
}

		/*----------------------------------------------------------------------*/

<GetExpr>{
	"exists"{BB}*"("		{		// the beginning of function EXISTS
		PopState();
		PushState( GetParens );
	}
	
	"("		{		// the expression is wrapped in parens
		PopState();
		PushState( GetParens );
	}

	[^\n]*	{		// for now assume the statement will be just one line
		PopState();
	}
}

<GetStmt>{
	{BB}"begin"		{		// the statement is a block
		PopState();
		PushState( GetStmtBlock );
	}
	
	[^\n]*	{		// for now assume the statement will be just one line
		PopState();
	}
}


		/*----------------------------------------------------------------------*/

{KEYWORD}		{
		if( docBlock.isEmpty() == false )
		{
			handleCommentBlock( docBlock, false );
			docBlock.resize( 0 );
		}
		if( YY_START )
				 printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] Keyword\n",
				        yytext, YY_START, yyLineNr);
	}

{SQLIDENTIFIER}		{
		if( docBlock.isEmpty() == false )
		{
			handleCommentBlock( docBlock, false );
			docBlock.resize( 0 );
		}
		if( YY_START )
				 printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] SQL identifier\n",
				        yytext, YY_START, yyLineNr);
	}

<*>^"go"{BB}			{		// reset to INITIAL  issue warning if we are not in that state : stand alone word
		if( docBlock.isEmpty() == false )
		{
			handleCommentBlock( docBlock, false );
			docBlock.resize( 0 );
		}
		if( YY_START )
		{
			printf("\t[SQLSCANNER] '%s' [ state %d ]  [line %d] GO Encountered\n", 
					yytext, YY_START, yyLineNr);

			// TODO: issue warning when we encounter this
			PopState();
		}
	}

		/* ------------ End rules -------------- */
<*>{BB}		/* eat white space */


<*>({NONEMPTY}|{EXPCHAR})           { // This should go one character at a time.
				// let me know when i am eating non white space while trying
				//	to do something meaningful
		if( YY_START )			
				  printf("\t[SQLSCANNER] '%s' [ state %d ]  [line %d] NO MATCH\n",
				        yytext, YY_START, yyLineNr);
	}

<*>{NEWLINE}	{
				 //printf("\t[sqlscanner] %d NEWLINE [line %d] no match\n",
				 //       YY_START, yyLineNr);

		lineCount();
	}

<*>.		{
				// let me know when i am eating non white space while trying
				//	to do something meaningful
		if( YY_START )			
			printf("\t[sqlscanner] '%s' [ state %d ]  [line %d] (dot) no match\n",
					yytext, YY_START, yyLineNr);
	}

%%

//----------------------------------------------------------------------------

static void parseCompounds(Entry *rt)
{
  //printf("parseCompounds(%s)\n",rt->name.data());
  EntryListIterator eli(*rt->sublist);
  Entry *ce;
  for (;(ce=eli.current());++eli)
  {
    if (!ce->program.isEmpty())
    {
      //printf("-- %s ---------\n%s\n---------------\n",
      //  ce->name.data(),ce->program.data());
      // init scanner state
      inputString = ce->program;
      inputPosition = 0;
      sqlscanYYrestart( sqlscanYYin ) ;
      if (ce->section&Entry::COMPOUND_MASK)
      {
        current_root = ce ;
        //SetState( Search );
      }
      else if (ce->parent)
      {
        current_root = ce->parent;
		//printf("Searching for member variables in %s parent=%s\n",
		//    ce->name.data(),ce->parent->name.data());
		//SetState( SearchMemVars );
      }
      yyFileName = ce->fileName;
      yyLineNr = ce->startLine ;
      if (current) delete current;
      current = new Entry;

      groupEnterCompound(yyFileName,yyLineNr,ce->name);
      
      sqlscanYYlex() ;
      delete current; current=0;
      ce->program.resize(0);

      groupLeaveCompound(yyFileName,yyLineNr,ce->name);

    }
    parseCompounds(ce);
  }
}

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

//----------------------------------------------------------------------------

static void parseMain(const char *fileName,const char *fileBuf,Entry *rt)
{
  initParser();

  inputString = fileBuf;
  inputPosition = 0;

  protection    = Public;
  mtype         = Method;
  gstat         = FALSE;
  virt          = Normal;
  current_root  = rt;
  //g_expectModuleDocs = TRUE;
  g_specialBlock = FALSE;
  //g_insideConstructor = FALSE;


  inputFile.setName(fileName);
  if (inputFile.open(IO_ReadOnly))
  {
    yyLineNr= 1 ; 
    yyFileName = fileName;
    //setContext();
    msg("Parsing file %s...\n",yyFileName.data());

    QFileInfo fi(fileName);

    current            = new Entry;
    current->name      = "SQL";
    current->section   = Entry::NAMESPACE_SEC;
    current->type      = "namespace";
    current->brief	   = "Database stuff.";
    current->fileName  = yyFileName;
    current->startLine = yyLineNr;
    current->bodyLine  = yyLineNr;

    rt->addSubEntry(current);

    current_root  = current ;
    initParser();
    current       = new Entry;

    groupEnterFile(yyFileName,yyLineNr);

	// we ARE a source file    
	current->name    = yyFileName;
	current->section = Entry::SOURCE_SEC;
	current_root->addSubEntry(current);
	current          = new Entry;


    current->reset();
    sqlscanYYrestart( sqlscanYYin );
    PushState( INITIAL );
		// start the parse of this file
    sqlscanYYlex();

    groupLeaveFile(yyFileName,yyLineNr);

    current_root->program.resize(0);
    delete current; 
    current=0;

    parseCompounds(current_root);

    inputFile.close();
  }
  
}

//----------------------------------------------------------------------------


void SQLLanguageScanner::parseInput(const char *fileName,const char *fileBuf,Entry *root)
{
  g_thisParser = this;
  ::parseMain(fileName,fileBuf,root);

  // May print the AST for debugging purposes
  // printAST(global_root);
}

bool SQLLanguageScanner::needsPreprocessing(const QCString &)
{
  return FALSE;
}

void SQLLanguageScanner::parseCode(CodeOutputInterface &codeOutIntf,
    const char *scopeName,
    const QCString &input,
    bool isExampleBlock,
    const char *exampleName,
    FileDef *fileDef,
    int startLine,
    int endLine,
    bool inlineFragment,
    MemberDef *memberDef
    )
{
	//printf( "\t\tParse Code Time!!!\n" );
  ::parseSQLCode(codeOutIntf,scopeName,input,isExampleBlock,exampleName,
                    fileDef,startLine,endLine,inlineFragment,memberDef);
}

void SQLLanguageScanner::parsePrototype(const char *text)
{
  //::parsePrototype(text);

}

void SQLLanguageScanner::resetCodeParserState()
{
  resetSQLCodeParserState();
}

//----------------------------------------------------------------------------

#if !defined(YY_FLEX_SUBMINOR_VERSION) 
//----------------------------------------------------------------------------
extern "C" { // some bogus code to keep the compiler happy
  void pyscannerYYdummy() { yy_flex_realloc(0,0); } 
}
#endif

