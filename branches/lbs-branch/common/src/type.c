/*
** type.c 2010-12-28 xueyingfei
**
** Copyright flying/xueyingfei.
**
** This file is part of MaxTable.
**
** Licensed under the Apache License, Version 2.0
** (the "License"); you may not use this file except in compliance with
** the License. You may obtain a copy of the License at
**
** http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
** implied. See the License for the specific language governing
** permissions and limitations under the License.
*/


#include "global.h"
#include "type.h"
#include "utils.h"


char Int_str []			= "int";
char Short_str []		= "short";
char Tinyint_str []		= "tinyint";
char Float_str []		= "float";
char Char_str []		= "char";
char Varchar_str []		= "varchar";
char Binary_str []		= "binary";
char Varbinary_str []		= "varbinary";
char Text_str []		= "text";
char Image_str []		= "image";
char Money_str []		= "money";
char Datetime_str []		= "datetime";
char Datetimen_str []		= "datetimn";
char Datea_str []		= "date";
char Timea_str []		= "time";
char Numeric_str []		= "numeric";
char Decimal_str[]		= "decimal";
char Timestamp_str []		= "timestamp";

DTYPE DTypeInfo[] =
{ 
  /* 00 */ { NULL,		INVALID_TYPE,	0},

  /* 01 */ { Char_str,		CHAR,		1},

  /* 02 */ { Varchar_str,	VARCHAR,	-1},

  /* 03 */ { Binary_str,		BINARY,		-1},

  /* 04 */ { Varbinary_str,	VARBINARY,	-1},

  /* 05 */ { Tinyint_str,	INT1,		1},

  /* 06 */ { Short_str,		INT2,		2},	

  /* 07 */ { Int_str,		INT4,		4},

  /* 08 */ { Float_str,		FLT8,		8},
 
  /* 9 */ { Numeric_str,		NUMERIC,	8},

  /* 10 */ { Money_str,		MONEY,		4},

  /* 11 */ { Datetime_str,	DATETIM,	8},

  /* 12 */ { Text_str,		TEXT,		-1},

  /* 13 */ { Image_str,		IMAGE,		-1},
 
  /* 14 */ { Decimal_str,	DECML,		8},

  /* 15 */ { Timestamp_str,	TIMESTAMP,	8},

  /* 16 */ { Datea_str,		DATE,		8},

  /* 17 */ { Timea_str,		TIME,		8}
};




int
type_get_index_by_name(char *type)
{
	int i;

	for (i = 1; i < (TYPE_MAX_NUM + 1); i++)
	{
		if (strcmp(type, TYPE_GET_TYPE_NAME(i))== 0)
		{
			break;
		}
	}

	return i;
}

int
type_get_index_by_typenum(int type_num)
{
	int i;

	for (i = 1; i < (TYPE_MAX_NUM + 1); i++)
	{
		if (type_num == TYPE_GET_TYPE_NUM(i))
		{
			break;
		}
	}

	return i;
}

int
type_cast_ch2i(char *src, int src_len)
{
        int     len;

        len = MIN(src_len, 4);
        
        src[len] = '\0';

        return *(int *)src;
}


int
type_char2int(char *cp)
{
	char    *op;
	int     val = 0;

	op = (char *) &val;

	*op++ = *cp++;
	*op++ = *cp++;
	*op++ = *cp++;
	*op = *cp;

	return (val);
}


/* Place Holder for the casting */
void
type_cast(int s_type, int d_type, char *s_val, char *d_val)
{
	
	switch (s_type)
	{
	    case CHAR:
	    
	    case VARCHAR:
	    
	    case BINARY:
	    
	    case VARBINARY:
	    
	    case INT1:
	    
	    case INT2:
	    	break;
		
	    case INT4:

		
	    	break;
	    case FLT8:
	    
	    case NUMERIC:
	    
	    case MONEY:
	    
	    case DATETIM:
	    
	    case TEXT:
	    
	    case IMAGE:
	    
	    case DECML:
	    
	    case TIMESTAMP:
	    
	    case DATE:
	    
	    case TIME:

	    default:
	    	break;
	
	}
}
