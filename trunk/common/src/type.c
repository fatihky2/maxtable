/*
** Copyright (C) 2011 Xue Yingfei
**
** This file is part of MaxTable.
**
** Maxtable is free software: you can redistribute it and/or modify
** it under the terms of the GNU General Public License as published by
** the Free Software Foundation, either version 3 of the License, or
** (at your option) any later version.
**
** Maxtable is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with Maxtable. If not, see <http://www.gnu.org/licenses/>.
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
