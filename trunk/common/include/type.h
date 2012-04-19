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

#ifndef TYPE_H_
#define TYPE_H_

typedef struct dtype
{
	char	*type_name;		/* Type name */
	int	type_num;		/* Type number */
	int	type_len;		/* Type len, fixed type and var-type */
} DTYPE;

#define	INVALID_TYPE	0

#define CHAR		1

#define VARCHAR		2

#define BINARY		3

#define VARBINARY	4

#define INT1		5

#define INT2		6

#define INT4		7

#define FLT8		8

#define NUMERIC		9

#define MONEY		10

#define DATETIM		11

#define TEXT		12

#define IMAGE		13

#define DECML		14

#define TIMESTAMP	15

#define DATE		16

#define TIME		17


#define TYPE_MAX_NUM	17


extern	DTYPE	DTypeInfo[18];



#define TYPE_IS_FIXED(type)             ((DTypeInfo[type].type_len > 0) ? TRUE : FALSE)

#define TYPE_IS_INVALID(type)           ((type <= INVALID_TYPE) || (type > TYPE_MAX_NUM))

#define TYPE_GET_LEN(type)              (DTypeInfo[type].type_len)

#define TYPE_GET_TYPE_NUM(type)         (DTypeInfo[type].type_num)

#define TYPE_GET_TYPE_NAME(type)        (DTypeInfo[type].type_name)


int
type_get_index_by_name(char *type);

int
type_get_index_by_typenum(int type_num);

int
type_cast_ch2i(char *src, int src_len);

int
type_char2int(char *cp);

#endif
