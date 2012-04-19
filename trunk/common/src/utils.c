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
#include "list.h"
#include "utils.h"
#include "file_op.h"
#include "strings.h"


char* 
trim(char *str, char deli)
{
	//remove left side
	while(*str == deli)
	{
		str++;
	}
	//remove right side
	char* tmp = str;
	while(*tmp != '\0')tmp++;
	tmp--;

	while(*tmp == deli)
	{
		*tmp = '\0';
		tmp--;
	}
	return str;
}

int
match(char* dest, char *src)
{
	return !strcasecmp(dest, src);
}

void
str1_to_str2(char *des, char split, char *src)
{
	char * ptr;
	ptr = des + STRLEN(des);
	*ptr++ = split;
	*ptr = 0;
	strcpy(ptr,src);

	return;
}

void
traceprint (char *fmt, ...)
{
	va_list	ap;
	char		msgbuf[2048];

	va_start(ap, fmt);       /* initialize argument list pointer */

	vsnprintf(msgbuf, 2048, fmt, ap);
	va_end(ap);

	fprintf(stderr, "%s", msgbuf);

	return;
}


void
backmove(register long *from, register long *to, register int length)
{
	int	i;
	char *temp;
	
	temp = (char *)from;
	temp += length;
	from = (long *)temp;
	temp = (char *)to;
	temp += length;
	to = (long *)temp;



	/*
	** If there are more than 8 bytes and both addesses are even, then
	** move 4 bytes at a time until you get to the last 1,2,3 or 4 bytes. 
	** They are moved one byte at a time.
	*/
	if (length >= 8 && !(((long) from | (long) to) & ( sizeof (long) - 1 )))
	{
		/*
		** Move long words
		*/
		length -= 8;
		do
		{
			*--to = *--from;
			length -= 8;
		} while (length > 0);

		length = 8 + length;
	}

	/* move the bytes */
	i = -1;
	while (i >= -length)
	{
		((char *)to)[i] = ((char *)from)[i];
		i--;
	}
}

void
backmove2(register char *from, register char *to, register int length)
{
        int     i;


        from += length;
        to += length;


	/*
	** If there are more than 8 bytes and both addesses are even, then
	** move 4 bytes at a time until you get to the last 1,2,3 or 4 bytes.
	** They are moved one byte at a time.
	*/
        if (length >= 8 && !(((long) ((long *)from) | (long)((long *) to)) & (sizeof(long) - 1)))
        {
                /*
                ** Move long words
                */
                length -= 4;
                do
                {
                        *--to = *--from;
                        length -= 4;
                } while (length > 0);

                length = 4 + length;
        }

        /* move the bytes */
        i = -1;
        while (i >= -length)
        {
                to[i] = from[i];
                i--;
        }
}

void
backmove1(register char *from, register char *to, register int length)
{
        int     i;


        from += length;
        to += length;


        /* move the bytes */
        i = -1;
        while (i >= -length)
        {
                to[i] = from[i];
                i--;
        }
}



void
build_file_name(char	*filehdr, char *filename, int fileno)
{
	MEMSET(filename, sizeof(filename));
	sprintf(filename, "%s%d", filehdr,fileno);
}


unsigned long
hashstring(char *bytes, int nbytes, int seed)
{
	register unsigned long	c, len, i;


	len = nbytes;
	c = seed;           	


	while ((i < 12) && (i < (len - 2)))
	{
		c += ((unsigned long) bytes[len - i] << 24);
		c += ((unsigned long) bytes[len - i - 1] << 16);
		c += ((unsigned long) bytes[len - i - 2] << 8);

		i += 3;
	}

	return c;
}

int
m_atoi(char *strval, int strlen)
{
	char tmp_str[32];


	if (strlen > 32)
	{
		return -1;
	}
	
	MEMSET(tmp_str, 32);
	MEMCPY(tmp_str, strval, strlen);
	
	return atoi(tmp_str);
}



