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



int
str1nstr (char *buf, char *sub, int len)
{
	char	*bp;
	char	*sp;
	int	i;
	int	j;


	i = 0;
	
	if (!*sub)
	{
		return i;
	}
	
	while (*buf && (i < len))
	{
		bp = buf;
		sp = sub;
		j = 0;
		do 
		{
			if (!*sp)
			{
				return (i + j);
			}

			j++;

		} while (*bp++ == *sp++);
		buf++;
		i++;
	}
	
	return i;
}



int
str01str (char *buf, char *sub, int len)
{
	char	*bp;
	char	*sp;
	int	i;
	
	i = 0;
	if (!*sub)
	{
		return (i - 1);
	}
	
	while (*buf &&  (i < len))
	{
		bp = buf;
		sp = sub;

		do 
		{
			if (!*sp)
			{
				return (i - 1);
			}
	
		} while (*bp++ == *sp++);
		buf++;
		i++;
	}
	
	return (i - 1);
}


int
strmnstr (char *buf, char *sub, int len)
{
	char	*bp;
	char	*sp;
	int	i;
	int	j;
	int	k;


	i = 0;
	
	if (!*sub)
	{
		return i;
	}
	
	while (*buf && (i < len))
	{
		bp = buf;
		sp = sub;
		j = 0;
		do 
		{
			if (!*sp)
			{
				k = (i + j);
				break;
			}

			j++;

		} while (*bp++ == *sp++);
		buf++;
		i++;
	}
	
	return k;

}



int
str0n_trunc_0t(char *buf, int len, int *start, int *end)
{
	char	*str;

	
	*start = 0;
	*end = len;
	str = buf;

	if (len == 0)
	{
		return TRUE;
	}
	
	
	while (((*str == ' ') || (*str == '\t')) && ((*start) < len))
	{
		str++;
		(*start)++;		
	}

	str = &(buf[len - 1]);
	
	while (((*str == ' ') || (*str == '\t')) && ((*end) > (*start)))
	{
		str--;
		(*end)--;		
	}

	return TRUE;
}

