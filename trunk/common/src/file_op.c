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
#include "strings.h"


int
file_read(char *file_path, char *buf, int file_size)
{
	FILE *fp;

	if(file_size == 0) 
	{
		return 0;
	}

	fp = fopen(file_path, "r");
	fread(buf, file_size,1, fp);
	fclose(fp);

	return file_size;
}

void 
file_crt_or_rewrite(char *file_name, char* content)
{
	FILE *fp = fopen(file_name, "w");
	
	if (fp == NULL) 
	{
		exit(-1);
	}
	if(content != NULL)
	{
		fwrite(content, STRLEN(content), 1, fp);
	}
	fclose(fp);
}

int
file_exist(char* file_path)
{
	int	result = FALSE;
	FILE *fp = fopen(file_path, "r");
	if(fp != NULL)
	{
		result = TRUE;
		fclose(fp);
	}
	return result;
}

int
file_get_size(char *file_path)
{
	FILE *fp = fopen(file_path, "r");

	if(fp == NULL)
	{
		exit(-1);
	}
	
	fseek(fp,0,SEEK_END);
	int size = ftell(fp);
	fclose(fp);

	return size;
}

