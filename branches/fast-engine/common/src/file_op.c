/*
** file_op.c 2010-08-19 xueyingfei
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

