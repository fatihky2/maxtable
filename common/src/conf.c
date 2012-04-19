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
#include "utils.h"
#include "conf.h"
#include "file_op.h"
#include "token.h"
#include "memcom.h"
#include "strings.h"

char	Kfsserver[32];
int	Kfsport;

CONFIG * 
conf_build(char *str, char deli)
{
	CONFIG	*conf_tmp;
	int	tnum = 0;
	int	len;
	char	*config;
	char	*start;
	

	if (str == NULL || STRLEN(str) == 0)
	{
		return NULL;
	}

	conf_tmp = (CONFIG *)MEMALLOCHEAP(sizeof(CONFIG));

	while (*str == ' ' || *str == '\t' || *str == deli) 
	{
		str++;
	}
	
	while (*str != '\0') 
	{
		start = str;
		while (*str != '\0' && *str != deli)
		{
			str++;
		}

		/* Skip the comment. */
		if (*start == '#')
		{
			while (*str == ' ' || *str == '\t' || *str == deli) 
			{
				str++;
			}
			
			continue;
		}
		
		len = str - start;
		config = (char *)MEMALLOCHEAP(len + 1);
		strncpy(config, start, len);
		config[len] = '\0';
		
		conf_tmp->conf_opt_infor[tnum].conf_value = config;
		conf_tmp->conf_opt_infor[tnum].conf_value_size = len;

		while (*str == ' ' || *str == '\t' || *str == deli) 
		{
			str++;
		}
		
		tnum++;
	}

	conf_tmp->conf_opt_size = tnum;

	return conf_tmp;
}



/*
** since the tokens has a nested array, it needs a method to free
**/
int 
conf_destroy(CONFIG* conf)
{
	int i;
	
	for(i=0; i< conf->conf_opt_size; i++)
	{
		if (conf->conf_opt_infor[i].conf_value != NULL) 
		{
			MEMFREEHEAP(conf->conf_opt_infor[i].conf_value);
		}
	}
	
	MEMFREEHEAP(conf);
	return TRUE;
}

int
conf_get_key(char *key, char* line)
{
	CONFIG  *config;
	char    *key_temp;

	config = conf_build(line, CONF_SEPARATOR);

	Assert(config->conf_opt_size == 2);

	/* Key = Value*/
	key_temp = trim(config->conf_opt_infor[0].conf_value, ' ');
//	key_temp = config->conf_opt_infor[0].conf_value;
	MEMCPY(key, key_temp, STRLEN(key_temp));

	conf_destroy(config);
	return TRUE;
}

int
conf_get_value(char *value, char* line)
{
	CONFIG *config;
	char    *value_temp;

	config = conf_build(line, CONF_SEPARATOR);

	Assert(config->conf_opt_size == 2);

	value_temp = trim(config->conf_opt_infor[1].conf_value, ' ');
//	value_temp = config->conf_opt_infor[1].conf_value;
	MEMCPY(value, value_temp, STRLEN(value_temp));

	conf_destroy(config);

	return TRUE;
}

/* if the target key has not been found, the method will return NULL */
int 
conf_get_value_by_key(char *value, char *file_path, char* target_key)
{
	char	*conf_opt;
	char    key[TOKEN_NAME_MAX_LEN];
	char	*buffer;
	CONFIG	*config;
	int	i;
	int	file_size;	

	Assert(value != NULL);

	file_size = file_get_size(file_path);

	buffer = (char *)MEMALLOCHEAP(file_size);

	file_read(file_path, buffer, file_size);

	config = conf_build(buffer, LINE_SEPARATOR);

	for(i=0; i<config->conf_opt_size; i++)
	{
		conf_opt = config->conf_opt_infor[i].conf_value;

		Assert(STRLEN(conf_opt) != 0);

		MEMSET(key, TOKEN_NAME_MAX_LEN);
		conf_get_key(key, conf_opt);
		if(match(key, target_key))
		{
			conf_get_value(value, conf_opt);
			break;
		}
	}

	conf_destroy(config);

	trim(value, ' ');

	MEMFREEHEAP(buffer);
	return TRUE;
}

int
conf_get_path(int argc, char *argv[], char **conf_path)
{
	if(   (argc == 3) 
	   && match(argv[1], CONF_OPTION_KEY) 
	   && file_exist(argv[2]) == TRUE)
	{
		*conf_path = argv[2];
	}

	//Make sure conf file exist
	if(file_exist(*conf_path) == FALSE)
	{
		file_crt_or_rewrite(*conf_path, NULL);
	}

	return TRUE;
}

