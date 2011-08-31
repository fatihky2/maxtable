/*
** conf.c 2010-07-16 xueyingfei
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
#include "utils.h"
#include "conf.h"
#include "file_op.h"
#include "token.h"
#include "memcom.h"
#include "strings.h"


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

	assert(config->conf_opt_size == 2);

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

	assert(config->conf_opt_size == 2);

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

	assert(value != NULL);

	file_size = file_get_size(file_path);

	buffer = (char *)MEMALLOCHEAP(file_size);

	file_read(file_path, buffer, file_size);

	config = conf_build(buffer, LINE_SEPARATOR);

	for(i=0; i<config->conf_opt_size; i++)
	{
		conf_opt = config->conf_opt_infor[i].conf_value;

		assert(STRLEN(conf_opt) != 0);

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

