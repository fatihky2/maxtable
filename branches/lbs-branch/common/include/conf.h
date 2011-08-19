/*
** conf.h 2010-07-20 xueyingfei
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
#ifndef CONF_H_
#define CONF_H_

#include "global.h"

#define CONF_OPTION_KEY "-conf"

#define CONF_PORT_KEY "port"

#define CONF_REGION_LIST_KEY "regionList"

typedef struct reginfor
{
	char*		conn;
	int		avail_space;
	int	 	connecting;
	int		serving;
}REGINFOR;

/* Used by config functions. */

#define CONF_MAX_OPTION	32

typedef struct config_opt
{
	char		*conf_value;
	int		conf_value_size;
} CONFIG_OPT;

typedef struct config
{
	int		conf_opt_size;
	CONFIG_OPT	conf_opt_infor[CONF_MAX_OPTION];
}CONFIG;

CONFIG * 
conf_build(char *str, char deli);

int 
conf_destroy(CONFIG* conf);

int
conf_get_key(char *key, char* line);

int
conf_get_value(char *value, char* line);

int 
conf_get_value_by_key(char *value, char *file_path, char* target_key);

int
conf_get_path(int argc, char *argv[], char **conf_path);


#endif /* CONF_H_ */
