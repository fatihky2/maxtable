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

#ifndef CONF_H_
#define CONF_H_

#include "global.h"

#define CONF_OPTION_KEY "-c"

#define CONF_PORT_KEY "port"

#define CONF_REGION_LIST_KEY "rangers"

typedef struct reginfor
{
	char*		conn;
	int		avail_space;
	int	 	connecting;
	int		serving;
}REGINFOR;

#define CONF_META_IP		"metaserver"
#define CONF_META_PORT		"metaport"
#define	CONF_RG_IP		"rangerserver"
#define CONF_RG_PORT		"rangerport"
#define CONF_BIGDATA_PORT	"bigdataport"
#define CONF_KFS_IP		"kfsserver"
#define CONF_KFS_PORT		"kfsport"

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
