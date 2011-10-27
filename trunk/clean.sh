#!/bin/bash

#rm -rf /mnt/metaserver/table
#rm -rf /mnt/metaserver/index
#rm -rf /mnt/metaserver/rg_server
#rm -rf /mnt/ranger/rg_table
#rm -rf /mnt/metaserver/meta_table

rm -rf ./table
rm -rf ./index
rm -rf ./rg_server
rm -rf ./rg_table
rm -rf ./meta_table
rm -rf ./rgbackup
rm -rf ./rglog

rm startClient
rm startMaster
rm startRanger
rm sample
rm imql

cp config/cli.conf.template config/cli.conf
cp config/master.conf.template config/master.conf
cp config/ranger.conf.template config/ranger.conf
