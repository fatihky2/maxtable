#!/bin/bash

rm -rf /mnt/metaserver/table
rm -rf /mnt/metaserver/index
rm -rf /mnt/metaserver/rg_server
rm -rf /mnt/ranger/rg_table
rm -rf /mnt/metaserver/meta_table
rm startClient
rm startMaster
rm startRegion
rm sample

cp config/cli.conf.template config/cli.conf
cp config/master.conf.template config/master.conf
cp config/region.conf.template config/region.conf
