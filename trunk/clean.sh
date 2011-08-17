#!/bin/bash

rm -rf table
rm -rf index
rm -rf rg_server
rm -rf rg_table
rm -rf meta_table
rm startClient
rm startMaster
rm startRegion

cp config/cli.conf.template config/cli.conf
cp config/master.conf.template config/master.conf
cp config/region.conf.template config/region.conf
