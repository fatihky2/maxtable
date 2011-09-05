#!/bin/bash

echo "Compiling MaxTable"
make client -B
make master -B
make region -B
#make memTest -B
make sample -B
make libservice -B
