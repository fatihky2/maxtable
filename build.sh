#!/bin/bash

echo "Compiling MaxTable"
make cli -B
make master -B
make region -B
#make memTest -B
