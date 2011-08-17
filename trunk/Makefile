CC		= gcc -g -Wall
CFLAGS		= -I./common/include/ -I./client/include -I./master/include -I./region/include -lpthread
COMMON_SRC	= common/src/*.c
REGION_SRC	= region/src/*.c
MASTER_SRC	= master/src/*.c
CLI_SRC		= client/src/*.c

all:

client:
	$(CC) $(CFLAGS) ${COMMON_SRC} ${CLI_SRC} -D MEMMGR_TEST -D MAXTABLE_UNIT_TEST -o startClient
#	$(CC) $(CFLAGS) ${COMMON_SRC} ${CLI_SRC} -D MEMMGR_TEST -o startClient

master:
	$(CC) $(CFLAGS) ${COMMON_SRC} ${MASTER_SRC} -D MEMMGR_TEST -o startMaster

region:
	$(CC) $(CFLAGS) ${COMMON_SRC} ${REGION_SRC} -D MEMMGR_TEST -o startRegion
	
memTest:
	$(CC) $(CFLAGS) ${COMMON_SRC} -D MEMMGR_UNIT_TEST -o memTest
