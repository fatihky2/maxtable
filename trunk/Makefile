CC		= gcc -g -Wall
CPP		= g++ -g -Wall
#CC		= gcc -Wall
#CPP		= g++ -Wall
AR		= ar cr
CFLAGS		= -I./common/include/ -I./client/include -I./master/include -I./region/include -I./interface/include -I./service/include -lpthread
COMMON_SRC	= common/src/*.c
REGION_SRC	= region/src/*.c
MASTER_SRC	= master/src/*.c
CLI_SRC		= client/src/*.c
INTERFACE_SRC	= interface/src/*.c
SERVICE_SRC = service/src/*.cpp

LIB_SRCS = interface/src/*.c common/src/*.c

LIB_SRCS_C = $(wildcard ${LIB_SRCS})
LIB_OBJS_C = $(patsubst %.c,%.o,$(LIB_SRCS_C))
LIB_SRCS_CPP = $(wildcard ${SERVICE_SRC})
LIB_OBJS_CPP = $(patsubst %.cpp,%.o,$(LIB_SRCS_CPP))

all: client master region sample

%.o : %.c
	$(CC) -o $@ -c $< $(CFLAGS)
%.o : %.cpp
	$(CPP) -o $@ -c $< $(CFLAGS)

client: ${COMMON_SRC} ${CLI_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${CLI_SRC} -D MEMMGR_TEST -D MAXTABLE_UNIT_TEST -o startClient
	$(CC) $(CFLAGS) ${COMMON_SRC} ${CLI_SRC} -D MEMMGR_TEST -o imql

master: ${COMMON_SRC} ${MASTER_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${MASTER_SRC} -D MEMMGR_TEST -o startMaster

region: ${COMMON_SRC} ${REGION_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${REGION_SRC} -D MEMMGR_TEST -o startRegion
	
memTest: ${COMMON_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} -D MEMMGR_UNIT_TEST -o memTest
	
sample: ${COMMON_SRC} ${INTERFACE_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${INTERFACE_SRC} -o sample
        
service: ${LIB_OBJS_C} ${LIB_OBJS_CPP}
	$(AR) libservice.a ${LIB_OBJS_C} ${LIB_OBJS_CPP}

service_sample: service/src/sample.cpp libservice.a
	$(CPP) $(CFLAGS) service/src/sample.cpp libservice.a -o service_sample

clean: 
	rm -rf startClient startMaster startRegion sample libservice.a ${LIB_OBJS_C} ${LIB_OBJS_CPP}
