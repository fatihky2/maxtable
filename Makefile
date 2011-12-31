CC		= gcc -g -Wall
CPP		= g++ -g -Wall
#CC		= gcc -Wall
#CPP		= g++ -Wall
AR		= ar cr
CFLAGS		= -I./common/include/ -I./client/include -I./master/include -I./ranger/include -I./interface/include -I./service/include -lpthread
KFSFLAG		= -I${MT_DFS_INCLUDE_PATH} -L${MT_DFS_CLI_LIB_PATH} -lkfsClient -lboost_regex
COMMON_SRC	= common/src/*.c
REGION_SRC	= ranger/src/*.c
MASTER_SRC	= master/src/*.c
CLI_SRC		= client/src/*.c
INTERFACE_SRC	= interface/src/*.c
SERVICE_SRC	= service/src/*.cpp

LIB_SRCS	= interface/src/*.c common/src/*.c

LIB_SRCS_C	= $(wildcard ${LIB_SRCS})
LIB_OBJS_C	= $(patsubst %.c,%.o,$(LIB_SRCS_C))
LIB_SRCS_CPP	= $(wildcard ${SERVICE_SRC})
LIB_OBJS_CPP	= $(patsubst %.cpp,%.o,$(LIB_SRCS_CPP))

ifeq (${MT_SCHEME}, KEY_VALUE)
MT_KEY_VALUE	= -D MT_KEY_VALUE
else
MT_KEY_VALUE	=
endif

ifeq (${MT_BACKEND}, KFS)

all: kfsfacer_o kfsfacer_a client master ranger kfsmain memTest sample client_lib clients_lib

else ifeq (${MT_BACKEND}, LOCAL)

all: client master ranger memTest sample client_lib clients_lib

endif

%.o : %.c
	$(CC) -o $@ -c $< $(CFLAGS) -fPIC
%.o : %.cpp
	$(CPP) -o $@ -c $< $(CFLAGS)


ifeq (${MT_BACKEND}, KFS)

kfsfacer_o: dfsfacer/src/kfsfacer.cc
	$(CPP) -I./dfsfacer/include/ $(KFSFLAG) -c dfsfacer/src/kfsfacer.cc

kfsfacer_a: kfsfacer.o
	$(AR) kfsfacer.a kfsfacer.o 
        
master: ${COMMON_SRC} ${MASTER_SRC}
	$(CC) $(CFLAGS) $(KFSFLAG) ${COMMON_SRC} ${MASTER_SRC} -L. kfsfacer.a -lstdc++ -D MEMMGR_TEST -D MAXTABLE_BENCH_TEST -D MT_KFS_BACKEND ${MT_KEY_VALUE} -o startMaster

ranger: ${COMMON_SRC} ${REGION_SRC}
	$(CC) $(CFLAGS) $(KFSFLAG) ${COMMON_SRC} ${REGION_SRC} -L. kfsfacer.a -lstdc++ -D MEMMGR_TEST -D MAXTABLE_BENCH_TEST -D MT_KFS_BACKEND -D MT_ASYNC_IO ${MT_KEY_VALUE} -o startRanger

client: ${COMMON_SRC} ${CLI_SRC}
	$(CC) $(CFLAGS) $(KFSFLAG) ${COMMON_SRC} ${CLI_SRC} -L. kfsfacer.a -lstdc++ -D MEMMGR_TEST -D MAXTABLE_UNIT_TEST -D MT_KFS_BACKEND ${MT_KEY_VALUE} -o startClient
	$(CC) $(CFLAGS) $(KFSFLAG) ${COMMON_SRC} ${CLI_SRC} -L. kfsfacer.a -lstdc++ -D MEMMGR_TEST -D MT_KFS_BACKEND ${MT_KEY_VALUE} -o imql        
  
sample: ${COMMON_SRC} ${INTERFACE_SRC}
	$(CC) $(CFLAGS) $(KFSFLAG) ${COMMON_SRC} ${INTERFACE_SRC} -L. kfsfacer.a -lstdc++ -D DEBUG -D MAXTABLE_SAMPLE_TEST -D MT_KFS_BACKEND ${MT_KEY_VALUE} -o sample

kfsmain:
	$(CPP) -I./dfsfacer/include/ $(KFSFLAG) dfsfacer/src/kfsfacer.cc -D MEMMGR_KFS_TEST -o kfsmain

else ifeq (${MT_BACKEND}, LOCAL)

master: ${COMMON_SRC} ${MASTER_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${MASTER_SRC} -D DEBUG -D MEMMGR_TEST -D MAXTABLE_BENCH_TEST ${MT_KEY_VALUE} -o startMaster

ranger: ${COMMON_SRC} ${REGION_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${REGION_SRC} -D DEBUG -D MEMMGR_TEST -D MAXTABLE_BENCH_TEST -D MT_ASYNC_IO ${MT_KEY_VALUE} -o startRanger
	
client: ${COMMON_SRC} ${CLI_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${CLI_SRC} -D DEBUG -D MEMMGR_TEST -D MAXTABLE_UNIT_TEST -o startClient
	$(CC) $(CFLAGS) ${COMMON_SRC} ${CLI_SRC} -D MEMMGR_TEST -o imql
	
sample: ${COMMON_SRC} ${INTERFACE_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${INTERFACE_SRC} -D MAXTABLE_SAMPLE_TEST ${MT_KEY_VALUE} -o sample
	
endif
	
memTest: ${COMMON_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} -D MEMMGR_UNIT_TEST -o memTest

benchmark: ${COMMON_SRC} ${INTERFACE_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${INTERFACE_SRC} -D MAXTABLE_BENCH_TEST -o benchmark 
	
   
service: ${LIB_OBJS_C} ${LIB_OBJS_CPP}
	$(AR) libmtService.a ${LIB_OBJS_C} ${LIB_OBJS_CPP}

service_sample: service/src/sample.cpp libmtService.a
	$(CPP) $(CFLAGS) service/src/sample.cpp libmtService.a -o service_sample

client_lib: ${LIB_OBJS_C}
	$(CC) -shared -fPIC -o libmtClient.so ${LIB_OBJS_C}

clients_lib: ${LIB_OBJS_C}
	$(AR) libmtcli.a ${LIB_OBJS_C}

clean: 
	rm -rf startClient startMaster startRanger imql kfsmain memTest benchmark sample libmtService.a libmtcli.a ${LIB_OBJS_C} ${LIB_OBJS_CPP} libmtClient.so kfsfacer.o kfsfacer.a
	rm -rf ./table ./index ./rg_server ./rg_table ./meta_table ./rgbackup ./rglog
