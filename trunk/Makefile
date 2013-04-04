#CC		= gcc -g -Wall
#CPP		= g++ -g -Wall
CC		= gcc -Wall
CPP		= g++ -Wall
AR		= ar cr
CFLAGS		= -I./common/include/ -I./client/include -I./master/include -I./ranger/include -I./interface/include -I./service/include -I/usr/lib/jvm/java-6-openjdk/include/ -D_GNU_SOURCE -D_XOPEN_SOURCE=500
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

JNI_DIRS        = interface/src/*.c common/src/*.c interface/jni/*.c
JNI_SRCS	= $(wildcard ${JNI_DIRS})
JNI_OBJS	= $(patsubst %.c,%.o,$(JNI_SRCS))

ifeq (${MT_SCHEME}, KEY_VALUE)
MT_KEY_VALUE	= -D MT_KEY_VALUE
else
MT_KEY_VALUE	=
endif

ifeq (${MT_BACKEND}, KFS)

all: kfsfacer_o kfsfacer_a client master ranger kfsmain memTest sample client_lib clients_lib jni_lib java_api mapred_sample

else ifeq (${MT_BACKEND}, LOCAL)

all: client master ranger memTest sample client_lib clients_lib mapred_sample jni_lib java_api

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
	$(CC) $(CFLAGS) $(KFSFLAG) ${COMMON_SRC} ${REGION_SRC} -L. kfsfacer.a -lstdc++ -D MEMMGR_TEST -D RANGER -D MAXTABLE_BENCH_TEST -D MT_KFS_BACKEND -D MT_ASYNC_IO ${MT_KEY_VALUE} -o startRanger

client: ${COMMON_SRC} ${CLI_SRC}
	$(CC) $(CFLAGS) $(KFSFLAG) ${COMMON_SRC} ${CLI_SRC} -L. kfsfacer.a -lstdc++ -D MEMMGR_TEST -D MAXTABLE_UNIT_TEST -D MT_KFS_BACKEND ${MT_KEY_VALUE} -o startClient
	$(CC) $(CFLAGS) $(KFSFLAG) ${COMMON_SRC} ${CLI_SRC} -L. kfsfacer.a -lstdc++ -D MEMMGR_TEST -D MT_KFS_BACKEND ${MT_KEY_VALUE} -o imql        
  
sample: ${COMMON_SRC} ${INTERFACE_SRC}
	$(CC) $(CFLAGS) $(KFSFLAG) ${COMMON_SRC} ${INTERFACE_SRC} -L. kfsfacer.a -lstdc++ -D MAXTABLE_SAMPLE_TEST -D MT_KFS_BACKEND ${MT_KEY_VALUE} -o sample

kfsmain:
	$(CPP) -I./dfsfacer/include/ $(KFSFLAG) dfsfacer/src/kfsfacer.cc -D MEMMGR_KFS_TEST -o kfsmain

else ifeq (${MT_BACKEND}, LOCAL)

master: ${COMMON_SRC} ${MASTER_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${MASTER_SRC} -D MEMMGR_TEST -D MAXTABLE_BENCH_TEST ${MT_KEY_VALUE} -o startMaster -lpthread

ranger: ${COMMON_SRC} ${REGION_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${REGION_SRC} -D MEMMGR_TEST -D MAXTABLE_BENCH_TEST -D RANGER -D MT_ASYNC_IO ${MT_KEY_VALUE} -o startRanger -lpthread
	
client: ${COMMON_SRC} ${CLI_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${CLI_SRC} -D MEMMGR_TEST -D MAXTABLE_UNIT_TEST -o startClient -lpthread
	$(CC) $(CFLAGS) ${COMMON_SRC} ${CLI_SRC} -D MEMMGR_TEST -o imql -lpthread
	
sample: ${COMMON_SRC} ${INTERFACE_SRC}
	$(CC) $(CFLAGS) ${COMMON_SRC} ${INTERFACE_SRC} -D MAXTABLE_SAMPLE_TEST ${MT_KEY_VALUE} -o sample -lpthread
	
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
	$(CC) -shared -fPIC -o libmtClient.so ${LIB_OBJS_C} -lpthread

clients_lib: ${LIB_OBJS_C}
	$(AR) libmtcli.a ${LIB_OBJS_C}

jni_lib: ${JNI_OBJS}
	$(CC) -shared -fPIC -o libmt_access.so ${JNI_OBJS}

java_api:
	ant -f build.xml client_jar
	ant -f build.xml mapreduce_jar
	ant -f build.xml mapreduce_sample
	ant -f build.xml examples

mapred_sample:
	$(CC) $(CFLAGS) mapreduce_test.c libmtcli.a -o mapred_sample

clean: 
	rm -rf startClient startMaster startRanger mapred_sample imql kfsmain memTest benchmark sample libmtService.a libmtcli.a ${LIB_OBJS_C} ${LIB_OBJS_CPP} ${JNI_OBJS} libmtClient.so kfsfacer.o kfsfacer.a libmt_access.so
	rm -rf ./table ./index ./rg_server ./rg_table ./meta_table ./rgbackup ./rglog ./rg_state ./meta_tablet_backup ./rg_state
	ant -f build.xml clean
