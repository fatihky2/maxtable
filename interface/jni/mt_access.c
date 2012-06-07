#include "org_maxtable_client_MtAccess.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "rpcfmt.h"
#include "interface.h"

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_createContext
  (JNIEnv * jenv, jclass jcls)
{
	//printf("hello world\n");
	mt_cli_crt_context();
}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_destroyContext
  (JNIEnv * jenv, jclass jcls)
{
	mt_cli_destroy_context();
}

JNIEXPORT jlong JNICALL Java_org_maxtable_client_MtAccess_openConnection
  (JNIEnv * jenv, jclass jcls, jstring jmeta_server_host, jint jmeta_server_port)
{
	CONN 	*connection;

	char *meta_server_host = (char *)(*jenv)->GetStringUTFChars(jenv, jmeta_server_host, 0);

	if(!mt_cli_open_connection(meta_server_host, jmeta_server_port, &connection))
	{		
		(*jenv)->ReleaseStringUTFChars(jenv, jmeta_server_host, meta_server_host);
		return 0;
	}

	(*jenv)->ReleaseStringUTFChars(jenv, jmeta_server_host, meta_server_host);
	return (jlong)(connection);
}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_closeConnection
  (JNIEnv * jenv, jclass jcls, jlong jconn)
{
	CONN 	*connection = (CONN *) jconn;

	mt_cli_close_connection(connection);
}
JNIEXPORT jlong JNICALL Java_org_maxtable_client_MtAccess_openExecute
  (JNIEnv * jenv, jclass jcls, jlong jconn, jstring jcmd)
{
	MT_CLI_EXEC_CONTEX *exec_ctx = (MT_CLI_EXEC_CONTEX *)malloc(sizeof(MT_CLI_EXEC_CONTEX));
	memset(exec_ctx, 0, sizeof(MT_CLI_EXEC_CONTEX));
	
	CONN	*connection = (CONN *) jconn;

	char *cmd = (char *)(*jenv)->GetStringUTFChars(jenv, jcmd, 0);
	
	mt_cli_open_execute(connection, cmd, strlen(cmd),exec_ctx);

	(*jenv)->ReleaseStringUTFChars(jenv, jcmd, cmd);
	return (jlong)(exec_ctx);
}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_closeExecute
  (JNIEnv * jenv, jclass jcls, jlong jexe_ptr)
{
	MT_CLI_EXEC_CONTEX *exec_ctx = (MT_CLI_EXEC_CONTEX *)jexe_ptr;
	
	mt_cli_close_execute(exec_ctx);	

	free(exec_ctx);
}

JNIEXPORT jlong JNICALL Java_org_maxtable_client_MtAccess_getNextrow
  (JNIEnv * jenv, jclass jcls, jlong jexe_ptr)
{
	MT_CLI_EXEC_CONTEX *exec_ctx = (MT_CLI_EXEC_CONTEX *)jexe_ptr;
	
	int	rlen = 0;
	char	*rp = NULL;

	rp = mt_cli_get_nextrow(exec_ctx, &rlen);

	//char * ret_rp = (char *)malloc(rlen+1);
	//memcpy(ret_rp, rp, rlen);
	//ret_rp[rlen] = '\0';
	//printf("row: %d, len: %d\n", strlen(ret_rp), rlen);

	//jstring s = (*jenv)->NewStringUTF(jenv, ret_rp);
	return (jlong)(rp);
}

JNIEXPORT jint JNICALL Java_org_maxtable_client_MtAccess_colTypeFixed
  (JNIEnv * jenv, jclass jcls, jlong jexe_ptr, jint jcol_idx)
{
        MT_CLI_EXEC_CONTEX *exec_ctx = (MT_CLI_EXEC_CONTEX *)jexe_ptr;

        return mt_cli_coltype_fixed(exec_ctx, jcol_idx);
}

JNIEXPORT jstring JNICALL Java_org_maxtable_client_MtAccess_getVarColValue
  (JNIEnv * jenv, jclass jcls, jlong jexe_ptr, jlong jrow_buf, jint jcol_idx)
{
	MT_CLI_EXEC_CONTEX *exec_ctx = (MT_CLI_EXEC_CONTEX *)jexe_ptr;
	//const char *row_buf = (*jenv)->GetStringUTFChars(jenv, jrow_buf, 0);
	char *row_buf = (char *)jrow_buf;
	
	int	collen = 0;				
	char	*col;
		
	col = mt_cli_get_colvalue(exec_ctx, row_buf, jcol_idx, &collen);

	char * ret_rp = (char *)malloc(collen+1);
	memcpy(ret_rp, col, collen);
	ret_rp[collen] = '\0';
	//printf("col: %d, len: %d, val: %s\n", jcol_idx, collen, ret_rp);

	jstring s = (*jenv)->NewStringUTF(jenv, ret_rp);

	free(ret_rp);

	return s;
}
JNIEXPORT jint JNICALL Java_org_maxtable_client_MtAccess_getFixedColValue
  (JNIEnv * jenv, jclass jcls, jlong jexe_ptr, jlong jrow_buf, jint jcol_idx)
{
	MT_CLI_EXEC_CONTEX *exec_ctx = (MT_CLI_EXEC_CONTEX *)jexe_ptr;
        //const char *row_buf = (*jenv)->GetStringUTFChars(jenv, jrow_buf, 0);
        char *row_buf = (char *)jrow_buf;

        int     collen = 0;
        char    *col;

        col = mt_cli_get_colvalue(exec_ctx, row_buf, jcol_idx, &collen);

	int ret = *((int *)col);
		
	//printf("fix col: %d\n", ret);

	return ret;
}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_getSplits
  (JNIEnv * jenv, jclass jcls, jlong conn_ptr, jstring table_name, jobject obj)
{
	CONN    *connection = (CONN *) conn_ptr;

	char *tablename = (char *)(*jenv)->GetStringUTFChars(jenv, table_name, 0);

	MT_SPLIT * splits = NULL;
	int splitcount = 0;
	mt_mapred_get_splits(connection, &splits, &splitcount, tablename);

	printf("c_getsplits: total %d splits got\n", splitcount);

	int i;
	for(i = 0; i < splitcount; i++)
	{
		MT_SPLIT * current = splits + i;
		printf("c_getsplits: %s, %d, %s, %d\n", current->range_ip, current->range_port, current->tablet_name, current->meta_port);
	}

	(*jenv)->ReleaseStringUTFChars(jenv, table_name, tablename);

	printf("c_getsplits: step0\n");

	jclass    m_cls   = (*jenv)->GetObjectClass(jenv, obj);
	//(*jenv)->FindClass(jenv, "org/maxtable/client/cRet");

	//printf("c_getsplits: step0.1\n");

        //jmethodID m_mid   = (*jenv)->GetMethodID(jenv, m_cls,"<init>","()V");

	printf("c_getsplits: step1\n");

        jfieldID  m_fid_1 = (*jenv)->GetFieldID(jenv, m_cls,"ptr","J");
        jfieldID  m_fid_2 = (*jenv)->GetFieldID(jenv, m_cls,"length","I");

	printf("c_getsplits: step2\n");

        //jobject   m_obj   = (*jenv)->NewObject(jenv, m_cls,m_mid);

	printf("c_getsplits: step3\n");

        (*jenv)->SetLongField(jenv, obj,m_fid_1,(jlong)(splits));
        (*jenv)->SetIntField(jenv, obj,m_fid_2,splitcount);

	printf("c_getsplits: step4\n");

	//return m_obj;
}

JNIEXPORT jobject JNICALL Java_org_maxtable_client_MtAccess_getSplit
  (JNIEnv * env, jclass jcls, jlong split_ptr, jint split_num)
{
	MT_SPLIT * split = (MT_SPLIT *) split_ptr;
	split += split_num;

	
	jclass    m_cls   = (*env)->FindClass(env, "org/maxtable/client/MtAccess$Split");


        jmethodID m_mid   = (*env)->GetMethodID(env, m_cls,"<init>","()V");


        jfieldID  m_fid_1 = (*env)->GetFieldID(env, m_cls,"tableName","Ljava/lang/String;");
        jfieldID  m_fid_2 = (*env)->GetFieldID(env, m_cls,"tabletName","Ljava/lang/String;");
        jfieldID  m_fid_3 = (*env)->GetFieldID(env, m_cls,"rangeIp","Ljava/lang/String;");
        jfieldID  m_fid_4 = (*env)->GetFieldID(env, m_cls,"rangePort","I");
	jfieldID  m_fid_5 = (*env)->GetFieldID(env, m_cls,"metaIp","Ljava/lang/String;");
	jfieldID  m_fid_6 = (*env)->GetFieldID(env, m_cls,"metaPort","I");

        jobject   m_obj   = (*env)->NewObject(env, m_cls,m_mid);

        (*env)->SetObjectField(env, m_obj,m_fid_1,(*env)->NewStringUTF(env, split->table_name));
        (*env)->SetObjectField(env, m_obj,m_fid_2,(*env)->NewStringUTF(env, split->tablet_name));
        (*env)->SetObjectField(env, m_obj,m_fid_3,(*env)->NewStringUTF(env, split->range_ip));
	(*env)->SetIntField(env, m_obj,m_fid_4,split->range_port);
	(*env)->SetObjectField(env, m_obj,m_fid_5,(*env)->NewStringUTF(env, split->meta_ip));
	printf("c_getsplit: %d\n", split->meta_port);
        (*env)->SetIntField(env, m_obj,m_fid_6,split->meta_port);

	return m_obj;
}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_freeSplits
  (JNIEnv * jenv, jclass jcls, jlong split_ptr)
{
	MT_SPLIT * splits = (MT_SPLIT *) split_ptr;
	
	mt_mapred_free_splits(splits);
}

JNIEXPORT jlong JNICALL Java_org_maxtable_client_MtAccess_createReader
  (JNIEnv * env, jclass jcls, jobject jsplit)
{
	printf("c_createreader:step1\n");

	MT_SPLIT * split = (MT_SPLIT *)malloc(sizeof(MT_SPLIT));
	memset(split, 0, sizeof(MT_SPLIT));
	 
	jclass   m_cls   =   (*env)-> GetObjectClass(env,  jsplit);
	jfieldID  m_fid_1 = (*env)->GetFieldID(env, m_cls,"tableName","Ljava/lang/String;");
        jfieldID  m_fid_2 = (*env)->GetFieldID(env, m_cls,"tabletName","Ljava/lang/String;");
        jfieldID  m_fid_3 = (*env)->GetFieldID(env, m_cls,"rangeIp","Ljava/lang/String;");
        jfieldID  m_fid_4 = (*env)->GetFieldID(env, m_cls,"rangePort","I");
        jfieldID  m_fid_5 = (*env)->GetFieldID(env, m_cls,"metaIp","Ljava/lang/String;");
        jfieldID  m_fid_6 = (*env)->GetFieldID(env, m_cls,"metaPort","I");

	jstring jstr;
	char * cstr;
	int strlen;

 	jstr = (*env)->GetObjectField(env, jsplit, m_fid_1);
	cstr = (char *)(*env)->GetStringUTFChars(env, jstr, 0);
	strlen = (*env)->GetStringLength(env, jstr);
	memcpy(split->table_name, cstr, strlen);
	(*env)->ReleaseStringUTFChars(env, jstr, cstr);

	jstr = (*env)->GetObjectField(env, jsplit, m_fid_2);
        cstr = (char *)(*env)->GetStringUTFChars(env, jstr, 0);
        strlen = (*env)->GetStringLength(env, jstr);
        memcpy(split->tablet_name, cstr, strlen);
	(*env)->ReleaseStringUTFChars(env, jstr, cstr);

	jstr = (*env)->GetObjectField(env, jsplit, m_fid_3);
        cstr = (char *)(*env)->GetStringUTFChars(env, jstr, 0);
        strlen = (*env)->GetStringLength(env, jstr);
        memcpy(split->range_ip, cstr, strlen);
        (*env)->ReleaseStringUTFChars(env, jstr, cstr);

	split->range_port = (*env)->GetIntField(env,jsplit,m_fid_4);

	jstr = (*env)->GetObjectField(env, jsplit, m_fid_5);
        cstr = (char *)(*env)->GetStringUTFChars(env, jstr, 0);
        strlen = (*env)->GetStringLength(env, jstr);
        memcpy(split->meta_ip, cstr, strlen);
        (*env)->ReleaseStringUTFChars(env, jstr, cstr);

        split->meta_port = (*env)->GetIntField(env,jsplit,m_fid_6);

	MT_READER * mtreader = NULL;

	mt_mapred_create_reader(&mtreader, split);
	
	printf("c_createreader: %d\n", mtreader->data_connection.rg_server_port);

	return (jlong)(mtreader);

}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_freeReader
  (JNIEnv * jenv, jclass jcls, jlong reader_ptr)
{
	MT_READER * reader = (MT_READER *) reader_ptr;

        mt_mapred_free_reader(reader);
}


JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_getNextKeyValue
  (JNIEnv * jenv, jclass jcls, jlong reader_ptr, jobject obj)
{
	MT_READER * reader = (MT_READER *) reader_ptr;

	int rp_len;
	char * rp;

	jclass    m_cls   = (*jenv)->GetObjectClass(jenv, obj);//(*jenv)->FindClass(jenv, "cRet");

        //jmethodID m_mid   = (*jenv)->GetMethodID(jenv, m_cls,"<init>","()V");

        jfieldID  m_fid_1 = (*jenv)->GetFieldID(jenv, m_cls,"ptr","J");
        jfieldID  m_fid_2 = (*jenv)->GetFieldID(jenv, m_cls,"length","I");

        //jobject   m_obj   = (*jenv)->NewObject(jenv, m_cls,m_mid);

	//printf("c_getnext: %d\n", reader->data_connection.rg_server_port);

	if(rp = mt_mapred_get_nextvalue(reader, &rp_len))
	{
		(*jenv)->SetLongField(jenv, obj,m_fid_1,(jlong)(rp));
        	(*jenv)->SetIntField(jenv, obj,m_fid_2,rp_len);
		//printf("c_getnext: get row %d: %s with size %d\n", reader->block_cache->cache_index, rp, rp_len);
	}
	else
	{
		(*jenv)->SetLongField(jenv, obj,m_fid_1,(jlong)(0));
                (*jenv)->SetIntField(jenv, obj,m_fid_2,0);
		//printf("c_getnext: reader is over\n");
	}


	//return m_obj;
}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_getKey
  (JNIEnv * jenv, jclass jcls, jlong reader_ptr, jlong row_ptr, jobject obj)
{
	MT_READER * reader = (MT_READER *) reader_ptr;
	char * rp = (char *) row_ptr;

	int value_len;
	char * value = mt_mapred_get_currentvalue(reader, rp, 0, &value_len);

	jclass    m_cls   = (*jenv)->GetObjectClass(jenv, obj);//(*jenv)->FindClass(jenv, "cRet");

        //jmethodID m_mid   = (*jenv)->GetMethodID(jenv, m_cls,"<init>","()V");

        jfieldID  m_fid_1 = (*jenv)->GetFieldID(jenv, m_cls,"ptr","J");
        jfieldID  m_fid_2 = (*jenv)->GetFieldID(jenv, m_cls,"length","I");

        //jobject   m_obj   = (*jenv)->NewObject(jenv, m_cls,m_mid);

        (*jenv)->SetLongField(jenv, obj,m_fid_1,(jlong)(value));
        (*jenv)->SetIntField(jenv, obj,m_fid_2,value_len);

	printf("c_getkey: get value: %s with size %d\n", value, value_len);

        //return m_obj;
}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_getValue
  (JNIEnv * jenv, jclass jcls, jlong reader_ptr, jlong row_ptr, jint rp_len, jobject obj)
{
        MT_READER * reader = (MT_READER *) reader_ptr;
        char * rp = (char *) row_ptr;

        int value_len;
        char * value = mt_mapred_reorg_value(reader, rp, rp_len, &value_len);

        jclass    m_cls   = (*jenv)->GetObjectClass(jenv, obj);//(*jenv)->FindClass(jenv, "cRet");

        //jmethodID m_mid   = (*jenv)->GetMethodID(jenv, m_cls,"<init>","()V");

        jfieldID  m_fid_1 = (*jenv)->GetFieldID(jenv, m_cls,"ptr","J");
        jfieldID  m_fid_2 = (*jenv)->GetFieldID(jenv, m_cls,"length","I");

        //jobject   m_obj   = (*jenv)->NewObject(jenv, m_cls,m_mid);

        (*jenv)->SetLongField(jenv, obj,m_fid_1,(jlong)(value));
        (*jenv)->SetIntField(jenv, obj,m_fid_2,value_len);

        printf("c_getvalue: get value: %s with size %d\n", value, value_len);

        //return m_obj;
}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_freeValue
  (JNIEnv * jenv, jclass jcls, jlong value_ptr)
{
	char * value = (char *) value_ptr;
	free(value);
}

JNIEXPORT void JNICALL Java_org_maxtable_client_MtAccess_arrayCopy
  (JNIEnv * jenv, jclass jcls, jbyteArray value_array, jlong value_ptr, jint value_length)
{
	char * value = (char *) value_ptr;

	jbyte * arrayBody = (*jenv)->GetByteArrayElements(jenv, value_array, 0);
        char * temp = (char *)arrayBody;

	memcpy(temp, value, value_length);

	(*jenv)->ReleaseByteArrayElements(jenv, value_array, arrayBody, 0);	
}
