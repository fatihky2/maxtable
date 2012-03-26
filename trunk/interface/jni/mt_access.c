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

	const char *meta_server_host = (*jenv)->GetStringUTFChars(jenv, jmeta_server_host, 0);

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
	CONN 	*connection = (conn *) jconn;

	mt_cli_close_connection(connection);
}
JNIEXPORT jlong JNICALL Java_org_maxtable_client_MtAccess_openExecute
  (JNIEnv * jenv, jclass jcls, jlong jconn, jstring jcmd)
{
	MT_CLI_EXEC_CONTEX *exec_ctx = (MT_CLI_EXEC_CONTEX *)malloc(sizeof(MT_CLI_EXEC_CONTEX));
	memset(exec_ctx, 0, sizeof(MT_CLI_EXEC_CONTEX));
	
	CONN	*connection = (conn *) jconn;

	const char *cmd = (*jenv)->GetStringUTFChars(jenv, jcmd, 0);
	
	mt_cli_open_execute(connection, cmd, exec_ctx);

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
