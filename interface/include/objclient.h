
#include <errno.h>
#include <string.h>

#include <string>
#include <vector>
#include <map>
#include <stdexcept>
#include <ctime>
#include <sstream>

#include "objclient.h"

extern "C"
{
#include "interface.h"
}

namespace objclient {

class connection
{
public:
	explicit connection(const string &ip, int port)
	{
		mt_cli_context_crt();
		if(!mt_cli_connection(ip.c_str(), port, &clientConn))
		{
			cout << "error when connecting to maxtable." << endl;
			mt_cli_context_destroy();
		}
	}

	~connection()
	{
		mt_cli_exit(clientConn);
		mt_cli_context_destroy();
	}

	conn * getConnection()
	{
		return clientConn;
	}
private:
	conn * clientConn;
};

template<typename T>
class table {
public:
        
	explicit table(connection * client_conn, const string &table_name)
	{
		tableName = table_name;
		clientConn = client_conn->getConnection();
		
		if(clientConn)
		{
			//schema string is somethink like "id varchar, value1 int, value2 varchar"
			string createStr = T::genCreateSql(tableName);

			char resp[256];
			int len;
			MT_CLI_EXEC_CONTEX exec_ctx;

			//if(!mt_cli_execute(clientConn, createStr.c_str(), resp, &len))//to fix me, need to handle case table existed
			if(!mt_cli_open_execute(clientConn, createStr.c_str(), &exec_ctx)) 
			{
				cout << "error when create table: " << resp << endl;
				return
			}
			mt_cli_close_execute(&exec_ctx); 
		}
		else
		{
			cout << "no connection to maxtable." << endl;
			return
		}		
	}

	~table() 
	{
	}

	bool insertRow(const T &obj) 
	{
		string insertStr = obj.genInsertSql(tableName);

		char resp[256];
		int len;
		MT_CLI_EXEC_CONTEX exec_ctx;

		//if(!mt_cli_execute(clientConn, insertStr.c_str(), resp, &len))
		if(!mt_cli_open_execute(clientConn, insertStr.c_str(), &exec_ctx)) 
		{
			cout << "error when insert table: " << resp << endl;
			return false;
		}
		mt_cli_close_execute(&exec_ctx); 
		return true;
	}

	
	bool insert(const T &obj) 
	{
		if (clientConn) //to fix me, need to check if table existed, not if connection existed.
		{
			return insertRow(obj);
		} 
		else 
		{
			cout << "no connection to maxtable." << endl;
			return false;
		}
	}

	#define rowSize 1024
	bool selectRow(const string &key, const T &out) 
	{
		ostringstream oss;
		oss << "select " << tableName << "(" << key << ")";
		string selectStr = oss.str();

		char *resp;//[rowSize];
		int len;
		MT_CLI_EXEC_CONTEX exec_ctx;

		//if(!mt_cli_execute(clientConn, selectStr.c_str(), resp, &len))
		if(!mt_cli_open_execute(clientConn, selectStr.c_str(), &exec_ctx)) 
		{
			cout << "error when select table: " << resp << endl;
			return false;
		}
		resp = mt_cli_get_row(&exec_ctx, 0);//0 is right?...
		//string respStr(resp);
		out.getSelectValue(&exec_ctx, resp);
		mt_cli_close_execute(&exec_ctx); 
		return true;
	}

	
	bool select(const string &key, const T &out) 
	{
		if (clientConn) //to fix me, need to check if table existed, not if connection existed.
		{
			return selectRow(key, out);
		} 
		else 
		{
			cout << "no connection to maxtable." << endl;
			return false;
		}
	}

		
private:
	string tableName;
	conn * clientConn;
};
}

