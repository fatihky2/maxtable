#include <iostream>

#include "service.h"

int ign;
namespace transoft_network_service
{
    switch_service::switch_service()
    {
    }

    switch_service::switch_service(char * service_name, char * service_ip, int service_port)
    :service_id(service_name)
    {
        if(mt_cli_open_connection(service_ip, service_port, &service_connection))
        {
            char resp[buffer_size], cmd[buffer_size];
    			
            memset(cmd, 0, 256);
            sprintf(cmd, "create table %s(id varchar, desc varchar)", service_name);
  //          if(!mt_cli_exec_crtseldel(service_connection, cmd, resp, &ign))
  //              cout << "error when building switch service target." << endl; 
        }
    }

    switch_service::~switch_service()
    {
        mt_cli_close_connection(service_connection);
    }

    bool switch_service::set_switch_service(const switch_inst & inst)
    {
        char resp[buffer_size], cmd[buffer_size];
        memset(resp, 0, 256);
        memset(cmd, 0, 256);
        sprintf(cmd, "insert into %s(%s, %s)", service_id.c_str(), inst.id.c_str(), inst.desc.c_str());
/*
        if(!mt_cli_exec_crtseldel(service_connection, cmd, resp, &ign))
        {
            cout << "error when setting switch service target." << endl;
            return false;
        }
*/
        cout << "set result: " << resp << endl;
        return true;
        
    }

    bool switch_service::get_switch_service(const string & id, switch_inst & inst)
    {
        char resp[buffer_size], cmd[buffer_size];
        memset(resp, 0, 256);
        memset(cmd, 0, 256);
        sprintf(cmd, "select %s(%s)", service_id.c_str(), id.c_str());
  //      if(!mt_cli_exec_crtseldel(service_connection, cmd, resp, &ign))
   //     {
  //          cout << "error when getting switch service target." << endl;
 //           return false;
 //       }
        inst.id = id;
        inst.desc = string(resp).substr(strlen(id.c_str()));
        cout << "get result: " << inst.id << "+" << inst.desc << endl;
        return true;
        
    }
}
