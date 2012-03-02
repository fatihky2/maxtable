#ifndef	__SERVICE_H
#define __SERVICE_H
#include <iostream>
#include <string>
using namespace std;

extern "C"
{
#include "interface.h"
}


namespace transoft_network_service
{
    #define buffer_size 256
    
    typedef struct _switch_inst
    {
        string id;
        string desc;
        
    }switch_inst;
    
    class switch_service
    {
    private:
        CONN * service_connection;
        string service_id;
    public:
        switch_service();
        switch_service(char * service_name, char *service_ip = "127.0.0.1", int service_port = 1959);
        virtual ~switch_service();
        bool set_switch_service(const switch_inst &inst);
        bool get_switch_service(const string &id, switch_inst &inst);
    };
}


#endif

