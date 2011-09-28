#include <iostream>
#include <string>
#include "service.h"
using namespace transoft_network_service;

int main()
{
    switch_service sample_service("sample_service", "127.0.0.1", 1959);
    switch_inst inst, inst_ret;
    inst.id = "sample_id";
    inst.desc = "just_one_sample";
    bool set_ret = sample_service.set_switch_service(inst);
    bool get_ret = sample_service.get_switch_service(string("sample_id"), inst_ret);
    cout << "result is in inst_ret, data is: " << inst_ret.id << "+" << inst_ret.desc << endl;
    return 0; 
}
