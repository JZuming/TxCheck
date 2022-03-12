#ifndef DBMS_INFO_HH
#define DBMS_INFO_HH

#include "config.h"
#include <string>
#include <map>
#include <iostream>

using namespace std;

struct dbms_info {
    string dbms_name;
    string test_db;
    int test_port;
    bool serializable;
    bool can_trigger_error_in_txn;

    dbms_info(map<string,string>& options);
};

#endif