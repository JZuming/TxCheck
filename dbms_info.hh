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
    int ouput_or_affect_num;
    bool can_trigger_error_in_txn;

    dbms_info(map<string,string>& options);
    dbms_info() {
        dbms_name = "";
        test_db = "";
        test_port = 0;
        ouput_or_affect_num = 0;
        can_trigger_error_in_txn = false;
    };
    void operator=(dbms_info& target) {
        dbms_name = target.dbms_name;
        test_db = target.test_db;
        test_port = target.test_port;
        ouput_or_affect_num = target.ouput_or_affect_num;
        can_trigger_error_in_txn = target.can_trigger_error_in_txn;
    }
};

#endif