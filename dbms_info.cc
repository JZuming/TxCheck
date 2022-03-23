#include "dbms_info.hh"

dbms_info::dbms_info(map<string,string>& options)
{    
    if (options.count("sqlite")) {
        #ifdef HAVE_LIBSQLITE3

        dbms_name = "sqlite";
        serializable = true;
        test_port = 0; // no port
        test_db = options["sqlite"];
        can_trigger_error_in_txn = true;
        
        #else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
        throw runtime_error("Does not support SQLite");
        #endif
    } else if (options.count("tidb-db") && options.count("tidb-port")) {
        #ifdef HAVE_LIBMYSQLCLIENT
        
        dbms_name = "tidb";
        serializable = false;
        test_port = stoi(options["tidb-port"]);
        test_db = options["tidb-db"];
        can_trigger_error_in_txn = true;
        
        #else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MySQL support." << endl;
        throw runtime_error("Does not support TiDB");
        #endif
    } else if (options.count("mysql-db") && options.count("mysql-port")) {
        #ifdef HAVE_LIBMYSQLCLIENT
        
        dbms_name = "mysql";
        serializable = true;
        test_port = stoi(options["mysql-port"]);
        test_db = options["mysql-db"];
        can_trigger_error_in_txn = true;
        
        #else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MySQL support." << endl;
        throw runtime_error("Does not support MySQL");
        #endif
    } else if (options.count("cockroach-db") && options.count("cockroach-port")) {
        dbms_name = "cockroach";
        serializable = true;
        test_port = stoi(options["cockroach-port"]);
        test_db = options["cockroach-db"];
        can_trigger_error_in_txn = false;
    } 
    else {
        cerr << "Sorry,  you should specify a dbms and its database, or your dbms is not supported" << endl;
        throw runtime_error("Does not define target dbms and db");
    }

    if (options.count("output-or-affect-num")) 
        ouput_or_affect_num = stoi(options["output-or-affect-num"]);
    else 
        ouput_or_affect_num = 0;

    return;
}