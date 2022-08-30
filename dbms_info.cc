#include "dbms_info.hh"

dbms_info::dbms_info(map<string,string>& options)
{    
    if (false) {}
    #ifdef HAVE_LIBSQLITE3
    else if (options.count("sqlite")) {
        dbms_name = "sqlite";
        test_port = 0; // no port
        test_db = options["sqlite"];
        can_trigger_error_in_txn = true;
    }
    #endif 
    #ifdef HAVE_TIDB
    else if (options.count("tidb-db") && options.count("tidb-port")) {
        dbms_name = "tidb";
        test_port = stoi(options["tidb-port"]);
        test_db = options["tidb-db"];
        can_trigger_error_in_txn = true;
    }
    #endif
    #ifdef HAVE_MYSQL
    else if (options.count("mysql-db") && options.count("mysql-port")) {
        dbms_name = "mysql";
        test_port = stoi(options["mysql-port"]);
        test_db = options["mysql-db"];
        can_trigger_error_in_txn = true;
    }
    #endif
    #ifdef HAVE_MARIADB
    else if (options.count("mariadb-db") && options.count("mariadb-port")) {
        dbms_name = "mariadb";
        test_port = stoi(options["mariadb-port"]);
        test_db = options["mariadb-db"];
        can_trigger_error_in_txn = true;
    }
    #endif
    #ifdef HAVE_OCEANBASE
    else if (options.count("oceanbase-db") && options.count("oceanbase-port")) {
        dbms_name = "oceanbase";
        test_port = stoi(options["oceanbase-port"]);
        test_db = options["oceanbase-db"];
        can_trigger_error_in_txn = true;
    }
    #endif 
    #ifdef HAVE_MONETDB
    else if (options.count("monetdb-db") && options.count("monetdb-port")) {
        dbms_name = "monetdb";
        test_port = stoi(options["monetdb-port"]);
        test_db = options["monetdb-db"];
        can_trigger_error_in_txn = false;
    } 
    #endif
    else if (options.count("cockroach-db") && options.count("cockroach-port")) {
        dbms_name = "cockroach";
        test_port = stoi(options["cockroach-port"]);
        test_db = options["cockroach-db"];
        can_trigger_error_in_txn = false;
    } 
    else if (options.count("postgres-db") && options.count("postgres-port")) {
        dbms_name = "postgres";
        test_port = stoi(options["postgres-port"]);
        test_db = options["postgres-db"];
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