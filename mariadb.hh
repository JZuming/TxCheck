#ifndef MARIADB_HH
#define MARIADB_HH

extern "C"  {
#include <mysql/mysql.h>
}

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"

#include <sys/time.h> // for gettimeofday

#define MYSQL_STMT_BLOCK_MS 100

struct mariadb_connection {
    MYSQL mysql;
    string test_db;
    // unsigned int test_port;
    mariadb_connection(string db, unsigned int port);
    ~mariadb_connection();
};

struct schema_mariadb : schema, mariadb_connection {
    schema_mariadb(string db, unsigned int port);
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_mariadb : dut_base, mariadb_connection {
    virtual void test(const string &stmt, vector<vector<string>>* output = NULL, int* affected_row_num = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);
    virtual int save_backup_file(string path);
    
    virtual string commit_stmt();
    virtual string abort_stmt();
    virtual string begin_stmt();

    static pid_t fork_db_server();
    
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    dut_mariadb(string db, unsigned int port);

    void block_test(const std::string &stmt, std::vector<std::string>* output = NULL, int* affected_row_num = NULL);
    bool check_whether_block();
    bool has_sent_sql;
    string sent_sql;
    bool txn_abort;
    unsigned long thread_id;
};

#endif