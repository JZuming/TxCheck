#ifndef MYSQL_HH
#define MYSQL_HH

extern "C"  {
#include <mysql/mysql.h>
}

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"

#include <sys/time.h> // for gettimeofday

#define MYSQL_STMT_BLOCK_MS 1000

struct mysql_connection {
    MYSQL mysql;
    string test_db;
    unsigned int test_port;
    mysql_connection(string db, unsigned int port);
    ~mysql_connection();
};

struct schema_mysql : schema, mysql_connection {
    schema_mysql(string db, unsigned int port);
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_mysql : dut_base, mysql_connection {
    virtual void test(const std::string &stmt, std::vector<std::string>* output = NULL, int* affected_row_num = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);
    virtual int save_backup_file(string path);
    
    virtual bool is_commit_abort_stmt(string& stmt);
    virtual bool is_begin_stmt(string& stmt);
    virtual void wrap_stmts_as_trans(vector<std::string> &stmt_vec, bool is_commit);

    static pid_t fork_db_server();
    
    virtual void trans_test(const std::vector<std::string> &stmt_vec
                          , std::vector<std::string>* exec_stmt_vec
                          , vector<vector<string>>* output = NULL
                          , int commit_or_not = 1);
    
    virtual void get_content(vector<string>& tables_name, map<string, vector<string>>& content);
    dut_mysql(string db, unsigned int port);

    void block_test(const std::string &stmt, std::vector<std::string>* output = NULL, int* affected_row_num = NULL);
    bool check_whether_block();
    bool has_sent_sql;
    string sent_sql;
    bool txn_abort;
    unsigned long thread_id;
};

#endif