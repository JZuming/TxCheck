/// @file
/// @brief schema and dut classes for SQLite 3

#ifndef COCKROACH_HH
#define COCKROACH_HH

extern "C"  {
#include <libpq-fe.h>
#include <unistd.h>
}

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"

#include <sys/time.h>
#include <set>

#define COCKROACH_STMT_BLOCK_MS 1500

struct cockroachdb_connection {
    PGconn *conn;
    string test_db;
    unsigned int test_port;
    cockroachdb_connection(string db, unsigned int port);
    ~cockroachdb_connection();
};

struct schema_cockroachdb : schema, cockroachdb_connection {
    schema_cockroachdb(string db, unsigned int port);
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_cockroachdb : dut_base, cockroachdb_connection {
    virtual void test(const std::string &stmt, std::vector<std::string>* output = NULL, int* affected_row_num = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);
    virtual int save_backup_file(string path);
    
    virtual bool is_commit_abort_stmt(string& stmt);
    virtual void wrap_stmts_as_trans(vector<std::string> &stmt_vec, bool is_commit);

    static pid_t fork_db_server();
    
    virtual void trans_test(const std::vector<std::string> &stmt_vec
                          , std::vector<std::string>* exec_stmt_vec
                          , vector<vector<string>>* output = NULL
                          , int commit_or_not = 1);
    
    virtual void get_content(vector<string>& tables_name, map<string, vector<string>>& content);
    dut_cockroachdb(string db, unsigned int port);

    bool has_sent_sql;
    string sent_sql;
};

#endif