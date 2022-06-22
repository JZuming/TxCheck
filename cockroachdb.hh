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

#define COCKROACH_STMT_BLOCK_MS 3000

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
    virtual void test(const std::string &stmt, vector<vector<string>>* output = NULL, int* affected_row_num = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);
    static int save_backup_file(string path);
    
    virtual string commit_stmt();
    virtual string abort_stmt();
    virtual string begin_stmt();

    static pid_t fork_db_server();
    
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    dut_cockroachdb(string db, unsigned int port);

    bool has_sent_sql;
    string sent_sql;
};

#endif