/// @file
/// @brief schema and dut classes for SQLite 3

#ifndef SQLITE_HH
#define SQLITE_HH

extern "C"  {
#include <sqlite3.h>
}

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"

struct sqlite_connection {
    sqlite3 *db;
    string db_file;
    char *zErrMsg = 0;
    int rc;
    void q(const char *query);
    sqlite_connection(std::string &conninfo);
    ~sqlite_connection();
};

struct schema_sqlite : schema, sqlite_connection {
    schema_sqlite(std::string &conninfo, bool no_catalog);
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_sqlite : dut_base, sqlite_connection {
    virtual void test(const std::string &stmt, vector<vector<string>>* output = NULL, int* affected_row_num = NULL);
    virtual void reset(void);
    
    virtual void backup(void);
    virtual void reset_to_backup(void);
    virtual int save_backup_file(string path);

    virtual string commit_stmt();
    virtual string abort_stmt();
    virtual string begin_stmt();
    
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    dut_sqlite(std::string &conninfo);
};

#endif