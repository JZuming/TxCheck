#ifndef OCEANBASE_HH
#define OCEANBASE_HH

extern "C"  {
#include <mysql.h>
#include <unistd.h>
}

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"

#include <sys/time.h> // for gettimeofday
#include <iostream>
#include <set>

#define MYSQL_STMT_BLOCK_MS 100

using namespace std;

struct oceanbase_connection {
    MYSQL mysql;
    string test_db;
    unsigned int test_port;
    oceanbase_connection(string db, unsigned int port);
    ~oceanbase_connection();
};

struct schema_oceanbase : schema, oceanbase_connection {
    schema_oceanbase(string db, unsigned int port);
    virtual void update_schema();
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_oceanbase : dut_base, oceanbase_connection {
    virtual void test(const string &stmt, vector<vector<string>>* output = NULL, int* affected_row_num = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);
    static int save_backup_file(string path);
    
    virtual string commit_stmt();
    virtual string abort_stmt();
    virtual string begin_stmt();

    static pid_t fork_db_server();
    
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    dut_oceanbase(string db, unsigned int port);

    void block_test(const std::string &stmt, std::vector<std::string>* output = NULL, int* affected_row_num = NULL);
    bool check_whether_block();
    bool has_sent_sql;
    int query_status;
    string sent_sql;
    bool txn_abort;
    unsigned long thread_id;
};

#endif