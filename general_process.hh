#ifndef GENERAL_PROCESS_HH
#define GENERAL_PROCESS_HH

#include <string> // for string
#include <map> // for map
#include <memory> //for shared_ptr
#include <schema.hh> // for schema
#include <dut.hh> // for dut_base
#include <sys/stat.h> // for mkdir
#include <algorithm> // for sort

#include "config.h" // for PACKAGE_NAME

// for supported dbms ---
#ifdef HAVE_LIBSQLITE3
#include "sqlite.hh"
#endif

#ifdef HAVE_LIBMYSQLCLIENT
    #ifdef HAVE_TIDB
    #include "tidb.hh"
    #endif
    
    #ifdef HAVE_MYSQL
    #include "mysql.hh"
    #endif

    #ifdef HAVE_MARIADB
    #include "mariadb.hh"
    #endif
#endif

#ifdef HAVE_MONETDB
#include "monetdb.hh"
#endif

#include "cockroachdb.hh"
#include "postgres.hh"
// ---

#include "grammar.hh" // for statement gen
#include "dbms_info.hh" // for dbms_info
#include "transaction_test.hh" // for transaction
#include "instrumentor.hh" // for stmt_usage

extern "C" { //for sigusr1
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
}

using namespace std;

#define NORMAL_BUG_FILE "bug_trigger_stmt.sql"
#define GEN_STMT_FILE "gen_stmts.sql"

#define KILL_PROC_TIME_MS 10000
#define WAIT_FOR_PROC_TIME_MS 20000

#define RESET   "\033[0m"
#define BLACK   "\033[30m"      /* Black */
#define RED     "\033[31m"      /* Red */
#define GREEN   "\033[32m"      /* Green */
#define YELLOW  "\033[33m"      /* Yellow */
#define BLUE    "\033[34m"      /* Blue */
#define MAGENTA "\033[35m"      /* Magenta */
#define CYAN    "\033[36m"      /* Cyan */
#define WHITE   "\033[37m"      /* White */
#define BOLDBLACK   "\033[1m\033[30m"      /* Bold Black */


struct thread_data {
    dbms_info* d_info;
    vector<string>* trans_stmts;
    vector<string>* exec_trans_stmts;
    vector<vector<string>>* stmt_output;
    int commit_or_not;
};

struct test_thread_arg {
    dbms_info* d_info;
    string* stmt;
    vector<string>* stmt_output;
    int* affected_row_num;
    exception e;
    bool has_exception;
};

void gen_stmts_for_one_txn(shared_ptr<schema> &db_schema,
                        int trans_stmt_num,
                        vector<shared_ptr<prod>>& trans_rec,
                        dbms_info& d_info);

bool compare_output(vector<vector<vector<string>>>& a_output,
                    vector<vector<vector<string>>>& b_output);

bool compare_content(map<string, vector<vector<string>>>&a_content, 
                     map<string, vector<vector<string>>>&b_content);

pid_t fork_db_server(dbms_info& d_info);

shared_ptr<schema> get_schema(dbms_info& d_info);
shared_ptr<dut_base> dut_setup(dbms_info& d_info);
int save_backup_file(string path, dbms_info& d_info);

void user_signal(int signal);

void dut_reset(dbms_info& d_info);
void dut_backup(dbms_info& d_info);
void dut_reset_to_backup(dbms_info& d_info);
void dut_get_content(dbms_info& d_info, 
                    map<string, vector<vector<string>>>& content);

int generate_database(dbms_info& d_info);
void kill_process_with_SIGTERM(pid_t process_id);

bool reproduce_routine(dbms_info& d_info,
                        vector<shared_ptr<prod>>& stmt_queue, 
                        vector<int>& tid_queue,
                        vector<stmt_usage> usage_queue,
                        string& err_info);

bool minimize_testcase(dbms_info& d_info,
                        vector<shared_ptr<prod>>& stmt_queue, 
                        vector<int>& tid_queue,
                        vector<stmt_usage> usage_queue);

string print_stmt_to_string(shared_ptr<prod> stmt);

int make_dir_error_exit(string folder);

extern pthread_mutex_t mutex_timeout;  
extern pthread_cond_t  cond_timeout;

#endif