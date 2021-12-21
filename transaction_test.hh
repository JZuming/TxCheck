#include "config.h"

#include <iostream>
#include <chrono>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

#include <thread>
#include <typeinfo>

#include "random.hh"
#include "grammar.hh"
#include "relmodel.hh"
#include "schema.hh"
#include "gitrev.h"

#include "log.hh"
#include "dump.hh"
#include "impedance.hh"
#include "dut.hh"

#ifdef HAVE_LIBSQLITE3
#include "sqlite.hh"
#endif

#ifdef HAVE_LIBMYSQLCLIENT
#include "mysql.hh"
#endif

#ifdef HAVE_MONETDB
#include "monetdb.hh"
#endif

#include "cockroachdb.hh"
#include "postgres.hh"

#include <sys/time.h>
#include <sys/wait.h>

#include <sys/stat.h> 

using namespace std;

using namespace std::chrono;

extern "C" {
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
}

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
    map<string,string>* options;
    vector<string>* trans_stmts;
    vector<string>* exec_trans_stmts;
    vector<vector<string>>* stmt_output;
    int commit_or_not;
};

struct test_thread_arg {
    map<string,string>* options;
    string* stmt;
    vector<string>* stmt_output;
    int* affected_row_num;
    exception e;
    bool has_exception;
};

struct transaction {
    shared_ptr<dut_base> dut;
    
    vector<string> stmts;
    vector<vector<string>> stmt_outputs;
    vector<string> stmt_err_info;
    map<string, vector<string>> committed_content; // database content after commit

    vector<string> normal_test_stmts;
    vector<vector<string>> normal_test_stmt_outputs;
    vector<string> normal_test_stmt_err_info;
    map<string, vector<string>> executed_content; // database content after execution

    int stmt_num;
    int status;
};

class transaction_test {
public:
    static int record_bug_num;
    static pid_t server_process_id;

    transaction* trans_arr;

    map<string,string>* options;
    file_random_machine* random_file;
    string output_path_dir;

    int must_commit_num;
    bool need_affect;

    bool is_serializable;
    int trans_num;
    int stmt_num;

    vector<int> tid_queue;
    vector<string> stmt_queue;

    map<string, vector<string>> trans_content;
    map<string, vector<string>> normal_content;

    void arrage_trans_for_tid_queue();
    void assign_trans_status();
    void gen_stmt_for_each_trans();
    
    void trans_test();
    void normal_test();
    bool check_result();

    bool fork_if_server_closed();

    transaction_test(map<string,string>& options, file_random_machine* random_file, bool is_serializable);
    ~transaction_test();
    int test();
};


shared_ptr<schema> get_schema(map<string,string>& options);
shared_ptr<dut_base> dut_setup(map<string,string>& options);
void user_signal(int signal);
void* test_thread(void* argv);
void dut_test(map<string,string>& options, const string& stmt, bool need_affect);
void dut_reset(map<string,string>& options);
void dut_backup(map<string,string>& options);
void dut_reset_to_backup(map<string,string>& options);
void dut_get_content(map<string,string>& options, 
                    map<string, vector<string>>& content);
void interect_test(map<string,string>& options, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    vector<string>& rec_vec,
                    bool need_affect);
void normal_test(map<string,string>& options, 
                    shared_ptr<schema>& schema, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    vector<string>& rec_vec,
                    bool need_affect);
int generate_database(map<string,string>& options, file_random_machine* random_file);
void kill_process_with_SIGTERM(pid_t process_id);

extern pthread_mutex_t mutex_timeout;  
extern pthread_cond_t  cond_timeout;