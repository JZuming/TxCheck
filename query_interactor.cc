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

#ifdef HAVE_MONETDB
#include "monetdb.hh"
#endif

#include "postgres.hh"

#include <sys/time.h>
#include <sys/wait.h>

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

// #define __DEBUG_MODE__

struct thread_data {
    map<string,string>* options;
    vector<string>* trans_stmts;
    vector<string>* exec_trans_stmts;
    vector<vector<string>>* stmt_output;
    bool commit_or_not;
    bool is_live;
    pthread_mutex_t* mutex_timeout;  
    pthread_cond_t*  cond_timeout;
};

struct test_thread_arg {
    map<string,string>* options;
    string* stmt;
    exception e;
    bool has_exception;
};

shared_ptr<schema> get_schema(map<string,string>& options)
{
    shared_ptr<schema> schema;
    try {
        if (options.count("sqlite")) {
    #ifdef HAVE_LIBSQLITE3
            schema = make_shared<schema_sqlite>(options["sqlite"], true);
    #else
            cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
            throw runtime_error("Does not support SQLite");
    #endif
        } else if(options.count("monetdb")) {
    #ifdef HAVE_MONETDB
            schema = make_shared<schema_monetdb>(options["monetdb"]);
    #else
            cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
            throw runtime_error("Does not support MonetDB");
    #endif
        } else if(options.count("postgres")) 
            schema = make_shared<schema_pqxx>(options["postgres"], true);
        else {
            cerr << "Sorry,  you should specify a dbms and its database" << endl;
            throw runtime_error("Does not define target dbms and db");
        }
    } catch (exception &e) { // may occur occastional error
        cerr << RED << "get schema error" << RESET << endl;
        return get_schema(options);
    }
    return schema;
}

shared_ptr<dut_base> dut_setup(map<string,string>& options)
{
    shared_ptr<dut_base> dut;
    if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
        dut = make_shared<dut_sqlite>(options["sqlite"]);
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
        throw runtime_error("Does not support SQLite");
#endif
    } else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB	   
        dut = make_shared<dut_monetdb>(options["monetdb"]);
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
        throw runtime_error("Does not support MonetDB");
#endif
    } else if(options.count("postgres")) 
        dut = make_shared<dut_libpq>(options["postgres"]);
    else {
        cerr << "Sorry,  you should specify a dbms and its database" << endl;
        throw runtime_error("Does not define target dbms and db");
    }

    return dut;
}

pthread_mutex_t mutex_timeout;  
pthread_cond_t  cond_timeout;

pthread_mutex_t trans_1_mutex_timeout;  
pthread_cond_t  trans_1_cond_timeout;
pthread_mutex_t trans_2_mutex_timeout;  
pthread_cond_t  trans_2_cond_timeout;

void user_signal(int signal)  
{  
    if(signal != SIGUSR1) {  
        printf("unexpect signal %d\n", signal);  
        exit(1);  
    }  
     
    cerr << "get SIGUSR1, stop the thread" << endl;
    pthread_exit(0);
    return;  
}

static int child_pid = 0;
static bool child_timed_out = false;

void kill_process_signal(int signal)  
{  
    if(signal != SIGALRM) {  
        printf("unexpect signal %d\n", signal);  
        exit(1);  
    }

    if (child_pid > 0) {
        printf("child pid timeout, kill it\n"); 
        child_timed_out = true;
		kill(child_pid, SIGKILL);
	}

    cerr << "get SIGALRM, stop the process" << endl;
    return;  
}

void* test_thread(void* argv)
{
    auto data = (test_thread_arg *)argv;
    try {
        auto dut = dut_setup(*(data->options));
        dut->test(*(data->stmt));
    } catch (std::exception &e) {
        // cerr << "in test thread: " << e.what() << endl;
        data->e = e;
        data->has_exception = true;
    }
    
    pthread_mutex_lock(&mutex_timeout);  
    pthread_cond_signal(&cond_timeout);  
    pthread_mutex_unlock(&mutex_timeout);

    return NULL;
}

void dut_test(map<string,string>& options, const string& stmt)
{   
    // set timeout limit
    struct timespec m_time;
    m_time.tv_sec = time(NULL) + 2; // 2 seconds  
    m_time.tv_nsec = 0; 

    // set pthread parameter 
    pthread_t thread;
    test_thread_arg data;
    data.options = &options;
    auto str = stmt;
    data.stmt = &str;
    data.has_exception = false;

    pthread_create(&thread, NULL, test_thread, &data);
    pthread_mutex_lock(&mutex_timeout);  
    int res = pthread_cond_timedwait(&cond_timeout, &mutex_timeout, (const struct timespec *)&m_time);  
    pthread_mutex_unlock(&mutex_timeout);

    if (res == ETIMEDOUT) {
        cerr << "thread timeout!" << endl;
        pthread_kill(thread, SIGUSR1);

        // must join (to release the resource of thread)
        pthread_join(thread, NULL);
        throw runtime_error(string("timeout in this stmt"));
    }

    // must join (to release the resource of thread)
    pthread_join(thread, NULL);

    if (data.has_exception)
        throw data.e;
}

void dut_reset(map<string,string>& options)
{
    auto dut = dut_setup(options);
    dut->reset();
}

void dut_backup(map<string,string>& options)
{
    auto dut = dut_setup(options);
    dut->backup();
}

void dut_reset_to_backup(map<string,string>& options)
{
    auto dut = dut_setup(options);
    dut->reset_to_backup();
}

void *dut_trans_test(void *thread_arg)
{
    auto data = (thread_data *)thread_arg;
    auto dut = dut_setup(*(data->options));
    dut->trans_test(*(data->trans_stmts), data->exec_trans_stmts, data->stmt_output, data->commit_or_not);

    pthread_mutex_lock(data->mutex_timeout);
    pthread_cond_signal(data->cond_timeout);
    pthread_mutex_unlock(data->mutex_timeout);

    data->is_live = false;

    return NULL;
}

void normal_dut_trans_test(map<string,string>& options, 
                           vector<string>& stmts, 
                           vector<string>* exec_stmts,
                           vector<vector<string>>* stmt_output,
                           bool commit_or_not)
{
    auto dut = dut_setup(options);
    dut->trans_test(stmts, exec_stmts, stmt_output, commit_or_not);
}

void dut_get_content(map<string,string>& options, 
                    vector<string>& tables_name, 
                    map<string, vector<string>>& content)
{
    auto dut = dut_setup(options);
    dut->get_content(tables_name, content);
}

void interect_test(map<string,string>& options, shared_ptr<prod> (* tmp_statement_factory)(scope *), vector<string>& rec_vec)
{
    auto schema = get_schema(options);
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);

    try {
        dut_test(options, s.str());
        auto sql = s.str() + ";";
        rec_vec.push_back(sql);
    } catch(std::exception &e) { // ignore runtime error
        string err = e.what();
        if (err.find("syntax") != string::npos) {
            cerr << "\n" << e.what() << "\n" << endl;
            cerr << s.str() << endl;
        }
        interect_test(options, tmp_statement_factory, rec_vec);
    }
}

void normal_test(map<string,string>& options, shared_ptr<schema>& schema, shared_ptr<prod> (* tmp_statement_factory)(scope *), vector<string>& rec_vec)
{
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);

    try {
        dut_test(options, s.str());
        auto sql = s.str() + ";";
        rec_vec.push_back(sql);
    } catch(std::exception &e) { // ignore runtime error
        string err = e.what();
        if (err.find("syntax") != string::npos) {
            cerr << s.str() << endl;
        }
        if (err.find("timeout") != string::npos) {
            cerr << e.what() << endl;
        }
        normal_test(options, schema, tmp_statement_factory, rec_vec);
    }
}

bool compare_content(vector<string>& table_names, 
                    map<string, vector<string>>&con_content, 
                    map<string, vector<string>>&seq_content)
{
    for (auto& table:table_names) {
        auto& con_table_content = con_content[table];
        auto& seq_table_content = seq_content[table];

        auto size = con_table_content.size();
        if (size != seq_table_content.size()) {
            cerr << "table " + table + " sizes are not equal" << endl;
            return false;
        }

        for (auto i = 0; i < size; i++) {
            if (seq_table_content[i] != con_table_content[i]) {
                cerr << "table " + table + " content are not equal" << endl;
                return false;
            }
        }
    }

    return true;
}

bool compare_output(vector<vector<string>>& trans_output,
                    vector<vector<string>>& seq_output)
{
    auto size = trans_output.size();
    if (size != seq_output.size()) {
        cerr << "output sizes are not equel: "<< trans_output.size() << " " << seq_output.size() << endl;
        return false;
    }
    for (auto i = 0; i < size; i++) {
        auto& trans_stmt_output = trans_output[i];
        auto& seq_stmt_output = seq_output[i];
        
        auto stmt_output_size = trans_stmt_output.size();
        if (stmt_output_size != seq_stmt_output.size()) {
            cerr << "stmt[" << i << "] output sizes are not equel: " << trans_stmt_output.size() << " " << seq_stmt_output.size() << endl;
            return false;
        }

        for (auto j = 0; j < stmt_output_size; j++) {
            if (trans_stmt_output[j] != seq_stmt_output[j]) {
                cerr << "stmt[" << i << "][" << j << "] outputs are not equel: " << trans_stmt_output[j] << " " << seq_stmt_output[j] << endl;
                return false;
            }
        }
    }

    return true;
}

void write_output(vector<vector<string>>& output, string file_name)
{
    ofstream ofile(file_name);
    auto size = output.size();
    for (auto i = 0; i < size; i++) {
        ofile << "stmt " << i << " output" << endl;
        for (auto& str: output[i]) {
            ofile << " " << str;
        }
    }
    ofile.close();
}

int generate_database(map<string,string>& options, file_random_machine* random_file)
{
    vector<string> stage_1_rec;
    vector<string> stage_2_rec;
    
    // stage 1: DDL stage (create, alter, drop)
    cerr << YELLOW << "stage 1: generate the shared database" << RESET << endl;
    auto ddl_stmt_num = d6() + 1; // at least 2 statements to create 2 tables
    for (auto i = 0; i < ddl_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;

        interect_test(options, &ddl_statement_factory, stage_1_rec); // has disabled the not null, check and unique clause 
    }

    // stage 2: basic DML stage (only insert),
    cerr << YELLOW << "stage 2: insert data into the database" << RESET << endl;
    auto basic_dml_stmt_num = 20 + d20(); // 20-40 statements to insert data
    auto schema = get_schema(options); // schema will not change in this stage
    for (auto i = 0; i < basic_dml_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        normal_test(options, schema, &basic_dml_statement_factory, stage_2_rec);
    }

    // stage 3: backup database
    cerr << YELLOW << "stage 3: backup the database" << RESET << endl;
    dut_backup(options);

    return 0;
}

bool seq_res_comp(map<string,string>& options, vector<string> table_names,
                map<string, vector<string>>& concurrent_content,
                vector<vector<string>>& trans_1_output, vector<vector<string>>& trans_2_output,
                vector<string>& exec_trans_1_stmts, vector<string>& exec_trans_2_stmts,
                bool trans_1_commit, bool trans_2_commit)
{
    dut_reset_to_backup(options);

    vector<vector<string>> seq_1_output, seq_2_output;
    if (trans_1_commit)
        normal_dut_trans_test(options, exec_trans_1_stmts, NULL, &seq_1_output, trans_1_commit);
    if (trans_2_commit)
        normal_dut_trans_test(options, exec_trans_2_stmts, NULL, &seq_2_output, trans_2_commit);

    map<string, vector<string>> sequential_content;
    dut_get_content(options, table_names, sequential_content);

    if (!compare_content(table_names, concurrent_content, sequential_content)) {
        cerr << "trans content is not equal to seq content" << endl;
        return false;
    }
    if (trans_1_commit && !compare_output(trans_1_output, seq_1_output)) {
        cerr << "trans_1_output is not equal to seq_1_output" << endl;
        return false;
    }
    if (trans_2_commit && !compare_output(trans_2_output, seq_2_output)) {
        cerr << "trans_2_output is not equal to seq_2_output" << endl;
        return false;
    }

    return true;
}

void generate_transaction(map<string,string>& options, file_random_machine* random_file, 
                        vector<string>& trans_1_rec, vector<string>& trans_2_rec)
{
    cerr << YELLOW << "reset to backup" << RESET << endl;
    dut_reset_to_backup(options);

    auto schema = get_schema(options);

    cerr << YELLOW << "stage 4: generate SQL statements for transaction A and B" << RESET << endl;
    auto trans_1_stmt_num = 9 + d6(); // 10-15
    for (auto i = 0; i < trans_1_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        cerr << "trans 1: " << i << endl;
        normal_test(options, schema, &trans_statement_factory, trans_1_rec);
    }

    cerr << YELLOW << "reset to backup" << RESET << endl;
    dut_reset_to_backup(options);
    auto trans_2_stmt_num = 9 + d6(); // 10-15
    for (auto i = 0; i < trans_2_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        cerr << "trans 2: " << i << endl;
        normal_test(options, schema, &trans_statement_factory, trans_2_rec);
    }
}

void concurrently_execute_transaction(map<string,string>& options, 
                                    vector<string>& trans_1_rec, vector<string>& trans_2_rec,
                                    vector<string>& exec_trans_1_stmts, vector<string>& exec_trans_2_stmts,
                                    vector<vector<string>>& trans_1_output, vector<vector<string>>& trans_2_output,
                                    bool trans_1_commit, bool trans_2_commit,
                                    map<string, vector<string>>& concurrent_content, vector<string>& table_names)
{
    cerr << YELLOW << "dut_reset_to_backup"  << RESET << endl;
    dut_reset_to_backup(options);
    cerr << YELLOW << "stage 5: cocurrently execute transaction A and B"  << RESET << endl;
    
    // set timeout limit
    struct timespec m_time;
    m_time.tv_sec = time(NULL) + 10; // 10 seconds  
    m_time.tv_nsec = 0; 
    
    thread_data data_1, data_2;

    data_1.options = &options;
    data_1.trans_stmts = &trans_1_rec;
    data_1.exec_trans_stmts = &exec_trans_1_stmts;
    data_1.stmt_output = &trans_1_output;
    data_1.is_live = true;
    data_1.commit_or_not = trans_1_commit;
    data_1.mutex_timeout = &trans_1_mutex_timeout;
    data_1.cond_timeout = &trans_1_cond_timeout;

    data_2.options = &options;
    data_2.trans_stmts = &trans_2_rec;
    data_2.exec_trans_stmts = &exec_trans_2_stmts;
    data_2.stmt_output = &trans_2_output;
    data_2.is_live = true;
    data_2.commit_or_not = trans_2_commit;
    data_2.mutex_timeout = &trans_2_mutex_timeout;
    data_2.cond_timeout = &trans_2_cond_timeout;

    pthread_t tid_1, tid_2;
    pthread_create(&tid_1, NULL, dut_trans_test, &data_1);
    pthread_create(&tid_2, NULL, dut_trans_test, &data_2);

    int res_1 = 0, res_2 = 0;
    if (data_1.is_live) {
        pthread_mutex_lock(&trans_1_mutex_timeout);  
        res_1 = pthread_cond_timedwait(&trans_1_cond_timeout, &trans_1_mutex_timeout, (const struct timespec *)&m_time);  
        pthread_mutex_unlock(&trans_1_mutex_timeout);
    }
    cerr << "wake up from the trans 1" << endl;

    if (data_2.is_live) {
        pthread_mutex_lock(&trans_2_mutex_timeout);  
        res_2 = pthread_cond_timedwait(&trans_2_cond_timeout, &trans_2_mutex_timeout, (const struct timespec *)&m_time);  
        pthread_mutex_unlock(&trans_2_mutex_timeout);
    }
    cerr << "wake up from the trans 2" << endl;

    if (data_1.is_live || data_2.is_live) {
        if (res_1 == ETIMEDOUT || res_2 == ETIMEDOUT) {
            cerr << "concurrent test timeout!" << endl;
            if (res_1 == ETIMEDOUT)
                pthread_kill(tid_1, SIGUSR1);
            if (res_2 == ETIMEDOUT)
                pthread_kill(tid_2, SIGUSR1);
            
            // must join (to release the resource of thread)
            pthread_join(tid_1, NULL);
            pthread_join(tid_2, NULL);
            cerr << "throw timeout exception" << endl;
            throw runtime_error(string("concurrent timeout in this test"));
        }
    }

    // must join (to release the resource of thread)
    pthread_join(tid_1, NULL);
    pthread_join(tid_2, NULL);

    // collect database information
    auto schema = get_schema(options);
    for (auto& table:schema->tables) {
        table_names.push_back(table.ident());
    }
    dut_get_content(options, table_names, concurrent_content);
}

bool sequentially_check(map<string,string>& options, vector<string> table_names,
                        map<string, vector<string>>& concurrent_content,
                        vector<vector<string>>& trans_1_output, vector<vector<string>>& trans_2_output,
                        vector<string>& exec_trans_1_stmts, vector<string>& exec_trans_2_stmts,
                        bool trans_1_commit, bool trans_2_commit)
{
    cerr << YELLOW << "stage 6.1: first comparison: A -> B" << RESET << endl;
    if (seq_res_comp(options, table_names, concurrent_content, 
                trans_1_output, trans_2_output, 
                exec_trans_1_stmts, exec_trans_2_stmts,
                trans_1_commit, trans_2_commit)) {
        return false;
    }
    cerr << YELLOW << "stage 6.2: second comparison: B -> A" << RESET << endl;
    if (seq_res_comp(options, table_names, concurrent_content, 
                trans_2_output, trans_1_output, 
                exec_trans_2_stmts, exec_trans_1_stmts,
                trans_2_commit, trans_1_commit)) {
        return false;
    }
    return true;
}

int transaction_test(map<string,string>& options, file_random_machine* random_file)
{
    vector<string> trans_1_rec;
    vector<string> trans_2_rec;
    generate_transaction(options, random_file, trans_1_rec, trans_2_rec);

    vector<string> exec_trans_1_stmts, exec_trans_2_stmts;
    vector<vector<string>> trans_1_output, trans_2_output;
    map<string, vector<string>> concurrent_content;
    vector<string> table_names;
    bool trans_1_commit = true, trans_2_commit = true;
    if (d20() > 14)
        trans_1_commit = false;
    if (d20() > 14)
        trans_2_commit = false;

    concurrently_execute_transaction(options, trans_1_rec, trans_2_rec, 
                                    exec_trans_1_stmts, exec_trans_2_stmts,
                                    trans_1_output, trans_2_output,
                                    trans_1_commit, trans_2_commit,
                                    concurrent_content, table_names);
    
    auto res = sequentially_check(options, table_names, concurrent_content, 
                            trans_1_output, trans_2_output, 
                            exec_trans_1_stmts, exec_trans_2_stmts,
                            trans_1_commit, trans_2_commit);
    
    if (res == false)
        return 0;

    cerr << RED << "find a bug, and record the detail" << RESET << endl;
    ofstream ofile("exec_trans_1.sql");
    for (auto& stmt:exec_trans_1_stmts)
        ofile << stmt << endl;
    ofile.close();

    ofile.open("exec_trans_2.sql");
    for (auto& stmt:exec_trans_2_stmts)
        ofile << stmt << endl;
    ofile.close();

    ofile.open("trans_1.sql");
    for (auto& stmt:trans_1_rec)
        ofile << stmt << endl;
    ofile.close();

    ofile.open("trans_2.sql");
    for (auto& stmt:trans_2_rec)
        ofile << stmt << endl;
    ofile.close();

    return 1;
}

int random_test(map<string,string>& options)
{
    struct file_random_machine* random_file;
    if (options.count("random-seed")) {
        cerr << "random seed is " << options["random-seed"] << endl;
        random_file = file_random_machine::get(options["random-seed"]);
        file_random_machine::use_file(options["random-seed"]);
    }
    else
        random_file = NULL;
    
    if (random_file == NULL) {
        // smith::rng.seed(getpid());
        smith::rng.seed(time(NULL));
    }
    
    // reset the target DBMS to initial state
    dut_reset(options); 
    generate_database(options, random_file);

    int i = TEST_TIME_FOR_EACH_DB;
    while (i--) {
        try {
            auto ret = transaction_test(options, random_file);
            if (ret == 1) {
                exit(166);
            }
        } catch(std::exception &e) { // ignore runtime error
            cerr << e.what() << endl;
        } 
    }
    
    return 0;
}

int main(int argc, char *argv[])
{
    // analyze the options
    map<string,string> options;
    regex optregex("--(help|postgres|sqlite|monetdb|random-seed)(?:=((?:.|\n)*))?");
  
    for(char **opt = argv + 1 ;opt < argv + argc; opt++) {
        smatch match;
        string s(*opt);
        if (regex_match(s, match, optregex)) {
            options[string(match[1])] = match[2];
        } else {
            cerr << "Cannot parse option: " << *opt << endl;
            options["help"] = "";
        }
    }

    if (options.count("help")) {
        cerr <<
            "    --postgres=connstr   postgres database to send queries to" << endl <<
#ifdef HAVE_LIBSQLITE3
            "    --sqlite=URI         SQLite database to send queries to" << endl <<
#endif
#ifdef HAVE_MONETDB
            "    --monetdb=connstr    MonetDB database to send queries to" <<endl <<
#endif
            "    --random-seed=filename    random file for dynamic query interaction" << endl <<
            "    --help               print available command line options and exit" << endl;
        return 0;
    } else if (options.count("version")) {
        return 0;
    }

    // set timeout action
    struct sigaction action;  
    memset(&action, 0, sizeof(action));  
    sigemptyset(&action.sa_mask);  
    action.sa_flags = 0;  
    action.sa_handler = user_signal;  
    if (sigaction(SIGUSR1, &action, NULL)) {
        cerr << "sigaction error" << endl;
        exit(1);
    }

    // set timeout action for fork
    struct sigaction sa;  
    memset(&sa, 0, sizeof(sa));  
    sigemptyset(&sa.sa_mask);  
    sa.sa_flags = SA_RESTART; 
    sa.sa_handler = kill_process_signal;  
    if (sigaction(SIGALRM, &sa, NULL)) {
        cerr << "sigaction error" << endl;
        exit(1);
    }

    // init the lock
    pthread_mutex_init(&mutex_timeout, NULL);  
    pthread_cond_init(&cond_timeout, NULL);
    pthread_mutex_init(&trans_1_mutex_timeout, NULL);  
    pthread_cond_init(&trans_1_cond_timeout, NULL);
    pthread_mutex_init(&trans_2_mutex_timeout, NULL);  
    pthread_cond_init(&trans_2_cond_timeout, NULL);

    static itimerval itimer;
    while (1) {
        child_timed_out = true;

        cerr << RED << "New Test Database --------------------------" << RESET << endl;
        child_pid = fork();
        if (child_pid == 0) {
            random_test(options);
            exit(0);
        }

        // timeout is ms
        itimer.it_value.tv_sec = 60000 / 1000; // 60 s limit
        itimer.it_value.tv_usec = (60000 % 1000) * 1000; // us limit
        setitimer(ITIMER_REAL, &itimer, NULL);

        cerr << "begin waiting" << endl;

        // wait for the tests
        int status;
        auto res = waitpid(child_pid, &status, 0);
        if (res <= 0) {
            cerr << "waitpid() fail: " <<  res << endl;
            exit(-1);
        }

        // disable HandleTimeout
        if (!WIFSTOPPED(status)) 
            child_pid = 0;
        
        itimer.it_value.tv_sec = 0;
        itimer.it_value.tv_usec = 0;
        setitimer(ITIMER_REAL, &itimer, NULL);

        if (WIFSIGNALED(status)) {
            auto killSignal = WTERMSIG(status);
            if (child_timed_out && killSignal == SIGKILL) {
                cerr << "just timeout" << endl;
                continue;
            }
            else {
                cerr << RED << "find memory bug" << RESET << endl;
                return -1;
            }
        }

        if (WIFEXITED(status)) {
            auto exit_code =  WEXITSTATUS(status); // only low 8 bit (max 255)
            cerr << "exit code " << exit_code << endl;
            if (exit_code == 166) {
                cerr << RED << "find correctness bug" << RESET << endl;
                return -1;
            }
        }
    }

    return 0;
}