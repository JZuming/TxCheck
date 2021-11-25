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
    vector<string> executed_stmts;

    vector<string> normal_test_stmts;
    vector<vector<string>> normal_test_stmt_outputs;

    int stmt_num;
    int status;
    set<int> scheduled_after_tid;
};

class transaction_test {
    transaction* trans_arr;

    map<string,string>* options;
    file_random_machine* random_file;

    bool is_serializable;
    int trans_num;
    int stmt_num;

    vector<int> tid_queue;
    vector<string> stmt_queue;

    vector<int> executed_tid_queue;
    vector<string> executed_stmt_queue;

    map<string, vector<string>> trans_content;
    map<string, vector<string>> normal_content;

    void arrage_trans_for_tid_queue();
    void assign_trans_status();
    void gen_stmt_for_each_trans();

    bool schedule_last_stmt_pos(int stmt_index);
    
    void trans_test();
    void normal_test();
    bool check_result();

public:
    transaction_test(map<string,string>& options, file_random_machine* random_file, bool is_serializable);
    ~transaction_test();
    int test();
};

shared_ptr<schema> get_schema(map<string,string>& options)
{
    shared_ptr<schema> schema;
    static int try_time = 0;
    try {
        if (options.count("sqlite")) {
            #ifdef HAVE_LIBSQLITE3
            schema = make_shared<schema_sqlite>(options["sqlite"], true);
            #else
            cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
            throw runtime_error("Does not support SQLite");
            #endif
        } else if (options.count("mysql-db") && options.count("mysql-port")) {
            #ifdef HAVE_LIBMYSQLCLIENT
            schema = make_shared<schema_mysql>(options["mysql-db"], stoi(options["mysql-port"]));
            #else
            cerr << "Sorry, " PACKAGE_NAME " was compiled without MySQL support." << endl;
            throw runtime_error("Does not support MySQL");
            #endif
        } else if (options.count("monetdb")) {
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
        if (try_time >= 128) {
            cerr << "Fail in get_schema() " << try_time << " times, return" << endl;
            exit(144); // ignore this kind of error
        }
        try_time++;
        schema = get_schema(options);
        try_time--;
        return schema;
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
    } else if (options.count("mysql-db") && options.count("mysql-port")) {
        #ifdef HAVE_LIBMYSQLCLIENT
        dut = make_shared<dut_mysql>(options["mysql-db"], stoi(options["mysql-port"]));
        #else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MySQL support." << endl;
        throw runtime_error("Does not support MySQL");
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

void user_signal(int signal)  
{  
    if(signal != SIGUSR1) {  
        printf("unexpect signal %d\n", signal);  
        exit(1);  
    }  
     
    cerr << "get SIGUSR1, stop the thread" << endl;
    pthread_exit(0);
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
        dut->test(*(data->stmt), data->stmt_output, data->affected_row_num);
    } catch (std::exception &e) {
        // cerr << "In test thread: " << e.what() << endl;
        // cerr << *(data->stmt) << endl;
        // exit(144);
        data->e = e;
        data->has_exception = true;
    }
    
    pthread_mutex_lock(&mutex_timeout);  
    pthread_cond_signal(&cond_timeout);  
    pthread_mutex_unlock(&mutex_timeout);

    return NULL;
}

void dut_test(map<string,string>& options, const string& stmt, bool need_affect)
{   
    // set timeout limit
    struct timespec m_time;
    m_time.tv_sec = time(NULL) + STATEMENT_TIMEOUT;  
    m_time.tv_nsec = 0; 

    // set pthread parameter 
    vector<string> stmt_output;
    int affected_row_num = 0;

    pthread_t thread;
    test_thread_arg data;
    data.options = &options;
    auto str = stmt;
    data.stmt = &str;
    data.has_exception = false;
    data.affected_row_num = &affected_row_num;
    data.stmt_output = &stmt_output;

    // record the test case firstly
    ofstream ofile("cur_test_smt.sql");
    ofile << stmt << endl;
    ofile.close();

    pthread_create(&thread, NULL, test_thread, &data);

    pthread_mutex_lock(&mutex_timeout);  
    int res = pthread_cond_timedwait(&cond_timeout, &mutex_timeout, (const struct timespec *)&m_time);  
    pthread_mutex_unlock(&mutex_timeout);

    if (res == ETIMEDOUT) {
        cerr << "thread timeout!" << endl;
        pthread_kill(thread, SIGUSR1);
        pthread_join(thread, NULL);

        throw runtime_error(string("timeout in this stmt"));
    }

    pthread_join(thread, NULL);

    if (data.has_exception)
        throw data.e;
    
    if (need_affect && affected_row_num <= 0 && stmt_output.empty()) {
        throw runtime_error(string("affect result empty"));
    }
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

    return NULL;
}

void normal_dut_trans_test(map<string,string>& options, 
                           vector<string>& stmts, 
                           vector<string>* exec_stmts,
                           vector<vector<string>>* stmt_output,
                           int commit_or_not)
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

void interect_test(map<string,string>& options, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    vector<string>& rec_vec,
                    bool need_affect)
{
    auto schema = get_schema(options);
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);

    static int try_time = 0;
    try {
        dut_test(options, s.str(), need_affect);
        auto sql = s.str() + ";";
        rec_vec.push_back(sql);
    } catch(std::exception &e) { // ignore runtime error
        string err = e.what();
        if (err.find("syntax") != string::npos) {
            cerr << "\n" << e.what() << "\n" << endl;
            cerr << s.str() << endl;
        }
        if (try_time >= 128) {
            cerr << "Fail in interect_test() " << try_time << " times, return" << endl;
            exit(144); // ignore this kind of error
        }
        try_time++;
        interect_test(options, tmp_statement_factory, rec_vec, need_affect);
        try_time--;
    }
}

void normal_test(map<string,string>& options, 
                    shared_ptr<schema>& schema, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    vector<string>& rec_vec,
                    bool need_affect)
{
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);

    static int try_time = 0;
    try {
        dut_test(options, s.str(), need_affect);
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
        if (try_time >= 128) {
            cerr << "Fail in normal_test() " << try_time << " times, return" << endl;
            exit(144); // ignore this kind of error
        }
        try_time++;
        normal_test(options, schema, tmp_statement_factory, rec_vec, need_affect);
        try_time--;
    }
}

size_t BKDRHash(const char *str, size_t hash)  
{
    while (size_t ch = (size_t)*str++)  
    {         
        hash = hash * 131 + ch;   // 也可以乘以31、131、1313、13131、131313..  
        // 有人说将乘法分解为位运算及加减法可以提高效率，如将上式表达为：hash = hash << 7 + hash << 1 + hash + ch;  
        // 但其实在Intel平台上，CPU内部对二者的处理效率都是差不多的，  
        // 我分别进行了100亿次的上述两种运算，发现二者时间差距基本为0（如果是Debug版，分解成位运算后的耗时还要高1/3）；  
        // 在ARM这类RISC系统上没有测试过，由于ARM内部使用Booth's Algorithm来模拟32位整数乘法运算，它的效率与乘数有关：  
        // 当乘数8-31位都为1或0时，需要1个时钟周期  
        // 当乘数16-31位都为1或0时，需要2个时钟周期  
        // 当乘数24-31位都为1或0时，需要3个时钟周期  
        // 否则，需要4个时钟周期  
        // 因此，虽然我没有实际测试，但是我依然认为二者效率上差别不大          
    }  
    return hash;  
}

static void hash_output_to_set(vector<string> &output, vector<size_t>& hash_set)
{
    size_t hash = 0;
    size_t output_size = output.size();
    for (size_t i = 0; i < output_size; i++) {
        if (output[i] == "\n") {
            hash_set.push_back(hash);
            hash = 0;
            continue;
        }

        hash = BKDRHash(output[i].c_str(), hash);
    }

    // sort the set, because some output order is random
    sort(hash_set.begin(), hash_set.end());
    return;
}

void output_diff(string item_name, vector<string>& con_result, vector<string>& seq_result)
{
    ofstream ofile("/tmp/comp_diff.txt", ios::app);
    ofile << "============================" << endl;
    ofile << "item name: " << item_name << endl;
    ofile << "concurrent: " << endl;
    for (auto& str : con_result) {
        ofile << "    " << str;
    }
    ofile << endl;
    ofile << "sequential: " << endl;
    for (auto& str : seq_result) {
        ofile << "    " << str;
    }
    ofile.close();
}

bool is_number(const string &s) {
    if (s.empty() || s.length() <= 0) 
        return false;

    int point = 0;
    if (s.length() == 1 && (s[0] >'9' || s[0] < '0')) 
        return false;

    if(s.length() > 1) {
        if (s[0]!='.' && (s[0] >'9' || s[0] < '0')&&s[0]!='-' && s[0]!='+') 
            return false;
        
        if (s[0] == '.') 
            ++point;

        if ((s[0] == '+' || s[0] == '-') && (s[1] >'9' || s[1] < '0')) 
            return false;

        for (size_t i = 1; i < s.length(); ++i) {
            if (s[i]!='.' && (s[i] >'9' || s[i] < '0')) 
                return false;

            if (s[i] == '.') 
                ++point;
        }
    }

    if (point > 1) return false;
    
    return true;
}

bool nomoalize_content(vector<string> &content)
{
    auto size = content.size();

    for (int i = 0; i < size; i++) {
        auto str = content[i];
        double value = 0;
        
        if (!is_number(str) || str.find(".") == string::npos)
            continue;

        // value is a float
        value = stod(str);
        value = floor(value * 1000) / 1000; // keep 3 number after the point
        content[i] = to_string(value);
    }
    return true;
}

bool compare_content(map<string, vector<string>>&con_content, 
                     map<string, vector<string>>&seq_content)
{
    if (con_content.size() != seq_content.size()) {
        cerr << "con_content is not seq_content" << endl;
        return false;
    }
    
    for (auto iter = con_content.begin(); iter != con_content.begin(); iter++) {
        auto& table = iter->first;
        auto& con_table_content = iter->second;
        
        if (seq_content.count(table) == 0) {
            cerr << "seq_content does not have " << table << endl;
            return false;
        }

        auto& seq_table_content = seq_content[table];

        nomoalize_content(con_table_content);
        nomoalize_content(seq_table_content);

        vector<size_t> con_table_set, seq_table_set;
        hash_output_to_set(con_table_content, con_table_set);
        hash_output_to_set(seq_table_content, seq_table_set);

        auto size = con_table_set.size();
        if (size != seq_table_set.size()) {
            cerr << "table " + table + " sizes are not equal" << endl;
            output_diff(table, con_table_content, seq_table_content);
            return false;
        }

        for (auto i = 0; i < size; i++) {
            if (con_table_set[i] != seq_table_set[i]) {
                cerr << "table " + table + " content are not equal" << endl;
                output_diff(table, con_table_content, seq_table_content);
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
    
        nomoalize_content(trans_stmt_output);
        nomoalize_content(seq_stmt_output);
        
        vector<size_t> trans_hash_set, seq_hash_set;
        hash_output_to_set(trans_stmt_output, trans_hash_set);
        hash_output_to_set(seq_stmt_output, seq_hash_set);

        size_t stmt_output_size = trans_hash_set.size();
        if (stmt_output_size != seq_hash_set.size()) {
            cerr << "stmt[" << i << "] output sizes are not equel: " << trans_hash_set.size() << " " << seq_hash_set.size() << endl;
            output_diff("stmt["+ to_string(i) + "]", trans_stmt_output, seq_stmt_output);
            return false;
        }

        for (auto j = 0; j < stmt_output_size; j++) {
            if (trans_hash_set[j] != seq_hash_set[j]) {
                cerr << "stmt[" << i << "] output are not equel" << endl;
                output_diff("stmt["+ to_string(i) + "]", trans_stmt_output, seq_stmt_output);
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

        interect_test(options, &ddl_statement_factory, stage_1_rec, false); // has disabled the not null, check and unique clause 
    }

    // stage 2: basic DML stage (only insert),
    cerr << YELLOW << "stage 2: insert data into the database" << RESET << endl;
    auto basic_dml_stmt_num = 20 + d20(); // 20-40 statements to insert data
    auto schema = get_schema(options); // schema will not change in this stage
    for (auto i = 0; i < basic_dml_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        normal_test(options, schema, &basic_dml_statement_factory, stage_2_rec, false);
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
                int trans_1_commit, int trans_2_commit)
{
    dut_reset_to_backup(options);

    vector<vector<string>> seq_1_output, seq_2_output;
    if (trans_1_commit) {
        cerr << "trans_1 commit" << endl;
        normal_dut_trans_test(options, exec_trans_1_stmts, NULL, &seq_1_output, 2);
    }
        
    if (trans_2_commit) {
        cerr << "trans_2 commit" << endl;
        normal_dut_trans_test(options, exec_trans_2_stmts, NULL, &seq_2_output, 2);
    }
    
    map<string, vector<string>> sequential_content;
    dut_get_content(options, table_names, sequential_content);

    if (!compare_content(concurrent_content, sequential_content)) {
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

void gen_trans_stmts(map<string,string>& options, 
                        file_random_machine* random_file, 
                        shared_ptr<schema> &db_schema,
                        int trans_stmt_num,
                        vector<string>& trans_rec)
{
    cerr << YELLOW << "reset to backup" << RESET << endl;
    dut_reset_to_backup(options);
    for (auto i = 0; i < trans_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        normal_test(options, db_schema, &trans_statement_factory, trans_rec, true);
    }
}

void gen_current_trans(map<string,string>& options, file_random_machine* random_file, 
                        vector<string>& trans_1_rec, vector<string>& trans_2_rec)
{
    auto schema = get_schema(options);
    auto trans_1_stmt_num = d12(); // 1-12
    auto trans_2_stmt_num = d12(); // 1-12
    
    gen_trans_stmts(options, random_file, schema, trans_1_stmt_num, trans_1_rec);
    gen_trans_stmts(options, random_file, schema, trans_2_stmt_num, trans_2_rec);

    return;
}

void concurrently_execute_transaction(map<string,string>& options, 
                                    vector<string>& trans_1_rec, vector<string>& trans_2_rec,
                                    vector<string>& exec_trans_1_stmts, vector<string>& exec_trans_2_stmts,
                                    vector<vector<string>>& trans_1_output, vector<vector<string>>& trans_2_output,
                                    int trans_1_commit, int trans_2_commit,
                                    map<string, vector<string>>& concurrent_content, vector<string>& table_names)
{
    cerr << YELLOW << "dut_reset_to_backup"  << RESET << endl;
    dut_reset_to_backup(options);
    cerr << YELLOW << "stage 5: cocurrently execute transaction A and B"  << RESET << endl;
    
    thread_data data_1, data_2;

    data_1.options = &options;
    data_1.trans_stmts = &trans_1_rec;
    data_1.exec_trans_stmts = &exec_trans_1_stmts;
    data_1.stmt_output = &trans_1_output;
    data_1.commit_or_not = trans_1_commit;

    data_2.options = &options;
    data_2.trans_stmts = &trans_2_rec;
    data_2.exec_trans_stmts = &exec_trans_2_stmts;
    data_2.stmt_output = &trans_2_output;
    data_2.commit_or_not = trans_2_commit;

    pthread_t tid_1, tid_2;
    pthread_create(&tid_1, NULL, dut_trans_test, &data_1);
    pthread_create(&tid_2, NULL, dut_trans_test, &data_2);

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
                        int trans_1_commit, int trans_2_commit)
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

int old_transaction_test(map<string,string>& options, file_random_machine* random_file)
{
    vector<string> trans_1_rec;
    vector<string> trans_2_rec;
    gen_current_trans(options, random_file, trans_1_rec, trans_2_rec);

    ofstream ofile;
    ofile.open("trans_1.sql");
    for (auto& stmt:trans_1_rec)
        ofile << stmt << endl;
    ofile.close();

    ofile.open("trans_2.sql");
    for (auto& stmt:trans_2_rec)
        ofile << stmt << endl;
    ofile.close();

    vector<string> exec_trans_1_stmts, exec_trans_2_stmts;
    vector<vector<string>> trans_1_output, trans_2_output;
    map<string, vector<string>> concurrent_content;
    vector<string> table_names;
    int trans_1_commit = 1, trans_2_commit = 1;
    // if (d20() > 14)
    //     trans_1_commit = 0;
    // if (d20() > 14)
    //     trans_2_commit = 0;
    
    // tidb does not support serilization
    auto choice = d9();
    if (choice <= 4) {
        trans_1_commit = 1;
        trans_2_commit = 0;
    } else if (choice <= 8) {
        trans_1_commit = 0;
        trans_2_commit = 1;
    }
    else {
        trans_1_commit = 0;
        trans_2_commit = 0;
    }

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
    ofile.open("exec_trans_1.sql");
    for (auto& stmt:exec_trans_1_stmts)
        ofile << stmt << endl;
    ofile.close();

    ofile.open("exec_trans_2.sql");
    for (auto& stmt:exec_trans_2_stmts)
        ofile << stmt << endl;
    ofile.close();

    return 1;
}

void transaction_test::arrage_trans_for_tid_queue()
{
    for (int tid = 0; tid < trans_num; tid++)
        trans_arr[tid].stmt_num = 0;
    
    for (int i = 0; i < stmt_num; i++) {
        int tid = dx(trans_num) - 1; // [0, trans_num - 1]
        tid_queue.push_back(tid);
        trans_arr[tid].stmt_num++;
    }

    // each transaction at least has two statement (begin and commit/abort)
    for (int tid = 0; tid < trans_num; tid++) {
        if (trans_arr[tid].stmt_num >= 2)
            continue;
        
        tid_queue.push_back(tid);
        stmt_num++;
        trans_arr[tid].stmt_num++;
    }
    return;
}

void transaction_test::assign_trans_status()
{   
    if (is_serializable) {
        // interleaved transaction can be both committed
        for (int i = 0; i < trans_num; i ++) {
            trans_arr[i].status = d12() <=6 ? 1 : 2;
        }
        return;
    }
    
    // it is not serializable, so only one of the interleaved transactions can be commit

    // set transaction_status is zero
    for (int i = 0; i < trans_num; i++) {
        trans_arr[i].status = 0;
    }

    // set transaction_status is zero
    shared_ptr<int[]> transaction_start(new int[trans_num]);
    shared_ptr<int[]> transaction_end(new int[trans_num]);
    for (int i = 0; i < trans_num; i++) {
        transaction_start[i] = -1;
        transaction_end[i] = -1;
    }

    auto stmt_num = tid_queue.size();
    for (int i = 0; i < stmt_num; i ++) {
        auto tid = tid_queue[i];
        
        if (transaction_start[tid] == -1) {
            transaction_start[tid] = i;
            continue;
        }

        transaction_end[tid] = i;
    }

    for (int i = 0; i < trans_num; i++) {
        if (trans_arr[i].status != 0) // has been assigned
            continue;
        
        trans_arr[i].status = d12() <=6 ? 1 : 2; // assign
        if (trans_arr[i].status == 2) // abort, do not affect other transaction
            continue;
        
        // it is commit, so the interleaved one should be abort
        for (int j = i; j < trans_num; j++) { // the transations before i have been assigned
            if (transaction_start[j] < transaction_start[i] 
                    && transaction_start[i] < transaction_end[j])
                trans_arr[j].status = 2;
            else if (transaction_start[j] < transaction_end[i]
                    && transaction_end[i] < transaction_end[j])
                trans_arr[j].status = 2;
        }
    }

    return;
}

void transaction_test::gen_stmt_for_each_trans()
{    
    shared_ptr<int[]> stmt_pos_of_trans(new int[trans_num]);
    
    auto schema = get_schema(*options);
    for (int tid = 0; tid < trans_num; tid++) {
        trans_arr[tid].dut = dut_setup(*options);
        stmt_pos_of_trans[tid] = 0;
        
        // save 2 stmts for begin and commit/abort
        gen_trans_stmts(*options, random_file, schema, trans_arr[tid].stmt_num - 2, trans_arr[tid].stmts);
        trans_arr[tid].dut->wrap_stmts_as_trans(trans_arr[tid].stmts, trans_arr[tid].status == 1);
    }

    
    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto &stmt = trans_arr[tid].stmts[stmt_pos_of_trans[tid]];
        stmt_queue.push_back(stmt);
        stmt_pos_of_trans[tid]++;
    }

    // just debug
    int total_stmts_num = 0;
    for (int tid = 0; tid < trans_num; tid++) {
        total_stmts_num += trans_arr[tid].stmts.size();
    }
    if (total_stmts_num != tid_queue.size()) {
        cerr << "total_stmts_num is not equal to tid_queue.size(), some problems!!" << endl;
        exit(-1);
    }
}

bool transaction_test::schedule_last_stmt_pos(int stmt_index)
{
    // get tid
    auto tid = tid_queue[stmt_index];
    auto& stmt = stmt_queue[stmt_index];

    bool scheduled = false;
    for (int i = stmt_index + 1; i < stmt_num; i++) {
        auto other_tid = tid_queue[i];
        // has been scheduled after this transaction already.
        if (trans_arr[tid].scheduled_after_tid.count(other_tid))
            continue;
        
        auto& other_stmt = stmt_queue[i];
        if (!trans_arr[other_tid].dut->is_commit_abort_stmt(other_stmt))
            continue;
        
        trans_arr[tid].scheduled_after_tid.insert(other_tid);
        tid_queue.insert(tid_queue.begin() + i + 1, tid); // after the target stmt
        tid_queue.erase(tid_queue.begin() + stmt_index);

        stmt_queue.insert(stmt_queue.begin() + i + 1, stmt);
        stmt_queue.erase(stmt_queue.begin() + stmt_index);

        scheduled = true;
        break;
    }

    if (scheduled == true)
        return true;
    
    if (trans_arr[tid].scheduled_after_tid.count(-1))
        return false; // has scheduled to last before

    tid_queue.insert(tid_queue.begin() + stmt_num, tid);
    tid_queue.erase(tid_queue.begin() + stmt_index);
    stmt_queue.insert(stmt_queue.begin() + stmt_num, stmt);
    stmt_queue.erase(stmt_queue.begin() + stmt_index);

    trans_arr[tid].scheduled_after_tid.insert(-1);
    return true;
}

void transaction_test::trans_test()
{
    dut_reset_to_backup(*options);
    
    int stmt_index = 0;
    while(stmt_index < stmt_num) {
        auto tid = tid_queue[stmt_index];
        auto &stmt = stmt_queue[stmt_index];
        vector<string> output;
        
        try {
            trans_arr[tid].dut->test(stmt, &output);
            trans_arr[tid].executed_stmts.push_back(stmt);
            trans_arr[tid].stmt_outputs.push_back(output);

            executed_tid_queue.push_back(tid);
            executed_stmt_queue.push_back(stmt);
            stmt_index++;
        } catch(exception &e) {
            cerr << e.what() << endl;
            if (!trans_arr[tid].dut->is_commit_abort_stmt(stmt)) {
                stmt_index++; // just skip the stmt
                continue;
            }
            
            // the last stmt must be executed, because it is "commit" or "abort"
            auto scheduled = schedule_last_stmt_pos(stmt_index);
            if (scheduled)
                continue;
            
            if (trans_arr[tid].status == 2) {
                cerr << "something error, fail to abort in any position" << endl;
                exit(-1);
            }

            // change commit to abort;
            trans_arr[tid].status = 2;
            trans_arr[tid].stmts.erase(trans_arr[tid].stmts.begin());
            trans_arr[tid].stmts.pop_back();
            trans_arr[tid].dut->wrap_stmts_as_trans(trans_arr[tid].stmts, false);

            stmt_queue[stmt_index] = trans_arr[tid].stmts.back();
        }
    }

    // collect database information
    vector<string> table_names;
    auto schema = get_schema(*options);
    for (auto& table:schema->tables)
        table_names.push_back(table.ident());
    dut_get_content(*options, table_names, trans_content);
}

void transaction_test::normal_test()
{
    dut_reset_to_backup(*options);
    
    vector<int> trans_order;
    auto executed_stmt_size = executed_stmt_queue.size();
    for (int i = 0; i < executed_stmt_size; i++) {
        auto tid = executed_tid_queue[i];
        
        // just ignore the aborted stmt
        if (trans_arr[tid].status == 2)
            continue;
        
        // it is not the final stmt (commit)
        if (!trans_arr[tid].dut->is_commit_abort_stmt(executed_stmt_queue[i]))
            continue;
        
        trans_order.push_back(tid);
    }

    auto normal_dut = dut_setup(*options);
    for (auto tid:trans_order) {
        trans_arr[tid].normal_test_stmts = trans_arr[tid].executed_stmts;

        // erase "begin" and "commit"
        trans_arr[tid].normal_test_stmts.erase(trans_arr[tid].normal_test_stmts.begin());
        trans_arr[tid].normal_test_stmts.pop_back();

        auto normal_stmt_num = trans_arr[tid].normal_test_stmts.size();
        for (int i = 0; i < normal_stmt_num; i++) {
            auto& stmt = trans_arr[tid].normal_test_stmts[i];
            vector<string> output;

            normal_dut->test(stmt, &output);
            trans_arr[tid].normal_test_stmt_outputs.push_back(output);
        }
    }
    normal_dut.reset();

    // collect database information
    vector<string> table_names;
    auto schema = get_schema(*options);
    for (auto& table:schema->tables)
        table_names.push_back(table.ident());
    dut_get_content(*options, table_names, normal_content);
}

bool transaction_test::check_result()
{
    if (!compare_content(trans_content, normal_content)) {
        cerr << "trans_content is not equal to normal_content" << endl;
        return false;
    }

    for (auto i = 0; i < trans_num; i++) {
        if (trans_arr[i].status == 2) // just ignore abort
            continue;
        if (trans_arr[i].stmt_num <= 2) // just ignore the 0 stmts, and the one only have begin, commit
            continue;
        
        if (!compare_output(trans_arr[i].stmt_outputs, trans_arr[i].normal_test_stmt_outputs)) {
            cerr << "trans "<< i << " is not equal to normal one" << endl;
            return false;
        }
    }

    return true;
}

int transaction_test::test()
{
    trans_test();
    normal_test();
    if (check_result()) {
        return 0;
    }
}

transaction_test::transaction_test(map<string,string>& options_arg, 
                        file_random_machine* random_file_arg, 
                        bool is_seri)
{
    options = &options_arg;
    random_file = random_file_arg;
    
    trans_num = d6();
    stmt_num = trans_num * 5; // average statement number of each transaction is 5
    is_serializable = is_seri;

    trans_arr = new transaction[trans_num];

    arrage_trans_for_tid_queue();
    assign_trans_status();
    gen_stmt_for_each_trans();
}

transaction_test::~transaction_test()
{
    delete[] trans_arr;
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
        cerr << YELLOW << "initial seed as time(NULL)" << RESET << endl;
        smith::rng.seed(time(NULL));
    }
    
    // reset the target DBMS to initial state
    dut_reset(options); 
    generate_database(options, random_file);

    int i = TEST_TIME_FOR_EACH_DB;
    while (i--) {
        try {
            auto ret = old_transaction_test(options, random_file);
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
    regex optregex("--(help|postgres|sqlite|monetdb|random-seed|mysql-db|mysql-port)(?:=((?:.|\n)*))?");
  
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
            #ifdef HAVE_LIBMYSQLCLIENT
            "    --mysql-db=constr    mysql database name to send queries to (should used with" << endl << 
            "    --mysql-port=int     mysql server port number" << endl << 
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

    static itimerval itimer;
    // random_test(options);
    
    while (1) {
        child_timed_out = false;

        cerr << RED << "New Test Database --------------------------" << RESET << endl;
        child_pid = fork();
        if (child_pid == 0) {
            random_test(options);
            exit(0);
        }

        // timeout is ms
        itimer.it_value.tv_sec = DATABASE_TIMEOUT;
        itimer.it_value.tv_usec = 0; // us limit
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