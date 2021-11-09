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
};

shared_ptr<schema> get_schema(map<string,string>& options)
{
    shared_ptr<schema> schema;
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

void dut_test(map<string,string>& options, const string& stmt)
{
    auto dut = dut_setup(options);
    dut->test(stmt);
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
    dut->trans_test(*(data->trans_stmts), data->exec_trans_stmts, data->stmt_output);
    return NULL;
}

void normal_dut_trans_test(map<string,string>& options, 
                           vector<string>& stmts, 
                           vector<string>* exec_stmts,
                           vector<vector<string>>* stmt_output)
{
    auto dut = dut_setup(options);
    dut->trans_test(stmts, exec_stmts, stmt_output);
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
            cerr << "\n" << e.what() << "\n" << endl;
            cerr << s.str() << endl;
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
            cerr << "sizes are not equal" << endl;
            return false;
        }

        for (auto i = 0; i < size; i++) {
            if (seq_table_content[i] != con_table_content[i]) {
                cerr << "content are not equal" << endl;
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

    /* --- set up basic shared schema for two transaction --- */
    smith::rng.seed(options.count("seed") ? stoi(options["seed"]) : getpid());
    
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
                vector<string>& exec_trans_1_stmts, vector<string>& exec_trans_2_stmts)
{
    dut_reset_to_backup(options);

    vector<vector<string>> seq_1_output, seq_2_output;
    normal_dut_trans_test(options, exec_trans_1_stmts, NULL, &seq_1_output);
    normal_dut_trans_test(options, exec_trans_2_stmts, NULL, &seq_2_output);

    map<string, vector<string>> sequential_content;
    dut_get_content(options, table_names, sequential_content);

    if (!compare_content(table_names, concurrent_content, sequential_content)) {
        return false;
    }
    if (!compare_output(trans_1_output, seq_1_output)) {
        return false;
    }
    if (!compare_output(trans_2_output, seq_2_output)) {
        return false;
    }

    return true;
}

int transaction_test(map<string,string>& options, file_random_machine* random_file)
{
    auto schema = get_schema(options);

    vector<string> trans_1_rec;
    vector<string> trans_2_rec;

    // stage 4: generate sql statements for transaction (basic DDL (create), DML and DQL), and then execute them 1 -> 2
    cerr << YELLOW << "stage 4: generate SQL statements for transaction A and B" << RESET << endl;
    auto trans_1_stmt_num = 9 + d6(); // 10-15
    for (auto i = 0; i < trans_1_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        // interect_test(options, &trans_statement_factory, trans_1_rec);
        normal_test(options, schema, &trans_statement_factory, trans_1_rec);
    }

    // dut_reset_to_backup(options); // reset to prevent trans2 use the elements defined in trans1
    auto trans_2_stmt_num = 9 + d6(); // 10-15
    for (auto i = 0; i < trans_2_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        // interect_test(options, &trans_statement_factory, trans_2_rec);
        normal_test(options, schema, &trans_statement_factory, trans_2_rec);
    }

    // stage 5: reset to backup state, and then cocurrent transaction test
    cerr << YELLOW << "stage 5: cocurrently execute transaction A and B"  << RESET << endl;
    dut_reset_to_backup(options);
    thread_data data_1, data_2;
    vector<string> exec_trans_1_stmts, exec_trans_2_stmts;
    vector<vector<string>> trans_1_output, trans_2_output;

    data_1.options = &options;
    data_1.trans_stmts = &trans_1_rec;
    data_1.exec_trans_stmts = &exec_trans_1_stmts;
    data_1.stmt_output = &trans_1_output;

    data_2.options = &options;
    data_2.trans_stmts = &trans_2_rec;
    data_2.exec_trans_stmts = &exec_trans_2_stmts;
    data_2.stmt_output = &trans_2_output;

    pthread_t tid_1, tid_2;
    pthread_create(&tid_1, NULL, dut_trans_test, &data_1);
    pthread_create(&tid_2, NULL, dut_trans_test, &data_2);

    pthread_join(tid_1, NULL);
    pthread_join(tid_2, NULL);

    // collect database information
    map<string, vector<string>> concurrent_content;
    vector<string> table_names;
    for (auto& table:schema->tables) {
        table_names.push_back(table.ident());
    }
    dut_get_content(options, table_names, concurrent_content);

    // stage 6: reset to backup state, and then sequential transaction test
    cerr << YELLOW << "stage 6.1: first comparison: A -> B" << RESET << endl;
    if (seq_res_comp(options, table_names, concurrent_content, 
                trans_1_output, trans_2_output, 
                exec_trans_1_stmts, exec_trans_2_stmts)) {
        return 0;
    }
    cerr << YELLOW << "stage 6.2: second comparison: B -> A" << RESET << endl;
    if (seq_res_comp(options, table_names, concurrent_content, 
                trans_2_output, trans_1_output, 
                exec_trans_2_stmts, exec_trans_1_stmts)) {
        return 0;
    }
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
    
    // reset the target DBMS to initial state
    dut_reset(options); 

    generate_database(options, random_file);
    while (1) {
        auto ret = transaction_test(options, random_file);
        if (ret == 1)
            break;
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

    random_test(options);

    return 0;
}