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

struct thread_data {
    map<string,string>* options;
    vector<string>* trans_stmts;
    vector<string>* exec_trans_stmts;
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
    dut->trans_test(*(data->trans_stmts), *(data->exec_trans_stmts));
    pthread_exit(NULL);
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
        // cerr << "\n" << e.what() << "\n" << endl;
        string err = e.what();
        if (err.find("syntax") != string::npos)
            cerr << s.str() << endl;
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
        // cerr << "\n" << e.what() << "\n" << endl;
        string err = e.what();
        if (err.find("syntax") != string::npos)
            cerr << s.str() << endl;
        normal_test(options, schema, tmp_statement_factory, rec_vec);
    }
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

    vector<string> stage_1_rec;
    vector<string> stage_2_rec;
    vector<string> trans_1_rec;
    vector<string> trans_2_rec;

    /* --- set up basic shared schema for two transaction --- */
    smith::rng.seed(options.count("seed") ? stoi(options["seed"]) : getpid());

    // stage 1: DDL stage (create, alter, drop)
    cerr << "stage1: generate the shared database" << endl;
    auto ddl_stmt_num = d6() + 1; // at least 2 statements to create 2 tables
    for (auto i = 0; i < ddl_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;

        interect_test(options, &ddl_statement_factory, stage_1_rec); // has disabled the not null, check and unique clause 
    }
    ofstream o1("stage_1.sql");
    for (auto &stmt : stage_1_rec) {
        o1 << stmt << endl;
    }
    o1.close();

    // stage 2: basic DML stage (only insert),
    cerr << "stage2: insert data into the database" << endl;
    auto basic_dml_stmt_num = 20 + d20(); // 20-40 statements to insert data
    auto schema = get_schema(options); // schema will not change in this stage
    for (auto i = 0; i < basic_dml_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        normal_test(options, schema, &basic_dml_statement_factory, stage_2_rec);
    }
    ofstream o2("stage_2.sql");
    for (auto &stmt : stage_2_rec) {
        o2 << stmt << endl;
    }
    o2.close();    

    // stage 3: backup database
    cerr << "stage3: backup the database" << endl;
    dut_backup(options);

    // stage 4: generate sql statements for transaction (basic DDL (create), DML and DQL), and then execute them 1 -> 2
    cerr << "stage4: generate SQL statements for transaction A and B, and then execute A -> B" << endl;
    auto trans_1_stmt_num = 9 + d6(); // 10-15
    for (auto i = 0; i < trans_1_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        interect_test(options, &trans_statement_factory, trans_1_rec);
    }
    // TODO: now we can get the sequential result of transaction 1
    ofstream o3("trans_1.sql");
    for (auto &stmt : trans_1_rec) {
        o3 << stmt << endl;
    }
    o3.close();

    dut_reset_to_backup(options); // reset to prevent trans2 use the elements defined in trans1
    auto trans_2_stmt_num = 9 + d6(); // 10-15
    for (auto i = 0; i < trans_2_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        interect_test(options, &trans_statement_factory, trans_2_rec);
    }
    // TODO: now we can get the sequential result of transaction 2
    ofstream o4("trans_2.sql");
    for (auto &stmt : trans_2_rec) {
        o4 << stmt << endl;
    }
    o4.close();

    // stage 5: reset to backup state
    cerr << "stage5: reset to the backup statement" << endl;
    dut_reset_to_backup(options);

    // stage 6: cocurrent transaction test
    cerr << "stage6: cocurrently execute transaction A and B" << endl;
    thread_data data_1, data_2;
    vector<string> exec_trans_1_stmts, exec_trans_2_stmts;
    data_1.options = &options;
    data_1.trans_stmts = &trans_1_rec;
    data_1.exec_trans_stmts = &exec_trans_1_stmts;

    data_2.options = &options;
    data_2.trans_stmts = &trans_2_rec;
    data_2.exec_trans_stmts = &exec_trans_2_stmts;

    pthread_t tid_1, tid_2;
    pthread_create(&tid_1, NULL, dut_trans_test, &data_1);
    pthread_create(&tid_2, NULL, dut_trans_test, &data_2);

    pthread_join(tid_1, NULL);
    pthread_join(tid_2, NULL);
}