#include <stdexcept>
#include <cassert>
#include <cstring>
#include "mysql.hh"
#include <iostream>
#include <set>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

static regex e_unknown_database(".*Unknown database.*");
static regex e_crash(".*Lost connection.*");
  
extern "C"  {
#include <mysql/mysql.h>
#include <unistd.h>
}

mysql_connection::mysql_connection(string db, unsigned int port)
{
    test_db = db;
    test_port = port;
    
    if (!mysql_init(&mysql))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in mysql_connection!");

    // host null: the local host
    // user null: the current user (root)
    // password null: blank (empty) password field
    if (!mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, db.c_str(), port, NULL, 0)) {
        string err = mysql_error(&mysql);
        if (regex_match(err, e_unknown_database)) {
            cerr << db + " does not exist, use default db" << endl;
            if (!mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, NULL, port, NULL, 0))
                throw std::runtime_error(string(mysql_error(&mysql)) + " in mysql_connection!");
            
            cerr << "create database " + test_db << endl;
            string create_sql = "create database " + test_db + "; ";
            if (mysql_real_query(&mysql, create_sql.c_str(), create_sql.size()))
                throw std::runtime_error(string(mysql_error(&mysql)) + " in mysql_connection!");
            auto res = mysql_store_result(&mysql);
            mysql_free_result(res);

            cerr << "use database" + test_db << endl;
            string use_sql = "use " + test_db + "; ";
            if (mysql_real_query(&mysql, use_sql.c_str(), use_sql.size()))
                throw std::runtime_error(string(mysql_error(&mysql)) + " in mysql_connection!");
            res = mysql_store_result(&mysql);
            mysql_free_result(res);
        }
        else
            throw std::runtime_error(string(mysql_error(&mysql)) + " in mysql_connection!");
    }
}

mysql_connection::~mysql_connection()
{
    mysql_close(&mysql);
}

schema_mysql::schema_mysql(string db, unsigned int port)
  : mysql_connection(db, port)
{
    string get_table_query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
        WHERE TABLE_SCHEMA='" + db + "' AND \
              TABLE_TYPE='BASE TABLE' ORDER BY 1;";

    cerr << "Loading tables...";
    if (mysql_real_query(&mysql, get_table_query.c_str(), get_table_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_mysql!");
    
    auto result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", true, true);
        tables.push_back(tab);
    }
    mysql_free_result(result);
    cerr << "done." << endl;

    cerr << "Loading indexes...";
    string get_index_query = "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS \
                WHERE TABLE_SCHEMA='" + db + "' AND \
                    NON_UNIQUE=1 AND \
                    INDEX_NAME <> COLUMN_NAME AND \
                    INDEX_NAME <> 'PRIMARY' ORDER BY 1;";
    if (mysql_real_query(&mysql, get_index_query.c_str(), get_index_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_mysql!");

    result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        indexes.push_back(row[0]);
    }
    mysql_free_result(result);
    cerr << "done." << endl;

    cerr << "Loading columns and constraints...";
    for (auto& t : tables) {
        string get_column_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_NAME='" + t.ident() + "' AND \
                    TABLE_SCHEMA='" + db + "'  ORDER BY ORDINAL_POSITION;";
        if (mysql_real_query(&mysql, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_mysql!");
        result = mysql_store_result(&mysql);
        while (auto row = mysql_fetch_row(result)) {
            column c(row[0], sqltype::get(row[1]));
            t.columns().push_back(c);
        }
        mysql_free_result(result);
    }
    cerr << "done." << endl;

#define BINOP(n, a, b, r) do {\
    op o(#n, \
         sqltype::get(#a), \
         sqltype::get(#b), \
         sqltype::get(#r)); \
    register_operator(o); \
} while(0)

    BINOP(||, TEXT, TEXT, TEXT);
    BINOP(*, INTEGER, INTEGER, INTEGER);
    BINOP(/, INTEGER, INTEGER, INTEGER);
    BINOP(%, INTEGER, INTEGER, INTEGER);

    BINOP(+, INTEGER, INTEGER, INTEGER);
    BINOP(-, INTEGER, INTEGER, INTEGER);

    BINOP(>>, INTEGER, INTEGER, INTEGER);
    BINOP(<<, INTEGER, INTEGER, INTEGER);

    BINOP(&, INTEGER, INTEGER, INTEGER);
    BINOP(|, INTEGER, INTEGER, INTEGER);

    BINOP(<, INTEGER, INTEGER, BOOLEAN);
    BINOP(<=, INTEGER, INTEGER, BOOLEAN);
    BINOP(>, INTEGER, INTEGER, BOOLEAN);
    BINOP(>=, INTEGER, INTEGER, BOOLEAN);

    BINOP(=, INTEGER, INTEGER, BOOLEAN);
    BINOP(<>, INTEGER, INTEGER, BOOLEAN);
    BINOP(IS, INTEGER, INTEGER, BOOLEAN);
    BINOP(IS NOT, INTEGER, INTEGER, BOOLEAN);

    BINOP(AND, BOOLEAN, BOOLEAN, BOOLEAN);
    BINOP(OR, BOOLEAN, BOOLEAN, BOOLEAN);
  
#define FUNC(n, r) do {							\
    routine proc("", "", sqltype::get(#r), #n);				\
    register_routine(proc);						\
} while(0)

#define FUNC1(n, r, a) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    register_routine(proc);						\
} while(0)

#define FUNC2(n, r, a, b) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    proc.argtypes.push_back(sqltype::get(#b));				\
    register_routine(proc);						\
} while(0)

#define FUNC3(n, r, a, b, c) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    proc.argtypes.push_back(sqltype::get(#b));				\
    proc.argtypes.push_back(sqltype::get(#c));				\
    register_routine(proc);						\
} while(0)

    FUNC1(abs, INTEGER, INTEGER);
    FUNC1(abs, REAL, REAL);
    FUNC1(hex, TEXT, TEXT);
    FUNC1(length, INTEGER, TEXT);
    FUNC1(lower, TEXT, TEXT);
    FUNC1(ltrim, TEXT, TEXT);
    FUNC1(quote, TEXT, TEXT);
    FUNC1(round, INTEGER, REAL);
    FUNC1(rtrim, TEXT, TEXT);
    FUNC1(trim, TEXT, TEXT);
    FUNC1(upper, TEXT, TEXT);

    FUNC2(instr, INTEGER, TEXT, TEXT);
    FUNC2(round, REAL, REAL, INTEGER);
    FUNC2(substr, TEXT, TEXT, INTEGER);

    FUNC3(substr, TEXT, TEXT, INTEGER, INTEGER);
    FUNC3(replace, TEXT, TEXT, TEXT, TEXT);

#define AGG1(n, r, a) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    register_aggregate(proc);						\
} while(0)

#define AGG3(n, r, a, b, c, d) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    proc.argtypes.push_back(sqltype::get(#b));				\
    proc.argtypes.push_back(sqltype::get(#c));				\
    proc.argtypes.push_back(sqltype::get(#d));				\
    register_aggregate(proc);						\
} while(0)

#define AGG(n, r) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    register_aggregate(proc);						\
} while(0)

    AGG1(avg, INTEGER, INTEGER);
    AGG1(avg, REAL, REAL);
    AGG(count, INTEGER);
    AGG1(count, INTEGER, REAL);
    AGG1(count, INTEGER, TEXT);
    AGG1(count, INTEGER, INTEGER);

    AGG1(max, REAL, REAL);
    AGG1(max, INTEGER, INTEGER);
    AGG1(min, REAL, REAL);
    AGG1(min, INTEGER, INTEGER);
    AGG1(sum, REAL, REAL);
    AGG1(sum, INTEGER, INTEGER);

#define WIN(n, r) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    register_windows(proc);						\
} while(0)

#define WIN1(n, r, a) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    register_windows(proc);						\
} while(0)

#define WIN2(n, r, a, b) do {						\
    routine proc("", "", sqltype::get(#r), #n);				\
    proc.argtypes.push_back(sqltype::get(#a));				\
    proc.argtypes.push_back(sqltype::get(#b));				\
    register_windows(proc);						\
} while(0)

#ifndef TEST_CLICKHOUSE
    // ranking window function
    WIN(CUME_DIST, REAL);
    WIN(DENSE_RANK, INTEGER);
    WIN1(NTILE, INTEGER, INTEGER);
    WIN(RANK, INTEGER);
    WIN(ROW_NUMBER, INTEGER);
    WIN(PERCENT_RANK, REAL);

    // value window function
    WIN1(FIRST_VALUE, INTEGER, INTEGER);
    WIN1(FIRST_VALUE, REAL, REAL);
    WIN1(FIRST_VALUE, TEXT, TEXT);
    WIN1(LAST_VALUE, INTEGER, INTEGER);
    WIN1(LAST_VALUE, REAL, REAL);
    WIN1(LAST_VALUE, TEXT, TEXT);
    WIN1(LAG, INTEGER, INTEGER);
    WIN1(LAG, REAL, REAL);
    WIN1(LAG, TEXT, TEXT);
    WIN2(LEAD, INTEGER, INTEGER, INTEGER);
    WIN2(LEAD, REAL, REAL, INTEGER);
    WIN2(LEAD, TEXT, TEXT, INTEGER);
#endif
    
    booltype = sqltype::get("BOOLEAN");
    inttype = sqltype::get("INTEGER");

    internaltype = sqltype::get("internal");
    arraytype = sqltype::get("ARRAY");

    true_literal = "1 = 1";
    false_literal = "0 <> 0";

    generate_indexes();

    // enable "atomic_subselect" use specific tables
    for (auto &t: tables) {
        set<sqltype *> type_set_in_table;
        for (auto &c: t.columns()) { // filter repeated column types
            assert(c.type);
            type_set_in_table.insert(c.type);
        }

        for (auto uniq_type : type_set_in_table) {
            tables_with_columns_of_type.insert(pair<sqltype*, table*>(uniq_type, &t));
        }
    }

    // enable operator
    for (auto &o: operators) {
        operators_returning_type.insert(pair<sqltype*, op*>(o.result, &o));
    }

    // enable aggregate function
    for(auto &r: aggregates) {
        assert(r.restype);
        aggregates_returning_type.insert(pair<sqltype*, routine*>(r.restype, &r));
    }

    // enable routine function
    for(auto &r: routines) {
        assert(r.restype);
        routines_returning_type.insert(pair<sqltype*, routine*>(r.restype, &r));
        if(!r.argtypes.size())
            parameterless_routines_returning_type.insert(pair<sqltype*, routine*>(r.restype, &r));
    }

    // enable window function
    for(auto &r: windows) {
        assert(r.restype);
        windows_returning_type.insert(pair<sqltype*, routine*>(r.restype, &r));
    }
}

dut_mysql::dut_mysql(string db, unsigned int port)
  : mysql_connection(db, port)
{
}

void dut_mysql::test(const std::string &stmt, std::vector<std::string>* output)
{
    if (mysql_real_query(&mysql, stmt.c_str(), stmt.size())) {
        string err = mysql_error(&mysql);
        if (regex_match(err, e_crash)) {
            cerr << "find a crash: " + err << endl;
            exit(166);
        }
        throw std::runtime_error(err + " in mysql::test"); 
    }

    auto result = mysql_store_result(&mysql);
    if (result) {
        auto column_num = mysql_num_fields(result);
        while (auto row = mysql_fetch_row(result)) {
            for (int i = 0; i < column_num; i++) {
                string str;
                if (row[i] == NULL)
                    str = "NULL";
                else
                    str = row[i];
                output->push_back(str);
            }
            output->push_back("\n");
        }
    }
    mysql_free_result(result);
}

void dut_mysql::reset(void)
{
    string drop_sql = "drop database if exists " + test_db + "; ";
    if (mysql_real_query(&mysql, drop_sql.c_str(), drop_sql.size())) {
        string err = mysql_error(&mysql);
        throw std::runtime_error(err + " in mysql::reset");
    }
    auto res_sql = mysql_store_result(&mysql);
    mysql_free_result(res_sql);

    string create_sql = "create database " + test_db + "; ";
    if (mysql_real_query(&mysql, create_sql.c_str(), create_sql.size())) {
        string err = mysql_error(&mysql);
        throw std::runtime_error(err + " in mysql::reset");
    }
    res_sql = mysql_store_result(&mysql);
    mysql_free_result(res_sql);

    string use_sql = "use " + test_db + "; ";
    if (mysql_real_query(&mysql, use_sql.c_str(), use_sql.size())) {
        string err = mysql_error(&mysql);
        throw std::runtime_error(err + " in mysql::reset");
    }
    res_sql = mysql_store_result(&mysql);
    mysql_free_result(res_sql);
}

void dut_mysql::backup(void)
{
    string mysql_dump = "mysqldump -h 127.0.0.1 -P " + to_string(test_port) + " -u root " + test_db + " > /tmp/mysql_bk.sql";
    system(mysql_dump.c_str());
}

void dut_mysql::trans_test(const std::vector<std::string> &stmt_vec
                          , std::vector<std::string>* exec_stmt_vec
                          , vector<vector<string>>* output
                          , int commit_or_not)
{
    if (commit_or_not != 2) {
        cerr << pthread_self() << ": START TRANSACTION" << endl;
        test("START TRANSACTION;");
    }

    auto size = stmt_vec.size();
    for (auto i = 0; i < size; i++) {
        auto &stmt = stmt_vec[i];
        int try_time = 0;
        vector<string> stmt_output;
        while (1) {
            try {
                if (try_time >= MAX_TRY_TIME) {
                    cerr << pthread_self() << ": " << i << " skip " << stmt.substr(0, 20) << endl;
                    break;
                }
                try_time++;
                test(stmt, &stmt_output);
                if (exec_stmt_vec != NULL)
                    exec_stmt_vec->push_back(stmt);
                if (output != NULL)
                    output->push_back(stmt_output);
                cerr << pthread_self() << ": " << i << endl;
                break; // success and then break while loop
            } catch(std::exception &e) { // ignore runtime error
                string err = e.what();
                if (err.find("locked") != string::npos) {
                    continue; // not break and continue to test 
                }
                cerr << pthread_self() << ": " << i << " " << err << endl;
                break;
            }
        }
    }
    
    if (commit_or_not == 2)
        return;
    
    string last_sql;
    if (commit_or_not == 1) 
        last_sql = "COMMIT";
    else
        last_sql = "ROLLBACK";
    
    cerr << pthread_self() << " " << last_sql << endl;
    while (1) {
        try{
            test(last_sql);
            break;
        }catch(std::exception &e) { // ignore runtime error
            string err = e.what();
            if (err.find("locked") != string::npos) 
                continue; // not break and continue to test 
            cerr << pthread_self() << " " << err << endl;
            break;
        }
    }
    cerr << pthread_self() << ": " << last_sql << " done" << endl;
    return;
}

void dut_mysql::reset_to_backup(void)
{
    reset();
    string bk_file = "/tmp/mysql_bk.sql";
    if (access(bk_file.c_str(), F_OK ) == -1) 
        return;
    
    string mysql_source = "mysql -h 127.0.0.1 -P " + to_string(test_port) + " -u root -D " + test_db + " < /tmp/mysql_bk.sql";
    system(mysql_source.c_str());
}

void dut_mysql::get_content(vector<string>& tables_name, map<string, vector<string>>& content)
{
    for (auto& table:tables_name) {
        vector<string> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";

        if (mysql_real_query(&mysql, query.c_str(), query.size())) {
            string err = mysql_error(&mysql);
            throw std::runtime_error(err + " in mysql::get_content");
        }

        auto result = mysql_store_result(&mysql);
        if (result) {
            auto column_num = mysql_num_fields(result);
            while (auto row = mysql_fetch_row(result)) {
                for (int i = 0; i < column_num; i++) {
                    string str;
                    if (row[i] == NULL)
                        str = "NULL";
                    else
                        str = row[i];
                    table_content.push_back(str);
                }
                table_content.push_back("\n");
            }
        }
        mysql_free_result(result);

        content[table] = table_content;
    }
}

