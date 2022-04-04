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

#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

mysql_connection::mysql_connection(string db, unsigned int port)
{
    test_db = db;
    test_port = port;
    
    if (!mysql_init(&mysql))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);

    // password null: blank (empty) password field
    if (mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, test_db.c_str(), test_port, NULL, 0)) 
        return; // success
    
    string err = mysql_error(&mysql);
    if (!regex_match(err, e_unknown_database))
        throw std::runtime_error("BUG!!!" + string(mysql_error(&mysql)) + "\nLocation: " + debug_info);

    // error caused by unknown database, so create one
    std::cerr << test_db + " does not exist, use default db" << endl;
    if (!mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, NULL, port, NULL, 0))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    std::cerr << "create database " + test_db << endl;
    string create_sql = "create database " + test_db + "; ";
    if (mysql_real_query(&mysql, create_sql.c_str(), create_sql.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    auto res = mysql_store_result(&mysql);
    mysql_free_result(res);

    std::cerr << "use database " + test_db << endl;
    string use_sql = "use " + test_db + "; ";
    if (mysql_real_query(&mysql, use_sql.c_str(), use_sql.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    res = mysql_store_result(&mysql);
    mysql_free_result(res);
}

mysql_connection::~mysql_connection()
{
    mysql_close(&mysql);
}

schema_mysql::schema_mysql(string db, unsigned int port)
  : mysql_connection(db, port)
{
    // Loading tables...;
    string get_table_query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
        WHERE TABLE_SCHEMA='" + db + "' AND \
              TABLE_TYPE='BASE TABLE' ORDER BY 1;";
    
    if (mysql_real_query(&mysql, get_table_query.c_str(), get_table_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    auto result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", true, true);
        tables.push_back(tab);
    }
    mysql_free_result(result);

    // Loading views...;
    string get_view_query = "select distinct table_name from information_schema.views \
        where table_schema='" + db + "' order by 1;";
    if (mysql_real_query(&mysql, get_view_query.c_str(), get_view_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", false, false);
        tables.push_back(tab);
    }
    mysql_free_result(result);

    // Loading indexes...;
    string get_index_query = "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS \
                WHERE TABLE_SCHEMA='" + db + "' AND \
                    NON_UNIQUE=1 AND \
                    INDEX_NAME <> COLUMN_NAME AND \
                    INDEX_NAME <> 'PRIMARY' ORDER BY 1;";
    if (mysql_real_query(&mysql, get_index_query.c_str(), get_index_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);

    result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        indexes.push_back(row[0]);
    }
    mysql_free_result(result);

    // Loading columns and constraints...;
    for (auto& t : tables) {
        string get_column_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_NAME='" + t.ident() + "' AND \
                    TABLE_SCHEMA='" + db + "'  ORDER BY ORDINAL_POSITION;";
        if (mysql_real_query(&mysql, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info + "\nTable: " + t.ident());
        result = mysql_store_result(&mysql);
        while (auto row = mysql_fetch_row(result)) {
            column c(row[0], sqltype::get(row[1]));
            t.columns().push_back(c);
        }
        mysql_free_result(result);
    }

    booltype = sqltype::get("tinyint");
    inttype = sqltype::get("int");
    realtype = sqltype::get("double");
    texttype = sqltype::get("text");

#define BINOP(n, a, b, r) do {\
    op o(#n, a, b, r); \
    register_operator(o); \
} while(0)

    BINOP(||, texttype, texttype, texttype);
    BINOP(*, inttype, inttype, inttype);
    BINOP(/, inttype, inttype, inttype);
    BINOP(%, inttype, inttype, inttype);

    BINOP(+, inttype, inttype, inttype);
    BINOP(-, inttype, inttype, inttype);

    BINOP(>>, inttype, inttype, inttype);
    BINOP(<<, inttype, inttype, inttype);

    BINOP(&, inttype, inttype, inttype);
    BINOP(|, inttype, inttype, inttype);

    BINOP(<, inttype, inttype, booltype);
    BINOP(<=, inttype, inttype, booltype);
    BINOP(>, inttype, inttype, booltype);
    BINOP(>=, inttype, inttype, booltype);

    BINOP(=, inttype, inttype, booltype);
    BINOP(<>, inttype, inttype, booltype);

    BINOP(and, booltype, booltype, booltype);
    BINOP(or, booltype, booltype, booltype);
  
#define FUNC(n, r) do {							\
    routine proc("", "", r, #n);				\
    register_routine(proc);						\
} while(0)

#define FUNC1(n, r, a) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    register_routine(proc);						\
} while(0)

#define FUNC2(n, r, a, b) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    register_routine(proc);						\
} while(0)

#define FUNC3(n, r, a, b, c) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    proc.argtypes.push_back(c);				\
    register_routine(proc);						\
} while(0)

    FUNC1(abs, inttype, inttype);
    FUNC1(abs, realtype, realtype);
    FUNC1(hex, texttype, texttype);
    FUNC1(length, inttype, texttype);
    FUNC1(lower, texttype, texttype);
    FUNC1(ltrim, texttype, texttype);
    FUNC1(quote, texttype, texttype);
    FUNC1(round, inttype, realtype);
    FUNC1(rtrim, texttype, texttype);
    FUNC1(trim, texttype, texttype);
    FUNC1(upper, texttype, texttype);

    FUNC2(instr, inttype, texttype, texttype);
    FUNC2(round, realtype, realtype, inttype);
    FUNC2(substr, texttype, texttype, inttype);

    FUNC3(substr, texttype, texttype, inttype, inttype);
    FUNC3(replace, texttype, texttype, texttype, texttype);

#define AGG1(n, r, a) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    register_aggregate(proc);						\
} while(0)

#define AGG3(n, r, a, b, c, d) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    proc.argtypes.push_back(c);				\
    proc.argtypes.push_back(d);				\
    register_aggregate(proc);						\
} while(0)

#define AGG(n, r) do {						\
    routine proc("", "", r, #n);				\
    register_aggregate(proc);						\
} while(0)

    AGG1(avg, inttype, inttype);
    AGG1(avg, realtype, realtype);
    AGG(count, inttype);
    AGG1(count, inttype, realtype);
    AGG1(count, inttype, texttype);
    AGG1(count, inttype, inttype);

    AGG1(max, realtype, realtype);
    AGG1(max, inttype, inttype);
    AGG1(min, realtype, realtype);
    AGG1(min, inttype, inttype);
    AGG1(sum, realtype, realtype);
    AGG1(sum, inttype, inttype);

#define WIN(n, r) do {						\
    routine proc("", "", r, #n);				\
    register_windows(proc);						\
} while(0)

#define WIN1(n, r, a) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    register_windows(proc);						\
} while(0)

#define WIN2(n, r, a, b) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    register_windows(proc);						\
} while(0)

#ifndef TEST_CLICKHOUSE
    // ranking window function
    WIN(CUME_DIST, realtype);
    WIN(DENSE_RANK, inttype);
    WIN1(NTILE, inttype, inttype);
    WIN(RANK, inttype);
    WIN(ROW_NUMBER, inttype);
    WIN(PERCENT_RANK, realtype);

    // value window function
    WIN1(FIRST_VALUE, inttype, inttype);
    WIN1(FIRST_VALUE, realtype, realtype);
    WIN1(FIRST_VALUE, texttype, texttype);
    WIN1(LAST_VALUE, inttype, inttype);
    WIN1(LAST_VALUE, realtype, realtype);
    WIN1(LAST_VALUE, texttype, texttype);
    // WIN1(LAG, inttype, inttype);
    // WIN1(LAG, realtype, realtype);
    // WIN1(LAG, texttype, texttype);
    // WIN2(LEAD, inttype, inttype, inttype);
    // WIN2(LEAD, realtype, realtype, inttype);
    // WIN2(LEAD, texttype, texttype, inttype);
#endif

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
    sent_sql = "";
    has_sent_sql = false;
    txn_abort = false;
    thread_id = mysql_thread_id(&mysql);
    // block_test("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
}

static unsigned long long get_cur_time_ms(void) {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

bool dut_mysql::check_whether_block()
{
    dut_mysql another_dut(test_db, test_port);
    string get_block_tid = "SELECT waiting_pid FROM sys.innodb_lock_waits;";
    vector<string> output;
    another_dut.block_test(get_block_tid, &output);
    
    // check output
    string tid_str = to_string(thread_id);
    auto output_size = output.size();
    for (int i = 0; i < output_size; i++) {
        if (tid_str == output[i])
            return true;
    }

    return false;
}

void dut_mysql::block_test(const std::string &stmt, std::vector<std::string>* output, int* affected_row_num)
{
    if (mysql_real_query(&mysql, stmt.c_str(), stmt.size())) {
        string err = mysql_error(&mysql);
        if (regex_match(err, e_crash)) {
            throw std::runtime_error("BUG!!! " + err + " in mysql::test"); 
        }
        throw std::runtime_error(err + " in mysql::test"); 
    }

    if (affected_row_num)
        *affected_row_num = mysql_affected_rows(&mysql);

    auto result = mysql_store_result(&mysql);
    if (mysql_errno(&mysql) != 0) {
        string err = mysql_error(&mysql);
        throw std::runtime_error("mysql_store_result fails, stmt skipped: " + err + "\nLocation: " + debug_info); 
    }

    if (output && result) {
        auto row_num = mysql_num_rows(result);
        if (row_num == 0) {
            mysql_free_result(result);
            return;
        }

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

    return;
}

void dut_mysql::test(const std::string &stmt, std::vector<std::string>* output, int* affected_row_num)
{
    net_async_status status;
    if (txn_abort == true) {
        auto tmp_stmt = stmt;
        if (stmt == "COMMIT;") 
            throw std::runtime_error("txn aborted, can only rollback \nLocation: " + debug_info);
        if (stmt == "ROLLBACK;")
            return;
        throw std::runtime_error("txn aborted, stmt skipped \nLocation: " + debug_info);
    }

    if (has_sent_sql == false) {
        status = mysql_real_query_nonblocking(&mysql, stmt.c_str(), stmt.size());
        sent_sql = stmt;
        has_sent_sql = true;
    }

    if (sent_sql != stmt) 
        throw std::runtime_error("sent sql stmt changed in " + debug_info + 
            "\nsent_sql: " + sent_sql.substr(0, sent_sql.size() > 20 ? 20 : sent_sql.size()) +
            "\nstmt: " + stmt.substr(0, stmt.size() > 20 ? 20 : stmt.size())); 

    auto begin_time = get_cur_time_ms();
    while (1) {
        status = mysql_real_query_nonblocking(&mysql, stmt.c_str(), stmt.size());
        if (status != NET_ASYNC_NOT_READY) {
            break;
        }
            
        auto cur_time = get_cur_time_ms();
        if (cur_time - begin_time >= MYSQL_STMT_BLOCK_MS) {
            auto blocked = check_whether_block();
            if (blocked == true)
                throw std::runtime_error("blocked in " + debug_info); 
            begin_time = cur_time;
        }
    }

    if (status == NET_ASYNC_ERROR) {
        string err = mysql_error(&mysql);
        has_sent_sql = false;
        sent_sql = "";
        if (err.find("Deadlock found") != string::npos) 
            txn_abort = true;
        throw std::runtime_error("NET_ASYNC_ERROR(skipped): " + err + "\nLocation: " + debug_info); 
    }

    if (affected_row_num)
        *affected_row_num = mysql_affected_rows(&mysql);

    auto result = mysql_store_result(&mysql);
    if (mysql_errno(&mysql) != 0) {
        string err = mysql_error(&mysql);
        has_sent_sql = false;
        sent_sql = "";
        if (err.find("Deadlock found") != string::npos) 
            txn_abort = true;
        throw std::runtime_error("mysql_store_result fails, stmt skipped: " + err + "\nLocation: " + debug_info); 
    }

    if (output && result) {
        auto row_num = mysql_num_rows(result);
        if (row_num == 0) {
            mysql_free_result(result);
            has_sent_sql = false;
            sent_sql = "";
            return;
        }

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

    has_sent_sql = false;
    sent_sql = "";
    return;
}

void dut_mysql::reset(void)
{
    string drop_sql = "drop database if exists " + test_db + "; ";
    if (mysql_real_query(&mysql, drop_sql.c_str(), drop_sql.size())) {
        string err = mysql_error(&mysql);
        throw std::runtime_error(err + "\nLocation: " + debug_info);
    }
    auto res_sql = mysql_store_result(&mysql);
    mysql_free_result(res_sql);

    string create_sql = "create database " + test_db + "; ";
    if (mysql_real_query(&mysql, create_sql.c_str(), create_sql.size())) {
        string err = mysql_error(&mysql);
        throw std::runtime_error(err + "\nLocation: " + debug_info);
    }
    res_sql = mysql_store_result(&mysql);
    mysql_free_result(res_sql);

    string use_sql = "use " + test_db + "; ";
    if (mysql_real_query(&mysql, use_sql.c_str(), use_sql.size())) {
        string err = mysql_error(&mysql);
        throw std::runtime_error(err + "\nLocation: " + debug_info);
    }
    res_sql = mysql_store_result(&mysql);
    mysql_free_result(res_sql);
}

void dut_mysql::backup(void)
{
    string mysql_dump = "/usr/local/mysql/bin/mysqldump -h 127.0.0.1 -P " + to_string(test_port) + " -u root " + test_db + " > /tmp/mysql_bk.sql";
    int ret = system(mysql_dump.c_str());
    if (ret != 0) {
        std::cerr << "backup fail \nLocation: " + debug_info << endl;
        throw std::runtime_error("backup fail \nLocation: " + debug_info); 
    }
}

void dut_mysql::reset_to_backup(void)
{
    reset();
    string bk_file = "/tmp/mysql_bk.sql";
    if (access(bk_file.c_str(), F_OK ) == -1) 
        return;
    
    mysql_close(&mysql);
    
    string mysql_source = "/usr/local/mysql/bin/mysql -h 127.0.0.1 -P " + to_string(test_port) + " -u root -D " + test_db + " < /tmp/mysql_bk.sql";
    if (system(mysql_source.c_str()) == -1) 
        throw std::runtime_error(string("system() error, return -1") + "\nLocation: " + debug_info);

    if (!mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, test_db.c_str(), test_port, NULL, 0)) 
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
}

int dut_mysql::save_backup_file(string path)
{
    string cp_cmd = "cp /tmp/mysql_bk.sql " + path;
    return system(cp_cmd.c_str());
}

void dut_mysql::trans_test(const std::vector<std::string> &stmt_vec
                          , std::vector<std::string>* exec_stmt_vec
                          , vector<vector<string>>* output
                          , int commit_or_not)
{
    if (commit_or_not != 2) {
        cerr << pthread_self() << ": START TRANSACTION;" << endl;
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
                cerr << pthread_self() << ": " << i << " has error: " << err << endl;
                break;
            }
        }
    }
    
    if (commit_or_not == 2)
        return;
    
    string last_sql;
    if (commit_or_not == 1) 
        last_sql = "COMMIT;";
    else
        last_sql = "ROLLBACK;";
    
    cerr << pthread_self() << ": " << last_sql << endl;
    while (1) {
        try{
            test(last_sql);
            break;
        }catch(std::exception &e) { // ignore runtime error
            string err = e.what();
            if (err.find("locked") != string::npos) 
                continue; // not break and continue to test 
            cerr << pthread_self() << ": " << err << endl;
            break;
        }
    }
    cerr << pthread_self() << ": " << last_sql << " done" << endl;
    return;
}

void dut_mysql::get_content(vector<string>& tables_name, map<string, vector<string>>& content)
{
    for (auto& table:tables_name) {
        vector<string> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";

        if (mysql_real_query(&mysql, query.c_str(), query.size())) {
            string err = mysql_error(&mysql);
            cerr << "Cannot get content of " + table + "\nLocation: " + debug_info << endl;
            cerr << "Error: " + err + "\nLocation: " + debug_info << endl;
            continue;
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

string dut_mysql::begin_stmt() {
    return "START TRANSACTION";
}

string dut_mysql::commit_stmt() {
    return "COMMIT";
}

string dut_mysql::abort_stmt() {
    return "ROLLBACK";
}

pid_t dut_mysql::fork_db_server()
{
    pid_t child = fork();
    if (child < 0) {
        throw std::runtime_error(string("Fork db server fail") + "\nLocation: " + debug_info);
    }

    if (child == 0) {
        char *server_argv[128];
        int i = 0;
        server_argv[i++] = (char *)"/usr/local/mysql/bin/mysqld"; // path of tiup
        server_argv[i++] = (char *)"--basedir=/usr/local/mysql";
        server_argv[i++] = (char *)"--datadir=/usr/local/mysql/data";
        server_argv[i++] = (char *)"--plugin-dir=/usr/local/mysql/lib/plugin";
        server_argv[i++] = (char *)"--user=mysql";
        server_argv[i++] = NULL;
        execv(server_argv[0], server_argv);
        cerr << "fork mysql server fail \nLocation: " + debug_info << endl; 
    }
    
    sleep(3);
    cout << "server pid: " << child << endl;
    return child;
}
