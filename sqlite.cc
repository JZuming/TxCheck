#include <stdexcept>
#include <cassert>
#include <cstring>
#include "sqlite.hh"
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

static regex e_syntax("near \".*\": syntax error");
static regex e_user_abort("callback requested query abort");
  
extern "C"  {
#include <sqlite3.h>
#include <unistd.h>
}

extern "C" int my_sqlite3_busy_handler(void *, int)
{
    throw std::runtime_error("sqlite3 timeout");
}

extern "C" int callback(void *arg, int argc, char **argv, char **azColName)
{
    (void)arg;

    int i;
    for(i = 0; i < argc; i++){
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

extern "C" int table_callback(void *arg, int argc, char **argv, char **azColName)
{
    (void) argc; (void) azColName;
    auto tables = (vector<table> *)arg;
    bool view = (string("view") == argv[0]);
    table tab(argv[2], "main", !view, !view);
    tables->push_back(tab);
    return 0;
}

extern "C" int index_callback(void *arg, int argc, char **argv, char **azColName)
{
    (void) argc; (void) azColName;
    auto indexes = (vector<string> *)arg;
    if (argv[0] != NULL) {
        indexes->push_back(argv[0]);
    }
    return 0;
}

extern "C" int column_callback(void *arg, int argc, char **argv, char **azColName)
{
    (void) argc; (void) azColName;
    table *tab = (table *)arg;
    column c(argv[1], sqltype::get(argv[2]));
    tab->columns().push_back(c);
    return 0;
}

sqlite_connection::sqlite_connection(std::string &conninfo)
{
    rc = sqlite3_open_v2(conninfo.c_str(), &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI|SQLITE_OPEN_CREATE, 0);
    if (rc) {
        cerr << sqlite3_errmsg(db) << endl;
        throw std::runtime_error(sqlite3_errmsg(db));
    }
    db_file = conninfo;
    // cerr << "SQLITE_VERSION: " << SQLITE_VERSION << endl;
    // cerr << pthread_self() << ": connect" << endl;
}

void sqlite_connection::q(const char *query)
{
    rc = sqlite3_exec(db, query, callback, 0, &zErrMsg);
    if( rc != SQLITE_OK ){
        auto e = std::runtime_error(zErrMsg);
        sqlite3_free(zErrMsg);
        throw e;
    }
}

sqlite_connection::~sqlite_connection()
{
    if (db)
        sqlite3_close(db);
    // cerr << pthread_self() << ": disconnect" << endl;
}

schema_sqlite::schema_sqlite(std::string &conninfo, bool no_catalog)
  : sqlite_connection(conninfo)
{
    std::string query = "SELECT * FROM main.sqlite_master where type in ('table', 'view')";

    if (no_catalog)
        query+= " AND name NOT like 'sqlite_%%'";
  
    version = "SQLite " SQLITE_VERSION " " SQLITE_SOURCE_ID;

//   sqlite3_busy_handler(db, my_sqlite3_busy_handler, 0);
    // cerr << "Loading tables...";

    rc = sqlite3_exec(db, query.c_str(), table_callback, (void *)&tables, &zErrMsg);
    if (rc!=SQLITE_OK) {
        auto e = std::runtime_error(zErrMsg);
        sqlite3_free(zErrMsg);
        throw e;
    }

    if (!no_catalog) {
		// sqlite_master doesn't list itself, do it manually
		table tab("sqlite_master", "main", false, false);
		tables.push_back(tab);
    }
  
    // cerr << "done." << endl;

    // cerr << "Loading indexes...";
    string query_index = "SELECT name FROM sqlite_master WHERE type='index' ORDER BY 1;";
    rc = sqlite3_exec(db, query_index.c_str(), index_callback, (void *)&indexes, &zErrMsg);
    if (rc!=SQLITE_OK) {
        auto e = std::runtime_error(zErrMsg);
        sqlite3_free(zErrMsg);
        throw e;
    }

    // cerr << "Loading columns and constraints...";
    for (auto t = tables.begin(); t != tables.end(); ++t) {
        string q("pragma table_info(");
        q += t->name;
        q += ");";
        rc = sqlite3_exec(db, q.c_str(), column_callback, (void *)&*t, &zErrMsg);
        if (rc!=SQLITE_OK) {
            auto e = std::runtime_error(zErrMsg);
            sqlite3_free(zErrMsg);
            throw e;
        }
    }

    // cerr << "done." << endl;
    booltype = sqltype::get("BOOLEAN");
    inttype = sqltype::get("INTEGER");
    realtype = sqltype::get("DOUBLE");
    texttype = sqltype::get("TEXT");

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
    BINOP(IS, inttype, inttype, booltype);
    BINOP(IS NOT, inttype, inttype, booltype);

    BINOP(AND, booltype, booltype, booltype);
    BINOP(OR, booltype, booltype, booltype);
  
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

#define FUNC4(n, r, a, b, c, d) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    proc.argtypes.push_back(c);				\
    proc.argtypes.push_back(d);				\
    register_routine(proc);						\
} while(0)

#define FUNC5(n, r, a, b, c, d, e) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    proc.argtypes.push_back(c);				\
    proc.argtypes.push_back(d);				\
    proc.argtypes.push_back(e);				\
    register_routine(proc);						\
} while(0)

    FUNC(sqlite_source_id, texttype);
    FUNC(sqlite_version, texttype);

    FUNC1(abs, inttype, inttype);
    FUNC1(abs, realtype, realtype);

    FUNC1(char, texttype, inttype);
    FUNC2(char, texttype, inttype, inttype);
    FUNC3(char, texttype, inttype, inttype, inttype);

    FUNC2(coalesce, texttype, texttype, texttype);
    FUNC2(coalesce, inttype, inttype, inttype);
    FUNC2(coalesce, realtype, realtype, realtype);

    FUNC1(hex, texttype, texttype);

    FUNC3(iif, texttype, booltype, texttype, texttype);
    FUNC3(iif, inttype, booltype, inttype, inttype);
    FUNC3(iif, realtype, booltype, realtype, realtype);

    FUNC2(instr, inttype, texttype, texttype);
    FUNC1(length, inttype, texttype);
    FUNC1(lower, texttype, texttype);
    FUNC1(ltrim, texttype, texttype);

    FUNC2(max, inttype, inttype, inttype);
    FUNC2(max, realtype, realtype, realtype);
    FUNC2(min, inttype, inttype, inttype);
    FUNC2(min, realtype, realtype, realtype);

    FUNC2(nullif, inttype, inttype, inttype);
    FUNC2(nullif, realtype, realtype, realtype);
    FUNC2(nullif, texttype, texttype, texttype);

    FUNC1(quote, texttype, texttype);
    FUNC1(round, inttype, realtype);
    FUNC2(round, inttype, realtype, inttype);

    FUNC1(rtrim, texttype, texttype);
    FUNC1(sign, inttype, realtype);
    FUNC1(sign, inttype, inttype);

    FUNC1(sqlite_compileoption_get, texttype, inttype);
    FUNC1(sqlite_compileoption_used, inttype, texttype);
    FUNC1(trim, texttype, texttype);

    FUNC1(typeof, texttype, inttype);
    FUNC1(typeof, texttype, realtype);
    FUNC1(typeof, texttype, texttype);
    FUNC1(unicode, inttype, texttype);
    FUNC1(upper, texttype, texttype);

    FUNC1(zeroblob, texttype, inttype);

    FUNC2(glob, inttype, texttype, texttype);
    
    FUNC2(like, inttype, texttype, texttype);
    FUNC2(ltrim, texttype, texttype, texttype);
    FUNC2(rtrim, texttype, texttype, texttype);
    FUNC2(trim, texttype, texttype, texttype);
    
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

    AGG1(group_concat, texttype, texttype);
    AGG1(max, realtype, realtype);
    AGG1(max, inttype, inttype);
    AGG1(min, realtype, realtype);
    AGG1(min, inttype, inttype);
    AGG1(sum, realtype, realtype);
    AGG1(sum, inttype, inttype);
    AGG1(total, realtype, inttype);
    AGG1(total, realtype, realtype);

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

    WIN(row_number, inttype);
    WIN(rank, inttype);
    WIN(dense_rank, inttype);
    WIN(percent_rank, realtype);
    WIN(cume_dist, realtype);
    WIN1(ntile, inttype, inttype);
    
    WIN1(lag, inttype, inttype);
    WIN1(lag, realtype, realtype);
    WIN1(lag, texttype, texttype);

    WIN2(lead, inttype, inttype, inttype);
    WIN2(lead, realtype, realtype, inttype);
    WIN2(lead, texttype, texttype, inttype);
    
    WIN1(first_value, inttype, inttype);
    WIN1(first_value, realtype, realtype);
    WIN1(first_value, texttype, texttype);
    WIN1(last_value, inttype, inttype);
    WIN1(last_value, realtype, realtype);
    WIN1(last_value, texttype, texttype);

    WIN2(nth_value, inttype, inttype, inttype);
    WIN2(nth_value, realtype, realtype, inttype);
    WIN2(nth_value, texttype, texttype, inttype);
    
    internaltype = sqltype::get("internal");
    arraytype = sqltype::get("ARRAY");

    true_literal = "1=1";
    false_literal = "0<>0";

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
    sqlite3_close(db);
    db = 0;
}

dut_sqlite::dut_sqlite(std::string &conninfo)
  : sqlite_connection(conninfo)
{
    // q("PRAGMA main.auto_vacuum = 2");
}

extern "C" int dut_callback(void *arg, int argc, char **argv, char **azColName)
{
    (void) arg; (void) argc; (void) argv; (void) azColName;
    return 0;
    // return SQLITE_ABORT;
}

extern "C" int content_callback(void *data, int argc, char **argv, char **azColName){
    int i; (void) azColName;
    auto data_vec = (vector<vector<string>> *)data;
    if (data_vec == NULL)
        return 0;

    vector<string> row_output;
    for (i = 0; i < argc; i++) {
        if (argv[i] == NULL) {
            row_output.push_back("NULL");
            continue;
        }
        string str = argv[i];
        str.erase(0, str.find_first_not_of(" "));
        str.erase(str.find_last_not_of(" ") + 1);
        row_output.push_back(str);
    }
    data_vec->push_back(row_output);
    return 0;
}

void dut_sqlite::test(const std::string &stmt, vector<vector<string>>* output, int* affected_row_num)
{
    // alarm(6);
    rc = sqlite3_exec(db, stmt.c_str(), content_callback, (void *)output, &zErrMsg);
    if(rc != SQLITE_OK){
        try {
            if (regex_match(zErrMsg, e_syntax)) {
                throw dut::syntax(zErrMsg);
            }
            else if (regex_match(zErrMsg, e_user_abort)) {
	            sqlite3_free(zErrMsg);
	            return;
            } else {
                throw dut::failure(zErrMsg);
            }
        } catch (dut::failure &e) {
            sqlite3_free(zErrMsg);
            throw e;
        }
    }

    if (affected_row_num)
        *affected_row_num = sqlite3_changes(db);
}

void dut_sqlite::reset(void)
{
    if (db)
        sqlite3_close(db);
    remove(db_file.c_str());

    rc = sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI|SQLITE_OPEN_CREATE, 0);
    if (rc) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }
}

void dut_sqlite::backup(void)
{
    auto bk_db = db_file;
    auto pos = bk_db.find(".db");
    if (pos != string::npos) {
        bk_db.erase(pos, 3);
    }
    bk_db += "_bk.db";
    remove(bk_db.c_str());

    sqlite3 *dst_db;
    auto dst_rc = sqlite3_open_v2(bk_db.c_str(), &dst_db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI|SQLITE_OPEN_CREATE, 0);
    if (dst_rc) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }

    auto bck = sqlite3_backup_init(dst_db, "main", db, "main");
    if (bck == nullptr) {
        throw std::runtime_error("sqlite3_backup_init fail");
    }

    auto err =sqlite3_backup_step(bck, -1);
    if (err != SQLITE_DONE) {
        sqlite3_backup_finish(bck);
        throw std::runtime_error("sqlite3_backup_step fail");
    }

    err = sqlite3_backup_finish(bck);

    sqlite3_close(dst_db);
    return;
}

// if bk_db is empty, it will reset to empty
void dut_sqlite::reset_to_backup(void)
{
    auto bk_db = db_file;
    auto pos = bk_db.find(".db");
    if (pos != string::npos) {
        bk_db.erase(pos, 3);
    }
    bk_db += "_bk.db";
    
    if (db)
        sqlite3_close(db);
    remove(db_file.c_str());

    sqlite3 *src_db;
    auto src_rc = sqlite3_open_v2(bk_db.c_str(), &src_db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI|SQLITE_OPEN_CREATE, 0);
    if (src_rc) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }

    rc = sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI|SQLITE_OPEN_CREATE, 0);
    if (rc) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }

    auto bck = sqlite3_backup_init(db, "main", src_db, "main");
    if (bck == nullptr) {
        throw std::runtime_error("sqlite3_backup_init fail");
    }

    auto err =sqlite3_backup_step(bck, -1);
    if (err != SQLITE_DONE) {
        sqlite3_backup_finish(bck);
        throw std::runtime_error("sqlite3_backup_step fail");
    }

    err = sqlite3_backup_finish(bck);

    sqlite3_close(src_db);
    return;
}

int dut_sqlite::save_backup_file(string path, string db_name)
{
    auto bk_db = db_name;
    auto pos = bk_db.find(".db");
    if (pos != string::npos) {
        bk_db.erase(pos, 3);
    }
    bk_db += "_bk.db";

    string cp_cmd = "cp " + bk_db + " " + path;
    return system(cp_cmd.c_str());
}

void dut_sqlite::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table:tables_name) {
        vector<vector<string>> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";
        
        rc = sqlite3_exec(db, query.c_str(), content_callback, (void *)&table_content, &zErrMsg);
        if (rc != SQLITE_OK) {
            auto e = std::runtime_error(zErrMsg);
            sqlite3_free(zErrMsg);
            throw e;
        }

        content[table] = table_content;
    }
}

string dut_sqlite::commit_stmt() {
    return "COMMIT";
}

string dut_sqlite::abort_stmt() {
    return "ROLLBACK";
}

string dut_sqlite::begin_stmt() {
    return "BEGIN TRANSACTION";
}