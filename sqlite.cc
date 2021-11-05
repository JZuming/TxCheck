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
        throw std::runtime_error(sqlite3_errmsg(db));
    }
    db_file = conninfo;
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

    FUNC(last_insert_rowid, INTEGER);
    FUNC(random, INTEGER);
    FUNC(sqlite_source_id, TEXT);
    FUNC(sqlite_version, TEXT);
    FUNC(total_changes, INTEGER);

    FUNC1(abs, INTEGER, INTEGER);
    FUNC1(abs, REAL, REAL);
    FUNC1(hex, TEXT, TEXT);

    FUNC1(length, INTEGER, TEXT);
    FUNC1(lower, TEXT, TEXT);
    FUNC1(ltrim, TEXT, TEXT);
    FUNC1(quote, TEXT, TEXT);
    FUNC1(randomblob, TEXT, INTEGER);
    FUNC1(round, INTEGER, REAL);
    FUNC1(rtrim, TEXT, TEXT);

    FUNC1(sqlite_compileoption_get, TEXT, INTEGER);
    FUNC1(sqlite_compileoption_used, INTEGER, TEXT);
    FUNC1(trim, TEXT, TEXT);

    FUNC1(typeof, TEXT, INTEGER);
    FUNC1(typeof, TEXT, NUMERIC);
    FUNC1(typeof, TEXT, REAL);
    FUNC1(typeof, TEXT, TEXT);
    FUNC1(unicode, INTEGER, TEXT);
    FUNC1(upper, TEXT, TEXT);

    FUNC1(zeroblob, TEXT, INTEGER);

    FUNC2(glob, INTEGER, TEXT, TEXT);
    FUNC2(instr, INTEGER, TEXT, TEXT);
    FUNC2(like, INTEGER, TEXT, TEXT);
    FUNC2(ltrim, TEXT, TEXT, TEXT);
    FUNC2(rtrim, TEXT, TEXT, TEXT);
    FUNC2(trim, TEXT, TEXT, TEXT);
    FUNC2(round, INTEGER, REAL, INTEGER);
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

    AGG1(group_concat, TEXT, TEXT);
    AGG1(max, REAL, REAL);
    AGG1(max, INTEGER, INTEGER);
    AGG1(min, REAL, REAL);
    AGG1(min, INTEGER, INTEGER);
    AGG1(sum, REAL, REAL);
    AGG1(sum, INTEGER, INTEGER);
    AGG1(total, REAL, INTEGER);
    AGG1(total, REAL, REAL);

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
    
    booltype = sqltype::get("BOOLEAN");
    inttype = sqltype::get("INTEGER");

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

void dut_sqlite::test(const std::string &stmt)
{
    alarm(6);
    rc = sqlite3_exec(db, stmt.c_str(), dut_callback, 0, &zErrMsg);
    if(rc != SQLITE_OK){
        try {
            if (regex_match(zErrMsg, e_syntax))
	            throw dut::syntax(zErrMsg);
            else if (regex_match(zErrMsg, e_user_abort)) {
	            sqlite3_free(zErrMsg);
	            return;
            } else 
	            throw dut::failure(zErrMsg);
        } catch (dut::failure &e) {
            sqlite3_free(zErrMsg);
            throw;
        }
    }
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

void dut_sqlite::trans_test(const std::vector<std::string> &stmt_vec
                , std::vector<std::string> &exec_stmt_vec)
{
    test("BEGIN TRANSACTION;");
    auto size = stmt_vec.size();
    for (auto i = 0; i < size; i++) {
        auto &stmt = stmt_vec[i];
        int try_time = 0;
        while (1) {
            try {
                if (try_time >= MAX_TRY_TIME) {
                    cerr << pthread_self() << ": " << i << " skip " << stmt.substr(0, 20) << endl;
                    break;
                }
                try_time++;
                test(stmt);
                exec_stmt_vec.push_back(stmt);
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
    
    cerr << pthread_self() << " commit" << endl;
    while (1) {
        try{
            test("COMMIT;");
            break;
        }catch(std::exception &e) { // ignore runtime error
            string err = e.what();
            if (err.find("locked") != string::npos) {
                continue; // not break and continue to test 
            }
            cerr << pthread_self() << " " << err << endl;
            break;
        }
    }
    cerr << pthread_self() << " commit done" << endl;
    return;
}