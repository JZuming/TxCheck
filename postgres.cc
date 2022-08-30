#include "postgres.hh"
#include "config.h"
#include <iostream>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

static regex e_timeout("ERROR:  canceling statement due to statement timeout(\n|.)*");
static regex e_syntax("ERROR:  syntax error at or near(\n|.)*");

#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

bool pg_type::consistent(sqltype *rvalue)
{
    pg_type *t = dynamic_cast<pg_type*>(rvalue);
    if (!t) {
        cerr << "unknown type: " << rvalue->name  << endl;
        return false;
    }

    switch(typtype_) {
        case 'b': /* base type */
        case 'c': /* composite type */
        case 'd': /* domain */
        case 'r': /* range */
        case 'e': /* enum */
            return this == t;
        case 'p':
            if (name == "anyarray") {
                return t->typelem_ != InvalidOid;
            } else if (name == "anynonarray") {
                return t->typelem_ == InvalidOid;
            } else if(name == "anyenum") {
                return t->typtype_ == 'e';
            } else if (name == "any") {
                return true;
            } else if (name == "anyelement") {
                return t->typelem_ == InvalidOid;
            } else if (name == "anyrange") {
                return t->typtype_ == 'r';
            } else if (name == "record") {
                return t->typtype_ == 'c';
            } else if (name == "cstring") {
                return this == t;
            } else {
                return false;
            }
        default:
            throw std::logic_error("unknown typtype");
    }
}

dut_pqxx::dut_pqxx(std::string conninfo)
  : c(conninfo)
{
    c.set_variable("statement_timeout", "'1s'");
    c.set_variable("client_min_messages", "'ERROR'");
    c.set_variable("application_name", "'" PACKAGE "::dut'");
}

void dut_pqxx::test(const std::string &stmt)
{
    try {
        if(!c.is_open())
            c.activate();

        pqxx::work w(c);
        w.exec(stmt.c_str());
        w.abort();
    } catch (const pqxx::failure &e) {
        if ((dynamic_cast<const pqxx::broken_connection *>(&e))) {
        /* re-throw to outer loop to recover session. */
        throw dut::broken(e.what());
    }

    if (regex_match(e.what(), e_timeout))
        throw dut::timeout(e.what());
    else if (regex_match(e.what(), e_syntax))
        throw dut::syntax(e.what());
    else
        throw dut::failure(e.what());
    }
}


schema_pqxx::schema_pqxx(std::string &conninfo, bool no_catalog) : c(conninfo)
{
    c.set_variable("application_name", "'" PACKAGE "::schema'");

    pqxx::work w(c);
    pqxx::result r = w.exec("select version()");
    version = r[0][0].as<string>();

    r = w.exec("SHOW server_version_num");
    version_num = r[0][0].as<int>();

    // address the schema change in postgresql 11 that replaced proisagg and proiswindow with prokind
    string procedure_is_aggregate = version_num < 110000 ? "proisagg" : "prokind = 'a'";
    string procedure_is_window = version_num < 110000 ? "proiswindow" : "prokind = 'w'";

    cerr << "Loading types...";

    r = w.exec("select quote_ident(typname), oid, typdelim, typrelid, typelem, typarray, typtype "
            "from pg_type ");
  
    for (auto row = r.begin(); row != r.end(); ++row) {
        string name(row[0].as<string>());
        OID oid(row[1].as<OID>());
        string typdelim(row[2].as<string>());
        OID typrelid(row[3].as<OID>());
        OID typelem(row[4].as<OID>());
        OID typarray(row[5].as<OID>());
        string typtype(row[6].as<string>());
        // if (schema == "pg_catalog")
        //     continue;
        // if (schema == "information_schema")
        //     continue;

        pg_type *t = new pg_type(name,oid,typdelim[0],typrelid, typelem, typarray, typtype[0]);
        oid2type[oid] = t;
        name2type[name] = t;
        types.push_back(t);
    }

    booltype = name2type["bool"];
    inttype = name2type["int4"];

    internaltype = name2type["internal"];
    arraytype = name2type["anyarray"];

    cerr << "done." << endl;

    cerr << "Loading tables...";
    r = w.exec("select table_name, "
                "table_schema, "
                    "is_insertable_into, "
                    "table_type "
            "from information_schema.tables");
	     
    for (auto row = r.begin(); row != r.end(); ++row) {
        string schema(row[1].as<string>());
        string insertable(row[2].as<string>());
        string table_type(row[3].as<string>());

        if (no_catalog && ((schema == "pg_catalog") || (schema == "information_schema")))
            continue;
      
        tables.push_back(table(row[0].as<string>(),
                schema,
                ((insertable == "YES") ? true : false),
                ((table_type == "BASE TABLE") ? true : false)));
    }
	     
    cerr << "done." << endl;

    cerr << "Loading columns and constraints...";

    for (auto t = tables.begin(); t != tables.end(); ++t) {
        string q("select attname, "
            "atttypid "
            "from pg_attribute join pg_class c on( c.oid = attrelid ) "
            "join pg_namespace n on n.oid = relnamespace "
            "where not attisdropped "
            "and attname not in "
            "('xmin', 'xmax', 'ctid', 'cmin', 'cmax', 'tableoid', 'oid') ");
        q += " and relname = " + w.quote(t->name);
        q += " and nspname = " + w.quote(t->schema);

        r = w.exec(q);
        for (auto row : r) {
            column c(row[0].as<string>(), oid2type[row[1].as<OID>()]);
            t->columns().push_back(c);
        }

        q = "select conname from pg_class t "
        "join pg_constraint c on (t.oid = c.conrelid) "
        "where contype in ('f', 'u', 'p') ";
        q += " and relnamespace = " " (select oid from pg_namespace where nspname = " + w.quote(t->schema) + ")";
        q += " and relname = " + w.quote(t->name);

        for (auto row : w.exec(q)) {
            t->constraints.push_back(row[0].as<string>());
        }
    
    }
    cerr << "done." << endl;

    cerr << "Loading operators...";

    r = w.exec("select oprname, oprleft,"
                "oprright, oprresult "
                "from pg_catalog.pg_operator "
                        "where 0 not in (oprresult, oprright, oprleft) ");
    for (auto row : r) {
        op o(row[0].as<string>(),
        oid2type[row[1].as<OID>()],
        oid2type[row[2].as<OID>()],
        oid2type[row[3].as<OID>()]);
        register_operator(o);
    }

    cerr << "done." << endl;

    cerr << "Loading routines...";
    r = w.exec("select (select nspname from pg_namespace where oid = pronamespace), oid, prorettype, proname "
            "from pg_proc "
            "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') "
            "and proname <> 'pg_event_trigger_table_rewrite_reason' "
            "and proname <> 'pg_event_trigger_table_rewrite_oid' "
            "and proname !~ '^ri_fkey_' "
            "and not (proretset or " + procedure_is_aggregate + " or " + procedure_is_window + ") ");

    for (auto row : r) {
        routine proc(row[0].as<string>(),
            row[1].as<string>(),
            oid2type[row[2].as<long>()],
            row[3].as<string>());
        register_routine(proc);
    }

    cerr << "done." << endl;

    cerr << "Loading routine parameters...";

    for (auto &proc : routines) {
        string q("select unnest(proargtypes) "
            "from pg_proc ");
        q += " where oid = " + w.quote(proc.specific_name);
      
        r = w.exec(q);
        for (auto row : r) {
        sqltype *t = oid2type[row[0].as<OID>()];
        assert(t);
        proc.argtypes.push_back(t);
        }
    }
    cerr << "done." << endl;

    cerr << "Loading aggregates...";
    r = w.exec("select (select nspname from pg_namespace where oid = pronamespace), oid, prorettype, proname "
            "from pg_proc "
            "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') "
            "and proname not in ('pg_event_trigger_table_rewrite_reason') "
            "and proname not in ('percentile_cont', 'dense_rank', 'cume_dist', "
            "'rank', 'test_rank', 'percent_rank', 'percentile_disc', 'mode', 'test_percentile_disc') "
            "and proname !~ '^ri_fkey_' "
            "and not (proretset or " + procedure_is_window + ") "
            "and " + procedure_is_aggregate);

    for (auto row : r) {
        routine proc(row[0].as<string>(),
            row[1].as<string>(),
            oid2type[row[2].as<OID>()],
            row[3].as<string>());
        register_aggregate(proc);
    }

    cerr << "done." << endl;

    cerr << "Loading aggregate parameters...";

    for (auto &proc : aggregates) {
        string q("select unnest(proargtypes) "
            "from pg_proc ");
        q += " where oid = " + w.quote(proc.specific_name);
      
        r = w.exec(q);
        for (auto row : r) {
            sqltype *t = oid2type[row[0].as<OID>()];
            assert(t);
            proc.argtypes.push_back(t);
        }
    }
    cerr << "done." << endl;
    c.disconnect();
    generate_indexes();
}

extern "C" {
    void dut_libpq_notice_rx(void *arg, const PGresult *res);
}

void dut_libpq_notice_rx(void *arg, const PGresult *res)
{
    (void) arg;
    (void) res;
}

void dut_libpq::connect(string db, unsigned int port)
{
    if (conn) {
	    PQfinish(conn);
    }
    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), "root", NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        throw runtime_error("[CONNECTION FAIL] " + err + " in " + debug_info);
    }
}

dut_libpq::dut_libpq(string db, unsigned int port)
{
    test_db = db;
    test_port = port;
    connect(test_db, test_port);
    sent_sql = "";
    has_sent_sql = false;
    process_id = PQbackendPID(conn);
}

dut_libpq::~dut_libpq()
{
    PQfinish(conn);
}

void dut_libpq::command(const std::string &stmt)
{
    if (!conn)
	    connect(test_db, test_port);
    
    PGresult *res = PQexec(conn, stmt.c_str());

    switch (PQresultStatus(res)) {

        case PGRES_FATAL_ERROR:
        default:
        {
            const char *errmsg = PQresultErrorMessage(res);
            if (!errmsg || !strlen(errmsg))
                errmsg = PQerrorMessage(conn);

            const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
            if (!sqlstate || !strlen(sqlstate))
                sqlstate =  (CONNECTION_OK != PQstatus(conn)) ? "08000" : "?????";

            std::string error_string(errmsg);
            std::string sqlstate_string(sqlstate);
            PQclear(res);

            if (CONNECTION_OK != PQstatus(conn)) {
                PQfinish(conn);
                conn = 0;
                throw dut::broken(error_string.c_str(), sqlstate_string.c_str());
            }
            if (sqlstate_string == "42601")
                throw dut::syntax(error_string.c_str(), sqlstate_string.c_str());
            else
                throw dut::failure(error_string.c_str(), sqlstate_string.c_str());
        }

        case PGRES_NONFATAL_ERROR:
        case PGRES_TUPLES_OK:
        case PGRES_SINGLE_TUPLE:
        case PGRES_COMMAND_OK:
            PQclear(res);
            return;
    }
}

static unsigned long long get_cur_time_ms(void) {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

static bool check_ready(PGconn *conn)
{
    if (!PQconsumeInput(conn)) {
        string err = PQerrorMessage(conn);
        throw runtime_error(err + " in cockroachdb::test");
    }
    return !PQisBusy(conn);
}

static int check_bugs(PGconn *conn, PGresult *res)
{
    switch (PQresultStatus(res)) {
        case PGRES_COMMAND_OK:
        case PGRES_TUPLES_OK:
        case PGRES_NONFATAL_ERROR:
        case PGRES_SINGLE_TUPLE:
            return 0;
        
        case PGRES_FATAL_ERROR:
        default: 
            const char *errmsg = PQresultErrorMessage(res);
            if (!errmsg || !strlen(errmsg))
                errmsg = PQerrorMessage(conn);
            const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
            if (!sqlstate || !strlen(sqlstate))
                sqlstate =  (CONNECTION_OK != PQstatus(conn)) ? "08000" : "?????";
            
            string error_string(errmsg);
            string sqlstate_string(sqlstate);

            if (CONNECTION_OK != PQstatus(conn))
                return 1;
            if (sqlstate_string == "42601" || // syntax_error
                sqlstate_string == "54001" || // statement_too_complex
                sqlstate_string == "23505" || // unique_violation
                sqlstate_string == "42803" || // grouping_error
                sqlstate_string == "42804" || // datatype_mismatch
                sqlstate_string == "42883" || // undefined_function
                sqlstate_string == "23502" || // not_null_violation
                sqlstate_string == "22003" || // numeric_value_out_of_range
                sqlstate_string == "42P07" || // duplicate_table
                sqlstate_string == "22011" || // substring_error
                sqlstate_string == "22012" || // division_by_zero
                sqlstate_string == "21000" || // cardinality_violation
                sqlstate_string == "23514" || // check_violation
                sqlstate_string == "2BP01" || // dependent_objects_still_exist
                sqlstate_string == "22P02" || // invalid_text_representation
                sqlstate_string == "22014" || // invalid_argument_for_ntile_function
                sqlstate_string == "42P01" || // undefined_table
                sqlstate_string == "42703")   // undefined_column
                return 0; 
    }
    return 0;
}

bool dut_libpq::check_whether_block()
{
    dut_libpq another_dut(test_db, test_port);
    string get_block_tid = "SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock';";
    vector<string> output;
    another_dut.block_test(get_block_tid, &output);

    // check output
    string tid_str = to_string(process_id);
    auto output_size = output.size();
    for (int i = 0; i < output_size; i++) {
        if (tid_str == output[i])
            return true;
    }

    return false;
}

void dut_libpq::block_test(const string &stmt, vector<string>* output, int* affected_row_num)
{
    auto res = PQexec(conn, stmt.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        string err = PQerrorMessage(conn);
        auto has_bug = check_bugs(conn, res);
        PQclear(res);
        
        // clear the current result
        while (res != NULL) {
            res = PQgetResult(conn);
            PQclear(res);
        }
        
        if (has_bug)
            throw std::runtime_error("[BUG] " + err + " in " + debug_info);         
        if (err.find("internal error") != string::npos) 
            throw std::runtime_error("[BUG] " + err + " in " + debug_info); 
        if (err.find("commands ignored until end of transaction block") != string::npos) 
            throw runtime_error("skipped in " + debug_info);
        throw runtime_error(err + " in cockroachdb::test -> PQresultStatus");
    }

    if (affected_row_num) {
        auto char_num = PQcmdTuples(res);
        if (char_num != NULL) 
            *affected_row_num = atoi(char_num);
        else
            *affected_row_num = 0;
    }

    if (output) {
        auto field_num = PQnfields(res);
        auto row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            for (int j = 0; j < field_num; j++) {
                auto res_unit = PQgetvalue(res, i, j);
                string str;
                if (res_unit == NULL)
                    str = "NULL";
                else
                    str = res_unit;
                output->push_back(str);
            }
            output->push_back("\n");
        }
    }
    PQclear(res);

    return;    
}

void dut_libpq::test(const std::string &stmt, vector<vector<string>>* output, int* affected_row_num)
{
    // (void)affected_row_num;
    // (void)output;
    // command("ROLLBACK;");
    // command("BEGIN;");
    // command(stmt.c_str());
    // command("ROLLBACK;");
    if (affected_row_num) 
        *affected_row_num = 0;

    if (has_sent_sql == false) {
        if (stmt == "COMMIT;") {
            auto status = PQtransactionStatus(conn);
            if (status == PQTRANS_INERROR) {
                cerr << "Transaction state error, cannot commit in " << debug_info << endl;
                throw std::runtime_error("trigger PQTRANS_INERROR when commit in " + debug_info); 
            }
        }
        if (!PQsendQuery(conn, stmt.c_str())) {
            string err = PQerrorMessage(conn);
            throw runtime_error("skipped: " + err + " in " + debug_info);
        }
        has_sent_sql = true;
        sent_sql = stmt;
    }
    
    if (sent_sql != stmt) 
        throw std::runtime_error("sent sql stmt changed in " + debug_info + 
            "\nsent_sql: " + sent_sql +
            "\nstmt: " + stmt); 
    
    auto begin_time = get_cur_time_ms();
    while (1) {
        while (!check_ready(conn)) {
            auto cur_time = get_cur_time_ms();
            if (cur_time - begin_time >= POSTGRES_STMT_BLOCK_MS) {
                auto blocked = check_whether_block();
                if (blocked == true)
                    throw std::runtime_error("blocked in " + debug_info); 
                begin_time = cur_time;
            }
        }
        
        auto res = PQgetResult(conn);
        if (res == NULL)
            break;

        auto status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
            string err = PQerrorMessage(conn);
            auto has_bug = check_bugs(conn, res);
            PQclear(res);
            
            // clear the current result
            while (res != NULL) {
                res = PQgetResult(conn);
                PQclear(res);
            }

            has_sent_sql = false;
            sent_sql = "";
            
            if (has_bug)
                throw std::runtime_error("[BUG] " + err + " in " + debug_info);         
            if (err.find("internal error") != string::npos) 
                throw std::runtime_error("[BUG] " + err + " in " + debug_info); 
            if (err.find("commands ignored until end of transaction block") != string::npos) 
                throw runtime_error("skipped in " + debug_info);
            throw runtime_error(err + " in cockroachdb::test -> PQresultStatus");
        }

        if (affected_row_num) {
            auto char_num = PQcmdTuples(res);
            if (char_num != NULL) 
                *affected_row_num += atoi(char_num);
        }

        if (output) {
            auto field_num = PQnfields(res);
            auto row_num = PQntuples(res);
            for (int i = 0; i < row_num; i++) {
                vector<string> row_output;
                for (int j = 0; j < field_num; j++) {
                    auto res_unit = PQgetvalue(res, i, j);
                    string str;
                    if (res_unit == NULL)
                        str = "NULL";
                    else
                        str = res_unit;
                    row_output.push_back(str);
                }
                output->push_back(row_output);
            }
        }
        PQclear(res);
    }

    has_sent_sql = false;
    sent_sql = "";
    return;
}

void dut_libpq::reset(void)
{
    string use_defaultdb = "\connect postgres;";
    auto res = PQexec(conn, use_defaultdb.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    PQclear(res);

    string drop_sql = "drop database if exists " + test_db + " with (force);";
    res = PQexec(conn, drop_sql.c_str());
    status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    PQclear(res);

    string create_sql = "create database " + test_db + "; ";
    res = PQexec(conn, create_sql.c_str());
    status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    PQclear(res);

    string use_sql = "\connect " + test_db + "; ";
    res = PQexec(conn, use_sql.c_str());
    status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    PQclear(res);
}

void dut_libpq::backup(void)
{
     string pgsql_dump = "/usr/local/pgsql/bin/pg_dump -p " + to_string(test_port) + " -u root " + test_db + " > /tmp/pgsql_bk.sql";
    int ret = system(pgsql_dump.c_str());
    if (ret != 0) {
        std::cerr << "backup fail \nLocation: " + debug_info << endl;
        throw std::runtime_error("backup fail \nLocation: " + debug_info); 
    }
}

void dut_libpq::reset_to_backup(void)
{
    reset();
    string bk_file = "/tmp/pgsql_bk.sql";
    if (access(bk_file.c_str(), F_OK ) == -1) 
        return;
    
    PQfinish(conn);
    
    string pgsql_source = "/usr/local/pgsql/bin/psql -p 5432" + to_string(test_port) + " " + test_db + " < /tmp/pgsql_bk.sql";
    if (system(pgsql_source.c_str()) == -1) 
        throw std::runtime_error(string("system() error, return -1") + "\nLocation: " + debug_info);

    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), "root", NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        throw runtime_error("[CONNECTION FAIL] " + err + " in " + debug_info);
    }
}

int dut_libpq::save_backup_file(string path)
{
    string cp_cmd = "cp /tmp/pgsql_bk.sql " + path;
    return system(cp_cmd.c_str());
}

void dut_libpq::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table:tables_name) {
        vector<vector<string>> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";

        auto res = PQexec(conn, query.c_str());
        auto status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            string err = PQerrorMessage(conn);
            PQclear(res);
            cerr << "Cannot get content of " + table + "\nLocation: " + debug_info << endl;
            cerr << "Error: " + err + "\nLocation: " + debug_info << endl;
            continue;
        }

        auto field_num = PQnfields(res);
        auto row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            vector<string> row_output;
            for (int j = 0; j < field_num; j++) {
                auto res_unit = PQgetvalue(res, i, j);
                string str;
                if (res_unit == NULL)
                    str = "NULL";
                else
                    str = res_unit;
                row_output.push_back(str);
            }
            table_content.push_back(row_output);
        }
        PQclear(res);
        content[table] = table_content;
    }
    return;
}

string dut_libpq::commit_stmt() {
    return "COMMIT";
}

string dut_libpq::abort_stmt() {
    return "ROLLBACK";
}

string dut_libpq::begin_stmt() {
    return "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED";
}

pid_t dut_libpq::fork_db_server()
{
    pid_t child = fork();
    if (child < 0) {
        throw std::runtime_error(string("Fork db server fail") + "\nLocation: " + debug_info);
    }

    if (child == 0) {
        char *server_argv[128];
        int i = 0;
        server_argv[i++] = (char *)"/usr/local/pgsql/bin/postgres";
        server_argv[i++] = (char *)"-D";
        server_argv[i++] = (char *)"/usr/local/pgsql/data";
        server_argv[i++] = NULL;
        execv(server_argv[0], server_argv);
        cerr << "fork mysql server fail \nLocation: " + debug_info << endl; 
    }

    sleep(1);
    cout << "server pid: " << child << endl;
    return child;
}
