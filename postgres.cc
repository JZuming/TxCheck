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

static bool has_types = false;
static vector<pg_type *> static_type_vec;

static bool has_operators = false;
static vector<op> static_op_vec;

static bool has_routines = false;
static vector<routine> static_routine_vec;

static bool has_routine_para = false;
static map<string, vector<pg_type *>> static_routine_para_map;

static bool has_aggregates = false;
static vector<routine> static_aggregate_vec;

static bool has_aggregate_para = false;
static map<string, vector<pg_type *>> static_aggregate_para_map;

static unsigned long long get_cur_time_ms(void) {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

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
        case 'm': /* multirange */
            return this == t;
        case 'p':
            if (name == "anyarray") {
                return t->typelem_ != InvalidOid;
            } else if (name == "anynonarray") {
                return t->typelem_ == InvalidOid;
            } else if (name == "anyelement") {
                return t->typelem_ == InvalidOid;
            } else if(name == "anyenum") {
                return t->typtype_ == 'e';
            } else if (name == "anyrange") {
                return t->typtype_ == 'r';
            } else if (name == "record") {
                return t->typtype_ == 'c';
            } else if (name == "cstring") {
                return this == t;
            } else if (name == "any") {
                return true;
            } else {
                return false;
            }
        default:
            cerr << "error type: " << name << " " << oid_ << " " << typdelim_ << " "
                << typrelid_ << " " << typelem_ << " " << typarray_ << " " << typtype_ << endl;
            cerr << "t type: " << t->name << " " << t->oid_ << " " << t->typdelim_ << " "
                << t->typrelid_ << " " << t->typelem_ << " " << t->typarray_ << " " << t->typtype_ << endl;
            throw std::logic_error("unknown typtype");
    }
}

// dut_pqxx::dut_pqxx(std::string conninfo)
//   : c(conninfo)
// {
//     c.set_variable("statement_timeout", "'1s'");
//     c.set_variable("client_min_messages", "'ERROR'");
//     c.set_variable("application_name", "'" PACKAGE "::dut'");
// }

// void dut_pqxx::test(const std::string &stmt)
// {
//     try {
//         if(!c.is_open())
//             c.activate();

//         pqxx::work w(c);
//         w.exec(stmt.c_str());
//         w.abort();
//     } catch (const pqxx::failure &e) {
//         if ((dynamic_cast<const pqxx::broken_connection *>(&e))) {
//         /* re-throw to outer loop to recover session. */
//         throw dut::broken(e.what());
//     }

//     if (regex_match(e.what(), e_timeout))
//         throw dut::timeout(e.what());
//     else if (regex_match(e.what(), e_syntax))
//         throw dut::syntax(e.what());
//     else
//         throw dut::failure(e.what());
//     }
// }

static PGresult* pqexec_handle_error(PGconn *conn, string& query)
{
    auto res = PQexec(conn, query.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    return res;
}

schema_pqxx::schema_pqxx(string db, unsigned int port, bool no_catalog)
    : pgsql_connection(db, port)
{
    // c.set_variable("application_name", "'" PACKAGE "::schema'");

    // pqxx::work w(c);
    string version_sql = "select version();";
    auto res = pqexec_handle_error(conn, version_sql);
    version = PQgetvalue(res, 0, 0);
    PQclear(res);

    string version_num_sql = "SHOW server_version_num;";
    res = pqexec_handle_error(conn, version_num_sql);
    version_num = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);

    // address the schema change in postgresql 11 that replaced proisagg and proiswindow with prokind
    string procedure_is_aggregate = version_num < 110000 ? "proisagg" : "prokind = 'a'";
    string procedure_is_window = version_num < 110000 ? "proiswindow" : "prokind = 'w'";

    // auto begin_time = get_cur_time_ms();
    // cerr << "Loading types...";
    if (has_types == false) {
        string load_type_sql = "select quote_ident(typname), oid, typdelim, typrelid, typelem, typarray, typtype "
            "from pg_type ;";
        res = pqexec_handle_error(conn, load_type_sql);
        auto row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string name(PQgetvalue(res, i, 0));
            OID oid = atol(PQgetvalue(res, i, 1));
            string typdelim(PQgetvalue(res, i, 2));
            OID typrelid = atol(PQgetvalue(res, i, 3));
            OID typelem = atol(PQgetvalue(res, i, 4));
            OID typarray = atol(PQgetvalue(res, i, 5));
            string typtype(PQgetvalue(res, i, 6));

            auto t = new pg_type(name, oid, typdelim[0], typrelid, typelem, typarray, typtype[0]);
            static_type_vec.push_back(t);
        }
        PQclear(res);
        has_types = true;
    }
    for (auto t : static_type_vec) {
        oid2type[t->oid_] = t;
        name2type[t->name] = t;
        types.push_back(t);
    }

    if (name2type.count("_bool") > 0 && 
            name2type.count("int4") > 0 &&
            name2type.count("float8") > 0 &&
            name2type.count("text") > 0) {
        
        booltype = name2type["_bool"];
        inttype = name2type["int4"];
        realtype = name2type["float8"];
        texttype = name2type["text"];
    }
    else {
        cerr << "at least one of booltype, inttype, realtype, texttype is not exist in" << debug_info << endl;
        throw runtime_error("at least one of booltype, inttype, realtype, texttype is not exist in" + debug_info);
    }

    internaltype = name2type["internal"];
    arraytype = name2type["anyarray"];
    true_literal = "1 = 1";
    false_literal = "0 <> 0";
    // cerr << "done." << endl;
    // auto end_time = get_cur_time_ms();
    // cerr << "time using: " << end_time - begin_time << endl;

    // begin_time = get_cur_time_ms();
    // cerr << "Loading tables...";
    string load_table_sql = "select table_name, "
                                "table_schema, "
                                "is_insertable_into, "
                                "table_type "
                            "from information_schema.tables;";
    res = pqexec_handle_error(conn, load_table_sql);
    auto row_num = PQntuples(res);
    for (int i = 0; i < row_num; i++) {
        string table_name(PQgetvalue(res, i, 0));
        string schema(PQgetvalue(res, i, 1));
        string insertable(PQgetvalue(res, i, 2));
        string table_type(PQgetvalue(res, i, 3));

        if (no_catalog && ((schema == "pg_catalog") || (schema == "information_schema")))
            continue;
        
        tables.push_back(table(table_name, schema,
                ((insertable == "YES") ? true : false),
                ((table_type == "BASE TABLE") ? true : false)));
    }
    PQclear(res);    
    // cerr << "done." << endl;
    // end_time = get_cur_time_ms();
    // cerr << "time using: " << end_time - begin_time << endl;

    // begin_time = get_cur_time_ms();
    // cerr << "Loading columns and constraints...";
    for (auto t = tables.begin(); t != tables.end(); ++t) {
        string q("select attname, "
                    "atttypid "
                "from pg_attribute join pg_class c on( c.oid = attrelid ) "
                    "join pg_namespace n on n.oid = relnamespace "
                "where not attisdropped "
                    "and attname not in "
                    "('xmin', 'xmax', 'ctid', 'cmin', 'cmax', 'tableoid', 'oid') ");
        q += " and relname = '" + t->name + "'";
        q += " and nspname = '" + t->schema + "';";

        res = pqexec_handle_error(conn, q);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string column_name(PQgetvalue(res, i, 0));
            auto column_type = oid2type[atol(PQgetvalue(res, i, 1))];
            // cerr << "table: " << t->name << " col: " << column_name << "type: " << column_type->name << endl;
            column c(column_name, column_type);
            t->columns().push_back(c);
        }
        PQclear(res);

        q = "select conname from pg_class t "
                "join pg_constraint c on (t.oid = c.conrelid) "
                "where contype in ('f', 'u', 'p') ";
        q = q + " and relnamespace = (select oid from pg_namespace where nspname = '" + t->schema + "')";
        q = q + " and relname = '" + t->name + "';";

        res = pqexec_handle_error(conn, q);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            t->constraints.push_back(PQgetvalue(res, i, 0));
        }
        PQclear(res);
    }
    // cerr << "done." << endl;
    // end_time = get_cur_time_ms();
    // cerr << "time using: " << end_time - begin_time << endl;

    // begin_time = get_cur_time_ms();
    // cerr << "Loading operators...";
    if (has_operators == false) {
        string load_operators_sql = "select oprname, oprleft,"
                                    "oprright, oprresult "
                                "from pg_catalog.pg_operator "
                                "where 0 not in (oprresult, oprright, oprleft) ;";
        res = pqexec_handle_error(conn, load_operators_sql);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string op_name(PQgetvalue(res, i, 0));
            auto op_left_type = oid2type[atol(PQgetvalue(res, i, 1))];
            auto op_right_type = oid2type[atol(PQgetvalue(res, i, 2))];
            auto op_result_type = oid2type[atol(PQgetvalue(res, i, 3))];
            op o(op_name, op_left_type, op_right_type, op_result_type);
            static_op_vec.push_back(o);
        }
        PQclear(res);
        has_operators = true;
    }
    for (auto& o:static_op_vec) {
        register_operator(o);
    }
    // cerr << "done." << endl;
    // end_time = get_cur_time_ms();
    // cerr << "time using: " << end_time - begin_time << endl;

    // begin_time = get_cur_time_ms();
    // cerr << "Loading routines...";
    if (has_routines == false) {
        string load_routines_sql = 
            "select (select nspname from pg_namespace where oid = pronamespace), oid, prorettype, proname "
            "from pg_proc "
            "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') "
                "and proname <> 'pg_event_trigger_table_rewrite_reason' "
                "and proname <> 'pg_event_trigger_table_rewrite_oid' "
                "and proname !~ '^ri_fkey_' "
                "and not (proretset or " + procedure_is_aggregate + " or " + procedure_is_window + ") ;";
        res = pqexec_handle_error(conn, load_routines_sql);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string r_name(PQgetvalue(res, i, 0));
            string oid_str(PQgetvalue(res, i, 1));
            auto prorettype = oid2type[atol(PQgetvalue(res, i, 2))];
            string proname(PQgetvalue(res, i, 3));
            
            routine proc(r_name, oid_str, prorettype, proname);
            static_routine_vec.push_back(proc);
        }
        PQclear(res);
        has_routines = true;
    }
    for (auto& proc:static_routine_vec) {
        register_routine(proc);
    }
    // cerr << "done." << endl;
    // end_time = get_cur_time_ms();
    // cerr << "time using: " << end_time - begin_time << endl;

    // begin_time = get_cur_time_ms();
    // cerr << "Loading routine parameters...";
    if (has_routine_para == false) {
        for (auto &proc : routines) {
            string q("select unnest(proargtypes) from pg_proc ");
            q = q + " where oid = " + proc.specific_name + ";";

            res = pqexec_handle_error(conn, q);
            row_num = PQntuples(res);

            vector <pg_type *> para_vec;
            for (int i = 0; i < row_num; i++) {
                auto t = oid2type[atol(PQgetvalue(res, i, 0))];
                assert(t);
                para_vec.push_back(t);
            }
            static_routine_para_map[proc.specific_name] = para_vec;
            PQclear(res);
        }
        has_routine_para = true;
    }
    for (auto &proc : routines) {
        auto& para_vec = static_routine_para_map[proc.specific_name];
        for (auto t:para_vec) {
            proc.argtypes.push_back(t);
        }
    }
    // cerr << "done." << endl;
    // end_time = get_cur_time_ms();
    // cerr << "time using: " << end_time - begin_time << endl;
    
    // begin_time = get_cur_time_ms();
    // cerr << "Loading aggregates...";
    if (has_aggregates == false) {
        string load_aggregates_sql = 
            "select (select nspname from pg_namespace where oid = pronamespace), oid, prorettype, proname "
            "from pg_proc "
                "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') "
                "and proname not in ('pg_event_trigger_table_rewrite_reason') "
                "and proname not in ('percentile_cont', 'dense_rank', 'cume_dist', "
                "'rank', 'test_rank', 'percent_rank', 'percentile_disc', 'mode', 'test_percentile_disc') "
                "and proname !~ '^ri_fkey_' "
                "and not (proretset or " + procedure_is_window + ") "
                "and " + procedure_is_aggregate + ";";
        res = pqexec_handle_error(conn, load_aggregates_sql);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string nspname(PQgetvalue(res, i, 0));
            string oid_str(PQgetvalue(res, i, 1));
            auto prorettype = oid2type[atol(PQgetvalue(res, i, 2))];
            string proname(PQgetvalue(res, i, 3));

            routine proc(nspname, oid_str, prorettype, proname);
            static_aggregate_vec.push_back(proc);
        }
        PQclear(res);
        has_aggregates = true;
    }
    for (auto& proc:static_aggregate_vec) {
        register_aggregate(proc);
    }
    // cerr << "done." << endl;
    // end_time = get_cur_time_ms();
    // cerr << "time using: " << end_time - begin_time << endl;

    // begin_time = get_cur_time_ms();
    // cerr << "Loading aggregate parameters...";
    if (has_aggregate_para == false) {
        for (auto &proc : aggregates) {
            string q("select unnest(proargtypes) "
                "from pg_proc ");
            q = q + " where oid = " + proc.specific_name + ";";
            res = pqexec_handle_error(conn, q);
            row_num = PQntuples(res);
            vector<pg_type *> para_vec;
            for (int i = 0; i < row_num; i++) {
                auto t = oid2type[atol(PQgetvalue(res, i, 0))];
                assert(t);
                para_vec.push_back(t);
            }
            static_aggregate_para_map[proc.specific_name] = para_vec;
            PQclear(res);
        }
        has_aggregate_para = true;
    }
    for (auto &proc : aggregates) {
        auto& para_vec = static_aggregate_para_map[proc.specific_name];
        for (auto t:para_vec) {
            proc.argtypes.push_back(t);
        }
    }
    // cerr << "done." << endl;
    // end_time = get_cur_time_ms();
    // cerr << "time using: " << end_time - begin_time << endl;
    
    // begin_time = get_cur_time_ms();
    generate_indexes();
    // end_time = get_cur_time_ms();
    // cerr << "time using: " << end_time - begin_time << endl;
}

schema_pqxx::~schema_pqxx()
{
    // auto types_num = types.size();
    // for (int i = 0; i < types_num; i++) {
    //     pg_type* ptype = dynamic_cast<pg_type*>(types[i]);
    //     if (!ptype) // not a pg_type
    //         continue;
    //     types.erase(types.begin() + i);
    //     delete ptype;
    //     i--;
    //     types_num--;
    // }
}

extern "C" {
    void dut_libpq_notice_rx(void *arg, const PGresult *res);
}

void dut_libpq_notice_rx(void *arg, const PGresult *res)
{
    (void) arg;
    (void) res;
}

pgsql_connection::pgsql_connection(string db, unsigned int port)
{    
    test_db = db;
    test_port = port;
    
    conn = PQsetdbLogin("localhost", to_string(port).c_str(), NULL, NULL, db.c_str(), NULL, NULL);
    if (PQstatus(conn) == CONNECTION_OK)
        return; // succeed
    
    string err = PQerrorMessage(conn);
    if (err.find("does not exist") == string::npos) {
        cerr << "[CONNECTION FAIL]  " << err << " in " << debug_info << endl;
        throw runtime_error("[CONNECTION FAIL] " + err + " in " + debug_info);
    }
    
    cerr << "try to create database testdb" << endl;
    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, "postgres", NULL, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }

    string create_sql = "create database " + test_db + "; ";
    auto res = PQexec(conn, create_sql.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    PQclear(res);

    PQfinish(conn);
    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), NULL, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }
    cerr << "create successfully" << endl;
    return;
}

pgsql_connection::~pgsql_connection()
{
    PQfinish(conn);
}

dut_libpq::dut_libpq(string db, unsigned int port)
    : pgsql_connection(db, port)
{
    sent_sql = "";
    has_sent_sql = false;
    process_id = PQbackendPID(conn);
}

void dut_libpq::command(const std::string &stmt)
{    
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
        throw runtime_error(err + " in " + debug_info);
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
            throw runtime_error(err + " in " + debug_info);
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
    if (conn) 
        PQfinish(conn);
    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, "postgres", NULL, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }

    string drop_sql = "drop database if exists " + test_db + " with (force);";
    auto res = PQexec(conn, drop_sql.c_str());
    auto status = PQresultStatus(res);
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

    PQfinish(conn);
    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), NULL, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }
}

void dut_libpq::backup(void)
{
     string pgsql_dump = "/usr/local/pgsql/bin/pg_dump -p " + to_string(test_port) + " " + test_db + " > /tmp/pgsql_bk.sql";
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
    
    string pgsql_source = "/usr/local/pgsql/bin/psql -p " + to_string(test_port) + " " + test_db + " < /tmp/pgsql_bk.sql &> /dev/null";
    if (system(pgsql_source.c_str()) == -1) 
        throw std::runtime_error(string("system() error, return -1") + "\nLocation: " + debug_info);

    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), NULL, NULL);
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
    auto fd_out = open("server_out.txt", O_RDWR | O_CREAT | O_TRUNC, 0644);
    auto fd_err = open("server_err.txt", O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (child == 0) {
        setsid();
        dup2(fd_out, STDOUT_FILENO);
        dup2(fd_err, STDERR_FILENO);
        close(fd_out);
        close(fd_err);

        char *server_argv[128];
        int i = 0;
        server_argv[i++] = (char *)"/usr/local/pgsql/bin/postgres";
        server_argv[i++] = (char *)"-D";
        server_argv[i++] = (char *)"/usr/local/pgsql/data";
        server_argv[i++] = NULL;
        execv(server_argv[0], server_argv);
        cerr << "fork mysql server fail \nLocation: " + debug_info << endl; 
    }
    close(fd_out);
    close(fd_err);

    sleep(1);
    cout << "server pid: " << child << endl;
    return child;
}
