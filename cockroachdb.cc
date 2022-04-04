#include "cockroachdb.hh"

cockroachdb_connection::cockroachdb_connection(string db, unsigned int port)
{
    test_db = db;
    test_port = port;

    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), "root", NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        throw runtime_error("[CONNECTION FAIL] " + err + " in cockroachdb_connection");
    }
}

cockroachdb_connection::~cockroachdb_connection()
{
    PQfinish(conn);
}

schema_cockroachdb::schema_cockroachdb(string db, unsigned int port)
  : cockroachdb_connection(db, port)
{   
    // cerr << "Loading tables...";
    string get_table_query = "select table_name from information_schema.tables \
        where table_type = 'BASE TABLE' and table_schema = 'public';";
    
    auto res = PQexec(conn, get_table_query.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in get_table_query");
    }

    auto row_num = PQntuples(res);
    for (int i = 0; i < row_num; i++) {
        table tab(PQgetvalue(res, i, 0), "main", true, true);
        tables.push_back(tab);
    }
    PQclear(res);
    // cerr << "done." << endl;

    // cerr << "Loading views...";
    string get_view_query = "select table_name from information_schema.tables \
        where table_type = 'VIEW' and table_schema = 'public';";

    res = PQexec(conn, get_view_query.c_str());
    status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in get_view_query");
    }

    row_num = PQntuples(res);
    for (int i = 0; i < row_num; i++) {
        table tab(PQgetvalue(res, i, 0), "main", false, false);
        tables.push_back(tab);
    }
    PQclear(res);
    // cerr << "done." << endl;

    // cerr << "Loading columns and constraints...";
    for (auto& t : tables) {
        string get_column_query = "select column_name, data_type from information_schema.columns \
            where table_name='" + t.ident() + "' order by ordinal_position;";
        
        res = PQexec(conn, get_column_query.c_str());
        status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
            string err = PQerrorMessage(conn);
            PQclear(res);
            throw runtime_error(err + " in get_column_query");
        }
        
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            column c(PQgetvalue(res, i, 0), sqltype::get(PQgetvalue(res, i, 1)));
            t.columns().push_back(c);
        }
        PQclear(res);
    }
    // cerr << "done." << endl;

    booltype = sqltype::get("boolean");
    inttype = sqltype::get("bigint");
    realtype = sqltype::get("real");
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
    FUNC1(length, inttype, texttype);
    FUNC1(lower, texttype, texttype);
    FUNC1(ltrim, texttype, texttype);
    FUNC1(round, inttype, realtype);
    FUNC1(rtrim, texttype, texttype);
    FUNC1(trim, texttype, texttype);
    FUNC1(upper, texttype, texttype);

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

dut_cockroachdb::dut_cockroachdb(string db, unsigned int port)
  : cockroachdb_connection(db, port)
{
    sent_sql = "";
    has_sent_sql = false;
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

static unsigned long long get_cur_time_ms(void) {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

static bool check_blocked(PGconn *conn)
{
    if (!PQconsumeInput(conn)) {
        string err = PQerrorMessage(conn);
        throw runtime_error(err + " in cockroachdb::test");
    }
    return PQisBusy(conn);
}

void dut_cockroachdb::test(const std::string &stmt, std::vector<std::string>* output, int* affected_row_num)
{    
    if (has_sent_sql == false) {
        if (stmt == "COMMIT;") {
            auto status = PQtransactionStatus(conn);
            if (status == PQTRANS_INERROR) {
                cerr << "Transaction state error, cannot commit" << endl;
                throw std::runtime_error("PQTRANS_INERROR in cockroachdb::test -> PQtransactionStatus"); 
            }
        }
    
        if (!PQsendQuery(conn, stmt.c_str())) {
            string err = PQerrorMessage(conn);
            throw runtime_error(err + " in cockroachdb::test -> PQsendQuery");
        }

        has_sent_sql = true;
        sent_sql = stmt;
    }
    
    if (sent_sql != stmt) {
        throw std::runtime_error("sent sql stmt changed in cockroachdb::test"); 
    }
        
    while (1) {
        auto begin_time = get_cur_time_ms();
        while (check_blocked(conn)) {
            auto cur_time = get_cur_time_ms();
            if (cur_time - begin_time < COCKROACH_STMT_BLOCK_MS)
                continue;
            
            throw std::runtime_error("blocked in cockroachdb::test"); 
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
                throw std::runtime_error("[FIND BUG] " + err + " [in cockroachdb::test -> PQresultStatus]"); 
            
            if (err.find("internal error") != string::npos) 
                throw std::runtime_error("[FIND BUG] " + err + " [in cockroachdb::test -> PQresultStatus]"); 

            if (err.find("commands ignored until end of transaction block") != string::npos) 
                throw runtime_error("skipped in cockroachdb::test");

            throw runtime_error(err + " in cockroachdb::test -> PQresultStatus");
                
        }

        if (affected_row_num) {
            auto char_num = PQcmdTuples(res);
            if (char_num == NULL) {
                *affected_row_num = 0;
            } else {
                *affected_row_num = atoi(char_num);
            }
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
    }

    has_sent_sql = false;
    sent_sql = "";
}

void dut_cockroachdb::reset(void)
{
    string use_defaultdb = "use defaultdb;";
    auto res = PQexec(conn, use_defaultdb.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in cockroachdb::reset");
    }
    PQclear(res);

    string drop_sql = "drop database if exists " + test_db + " cascade; ";
    res = PQexec(conn, drop_sql.c_str());
    status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in cockroachdb::reset");
    }
    PQclear(res);

    string create_sql = "create database " + test_db + "; ";
    res = PQexec(conn, create_sql.c_str());
    status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in cockroachdb::reset");
    }
    PQclear(res);

    string use_sql = "use " + test_db + "; ";
    res = PQexec(conn, use_sql.c_str());
    status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in cockroachdb::reset");
    }
    PQclear(res);
}

void dut_cockroachdb::backup(void)
{
    string delete_backup = "cockroach userfile delete test.backup --insecure";
    if (system(delete_backup.c_str()) == -1) 
        throw std::runtime_error(string("system() error, return -1") + " in dut_cockroachdb::backup!");
    
    string backup_sql = "BACKUP DATABASE " + test_db + " TO 'userfile:///test.backup';";
    auto res = PQexec(conn, backup_sql.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in cockroachdb::backup");
    }
    PQclear(res);
}

void dut_cockroachdb::reset_to_backup(void)
{
    reset();
    string restore_sql = "RESTORE " + test_db + ".* FROM 'userfile:///test.backup';";
    auto res = PQexec(conn, restore_sql.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in cockroachdb::backup");
    }
    PQclear(res);
}

int dut_cockroachdb::save_backup_file(string path)
{
    string save_backup = "cockroach userfile get test.backup " + path + " --insecure";
    return system(save_backup.c_str());
}

void dut_cockroachdb::trans_test(const std::vector<std::string> &stmt_vec
                          , std::vector<std::string>* exec_stmt_vec
                          , vector<vector<string>>* output
                          , int commit_or_not)
{
    if (commit_or_not != 2) {
        cerr << pthread_self() << ": BEGIN;" << endl;
        test("BEGIN;");
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

void dut_cockroachdb::get_content(vector<string>& tables_name, map<string, vector<string>>& content)
{
    for (auto& table : tables_name) {
        vector<string> table_content;
        
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";
        auto res = PQexec(conn, query.c_str());
        auto status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
            string err = PQerrorMessage(conn);
            PQclear(res);
            cerr << "Cannot get content of " + table + " in dut_cockroachdb::get_content" << endl;
            cerr << "Error: " + err + " in dut_cockroachdb::get_content" << endl;
            continue;
        }

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
                table_content.push_back(str);
            }
            table_content.push_back("\n");
        }
        PQclear(res);
        content[table] = table_content;
    }
}

string dut_cockroachdb::commit_stmt() {
    return "COMMIT";
}

string dut_cockroachdb::abort_stmt() {
    return "ROLLBACK";
}

string dut_cockroachdb::begin_stmt() {
    return "BEGIN";
}

pid_t dut_cockroachdb::fork_db_server()
{
    pid_t child = fork();
    if (child < 0) {
        throw std::runtime_error(string("Fork db server fail") + " in dut_cockroachdb::fork_db_server!");
    }

    if (child == 0) {
        char *server_argv[128];
        int i = 0;
        server_argv[i++] = (char *)"/usr/local/bin/cockroach";
        server_argv[i++] = (char *)"start-single-node";
        server_argv[i++] = (char *)"--insecure";
        server_argv[i++] = (char *)"--listen-addr=localhost:26257";
        server_argv[i++] = (char *)"--http-addr=localhost:8080";
        server_argv[i++] = NULL;
        execv(server_argv[0], server_argv);
        cerr << "fork cockroachdb server fail in dut_cockroachdb::fork_db_server" << endl;
        exit(-1);
    }

    cout << "server pid: " << child << endl;
    return child;
}
