#include "oceanbase.hh"

static regex e_unknown_database(".*Unknown database.*");
static regex e_crash(".*Lost connection.*");
  
#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

#define RECONNECT_TRY_TIME 8

oceanbase_connection::oceanbase_connection(string db, unsigned int port)
{
    test_db = db;
    test_port = port;
    
    if (!mysql_init(&mysql))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    mysql_options(&mysql, MYSQL_OPT_NONBLOCK, 0);

    // password null: blank (empty) password field
    if (mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, test_db.c_str(), test_port, NULL, 0)) 
        return; // succeed
    
    string err = mysql_error(&mysql);
    int connect_fail_time = 0;
    while (err.find("Can't connect to MySQL server") != string::npos ||
            err.find("MySQL server has gone away") != string::npos) { // sometime it may fail
        cerr << "Can't connect to MySQL server, reconnect" << endl;
        connect_fail_time++;
        usleep(500000);
        auto ret = mariadb_reconnect(&mysql);
        if (ret == 0)
            return; // succeed
        err = mysql_error(&mysql);
        if (connect_fail_time > RECONNECT_TRY_TIME) // fail 5 times, stop trying
            break;
    }

    if (!regex_match(err, e_unknown_database))
        throw std::runtime_error("BUG!!!" + string(mysql_error(&mysql)) + "\nLocation: " + debug_info);

    // error caused by unknown database, so create one
    cerr << test_db + " does not exist, use default db" << endl;
    if (!mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, NULL, test_port, NULL, 0))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    cerr << "create database " + test_db << endl;
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

oceanbase_connection::~oceanbase_connection()
{
    mysql_close(&mysql);
}

schema_oceanbase::schema_oceanbase(string db, unsigned int port)
  : oceanbase_connection(db, port)
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

    // tidb numeric
    FUNC(PI, realtype);

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
    // add for tidb string
    FUNC1(ASCII, inttype, texttype);
    FUNC1(BIN, texttype, inttype);
    FUNC1(BIT_LENGTH, inttype, texttype);
    FUNC1(CHAR, texttype, inttype);
    FUNC1(CHAR_LENGTH, inttype, texttype);
    FUNC1(SPACE, texttype, inttype);
    FUNC1(REVERSE, texttype, texttype);
    FUNC1(ORD, inttype, texttype);
    FUNC1(OCT, texttype, inttype);
    FUNC1(UNHEX, texttype, texttype);
    // tidb numeric
    FUNC1(EXP, realtype, realtype);
    FUNC1(SQRT, realtype, realtype);
    FUNC1(LN, realtype, realtype);
    FUNC1(LOG, realtype, realtype);
    FUNC1(TAN, realtype, realtype);
    FUNC1(COT, realtype, realtype);
    FUNC1(SIN, realtype, realtype);
    FUNC1(COS, realtype, realtype);
    FUNC1(ATAN, realtype, realtype);
    FUNC1(ASIN, realtype, realtype);
    FUNC1(ACOS, realtype, realtype);
    FUNC1(RADIANS, realtype, realtype);
    FUNC1(DEGREES, realtype, realtype);
    FUNC1(CEILING, inttype, realtype);
    FUNC1(FLOOR, inttype, realtype);
    FUNC1(ROUND, inttype, realtype);
    FUNC1(SIGN, inttype, realtype);
    FUNC1(SIGN, inttype, inttype);
    FUNC1(CRC32, inttype, texttype);
    
    FUNC2(instr, inttype, texttype, texttype);
    FUNC2(round, realtype, realtype, inttype);
    FUNC2(substr, texttype, texttype, inttype);
    // tidb string
    FUNC2(INSTR, inttype, texttype, texttype);
    FUNC2(LEFT, texttype, texttype, inttype);
    FUNC2(RIGHT, texttype, texttype, inttype);
    FUNC2(REPEAT, texttype, texttype, inttype);
    FUNC2(STRCMP, inttype, texttype, texttype);
    // tidb numeric
    FUNC2(POW, realtype, realtype, realtype);
    FUNC2(LOG, realtype, realtype, realtype);
    FUNC2(MOD, inttype, inttype, inttype);
    FUNC2(ROUND, realtype, realtype, inttype);
    FUNC2(TRUNCATE, realtype, realtype, inttype);

    FUNC3(substr, texttype, texttype, inttype, inttype);
    FUNC3(replace, texttype, texttype, texttype, texttype);
    // add for tidb
    FUNC3(CONCAT, texttype, texttype, texttype, texttype);
    FUNC3(LPAD, texttype, texttype, inttype, texttype);
    FUNC3(RPAD, texttype, texttype, inttype, texttype);
    FUNC3(REPLACE, texttype, texttype, texttype, texttype);
    FUNC3(SUBSTRING, texttype, texttype, inttype, inttype);

    // add for tidb
    FUNC4(CONCAT_WS, texttype, texttype, texttype, texttype, texttype);
    FUNC4(ELT, texttype, inttype, texttype, texttype, texttype);
    FUNC4(FIELD, inttype, texttype, texttype, texttype, texttype);
    FUNC4(INSERT, texttype, texttype, inttype, inttype, texttype);

    FUNC5(EXPORT_SET, texttype, inttype, texttype, texttype, texttype, inttype);

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
    // WIN1(NTILE, inttype, inttype);
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

void schema_oceanbase::update_schema()
{
    tables.clear();
    indexes.clear();
    // Loading tables...;
    string get_table_query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
        WHERE TABLE_SCHEMA='" + test_db + "' AND \
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
        where table_schema='" + test_db + "' order by 1;";
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
                WHERE TABLE_SCHEMA='" + test_db + "' AND \
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
                    TABLE_SCHEMA='" + test_db + "'  ORDER BY ORDINAL_POSITION;";
        if (mysql_real_query(&mysql, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info + "\nTable: " + t.ident());
        result = mysql_store_result(&mysql);
        while (auto row = mysql_fetch_row(result)) {
            column c(row[0], sqltype::get(row[1]));
            t.columns().push_back(c);
        }
        mysql_free_result(result);
    }

    return;
}

dut_oceanbase::dut_oceanbase(string db, unsigned int port)
  : oceanbase_connection(db, port)
{
    sent_sql = "";
    has_sent_sql = false;
    query_status = 0;
    txn_abort = false;
    thread_id = mysql_thread_id(&mysql);
    // block_test("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;");
}

static unsigned long long get_cur_time_ms(void) {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

static shared_ptr<dut_oceanbase> another_dut;
static bool another_dut_init = false;

bool dut_oceanbase::check_whether_block()
{
    // dut_oceanbase another_dut(test_db, 0);
    if (another_dut_init == false) {
        another_dut = make_shared<dut_oceanbase>(test_db, test_port);
        another_dut_init = true;
    }
    string get_block_tid = "select SESSION_ID from oceanbase.v$lock_wait_stat;";
    vector<string> output;
    another_dut->block_test(get_block_tid, &output);
    
    // check output
    string tid_str = to_string(thread_id);
    auto output_size = output.size();
    for (int i = 0; i < output_size; i++) {
        if (tid_str == output[i])
            return true;
    }

    return false;
}

void dut_oceanbase::block_test(const std::string &stmt, std::vector<std::string>* output, int* affected_row_num)
{
    if (mysql_real_query(&mysql, stmt.c_str(), stmt.size())) {
        string err = mysql_error(&mysql);
        auto result = mysql_store_result(&mysql);
        mysql_free_result(result);
        if (err.find("Commands out of sync") != string::npos ||
                err.find("No memory or reach tenant memory limit") != string::npos) {// occasionally happens, retry the statement again
            cerr << err << " in block_test, repeat the statement again" << endl;
            block_test(stmt, output, affected_row_num);
            return;
        }
        if (regex_match(err, e_crash)) {
            int reconnect_time = 0;
            while (mariadb_reconnect(&mysql) != 0) {
                reconnect_time++;
                if (reconnect_time > RECONNECT_TRY_TIME) 
                    throw std::runtime_error(err + " in " + debug_info); 
                cerr << "Can't connect to OceanBase server, reconnecting:" << debug_info << endl;
                usleep(500000);
            }
            // succeed to reconnect
            block_test(stmt, output, affected_row_num);
            return;
        }
        throw std::runtime_error(err + " in " + debug_info); 
    }

    if (affected_row_num)
        *affected_row_num = mysql_affected_rows(&mysql);

    auto result = mysql_store_result(&mysql);
    if (mysql_errno(&mysql) != 0) {
        string err = mysql_error(&mysql);
        mysql_free_result(result);
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

void dut_oceanbase::test(const string &stmt, vector<vector<string>>* output, int* affected_row_num)
{
    int err;
    if (txn_abort == true) {
        if (stmt == "COMMIT;") 
            throw std::runtime_error("txn aborted, can only rollback \nLocation: " + debug_info);
        if (stmt == "ROLLBACK;")
            return;
        throw std::runtime_error("txn aborted, stmt skipped \nLocation: " + debug_info);
    }

    if (has_sent_sql == false) {
        query_status = mysql_real_query_start(&err, &mysql, stmt.c_str(), stmt.size());
        if (mysql_errno(&mysql) != 0) {
            string err = mysql_error(&mysql);
            auto result = mysql_store_result(&mysql);
            mysql_free_result(result);
            has_sent_sql = false;
            sent_sql = "";
            throw std::runtime_error("mysql_real_query_start fails, stmt skipped: " + err + "\nLocation: " + debug_info); 
        }
	    sent_sql = stmt;
        has_sent_sql = true;
    }

    if (sent_sql != stmt) 
        throw std::runtime_error("sent sql stmt changed in " + debug_info + 
            "\nsent_sql: " + sent_sql +
            "\nstmt: " + stmt); 

    auto begin_time = get_cur_time_ms();
    while (1) {
        query_status = mysql_real_query_cont(&err, &mysql, query_status);
        if (mysql_errno(&mysql) != 0) {
            string err = mysql_error(&mysql);
            has_sent_sql = false;
            sent_sql = "";
            auto result = mysql_store_result(&mysql);
            mysql_free_result(result);
            
            if (err.find("Commands out of sync") != string::npos) {// occasionally happens, retry the statement again
                cerr << err << ", repeat the statement again" << endl;
                test(stmt, output, affected_row_num);
                return;
            }
            if (err.find("Deadlock found") != string::npos) 
                txn_abort = true;
            throw std::runtime_error("mysql_real_query_cont fails, stmt skipped: " + err + "\nLocation: " + debug_info); 
        }
        if (query_status == 0) 
                break;
        
        auto cur_time = get_cur_time_ms();
        if (cur_time - begin_time >= MYSQL_STMT_BLOCK_MS) {
            auto blocked = check_whether_block();
            if (blocked == true)
                throw std::runtime_error("blocked in " + debug_info); 
            begin_time = cur_time;
        }
    }

    if (affected_row_num)
        *affected_row_num = mysql_affected_rows(&mysql);

    auto result = mysql_store_result(&mysql);
    if (mysql_errno(&mysql) != 0) {
        string err = mysql_error(&mysql);
        mysql_free_result(result);
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
            vector<string> row_output;
            for (int i = 0; i < column_num; i++) {
                string str;
                if (row[i] == NULL)
                    str = "NULL";
                else
                    str = row[i];
                row_output.push_back(str);
            }
            output->push_back(row_output);
        }
    }
    mysql_free_result(result);

    has_sent_sql = false;
    sent_sql = "";
    return;
}

void dut_oceanbase::reset(void)
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

void dut_oceanbase::backup(void)
{
    string mysql_dump = "/u01/obclient/bin/mysqldump -uroot -h 127.0.0.1 -P2881 " + test_db + " > /tmp/mysql_bk.sql";
    int ret = system(mysql_dump.c_str());
    if (ret != 0) {
        std::cerr << "backup fail \nLocation: " + debug_info << endl;
        throw std::runtime_error("backup fail \nLocation: " + debug_info); 
    }
}

void dut_oceanbase::reset_to_backup(void)
{
    reset();
    string bk_file = "/tmp/mysql_bk.sql";
    if (access(bk_file.c_str(), F_OK ) == -1) 
        return;
    
    mysql_close(&mysql);
    
    string mysql_source = "/u01/obclient/bin/obclient --force -uroot -h 127.0.0.1 -P2881 -D " + test_db + " < /tmp/mysql_bk.sql 2>/dev/null";
    if (system(mysql_source.c_str()) == -1) 
        throw std::runtime_error(string("system() error, return -1") + "\nLocation: " + debug_info);
    
    if (!mysql_init(&mysql))
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
    
    mysql_options(&mysql, MYSQL_OPT_NONBLOCK, 0);

    if (!mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, test_db.c_str(), test_port, NULL, 0)) 
        throw std::runtime_error(string(mysql_error(&mysql)) + "\nLocation: " + debug_info);
}

int dut_oceanbase::save_backup_file(string path)
{
    string cp_cmd = "cp /tmp/mysql_bk.sql " + path;
    return system(cp_cmd.c_str());
}

void dut_oceanbase::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table:tables_name) {
        vector<vector<string>> table_content;
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
                vector<string> row_output;
                for (int i = 0; i < column_num; i++) {
                    string str;
                    if (row[i] == NULL)
                        str = "NULL";
                    else
                        str = row[i];
                    row_output.push_back(str);
                }
                table_content.push_back(row_output);
            }
        }
        mysql_free_result(result);

        content[table] = table_content;
    }
}

string dut_oceanbase::begin_stmt() {
    return "START TRANSACTION";
}

string dut_oceanbase::commit_stmt() {
    return "COMMIT";
}

string dut_oceanbase::abort_stmt() {
    return "ROLLBACK";
}

pid_t dut_oceanbase::fork_db_server()
{
    pid_t child = -1;
    // start cluster server
    auto ret = system("/usr/bin/obd cluster start obce-single");
    if (ret == -1) {
        cerr << "fail to start obce-single \nLocation: " << debug_info << endl;
        throw runtime_error("fail to start obce-single \nLocation: " + debug_info);
    }

    // get pid 
    system("pidof observer > /tmp/observer_pid.txt");
    ifstream ifile("/tmp/observer_pid.txt");
    ifile >> child;
    ifile.close();

    sleep(3);
    another_dut.reset();
    another_dut_init = false;
    cout << "server pid: " << child << endl;
    return child;
}
