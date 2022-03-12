#include "general_process.hh"

int make_dir_error_exit(string folder)
{
    if (mkdir(folder.c_str(), 0700)) {
        cout << "fail to mkdir "<< folder << endl;
        exit(-1);
    }

    return 0;
}

shared_ptr<schema> get_schema(dbms_info& d_info)
{
    shared_ptr<schema> schema;
    static int try_time = 0;
    try {
        if (false) {}
        #ifdef HAVE_LIBSQLITE3
        else if (d_info.dbms_name == "sqlite") 
            schema = make_shared<schema_sqlite>(d_info.test_db, true);
        #endif

        #ifdef HAVE_LIBMYSQLCLIENT
        else if (d_info.dbms_name == "mysql") 
            schema = make_shared<schema_mysql>(d_info.test_db, d_info.test_port);
        else if (d_info.dbms_name == "tidb") 
            schema = make_shared<schema_tidb>(d_info.test_db, d_info.test_port);
        #endif

        else if (d_info.dbms_name == "cockroach")
            schema = make_shared<schema_cockroachdb>(d_info.test_db, d_info.test_port);
        else {
            cerr << d_info.dbms_name << " is not supported yet" << endl;
            throw runtime_error("Unsupported DBMS");
        }
    } catch (exception &e) { // may occur occastional error
        if (try_time >= 128) {
            cerr << "Fail in get_schema() " << try_time << " times, return" << endl;
            cerr << "exception: " << e.what() << endl;
	        throw e;
        }
        try_time++;
        schema = get_schema(d_info);
        try_time--;
        return schema;
    }
    return schema;
}

shared_ptr<dut_base> dut_setup(dbms_info& d_info)
{
    shared_ptr<dut_base> dut;
    if (false) {}
    #ifdef HAVE_LIBSQLITE3
    else if (d_info.dbms_name == "sqlite")
        dut = make_shared<dut_sqlite>(d_info.test_db);
    #endif

    #ifdef HAVE_LIBMYSQLCLIENT
    else if (d_info.dbms_name == "mysql")
        dut = make_shared<dut_mysql>(d_info.test_db, d_info.test_port);
    else if (d_info.dbms_name == "tidb")
        dut = make_shared<dut_tidb>(d_info.test_db, d_info.test_port);
    #endif

    else if (d_info.dbms_name == "cockroach")
        dut = make_shared<dut_cockroachdb>(d_info.test_db, d_info.test_port);
    else {
        cerr << d_info.dbms_name << " is not supported yet" << endl;
        throw runtime_error("Unsupported DBMS");
    }

    return dut;
}

pid_t fork_db_server(dbms_info& d_info)
{
    pid_t fork_pid;
    if (false) {}
    #ifdef HAVE_LIBSQLITE3
    else if (d_info.dbms_name == "sqlite")
        fork_pid = 0;
    #endif
    
    #ifdef HAVE_LIBMYSQLCLIENT
    else if (d_info.dbms_name == "mysql")
        fork_pid = dut_mysql::fork_db_server();
    else if (d_info.dbms_name == "tidb")
        fork_pid = dut_tidb::fork_db_server();
    #endif

    else if (d_info.dbms_name == "cockroach")
        fork_pid = dut_cockroachdb::fork_db_server();
    else {
        cerr << d_info.dbms_name << " is not supported yet" << endl;
        throw runtime_error("Unsupported DBMS");
    }

    return fork_pid;
}

void user_signal(int signal)  
{  
    if(signal != SIGUSR1) {  
        printf("unexpect signal %d\n", signal);  
        exit(1);  
    }  
     
    cerr << "get SIGUSR1, stop the thread" << endl;
    pthread_exit(0);
}

void* test_thread(void* argv)
{
    auto data = (test_thread_arg *)argv;
    try {
        auto dut = dut_setup(*(data->d_info));
        dut->test(*(data->stmt), data->stmt_output, data->affected_row_num);
    } catch (std::exception &e) {
        // cerr << "In test thread: " << e.what() << endl;
        // cerr << *(data->stmt) << endl;
        // exit(166);
        string err = e.what();
        if (err.find("BUG") != string::npos) {
            cerr << "BUG trigger in test_thread: " << err << endl;
        }
        data->e = e;
        data->has_exception = true;
    }
    
    pthread_mutex_lock(&mutex_timeout);  
    pthread_cond_signal(&cond_timeout);  
    pthread_mutex_unlock(&mutex_timeout);

    return NULL;
}

void dut_test(dbms_info& d_info, const string& stmt, bool need_affect)
{   
    // set timeout limit
    struct timespec m_time;
    m_time.tv_sec = time(NULL) + STATEMENT_TIMEOUT;  
    m_time.tv_nsec = 0; 

    // set pthread parameter 
    vector<string> stmt_output;
    int affected_row_num = 0;

    pthread_t thread;
    test_thread_arg data;
    data.d_info = &d_info;
    auto str = stmt;
    data.stmt = &str;
    data.has_exception = false;
    data.affected_row_num = &affected_row_num;
    data.stmt_output = &stmt_output;

    // record the test case firstly
    ofstream ofile("cur_test_smt.sql");
    ofile << stmt << endl;
    ofile.close();

    pthread_create(&thread, NULL, test_thread, &data);

    pthread_mutex_lock(&mutex_timeout);  
    int res = pthread_cond_timedwait(&cond_timeout, &mutex_timeout, (const struct timespec *)&m_time);  
    pthread_mutex_unlock(&mutex_timeout);

    if (res == ETIMEDOUT) {
        cerr << "thread timeout!" << endl;
        pthread_kill(thread, SIGUSR1);
        pthread_join(thread, NULL);

        throw runtime_error(string("timeout in this stmt"));
    }

    pthread_join(thread, NULL);

    if (data.has_exception)
        throw data.e;
    
    if (need_affect && affected_row_num <= 0 && stmt_output.empty()) {
        throw runtime_error(string("affect result empty"));
    }
}

void dut_reset(dbms_info& d_info)
{
    auto dut = dut_setup(d_info);
    dut->reset();
}

void dut_backup(dbms_info& d_info)
{
    auto dut = dut_setup(d_info);
    dut->backup();
}

void dut_reset_to_backup(dbms_info& d_info)
{
    cerr << YELLOW << "reset to backup" << RESET << endl;
    auto dut = dut_setup(d_info);
    dut->reset_to_backup();
}

void dut_get_content(dbms_info& d_info, 
                    map<string, vector<string>>& content)
{
    vector<string> table_names;
    auto schema = get_schema(d_info);
    for (auto& table:schema->tables)
        table_names.push_back(table.ident());
    
    auto dut = dut_setup(d_info);
    dut->get_content(table_names, content);
}

void interect_test(dbms_info& d_info, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    vector<string>& rec_vec,
                    bool need_affect)
{
    auto schema = get_schema(d_info);
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);

    static int try_time = 0;
    try {
        dut_test(d_info, s.str(), need_affect);
        auto sql = s.str() + ";";
        rec_vec.push_back(sql);
    } catch(std::exception &e) { // ignore runtime error
        string err = e.what();
        if (err.find("syntax") != string::npos) {
            cerr << "\n" << e.what() << "\n" << endl;
            cerr << s.str() << endl;
        }
        if (try_time >= 128) {
            cerr << "Fail in interect_test() " << try_time << " times, return" << endl;
            throw e;
        }
        try_time++;
        interect_test(d_info, tmp_statement_factory, rec_vec, need_affect);
        try_time--;
    }
}

void normal_test(dbms_info& d_info, 
                    shared_ptr<schema>& schema, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    vector<string>& rec_vec,
                    bool need_affect)
{
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);

    static int try_time = 0;
    try {
        dut_test(d_info, s.str(), need_affect);
        auto sql = s.str() + ";";
        rec_vec.push_back(sql);
    } catch(std::exception &e) { // ignore runtime error
        string err = e.what();
        if (err.find("syntax") != string::npos) {
            cerr << "trigger a syntax problem: " << err << endl;
            cerr << "sql: " << s.str();
        }

        if (err.find("timeout") != string::npos)
            cerr << "time out in normal test: " << err << endl;

        if (err.find("BUG") != string::npos) {
            cerr << "BUG is triggered in normal_test: " << err << endl;
            throw e;
        }
        
        if (try_time >= 128) {
            cerr << "Fail in normal_test() " << try_time << " times, return" << endl;
            throw e;
        }
        try_time++;
        normal_test(d_info, schema, tmp_statement_factory, rec_vec, need_affect);
        try_time--;
    }
}

static size_t BKDRHash(const char *str, size_t hash)  
{
    while (size_t ch = (size_t)*str++)  {         
        hash = hash * 131 + ch;   // 也可以乘以31、131、1313、13131、131313..  
    }  
    return hash;  
}

static void hash_output_to_set(vector<string> &output, vector<size_t>& hash_set)
{
    size_t hash = 0;
    size_t output_size = output.size();
    for (size_t i = 0; i < output_size; i++) {
        if (output[i] == "\n") {
            hash_set.push_back(hash);
            hash = 0;
            continue;
        }

        hash = BKDRHash(output[i].c_str(), hash);
    }

    // sort the set, because some output order is random
    sort(hash_set.begin(), hash_set.end());
    return;
}

static void output_diff(string item_name, vector<string>& con_result, vector<string>& seq_result)
{
    ofstream ofile("/tmp/comp_diff.txt", ios::app);
    ofile << "============================" << endl;
    ofile << "item name: " << item_name << endl;
    ofile << "concurrent: " << endl;
    for (auto& str : con_result) {
        ofile << "    " << str;
    }
    ofile << endl;
    ofile << "sequential: " << endl;
    for (auto& str : seq_result) {
        ofile << "    " << str;
    }
    ofile.close();
}

static bool is_number(const string &s) {
    if (s.empty() || s.length() <= 0) 
        return false;

    int point = 0;
    if (s.length() == 1 && (s[0] >'9' || s[0] < '0')) 
        return false;

    if(s.length() > 1) {
        if (s[0]!='.' && (s[0] >'9' || s[0] < '0')&&s[0]!='-' && s[0]!='+') 
            return false;
        
        if (s[0] == '.') 
            ++point;

        if ((s[0] == '+' || s[0] == '-') && (s[1] >'9' || s[1] < '0')) 
            return false;

        for (size_t i = 1; i < s.length(); ++i) {
            if (s[i]!='.' && (s[i] >'9' || s[i] < '0')) 
                return false;

            if (s[i] == '.') 
                ++point;
        }
    }

    if (point > 1) return false;
    
    return true;
}

static bool nomoalize_content(vector<string> &content)
{
    auto size = content.size();

    for (int i = 0; i < size; i++) {
        auto str = content[i];
        double value = 0;
        
        if (!is_number(str) || str.find(".") == string::npos)
            continue;

        // value is a float
        value = stod(str);
        value = round(value * 100) / 100; // keep 2 number after the point
        content[i] = to_string(value);
    }
    return true;
}

static bool compare_content(map<string, vector<string>>&con_content, 
                     map<string, vector<string>>&seq_content)
{
    if (con_content.size() != seq_content.size()) {
        cerr << "size not equal: " << con_content.size() << " " << seq_content.size() << endl;
        return false;
    }
    
    for (auto iter = con_content.begin(); iter != con_content.begin(); iter++) {
        auto& table = iter->first;
        auto& con_table_content = iter->second;
        
        if (seq_content.count(table) == 0) {
            cerr << "seq_content does not have " << table << endl;
            return false;
        }

        auto& seq_table_content = seq_content[table];

        nomoalize_content(con_table_content);
        nomoalize_content(seq_table_content);

        vector<size_t> con_table_set, seq_table_set;
        hash_output_to_set(con_table_content, con_table_set);
        hash_output_to_set(seq_table_content, seq_table_set);

        auto size = con_table_set.size();
        if (size != seq_table_set.size()) {
            cerr << "table " + table + " sizes are not equal" << endl;
            output_diff(table, con_table_content, seq_table_content);
            return false;
        }

        for (auto i = 0; i < size; i++) {
            if (con_table_set[i] != seq_table_set[i]) {
                cerr << "table " + table + " content are not equal" << endl;
                output_diff(table, con_table_content, seq_table_content);
                return false;
            }
        }
    }

    return true;
}

static bool compare_output(vector<vector<string>>& trans_output,
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
    
        nomoalize_content(trans_stmt_output);
        nomoalize_content(seq_stmt_output);
        
        vector<size_t> trans_hash_set, seq_hash_set;
        hash_output_to_set(trans_stmt_output, trans_hash_set);
        hash_output_to_set(seq_stmt_output, seq_hash_set);

        size_t stmt_output_size = trans_hash_set.size();
        if (stmt_output_size != seq_hash_set.size()) {
            cerr << "stmt[" << i << "] output sizes are not equel: " << trans_hash_set.size() << " " << seq_hash_set.size() << endl;
            output_diff("stmt["+ to_string(i) + "]", trans_stmt_output, seq_stmt_output);
            return false;
        }

        for (auto j = 0; j < stmt_output_size; j++) {
            if (trans_hash_set[j] != seq_hash_set[j]) {
                cerr << "stmt[" << i << "] output are not equel" << endl;
                output_diff("stmt["+ to_string(i) + "]", trans_stmt_output, seq_stmt_output);
                return false;
            }
        }
    }

    return true;
}

int generate_database(dbms_info& d_info)
{
    vector<string> stage_1_rec;
    vector<string> stage_2_rec;
    
    // stage 1: DDL stage (create, alter, drop)
    cerr << YELLOW << "stage 1: generate the shared database" << RESET << endl;
    auto ddl_stmt_num = d6() + 1; // at least 2 statements to create 2 tables
    for (auto i = 0; i < ddl_stmt_num; i++)
        interect_test(d_info, &ddl_statement_factory, stage_1_rec, false); // has disabled the not null, check and unique clause 

    // stage 2: basic DML stage (only insert),
    cerr << YELLOW << "stage 2: insert data into the database" << RESET << endl;
    auto basic_dml_stmt_num = 3 + d9(); // 4-10 statements to insert data
    auto schema = get_schema(d_info); // schema will not change in this stage
    for (auto i = 0; i < basic_dml_stmt_num; i++) 
        normal_test(d_info, schema, &basic_dml_statement_factory, stage_2_rec, true);


    // stage 3: backup database
    cerr << YELLOW << "stage 3: backup the database" << RESET << endl;
    dut_backup(d_info);

    return 0;
}

void new_gen_trans_stmts(shared_ptr<schema> &db_schema,
                        int trans_stmt_num,
                        vector<string>& trans_rec,
                        dbms_info& d_info)
{
    auto can_error = d_info.can_trigger_error_in_txn;
    if (can_error == false)
        dut_reset_to_backup(d_info);
    
    scope scope;
    db_schema->fill_scope(scope);
    for (int i = 0; i < trans_stmt_num; i++) {
        cerr << "generating statement ...";
        shared_ptr<prod> gen = trans_statement_factory(&scope);
        cerr << "done" << endl;

        ostringstream stmt_stream;
        gen->out(stmt_stream);
        auto stmt = stmt_stream.str() + ";";

        if (can_error == false) {
            try {
                cerr << "checking (executing) statement ...";
                auto dut = dut_setup(d_info);
                dut->test(stmt);
                cerr << "done" << endl;
            } catch (exception &e) {
                i = i - 1; // generate a stmt again
                string err = e.what();
                if (err.find("CONNECTION FAIL") != string::npos)
                    throw e;
                if (err.find("BUG") != string::npos)
                    throw e;
                cerr << "err: " << e.what() << ", try again" << endl;
                continue;
            }
        }

        trans_rec.push_back(stmt);
    }
}