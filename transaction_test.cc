#include "transaction_test.hh"

#define NORMAL_BUG_FILE "bug_trigger_stmt.sql"
#define GEN_STMT_FILE "gen_stmts.sql"

#define KILL_PROC_TIME_MS 10000
#define WAIT_FOR_PROC_TIME_MS 20000

int make_dir_error_exit(string folder)
{
    if (mkdir(folder.c_str(), 0700)) {
        cout << "fail to mkdir "<< folder << endl;
        exit(-1);
    }

    return 0;
}

shared_ptr<schema> get_schema(map<string,string>& options)
{
    shared_ptr<schema> schema;
    static int try_time = 0;
    try {
        if (options.count("sqlite")) {
            #ifdef HAVE_LIBSQLITE3
            schema = make_shared<schema_sqlite>(options["sqlite"], true);
            #else
            cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
            throw runtime_error("Does not support SQLite");
            #endif
        } else if (options.count("mysql-db") && options.count("mysql-port")) {
            #ifdef HAVE_LIBMYSQLCLIENT
            schema = make_shared<schema_tidb>(options["mysql-db"], stoi(options["mysql-port"]));
            #else
            cerr << "Sorry, " PACKAGE_NAME " was compiled without MySQL support." << endl;
            throw runtime_error("Does not support MySQL");
            #endif
        } else if (options.count("cockroach-db") && options.count("cockroach-port")) {
            schema = make_shared<schema_cockroachdb>(options["cockroach-db"], stoi(options["cockroach-port"]));

        } else if (options.count("monetdb")) {
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
    } catch (exception &e) { // may occur occastional error
        if (try_time >= 128) {
            cerr << "Fail in get_schema() " << try_time << " times, return" << endl;
            cerr << "exception: " << e.what() << endl;
	        throw e;
        }
        try_time++;
        schema = get_schema(options);
        try_time--;
        return schema;
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
    } else if (options.count("mysql-db") && options.count("mysql-port")) {
        #ifdef HAVE_LIBMYSQLCLIENT
        dut = make_shared<dut_tidb>(options["mysql-db"], stoi(options["mysql-port"]));
        #else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MySQL support." << endl;
        throw runtime_error("Does not support MySQL");
        #endif
    } else if (options.count("cockroach-db") && options.count("cockroach-port")) {
        dut = make_shared<dut_cockroachdb>(options["cockroach-db"], stoi(options["cockroach-port"]));

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

bool get_serializability(map<string,string>& options)
{
    if (options.count("sqlite")) {
        #ifdef HAVE_LIBSQLITE3
        return true;
        #else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
        throw runtime_error("Does not support SQLite");
        #endif
    } else if (options.count("mysql-db") && options.count("mysql-port")) {
        #ifdef HAVE_LIBMYSQLCLIENT
        return false;
        #else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MySQL support." << endl;
        throw runtime_error("Does not support MySQL");
        #endif
    } else if (options.count("cockroach-db") && options.count("cockroach-port")) {
        return false;
    } 
    else {
        cerr << "Sorry,  you should specify a dbms and its database, or your dbms is not supported" << endl;
        throw runtime_error("Does not define target dbms and db");
    }

    return false;
}

bool can_trigger_error_in_transaction(map<string,string>& options)
{
    if (options.count("sqlite")) {
        #ifdef HAVE_LIBSQLITE3
        return true;
        #else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
        throw runtime_error("Does not support SQLite");
        #endif
    } else if (options.count("mysql-db") && options.count("mysql-port")) {
        #ifdef HAVE_LIBMYSQLCLIENT
        return true;
        #else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MySQL support." << endl;
        throw runtime_error("Does not support MySQL");
        #endif
    } else if (options.count("cockroach-db") && options.count("cockroach-port")) {
        return false;
    } 
    else {
        cerr << "Sorry,  you should specify a dbms and its database, or your dbms is not supported" << endl;
        throw runtime_error("Does not define target dbms and db");
    }

    return false;
}

pid_t fork_db_server(map<string,string>& options)
{
    pid_t fork_pid;
    if (false) {}
#ifdef HAVE_LIBSQLITE3
    else if (options.count("sqlite")) {
        fork_pid = 0;
    }
#endif
#ifdef HAVE_LIBMYSQLCLIENT
    else if (options.count("mysql-db") && options.count("mysql-port")) {
        fork_pid = dut_tidb::fork_db_server();
    }
#endif
    else if (options.count("cockroach-db") && options.count("cockroach-port")) {
        fork_pid = dut_cockroachdb::fork_db_server();
    }
    else {
        cerr << "Sorry,  you should specify a dbms or your dbms not support" << endl;
        throw runtime_error("Does not define target dbms and db");
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
        auto dut = dut_setup(*(data->options));
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

void dut_test(map<string,string>& options, const string& stmt, bool need_affect)
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
    data.options = &options;
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
    cerr << YELLOW << "reset to backup" << RESET << endl;
    auto dut = dut_setup(options);
    dut->reset_to_backup();
}

void dut_get_content(map<string,string>& options, 
                    map<string, vector<string>>& content)
{
    vector<string> table_names;
    auto schema = get_schema(options);
    for (auto& table:schema->tables)
        table_names.push_back(table.ident());
    
    auto dut = dut_setup(options);
    dut->get_content(table_names, content);
}

void interect_test(map<string,string>& options, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    vector<string>& rec_vec,
                    bool need_affect)
{
    auto schema = get_schema(options);
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);

    static int try_time = 0;
    try {
        dut_test(options, s.str(), need_affect);
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
        interect_test(options, tmp_statement_factory, rec_vec, need_affect);
        try_time--;
    }
}

void normal_test(map<string,string>& options, 
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
        dut_test(options, s.str(), need_affect);
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
        normal_test(options, schema, tmp_statement_factory, rec_vec, need_affect);
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

int generate_database(map<string,string>& options, file_random_machine* random_file)
{
    vector<string> stage_1_rec;
    vector<string> stage_2_rec;
    
    // stage 1: DDL stage (create, alter, drop)
    cerr << YELLOW << "stage 1: generate the shared database" << RESET << endl;
    auto ddl_stmt_num = d6() + 1; // at least 2 statements to create 2 tables
    for (auto i = 0; i < ddl_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;

        interect_test(options, &ddl_statement_factory, stage_1_rec, false); // has disabled the not null, check and unique clause 
    }

    // stage 2: basic DML stage (only insert),
    cerr << YELLOW << "stage 2: insert data into the database" << RESET << endl;
    auto basic_dml_stmt_num = 3 + d9(); // 4-10 statements to insert data
    auto schema = get_schema(options); // schema will not change in this stage
    for (auto i = 0; i < basic_dml_stmt_num; i++) {
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
        
        normal_test(options, schema, &basic_dml_statement_factory, stage_2_rec, true);
    }

    // stage 3: backup database
    cerr << YELLOW << "stage 3: backup the database" << RESET << endl;
    dut_backup(options);

    return 0;
}

static void new_gen_trans_stmts(shared_ptr<schema> &db_schema,
                        int trans_stmt_num,
                        vector<string>& trans_rec,
                        map<std::string, std::string> *options,
                        bool can_trigger_err)
{
    if (can_trigger_err == false)
        dut_reset_to_backup(*options);
    
    scope scope;
    db_schema->fill_scope(scope);
    for (int i = 0; i < trans_stmt_num; i++) {
        cerr << "generating statement ...";
        shared_ptr<prod> gen = trans_statement_factory(&scope);
        cerr << "done" << endl;

        ostringstream stmt_stream;
        gen->out(stmt_stream);
        auto stmt = stmt_stream.str() + ";";

        if (can_trigger_err == false) {
            try {
                cerr << "checking (executing) statement ...";
                auto dut = dut_setup(*options);
                dut->test(stmt);
                cerr << "done" << endl;
            } catch (exception &e) {
                i = i - 1; // generate a stmt again
                continue;
            }
        }

        trans_rec.push_back(stmt);
    }
}

void transaction_test::arrage_trans_for_tid_queue()
{
    for (int tid = 0; tid < trans_num; tid++)
        trans_arr[tid].stmt_num = 0;
    
    for (int i = 0; i < stmt_num; i++) {
        int tid = dx(trans_num) - 1;
        tid_queue.push_back(tid);
        trans_arr[tid].stmt_num++;
    }

    // each transaction at least has two statement (begin and commit/abort)
    for (int tid = 0; tid < trans_num; tid++) {
        while (trans_arr[tid].stmt_num < 2) {
            int pos = dx(stmt_num) - 1;
            tid_queue.insert(tid_queue.begin() + pos, tid);
            stmt_num++;
            trans_arr[tid].stmt_num++;
        }
    }

    if (is_serializable)
        return;
    
    // for non-serializable, make the commit trans executing without interleaving
    int holder_tid = -7;
    for (int i = 0; i < stmt_num; i++) { // replace the tid with holder tid
        auto tid = tid_queue[i];
        if (tid < commit_num) 
            tid_queue[i] = holder_tid;
    }

    int tid_now = 0;
    int arranged_stmt_num = 0;
    for (int i = 0; i < stmt_num; i++) {
        if (tid_queue[i] != holder_tid)
            continue;
        
        tid_queue[i] = tid_now;
        arranged_stmt_num++;

        if (arranged_stmt_num < trans_arr[tid_now].stmt_num)
            continue;
        
        tid_now++;
        arranged_stmt_num = 0;

        if (tid_now >= commit_num)
            break;
    }

    return;
}

void transaction_test::assign_trans_status()
{   
    for (int i = 0; i < commit_num; i++) 
        trans_arr[i].status = 1;

    for (int i = commit_num; i < trans_num; i++) 
        trans_arr[i].status = 2;
    
    cerr << YELLOW << "show status" << RESET << endl;
    for (int i = 0; i < trans_num; i++) {
        cerr << i << " " << trans_arr[i].status << endl;
    }

    return;
}

void transaction_test::gen_stmt_for_each_trans()
{    
    shared_ptr<int[]> stmt_pos_of_trans(new int[trans_num]);
    
    auto schema = get_schema(*options);
    for (int tid = 0; tid < trans_num; tid++) {
        trans_arr[tid].dut = dut_setup(*options);
        stmt_pos_of_trans[tid] = 0;
        
        // save 2 stmts for begin and commit/abort
        smith::rng.seed(time(NULL));
        new_gen_trans_stmts(schema, trans_arr[tid].stmt_num - 2, trans_arr[tid].stmts, options, can_trigger_error);
        trans_arr[tid].dut->wrap_stmts_as_trans(trans_arr[tid].stmts, trans_arr[tid].status == 1);
    }

    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto &stmt = trans_arr[tid].stmts[stmt_pos_of_trans[tid]];
        stmt_queue.push_back(stmt);
        stmt_pos_of_trans[tid]++;
    }
}

// 2: fatal error (e.g. restart transaction, current transaction is aborted), skip the stmt
// 1: executed
// 0: blocked, not executed
int transaction_test::trans_test_unit(int stmt_pos)
{
    auto tid = tid_queue[stmt_pos];
    auto &stmt = stmt_queue[stmt_pos];
    vector<string> output;

    try {
        trans_arr[tid].dut->test(stmt, &output);
        if (!output.empty())
            trans_arr[tid].stmt_outputs.push_back(output);
        
        cerr << "S" << stmt_pos << " T" << tid << ": " << stmt.substr(0, stmt.size() > 20 ? 20 : stmt.size()) << endl;

        // if (trans_arr[tid].status == 1 && trans_arr[tid].dut->is_commit_abort_stmt(stmt)) {
        //     cerr << "getting content of trans " << tid << " (transaction one) " << endl;
        //     dut_get_content(*options, trans_arr[tid].committed_content); // get the content after commit
        // }

        return 1;
    
    } catch(exception &e) {
        string err = e.what();
        cerr << RED 
            << "S" << stmt_pos << " T" << tid << ": " << stmt.substr(0, stmt.size() > 20 ? 20 : stmt.size()) << ": fail, err: " 
            << err << RESET << endl;

        if (err.find("ost connection") != string::npos)
            throw e;
        
        if (err.find("blocked") != string::npos) {
            return 0;
        }

        if (err.find("skipped") != string::npos) {
            return 2;
        }

        if (err.find("sent sql stmt changed") != string::npos) {
            exit(-1);
        }
        
        // store the error info of non-commit statement
        if (!trans_arr[tid].dut->is_commit_abort_stmt(stmt)) {
            trans_arr[tid].stmt_err_info.push_back(err);
            return 1;
        }
        
        // abort fail, just do nothing, return executed (i.e. 1)
        if (trans_arr[tid].status == 2)
            return 1;
        
        // if commit fail, just abort
        trans_arr[tid].status = 2;
        trans_arr[tid].stmts.erase(trans_arr[tid].stmts.begin());
        trans_arr[tid].stmts.pop_back();
        trans_arr[tid].dut->wrap_stmts_as_trans(trans_arr[tid].stmts, false);
        stmt_queue[stmt_pos] = trans_arr[tid].stmts.back();

        stmt = stmt_queue[stmt_pos];
        try {
            trans_arr[tid].dut->test(stmt);
            cerr << "S" << stmt_pos << " T" << tid << ": " << stmt.substr(0, stmt.size() > 20 ? 20 : stmt.size()) << endl;
            return 1;
            
        } catch(exception &e2) {
            err = e2.what();
            cerr << RED 
            << "S" << stmt_pos << " T" << tid << ": " << stmt.substr(0, stmt.size() > 20 ? 20 : stmt.size()) << ": fail, err: " 
            << err << RESET << endl;
        }
    }

    return 0;
}

bool transaction_test::check_commit_trans_blocked()
{
    bool has_other_commit_blocked = false;
    for (int tmp_tid = 0; tmp_tid < trans_num; tmp_tid++) {
        if (trans_arr[tmp_tid].status == 1 && trans_arr[tmp_tid].is_blocked) {
            has_other_commit_blocked = true;
            break;
        }
    }
    return has_other_commit_blocked;
}


void transaction_test::retry_block_stmt(int cur_stmt_num, shared_ptr<int[]> status_queue)
{
    cerr << YELLOW << "retrying process begin..." << RESET << endl;

    // firstly try the first stmt of each blocked transaction
    set<int> first_tried_tid;
    for (int i = 0; i < cur_stmt_num; i++) {
        if (status_queue[i] == 1)
            continue;
        
        auto tid = tid_queue[i];
        if (trans_arr[tid].is_blocked == false)
            continue;

        if (first_tried_tid.count(tid) != 0) // have tried
            continue;
        
        first_tried_tid.insert(tid);
        auto is_executed = trans_test_unit(i);
        if (is_executed == 1) { // executed
            trans_arr[tid].is_blocked = false;
            status_queue[i] = 1;
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(stmt_queue[i]);
        } else if (is_executed == 2) { // skipped
            trans_arr[tid].is_blocked = false;
            status_queue[i] = 1;
        } else {// blocked
            trans_arr[tid].is_blocked = true;
        }
    }
    
    for (int stmt_pos = 0; stmt_pos < cur_stmt_num; stmt_pos++) {
        auto tid = tid_queue[stmt_pos];
        // skip the tried but still blocked transaction
        if (trans_arr[tid].is_blocked)
            continue;
        
        // skip the executed stmt
        if (status_queue[stmt_pos] == 1)
            continue;
        
        if (is_serializable == false && trans_arr[tid].status == 1) {
            if (check_commit_trans_blocked())
                continue;
        }

        auto is_executed = trans_test_unit(stmt_pos);
        // successfully execute the stmt, so label as not blocked
        if (is_executed == 1) {
            trans_arr[tid].is_blocked = false;
            status_queue[stmt_pos] = 1;
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(stmt_queue[stmt_pos]);

            if (trans_arr[tid].dut->is_commit_abort_stmt(stmt_queue[stmt_pos]))
                retry_block_stmt(stmt_pos, status_queue);
        } else if (is_executed == 2) { // skipped
            trans_arr[tid].is_blocked = false;
            status_queue[stmt_pos] = 1;
        }
        else { // still blocked
            trans_arr[tid].is_blocked = true;
        }
    }
    cerr << YELLOW << "retrying process end..." << RESET << endl;
}

void transaction_test::trans_test()
{
    dut_reset_to_backup(*options);
    cerr << YELLOW << "transaction test" << RESET << endl;
    shared_ptr<int[]> status_queue(new int[stmt_num]);
    
    for (int i = 0; i < stmt_num; i++) 
        status_queue[i] = 0;
    
    for (int stmt_index = 0; stmt_index < stmt_num; stmt_index++) {
        auto tid = tid_queue[stmt_index];
        auto& stmt = stmt_queue[stmt_index];
        
        if (trans_arr[tid].is_blocked)
            continue;
        
        // if there is some committed transaction blocked
        // this committed transaction cannot execute (not serialiazability)
        if (is_serializable == false && trans_arr[tid].status == 1) {
            if (check_commit_trans_blocked())
                continue;
        }
        
        auto is_executed = trans_test_unit(stmt_index);
        
        if (is_executed == 0) {
            trans_arr[tid].is_blocked = true;
            continue;
        }

        if (is_executed == 2) {
            status_queue[stmt_index] = 1;
            continue;
        }

        status_queue[stmt_index] = 1;
        real_tid_queue.push_back(tid);
        real_stmt_queue.push_back(stmt);
        
        // after a commit or abort, retry the statement
        if (trans_arr[tid].dut->is_commit_abort_stmt(stmt))
            retry_block_stmt(stmt_index, status_queue);
    }

    while (1) {
        int old_executed = 0;
        for (int i = 0; i < stmt_num; i++) {
            if (status_queue[i] == 1)
                old_executed++;
        }

        retry_block_stmt(stmt_num, status_queue);
        
        int new_executed = 0;
        for (int i = 0; i < stmt_num; i++) {
            if (status_queue[i] == 1)
                new_executed++;
        }

        if (old_executed == new_executed)
            break;
    }
    
    for (int i = 0; i < stmt_num; i++) {
        if (status_queue[i] == 1)
            continue;
        
        cerr << RED << "something error, some stmt is still not executed" << RESET << endl;
        exit(-1);
    }

    // collect database information
    dut_get_content(*options, trans_db_content);
}

void transaction_test::get_possible_order()
{
    vector<int> most_possible_trans_order;
    int real_stmt_num = real_tid_queue.size();
    for (int i = 0; i < real_stmt_num; i++) {
        auto tid = real_tid_queue[i];

        if (trans_arr[tid].status != 1) // do not consider abort transaction
            continue;
        
        // confirm their based on their commit order
        if (!trans_arr[tid].dut->is_commit_abort_stmt(real_stmt_queue[i]))
            continue;
            
        most_possible_trans_order.push_back(tid);
    }
    possible_normal_trans_order.push_back(most_possible_trans_order);

    // for non-serilizable, the commit transaction does not interleave, so only one possible result
    if (!is_serializable)
        return;

    // in serilized case, if commit num is not 2, it must be less than 2
    // in trans_test, committed_trans may change to aborted trans, so cannot use commit_num
    if (most_possible_trans_order.size() != 2)
        return;
    
    vector<int> another_possible_trans_order;
    another_possible_trans_order.push_back(most_possible_trans_order[1]);
    another_possible_trans_order.push_back(most_possible_trans_order[0]);
    possible_normal_trans_order.push_back(another_possible_trans_order);
    return;
}

void transaction_test::execute_possible_order()
{
    // get normal execute statement
    int real_stmt_num = real_tid_queue.size();
    for (int i = 0; i < real_stmt_num; i++) {
        auto real_tid = real_tid_queue[i];
        auto real_stmt = real_stmt_queue[i];
        trans_arr[real_tid].normal_stmts.push_back(real_stmt);
    }
    
    auto possible_order_num = possible_normal_trans_order.size();
    for (int i = 0; i < possible_order_num; i++) {
        auto trans_order = possible_normal_trans_order[i];   
        dut_reset_to_backup(*options);
        auto normal_dut = dut_setup(*options);
        
        for (auto tid : trans_order) {
            // if it is commit, erase "begin" and "commit"
            if (trans_arr[tid].status == 1) {
                trans_arr[tid].normal_stmts.erase(trans_arr[tid].normal_stmts.begin());
                trans_arr[tid].normal_stmts.pop_back();
            }

            vector<vector<string>> normal_output;
            vector<string> normal_err_info;

            auto normal_stmt_num = trans_arr[tid].normal_stmts.size();
            for (int i = 0; i < normal_stmt_num; i++) {
                auto& stmt = trans_arr[tid].normal_stmts[i];
                vector<string> output;
                try {
                    normal_dut->test(stmt, &output);
                    if (!output.empty())
                        normal_output.push_back(output);
                    
                    cerr << "T" << tid << ": " << stmt.substr(0, stmt.size() > 20 ? 20 : stmt.size()) << endl;
                } catch (exception &e) {
                    string err = e.what();
                    cerr << RED 
                        << "T" << tid << ": " << stmt.substr(0, stmt.size() > 20 ? 20 : stmt.size()) << ": fail, err: " 
                        << err << RESET << endl;
                    normal_err_info.push_back(err);
                }
            }

            trans_arr[tid].possible_normal_outputs.push_back(normal_output);
            trans_arr[tid].possible_normal_err_info.push_back(normal_err_info);
        }
        normal_dut.reset();
        map<string, vector<string>> normal_db_content;
        dut_get_content(*options, normal_db_content);
        possible_normal_db_content.push_back(normal_db_content);
    }

    return;
}

void transaction_test::normal_test()
{
    cerr << YELLOW << "normal test" << RESET << endl;
    get_possible_order();
    execute_possible_order();
    return;
}

bool transaction_test::check_one_order_result(int order_index)
{
    cerr << "check order: " << order_index << endl;
    if (!compare_content(trans_db_content, possible_normal_db_content[order_index])) {
        cerr << "trans_db_content is not equal to possible_normal_db_content[" << order_index << "]" << endl;
        return false;
    }

    for (auto i = 0; i < trans_num; i++) {
        if (trans_arr[i].stmt_num <= 2) // just ignore the 0 stmts, and the one only have begin, commit
            continue;
        if (trans_arr[i].status != 1) // donot check abort transaction
            continue;
        
        if (!compare_output(trans_arr[i].stmt_outputs, trans_arr[i].possible_normal_outputs[order_index])) {
            cerr << "trans "<< i << " output is not equal to possible_normal " << order_index << endl;
            return false;
        }
    }

    return true;
}

// true; no bug
// false: trigger a logic bug
bool transaction_test::check_result()
{
    auto order_num = possible_normal_trans_order.size();
    for (int index = 0; index < order_num; index++) {
        if (check_one_order_result(index))
            return true;
    }
    return false;
}

int transaction_test::test()
{
    try {
        arrage_trans_for_tid_queue();
        assign_trans_status();
        gen_stmt_for_each_trans();
    } catch(exception &e) {
        cerr << "Trigger a normal bugs when inializing the stmts" << endl;
        cerr << "Bug info: " << e.what() << endl;

        string dir_name = output_path_dir + "bug_" + to_string(record_bug_num) + "_normal/"; 
        record_bug_num++;

        make_dir_error_exit(dir_name);
        string cmd = "mv " + string(NORMAL_BUG_FILE) + " " + dir_name;
        system(cmd.c_str());
        
        // save database
        auto dut = dut_setup(*options);
        dut->save_backup_file(dir_name);

        exit(-1);
        // return 1; // not need to do other transaction thing
    }
    
    try {
        trans_test();
        normal_test();
        if (check_result())
            return 0;
    } catch(exception &e) {
        cerr << "error captured by test: " << e.what() << endl;
    }

    string dir_name = output_path_dir + "bug_" + to_string(record_bug_num) + "_trans/"; 
    record_bug_num++;
    make_dir_error_exit(dir_name);

    cerr << RED << "Saving database..." << RESET << endl;
    auto dut = dut_setup(*options);
    dut->save_backup_file(dir_name);
    
    cerr << RED << "Saving test cases..." << RESET;
    for (int i = 0; i < trans_num; i++) {
        string file_name = dir_name + "trans_" + to_string(i) + ".sql";
        ofstream ofile(file_name);
        for (auto& stmt : trans_arr[i].stmts) {
            ofile << stmt << endl;
            ofile << endl;
        }
        ofile.close();
    }

    string total_stmts_file = dir_name + "stmts.sql";
    ofstream total_stmt_output(total_stmts_file);
    for (int i = 0; i < stmt_num; i++) {
        total_stmt_output << stmt_queue[i] << endl;
        total_stmt_output << endl;
    }
    total_stmt_output.close();

    string total_tid_file = dir_name + "tid.txt";
    ofstream total_tid_output(total_tid_file);
    for (int i = 0; i < stmt_num; i++) {
        total_tid_output << tid_queue[i] << endl;
    }
    total_tid_output.close();

    cerr << RED << "done" << RESET << endl;
    
    exit(-1);
    return 1;
}

int transaction_test::record_bug_num = 0;
pid_t transaction_test::server_process_id = 0xabcde;

static unsigned long long get_cur_time_ms(void) {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

void kill_process_with_SIGTERM(pid_t process_id)
{
    kill(process_id, SIGTERM);
    int ret;
    auto begin_time = get_cur_time_ms();
    while (1) {
        ret = kill(process_id, 0);
        if (ret != 0)
            break;
        
        auto now_time = get_cur_time_ms();
        if (now_time - begin_time > KILL_PROC_TIME_MS)
            break;
    }
}

bool transaction_test::fork_if_server_closed()
{
    bool server_restart = false;
    auto time_begin = get_cur_time_ms();

    while (1) {
        try {
            auto dut = dut_setup(*options);
            if (server_restart)
                sleep(3);
            break; // connect successfully, so break;
        
        } catch (exception &e) { // connect fail
            auto ret = kill(server_process_id, 0);
            if (ret != 0) { // server has die
                cerr << "testing server die, restart it" << endl;

                kill_process_with_SIGTERM(server_process_id); // just for safe
                server_process_id = fork_db_server(*options);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }

            auto time_end = get_cur_time_ms();
            if (time_end - time_begin > WAIT_FOR_PROC_TIME_MS) {
                cerr << "testing server hang, kill it and restart" << endl;
                
                kill_process_with_SIGTERM(server_process_id);
                server_process_id = fork_db_server(*options);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }
        }
    }

    return server_restart;
}


transaction_test::transaction_test(map<string,string>& options_arg, 
                        file_random_machine* random_file_arg, 
                        bool is_seri,
                        bool can_trigger_err)
{
    options = &options_arg;
    random_file = random_file_arg;
    
    trans_num = d6() + 4; // 5 - 11
    stmt_num = trans_num * (3 + d12()); // average statement number of each transaction is 4 - 15
    
    is_serializable = is_seri;
    can_trigger_error = can_trigger_err;

    // commit_num = dx(trans_num);
    if (trans_num >= 2)
        commit_num = 2;
    else
        commit_num = trans_num;

    trans_arr = new transaction[trans_num];

    output_path_dir = "found_bugs/";
    struct stat buffer;
    if (stat(output_path_dir.c_str(), &buffer) != 0) {
        make_dir_error_exit(output_path_dir);
    }
}

transaction_test::~transaction_test()
{
    delete[] trans_arr;
}

bool reproduce_routine(map<string, string>& options,
                        bool is_serializable,
                        bool can_trigger_error,
                        vector<string>& stmt_queue, 
                        vector<int>& tid_queue)
{
    transaction_test re_test(options, NULL, is_serializable, can_trigger_error);
    re_test.stmt_queue = stmt_queue;
    re_test.tid_queue = tid_queue;
    re_test.stmt_num = re_test.tid_queue.size();

    int max_tid = -1;
    for (auto tid:tid_queue) {
        if (tid > max_tid)
            max_tid = tid;
    }

    re_test.trans_num = max_tid + 1;
    delete[] re_test.trans_arr;
    re_test.trans_arr = new transaction[re_test.trans_num];

    cerr << re_test.trans_num << " " << re_test.tid_queue.size() << " " << re_test.stmt_queue.size() << endl;
    if (re_test.tid_queue.size() != re_test.stmt_queue.size()) {
        cerr << "tid queue size should equal to stmt queue size" << endl;
        return 0;
    }

    // init each transaction stmt
    for (int i = 0; i < re_test.stmt_num; i++) {
        auto tid = re_test.tid_queue[i];
        re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
    }

    for (int tid = 0; tid < re_test.trans_num; tid++) {
        re_test.trans_arr[tid].dut = dut_setup(options);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty()) {
            re_test.trans_arr[tid].status = 2;
            continue;
        }

        if (re_test.trans_arr[tid].stmts.back().find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = 1;
        else
            re_test.trans_arr[tid].status = 2;
    }

    re_test.trans_test();
    re_test.normal_test();
    if (!re_test.check_result()) {
        cerr << "reproduce successfully" << endl;
        return true;
    }

    return false;
}