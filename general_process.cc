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
                    map<string, vector<vector<string>>>& content)
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
        auto dut = dut_setup(d_info);
        auto sql = s.str() + ";";
        int affect_num = 0;
        dut->test(sql, NULL, &affect_num);
        
        if (need_affect && affect_num <= 0)
            throw runtime_error(string("affect result empty"));
        
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
        auto dut = dut_setup(d_info);
        auto sql = s.str() + ";";
        int affect_num = 0;
        dut->test(sql, NULL, &affect_num);
        
        if (need_affect && affect_num <= 0)
            throw runtime_error(string("affect result empty"));
        cerr << sql.substr(0, sql.size() > 10 ? 10 : sql.size()) << " affect: " << affect_num << endl;
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

static void hash_output_to_set(vector<vector<string>> &output, vector<size_t>& hash_set)
{
    size_t hash = 0;
    auto row_size = output.size();
    for (int i = 0; i < row_size; i++) {
        auto column_size = output[i].size();
        for (int j = 0; j < column_size; j++)
            hash = BKDRHash(output[i][j].c_str(), hash);
        hash_set.push_back(hash);
        hash = 0;
    }

    // sort the set, because some output order is random
    sort(hash_set.begin(), hash_set.end());
    return;
}

static void output_diff(string item_name, vector<vector<string>>& a_result, vector<vector<string>>& b_result)
{
    ofstream ofile("/tmp/comp_diff.txt", ios::app);
    ofile << "============================" << endl;
    ofile << "item name: " << item_name << endl;
    ofile << "A result: " << endl;
    for (auto& row_str : a_result) {
        for (auto& str : row_str)
            ofile << "    " << str;
    }
    ofile << endl;
    ofile << "B result: " << endl;
    for (auto& row_str : b_result) {
        for (auto& str : row_str)
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

static bool nomoalize_content(vector<vector<string>> &content)
{
    auto size = content.size();

    for (int i = 0; i < size; i++) {
        auto column_num = content.size();
        for (int j = 0; j < column_num; j++) {
            auto str = content[i][j];
            double value = 0;
            
            if (!is_number(str) || str.find(".") == string::npos)
                continue;

            // value is a float
            value = stod(str);
            value = round(value * 100) / 100; // keep 2 number after the point
            content[i][j] = to_string(value);\
        }
    }
    return true;
}

bool compare_content(map<string, vector<vector<string>>>&a_content, 
                     map<string, vector<vector<string>>>&b_content)
{
    if (a_content.size() != b_content.size()) {
        cerr << "size not equal: " << a_content.size() << " " << b_content.size() << endl;
        return false;
    }
    
    for (auto iter = a_content.begin(); iter != a_content.begin(); iter++) {
        auto& table = iter->first;
        auto& con_table_content = iter->second;
        
        if (b_content.count(table) == 0) {
            cerr << "b_content does not have " << table << endl;
            return false;
        }

        auto& seq_table_content = b_content[table];

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

bool compare_output(vector<vector<vector<string>>>& a_output,
                    vector<vector<vector<string>>>& b_output)
{
    auto size = a_output.size();
    if (size != b_output.size()) {
        cerr << "stmt output sizes are not equel: "<< a_output.size() << " " << b_output.size() << endl;
        return false;
    }

    for (auto i = 0; i < size; i++) { // for each stmt
        auto& a_stmt_output = a_output[i];
        auto& b_stmt_output = b_output[i];
    
        nomoalize_content(a_stmt_output);
        nomoalize_content(b_stmt_output);
        
        vector<size_t> a_hash_set, b_hash_set;
        hash_output_to_set(a_stmt_output, a_hash_set);
        hash_output_to_set(b_stmt_output, b_hash_set);

        size_t stmt_output_size = a_hash_set.size();
        if (stmt_output_size != b_hash_set.size()) {
            cerr << "stmt[" << i << "] output sizes are not equel: " << a_hash_set.size() << " " << b_hash_set.size() << endl;
            output_diff("stmt["+ to_string(i) + "]", a_stmt_output, b_stmt_output);
            return false;
        }

        for (auto j = 0; j < stmt_output_size; j++) {
            if (a_hash_set[j] != b_hash_set[j]) {
                cerr << "stmt[" << i << "] output are not equel" << endl;
                output_diff("stmt["+ to_string(i) + "]", a_stmt_output, b_stmt_output);
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
    cerr << YELLOW << "stage 1: generate the shared database ..." << RESET;
    auto ddl_stmt_num = d6() + 1; // at least 2 statements to create 2 tables
    for (auto i = 0; i < ddl_stmt_num; i++)
        interect_test(d_info, &ddl_statement_factory, stage_1_rec, false); // has disabled the not null, check and unique clause 
    cerr << YELLOW << "finished" << RESET << endl;

    // stage 2: basic DML stage (only insert),
    cerr << YELLOW << "stage 2: insert data into the database ..." << RESET;
    auto basic_dml_stmt_num = 10 + d9(); // 11-20 statements to insert data
    auto schema = get_schema(d_info); // schema will not change in this stage
    for (auto i = 0; i < basic_dml_stmt_num; i++) 
        normal_test(d_info, schema, &basic_dml_statement_factory, stage_2_rec, true);
    cerr << YELLOW << "finished" << RESET << endl;


    // stage 3: backup database
    cerr << YELLOW << "stage 3: backup the database ..." << RESET;
    dut_backup(d_info);
    cerr << YELLOW << "finished" << RESET << endl;

    return 0;
}

void gen_stmts_for_one_txn(shared_ptr<schema> &db_schema,
                        int trans_stmt_num,
                        vector<shared_ptr<prod>>& trans_rec,
                        dbms_info& d_info)
{
    auto can_error = d_info.can_trigger_error_in_txn;
    // if (can_error == false || d_info.ouput_or_affect_num > 0)
    //     dut_reset_to_backup(d_info);
    
    scope scope;
    db_schema->fill_scope(scope);
    int stmt_num = 0;
    bool succeed = true;
    int fail_time = 0;
    int choice = -1;
    while (1) {
        cerr << "generating statement ...";
        if (succeed) 
            choice = d9();
        else { // if fail, do not change choice
            fail_time++;
            if (fail_time >= 8) {
                choice = d9();
                fail_time = 0;
            }
        }
        cerr << "choice: " << choice;
        shared_ptr<prod> gen = txn_statement_factory(&scope, choice);
        succeed = false;
        cerr << "...done" << endl;

        ostringstream stmt_stream;
        gen->out(stmt_stream);
        auto stmt = stmt_stream.str() + ";";

        if (can_error == false || d_info.ouput_or_affect_num > 0) {
            try {
                cerr << "checking (executing) statement ...";
                auto dut = dut_setup(d_info);
                int affect_num = 0;
                vector<vector<string>> output;
                dut->test(stmt, &output, &affect_num);
                if (output.size() + affect_num < d_info.ouput_or_affect_num)
                    continue;
                cerr << "done" << endl;
            } catch (exception &e) {
                string err = e.what();
                if (err.find("CONNECTION FAIL") != string::npos)
                    throw e;
                if (err.find("BUG") != string::npos)
                    throw e;
                cerr << "err: " << e.what() << ", try again" << endl;
                continue;
            }
        }
        trans_rec.push_back(gen);
        succeed = true;
        stmt_num++;
        if (stmt_num == trans_stmt_num)
            break;
    }
}

bool minimize_testcase(dbms_info& d_info,
                        vector<shared_ptr<prod>>& stmt_queue, 
                        vector<int>& tid_queue)
{
    cerr << "Check reproduce..." << endl;
    auto r_check = reproduce_routine(d_info, stmt_queue, tid_queue);
    if (!r_check) {
        cerr << "No" << endl;
        return false;
    }
    cerr << "Yes" << endl;
    
    int max_tid = -1;
    for (auto tid:tid_queue) {
        if (tid > max_tid)
            max_tid = tid;
    }
    int txn_num = max_tid + 1;
    
    auto final_stmt_queue = stmt_queue;
    vector<int> final_tid_queue = tid_queue;
    
    // txn level minimize
    for (int tid = 0; tid < txn_num; tid++) {
        cerr << "Try to delete txn " << tid << "..." << endl;

        auto tmp_stmt_queue = final_stmt_queue;
        vector<int> tmp_tid_queue = final_tid_queue;

        // delete current tid
        for (int i = 0; i < tmp_tid_queue.size(); i++) {
            if (tmp_tid_queue[i] != tid)
                continue;
            
            tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
            tmp_tid_queue.erase(tmp_tid_queue.begin() + i);
            i--;
        }

        // adjust tid queue
        for (int i = 0; i < tmp_tid_queue.size(); i++) {
            if (tmp_tid_queue[i] < tid)
                continue;
            
            tmp_tid_queue[i]--;
        }

        auto ret = reproduce_routine(d_info, tmp_stmt_queue, tmp_tid_queue);
        if (ret == false)
            continue;

        // reduction succeed
        cerr << "Succeed to delete txn " << tid << "\n\n\n" << endl;
        final_stmt_queue = tmp_stmt_queue;
        final_tid_queue = tmp_tid_queue;
        tid--;
        txn_num--;
    }

    // stmt level minimize
    auto stmt_num = final_tid_queue.size();
    auto dut = dut_setup(d_info);
    for (int i = 0; i < stmt_num; i++) {
        cerr << "Try to delete stmt " << i << "..." << endl;

        auto tmp_stmt_queue = final_stmt_queue;
        vector<int> tmp_tid_queue = final_tid_queue;

        // do not delete commit or abort
        auto tmp_stmt_str = print_stmt_to_string(tmp_stmt_queue[i]);
        if (tmp_stmt_str.find(dut->begin_stmt()) != string::npos)
            continue;
        if (tmp_stmt_str.find(dut->commit_stmt()) != string::npos)
            continue;
        if (tmp_stmt_str.find(dut->abort_stmt()) != string::npos)
            continue;
        
        tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
        tmp_tid_queue.erase(tmp_tid_queue.begin() + i);

        auto ret = reproduce_routine(d_info, tmp_stmt_queue, tmp_tid_queue);
        if (ret == false)
            continue;
        
        // reduction succeed
        cerr << "Succeed to delete stmt " << "\n\n\n" << endl;
        final_stmt_queue = tmp_stmt_queue;
        final_tid_queue = tmp_tid_queue;
        i--;
        stmt_num--;
    }

    if (final_stmt_queue.size() == stmt_queue.size())
        return false;

    stmt_queue = final_stmt_queue;
    tid_queue = final_tid_queue;

    string mimimized_stmt_file = "min_stmts.sql";
    // save stmt queue
    ofstream mimimized_stmt_output(mimimized_stmt_file);
    for (int i = 0; i < stmt_queue.size(); i++) {
        mimimized_stmt_output << stmt_queue[i] << endl;
        mimimized_stmt_output << endl;
    }
    mimimized_stmt_output.close();

    // save tid queue
    string minimized_tid_file = "min_tid.txt";
    ofstream minimized_tid_output(minimized_tid_file);
    for (int i = 0; i < tid_queue.size(); i++) {
        minimized_tid_output << tid_queue[i] << endl;
    }
    minimized_tid_output.close();

    return true;
}


bool reproduce_routine(dbms_info& d_info,
                        vector<shared_ptr<prod>>& stmt_queue, 
                        vector<int>& tid_queue)
{
    transaction_test re_test(d_info);
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
        re_test.trans_arr[tid].dut = dut_setup(d_info);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty()) {
            re_test.trans_arr[tid].status = TXN_ABORT;
            continue;
        }

        auto stmt_str = print_stmt_to_string(re_test.trans_arr[tid].stmts.back());
        if (stmt_str.find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = TXN_COMMIT;
        else
            re_test.trans_arr[tid].status = TXN_ABORT;
    }

    re_test.trans_test();
    // re_test.normal_test();
    // if (!re_test.check_result()) {
        cerr << "reproduce successfully" << endl;
        return true;
    // }

    return false;
}

string print_stmt_to_string(shared_ptr<prod> stmt)
{
    ostringstream stmt_stream;
    stmt->out(stmt_stream);
    return stmt_stream.str() + ";";
}