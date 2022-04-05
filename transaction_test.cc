#include "transaction_test.hh"

#define MAX_CONCURRENT_TXN_NUM  5

void transaction_test::assign_txn_id()
{
    set<int> concurrent_tid;
    set<int> available_tid;
    int tid_insertd_stmt[trans_num];
    for (int i = 0; i < trans_num; i++) {
        available_tid.insert(i);
        tid_insertd_stmt[i] = 0;
    }

    while (available_tid.empty() == false) {
        int tid;
        if (concurrent_tid.size() < MAX_CONCURRENT_TXN_NUM) {
            auto idx = dx(available_tid.size()) - 1;
            tid = *next(available_tid.begin(), idx);
            concurrent_tid.insert(tid);
        }
        else {
            auto idx = dx(concurrent_tid.size()) - 1;
            tid = *next(concurrent_tid.begin(), idx);
        }

        tid_queue.push_back(tid);
        tid_insertd_stmt[tid]++;
        if (tid_insertd_stmt[tid] >= trans_arr[tid].stmt_num) {
            available_tid.erase(tid);
            concurrent_tid.erase(tid);
        }
    }

    return;
}

void transaction_test::assign_txn_status()
{   
    for (int i = 0; i < commit_num; i++) 
        trans_arr[i].status = TXN_COMMIT;

    for (int i = commit_num; i < trans_num; i++) 
        trans_arr[i].status = TXN_ABORT;
    
    cerr << YELLOW << "show status" << RESET << endl;
    for (int i = 0; i < trans_num; i++) {
        cerr << i << " " << trans_arr[i].status << endl;
    }

    return;
}

void transaction_test::gen_txn_stmts()
{    
    int stmt_pos_of_trans[trans_num];

    db_schema = get_schema(test_dbms_info);
    for (int tid = 0; tid < trans_num; tid++) {
        trans_arr[tid].dut = dut_setup(test_dbms_info);
        stmt_pos_of_trans[tid] = 0;
        
        // save 2 stmts for begin and commit/abort
        smith::rng.seed(time(NULL));
        gen_stmts_for_one_txn(db_schema, trans_arr[tid].stmt_num - 2, trans_arr[tid].stmts, test_dbms_info);
        // insert begin and end stmts
        trans_arr[tid].stmts.insert(trans_arr[tid].stmts.begin(), 
                make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->begin_stmt()));
        if (trans_arr[tid].status == TXN_COMMIT) 
            trans_arr[tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->commit_stmt()));
        else 
            trans_arr[tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->abort_stmt()));
    }

    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto &stmt = trans_arr[tid].stmts[stmt_pos_of_trans[tid]];
        stmt_queue.push_back(stmt);
        stmt_pos_of_trans[tid]++;
    }
}

void transaction_test::instrument_txn_stmts()
{
    instrumentor i(stmt_queue, tid_queue, db_schema);
    stmt_queue.clear();
    stmt_queue = i.final_stmt_queue;
    tid_queue.clear();
    tid_queue = i.final_tid_queue;
    stmt_use = i.final_stmt_usage;
}

// 2: fatal error (e.g. restart transaction, current transaction is aborted), skip the stmt
// 1: executed
// 0: blocked, not executed
int transaction_test::trans_test_unit(int stmt_pos)
{
    auto tid = tid_queue[stmt_pos];
    auto stmt = print_stmt_to_string(stmt_queue[stmt_pos]);
    vector<string> output;

    try {
        trans_arr[tid].dut->test(stmt, &output);
        if (!output.empty())
            trans_arr[tid].stmt_outputs.push_back(output);
        
        cerr << "S" << stmt_pos << " T" << tid << ": " << stmt.substr(0, stmt.size() > 20 ? 20 : stmt.size()) << endl;
        return 1;
    } catch(exception &e) {
        string err = e.what();
        cerr << RED 
            << "S" << stmt_pos << " T" << tid << ": " << stmt.substr(0, stmt.size() > 20 ? 20 : stmt.size()) << ": fail, err: " 
            << err << RESET << endl;

        if (err.find("ost connection") != string::npos)
            throw e;
        
        if (err.find("blocked") != string::npos)
            return 0;

        if (err.find("skipped") != string::npos)
            return 2;

        if (err.find("sent sql stmt changed") != string::npos) 
            exit(-1);
        
        // store the error info of non-commit statement
        if (stmt.find(trans_arr[tid].dut->commit_stmt()) == string::npos &&
            stmt.find(trans_arr[tid].dut->abort_stmt()) == string::npos) {

            trans_arr[tid].stmt_err_info.push_back(err);
            return 1;
        }
        
        // abort fail, just do nothing, return executed (i.e. 1)
        if (trans_arr[tid].status == TXN_ABORT)
            return 1;
        
        // if commit fail, just abort
        trans_arr[tid].status = TXN_ABORT;
        trans_arr[tid].stmts.pop_back();
        trans_arr[tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->abort_stmt()));
        stmt_queue[stmt_pos] = trans_arr[tid].stmts.back();

        stmt = print_stmt_to_string(stmt_queue[stmt_pos]);

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
        if (trans_arr[tmp_tid].status == TXN_COMMIT && trans_arr[tmp_tid].is_blocked) {
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
    
    auto is_serializable = test_dbms_info.serializable;
    for (int stmt_pos = 0; stmt_pos < cur_stmt_num; stmt_pos++) {
        auto tid = tid_queue[stmt_pos];
        // skip the tried but still blocked transaction
        if (trans_arr[tid].is_blocked)
            continue;
        
        // skip the executed stmt
        if (status_queue[stmt_pos] == 1)
            continue;
        
        if (is_serializable == false && trans_arr[tid].status == TXN_COMMIT) {
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
            
            auto stmt = print_stmt_to_string(stmt_queue[stmt_pos]);
            if (stmt.find(trans_arr[tid].dut->commit_stmt()) != string::npos ||
                    stmt.find(trans_arr[tid].dut->abort_stmt()) != string::npos) {
                retry_block_stmt(stmt_pos, status_queue);
            }
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
    dut_reset_to_backup(test_dbms_info);
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
        if (test_dbms_info.serializable == false && trans_arr[tid].status == TXN_COMMIT) {
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
        auto stmt_str = print_stmt_to_string(stmt);
        if (stmt_str.find(trans_arr[tid].dut->commit_stmt()) != string::npos ||
                stmt_str.find(trans_arr[tid].dut->abort_stmt()) != string::npos) {
            retry_block_stmt(stmt_index, status_queue);
        }
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
    dut_get_content(test_dbms_info, trans_db_content);
}

void transaction_test::get_possible_order()
{
    vector<int> most_possible_trans_order;
    int real_stmt_num = real_tid_queue.size();
    cout << "real_tid_queue: " << endl;
    for (int i = 0; i < real_stmt_num; i++) {
        cout << real_tid_queue[i] << " ";
    }
    cout << endl;
    
    for (int i = 0; i < real_stmt_num; i++) {
        auto tid = real_tid_queue[i];

        if (trans_arr[tid].status != TXN_COMMIT) // do not consider abort transaction
            continue;
        
        // confirm their based on their commit order
        auto stmt_str = print_stmt_to_string(real_stmt_queue[i]);
        if (stmt_str.find(trans_arr[tid].dut->commit_stmt()) == string::npos &&
                stmt_str.find(trans_arr[tid].dut->abort_stmt()) == string::npos)
            continue;
            
        most_possible_trans_order.push_back(tid);
    }
    possible_normal_trans_order.push_back(most_possible_trans_order);

    // for non-serilizable, the commit transaction does not interleave, so only one possible result
    if (!test_dbms_info.serializable)
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

    for (int tid = 0; tid < commit_num; tid++) {
        // if it is commit, erase "begin" and "commit"
        if (trans_arr[tid].status == TXN_COMMIT) {
            trans_arr[tid].normal_stmts.erase(trans_arr[tid].normal_stmts.begin());
            trans_arr[tid].normal_stmts.pop_back();
        }
    }

    auto possible_order_num = possible_normal_trans_order.size();
    for (int i = 0; i < possible_order_num; i++) {
        auto trans_order = possible_normal_trans_order[i];   
        dut_reset_to_backup(test_dbms_info);
        auto normal_dut = dut_setup(test_dbms_info);
        
        for (auto tid : trans_order) {
            vector<vector<string>> normal_output;
            vector<string> normal_err_info;

            auto normal_stmt_num = trans_arr[tid].normal_stmts.size();
            for (int i = 0; i < normal_stmt_num; i++) {
                auto stmt = print_stmt_to_string(trans_arr[tid].normal_stmts[i]);
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
        dut_get_content(test_dbms_info, normal_db_content);
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
        if (trans_arr[i].status != TXN_COMMIT) // donot check abort transaction
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

void transaction_test::save_test_case(string dir_name)
{
    cerr << RED << "Saving test cases..." << RESET;
    // save stmt queue
    string total_stmts_file = dir_name + "stmts.sql";
    ofstream total_stmt_output(total_stmts_file);
    for (int i = 0; i < stmt_num; i++) {
        total_stmt_output << print_stmt_to_string(stmt_queue[i]) << endl;
        total_stmt_output << endl;
    }
    total_stmt_output.close();

    // save tid queue
    string total_tid_file = dir_name + "tid.txt";
    ofstream total_tid_output(total_tid_file);
    for (int i = 0; i < stmt_num; i++) {
        total_tid_output << tid_queue[i] << endl;
    }
    total_tid_output.close();
    cerr << RED << "done" << RESET << endl;
}

int transaction_test::test()
{
    try {
        assign_txn_id();
        assign_txn_status();
        gen_txn_stmts();
        instrument_txn_stmts();
    } catch(exception &e) {
        cerr << "Trigger a normal bugs when inializing the stmts" << endl;
        cerr << "Bug info: " << e.what() << endl;

        string dir_name = output_path_dir + "bug_" + to_string(record_bug_num) + "_normal/"; 
        record_bug_num++;

        make_dir_error_exit(dir_name);
        string cmd = "mv " + string(NORMAL_BUG_FILE) + " " + dir_name;
        if (system(cmd.c_str()) == -1) 
        throw std::runtime_error(string("system() error, return -1") + " in transaction_test::test!");
        
        // save database
        auto dut = dut_setup(test_dbms_info);
        dut->save_backup_file(dir_name);

        exit(-1);
        // return 1; // not need to do other transaction thing
    }
    
    try {
        trans_test();
        // normal_test();
        // if (check_result())
            // return 0;
    } catch(exception &e) {
        cerr << "error captured by test: " << e.what() << endl;
    }

    string dir_name = output_path_dir + "bug_" + to_string(record_bug_num) + "_trans/"; 
    record_bug_num++;
    make_dir_error_exit(dir_name);

    cerr << RED << "Saving database..." << RESET << endl;
    auto dut = dut_setup(test_dbms_info);
    dut->save_backup_file(dir_name);
    
    save_test_case(dir_name);
    
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

bool transaction_test::try_to_kill_server()
{
    cerr << "try killing the server..." << endl;
    kill(server_process_id, SIGTERM);
    int ret;
    auto begin_time = get_cur_time_ms();
    bool flag = false;
    while (1) {
        ret = kill(server_process_id, 0);
        if (ret != 0) { // the process die
            flag = true;
            break;
        }

        int status;
        auto res = waitpid(server_process_id, &status, WNOHANG);
        if (res < 0) {
            cerr << "waitpid() fail: " <<  res << endl;
            throw runtime_error(string("waitpid() fail"));
        }
        if (res == server_process_id) { // the dead process is collected
            cerr << "waitpid succeed for the server process !!!" << endl;
            flag = true;
            break;
        }

        auto now_time = get_cur_time_ms();
        if (now_time - begin_time > KILL_PROC_TIME_MS) {
            flag = false;
            break;
        }
    }
    return flag;
}

bool transaction_test::fork_if_server_closed()
{
    bool server_restart = false;
    auto time_begin = get_cur_time_ms();

    while (1) {
        try {
            auto dut = dut_setup(test_dbms_info);
            if (server_restart)
                sleep(3);
            break; // connect successfully, so break;
        
        } catch (exception &e) { // connect fail
            auto ret = kill(server_process_id, 0);
            if (ret != 0) { // server has die
                cerr << "testing server die, restart it" << endl;

                while (try_to_kill_server() == false) {} // just for safe
                server_process_id = fork_db_server(test_dbms_info);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }

            auto time_end = get_cur_time_ms();
            if (time_end - time_begin > WAIT_FOR_PROC_TIME_MS) {
                cerr << "testing server hang, kill it and restart" << endl;
                
                while (try_to_kill_server() == false) {}
                server_process_id = fork_db_server(test_dbms_info);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }
        }
    }

    return server_restart;
}

transaction_test::transaction_test(dbms_info& d_info)
{
    trans_num = 10 + d9(); // 11 - 19
    // trans_num = 4;
    test_dbms_info = d_info;

    trans_arr = new transaction[trans_num];
    commit_num = trans_num / 2;
    stmt_num = 0;
    for (int i = 0; i < trans_num; i++) {
        trans_arr[i].stmt_num = 2 + d6(); // 3 - 8
        stmt_num += trans_arr[i].stmt_num;
    }

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