#include "transaction_test.hh"

#define MAX_CONCURRENT_TXN_NUM  3

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
    
    // cerr << YELLOW << "show status" << RESET << endl;
    // for (int i = 0; i < trans_num; i++) {
    //     cerr << i << " " << trans_arr[i].status << endl;
    // }

    return;
}

void transaction_test::gen_txn_stmts()
{    
    cerr << YELLOW << "stage 4: generating statements ..." << RESET << endl;
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
        stmt_use.push_back(stmt_usage(INIT_TYPE, false));
        stmt_pos_of_trans[tid]++;
    }
}

void transaction_test::instrument_txn_stmts()
{
    instrumentor i(stmt_queue, tid_queue, db_schema);
    stmt_queue = i.final_stmt_queue;
    tid_queue = i.final_tid_queue;
    stmt_use = i.final_stmt_usage;
    stmt_num = stmt_queue.size();

    for (int tid = 0; tid < trans_num; tid++)
        trans_arr[tid].stmts.clear();

    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto &stmt = stmt_queue[i];
        trans_arr[tid].stmts.push_back(stmt);
    }

    for (int tid = 0; tid < trans_num; tid++) 
        trans_arr[tid].stmt_num = trans_arr[tid].stmts.size();
}

void transaction_test::clean_instrument()
{
    vector<shared_ptr<prod>> clean_stmt_queue;
    vector<int> clean_tid_queue;
    vector<stmt_usage> clean_stmt_usage_queue;
    
    auto cur_stmt_num = stmt_queue.size();
    for (int i = 0; i < cur_stmt_num; i++) {
        if (stmt_use[i].is_instrumented == true)
            continue;
        clean_stmt_queue.push_back(stmt_queue[i]);
        clean_tid_queue.push_back(tid_queue[i]);
        clean_stmt_usage_queue.push_back(stmt_use[i]);
    }
    stmt_num = clean_stmt_queue.size();
    stmt_queue = clean_stmt_queue;
    tid_queue = clean_tid_queue;
    stmt_use = clean_stmt_usage_queue;

    for (int tid = 0; tid < trans_num; tid++)
        trans_arr[tid].stmts.clear();

    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto &stmt = stmt_queue[i];
        trans_arr[tid].stmts.push_back(stmt);
    }

    for (int tid = 0; tid < trans_num; tid++) 
        trans_arr[tid].stmt_num = trans_arr[tid].stmts.size();
}

// true: changed
// false: no need to change
bool transaction_test::change_txn_status(int target_tid, txn_status final_status)
{
    if (final_status != TXN_ABORT && final_status != TXN_COMMIT) {
        cerr << "[change_txn_status] illegal final_status: " << final_status << endl;
        throw runtime_error("illegal final_status");
    }
    
    if (trans_arr[target_tid].status == final_status) 
        return false;
    
    trans_arr[target_tid].status = final_status;
    trans_arr[target_tid].stmts.pop_back();
    if (final_status == TXN_ABORT)
        trans_arr[target_tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[target_tid].dut->abort_stmt()));
    else if (final_status == TXN_COMMIT)
        trans_arr[target_tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[target_tid].dut->commit_stmt()));
    
    auto commit_str = trans_arr[target_tid].dut->commit_stmt();
    auto abort_str = trans_arr[target_tid].dut->abort_stmt();
    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        if (target_tid != tid)
            continue;
        
        auto stmt = print_stmt_to_string(stmt_queue[i]);
        
        // compare size to prevent the case that begin statement contains "COMMIT", 
            // e.g. BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED (in postgres)
        auto is_commit = (stmt.find(commit_str) != string::npos) && 
                            (stmt.size() <= commit_str.size() + 3) &&
                            (stmt.size() >= commit_str.size());
        auto is_abort = (stmt.find(abort_str) != string::npos) && 
                            (stmt.size() <= abort_str.size() + 3) &&
                            (stmt.size() >= abort_str.size());
        if (!is_commit && !is_abort)
            continue;

        // find out the commit or abort stmt
        stmt_queue[i] = trans_arr[tid].stmts.back();
    }
    return true;
}

bool transaction_test::analyze_txn_dependency(shared_ptr<dependency_analyzer>& da)
{
    vector<stmt_output> init_content_vector;
    for (auto iter = init_db_content.begin(); iter != init_db_content.end(); iter++)
        init_content_vector.push_back(iter->second);
    
    vector<txn_status> real_txn_status;
    for (int tid = 0; tid < trans_num; tid++) 
        real_txn_status.push_back(trans_arr[tid].status);

    da = make_shared<dependency_analyzer>(init_content_vector, // init_output 
                            real_output_queue, // total_output
                            real_tid_queue, // final_tid_queue
                            real_stmt_usage, // final_stmt_usage
                            real_txn_status, // final_txn_status
                            trans_num, // t_num
                            1, // primary_key_idx
                            0); // write_op_key_idx

    cerr << "check_G1a ...!!" << endl;
    if (da->check_G1a() == true) {
        cerr << "check_G1a violate!!" << endl;
        return true;
    }
    cerr << "check_G1b ...!!" << endl;
    if (da->check_G1b() == true){
        cerr << "check_G1b violate!!" << endl;
        return true;
    }
    cerr << "check_G1c ...!!" << endl;
    if (da->check_G1c() == true){
        cerr << "check_G1c violate!!" << endl;
        return true;
    }
    // cerr << "check_G2_item ...!!" << endl;
    // if (da->check_G2_item() == true){
    //     cerr << "check_G2_item violate!!" << endl;
    //     return true;
    // }
    // cerr << "check_GSIa ...!!" << endl;
    // if (da->check_GSIa() == true){
    //     cerr << "check_GSIa violate!!" << endl;
    //     return true;
    // }
    // cerr << "check_GSIb ...!!" << endl;
    // if (da->check_GSIb() == true){
    //     cerr << "check_GSIb violate!!" << endl;
    //     return true;
    // }
    
    return false;
}

void transaction_test::clear_execution_status()
{
    for (int tid = 0; tid < trans_num; tid++) {
        trans_arr[tid].stmt_err_info.clear();
        trans_arr[tid].stmt_outputs.clear();
        trans_arr[tid].dut = dut_setup(test_dbms_info);
        trans_arr[tid].is_blocked = false;

        // clear the normal execution result
        trans_arr[tid].normal_stmts.clear();
        trans_arr[tid].normal_outputs.clear();
        trans_arr[tid].normal_err_info.clear();
    }
    init_db_content.clear();

    real_tid_queue.clear();
    real_stmt_queue.clear();
    real_output_queue.clear();
    real_stmt_usage.clear();
    trans_db_content.clear();

    // normal test related
    normal_stmt_output.clear();
    normal_stmt_err_info.clear();
    normal_stmt_db_content.clear();
}

// 2: fatal error (e.g. restart transaction, current transaction is aborted), skip the stmt
//    Note: for the unacceptable error, implemented dbms front-end should throw error containing "skipped"
// 1: executed
// 0: blocked, not executed
int transaction_test::trans_test_unit(int stmt_pos, stmt_output& output, bool debug_mode)
{
    auto tid = tid_queue[stmt_pos];
    auto stmt = print_stmt_to_string(stmt_queue[stmt_pos]);

    auto show_str = stmt.substr(0, stmt.size() > SHOW_CHARACTERS ? SHOW_CHARACTERS : stmt.size());
    replace(show_str.begin(), show_str.end(), '\n', ' ');
    
    try {
        trans_arr[tid].dut->test(stmt, &output);
        trans_arr[tid].stmt_outputs.push_back(output);
        trans_arr[tid].stmt_err_info.push_back("");
        if (debug_mode)
            cerr << stmt_pos << " T" << tid << " S" << trans_arr[tid].stmt_outputs.size() - 1 << ": " << show_str << endl;
        return 1;
    } catch(exception &e) {
        string err = e.what();
        // if (debug_mode)
            cerr << RED << stmt_pos << " T" << tid << " S" << trans_arr[tid].stmt_outputs.size()  << ": " << show_str << ": fail, err: " << err << RESET << endl;

        if (err.find("ost connection") != string::npos || err.find("BUG") != string::npos) // lost connection
            throw e;
        if (err.find("blocked") != string::npos)
            return 0;
        if (err.find("skipped") != string::npos) {
            stmt_output empty_output;
            output = empty_output;
            trans_arr[tid].stmt_outputs.push_back(empty_output);
            trans_arr[tid].stmt_err_info.push_back("");
            return 2;
        }
            
        if (err.find("sent sql stmt changed") != string::npos) 
            exit(-1);
        
        // store the error info of non-commit statement
        auto commit_str = trans_arr[tid].dut->commit_stmt();
        auto is_commit = (stmt.find(commit_str) != string::npos) && 
                            (stmt.size() <= commit_str.size() + 3) &&
                            (stmt.size() >= commit_str.size());
        if (!is_commit) { // it is not commit stmt 
            stmt_output empty_output;
            output = empty_output;
            trans_arr[tid].stmt_outputs.push_back(empty_output);
            trans_arr[tid].stmt_err_info.push_back(err);
            return 1;
        }
        
        // is commit stmt   
        // if commit fail, just abort
        trans_arr[tid].status = TXN_ABORT;
        trans_arr[tid].stmts.pop_back();
        trans_arr[tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->abort_stmt()));
        stmt_queue[stmt_pos] = trans_arr[tid].stmts.back();

        stmt = print_stmt_to_string(stmt_queue[stmt_pos]);
        show_str = stmt.substr(0, stmt.size() > SHOW_CHARACTERS ? SHOW_CHARACTERS : stmt.size());
        try {
            trans_arr[tid].dut->test(stmt);
            stmt_output empty_output;
            output = empty_output;
            trans_arr[tid].stmt_outputs.push_back(empty_output);
            trans_arr[tid].stmt_err_info.push_back("");
            if (debug_mode)
                cerr << "T" << tid << " S" << trans_arr[tid].stmt_outputs.size() - 1  << ": " << show_str << endl;
            return 1;
        } catch(exception &e2) {
            err = e2.what();
            if (debug_mode)
                cerr << RED << "T" << tid << " S" << trans_arr[tid].stmt_outputs.size()  << ": " << show_str << ": fail, err: " << err << RESET << endl;
        }
    }

    return 0;
}

void transaction_test::retry_block_stmt(int cur_stmt_num, int* status_queue, bool debug_mode)
{
    if (debug_mode)
        cerr << YELLOW << "retrying process begin..." << RESET << endl;

    // firstly try the first stmt of each blocked transaction
    set<int> first_tried_tid;
    for (int i = 0; i < cur_stmt_num; i++) {
        if (status_queue[i] == 1)
            continue;
        
        auto tid = tid_queue[i];
        auto su = stmt_use[i];
        if (trans_arr[tid].is_blocked == false)
            continue;

        if (first_tried_tid.count(tid) != 0) // have tried
            continue;
        
        first_tried_tid.insert(tid);
        stmt_output output;
        auto is_executed = trans_test_unit(i, output, debug_mode);
        if (is_executed == 1) { // executed
            trans_arr[tid].is_blocked = false;
            status_queue[i] = 1;
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(stmt_queue[i]);
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(stmt_use[i]);
        } else if (is_executed == 2) { // skipped
            trans_arr[tid].is_blocked = false;
            
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT));
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(stmt_usage(INIT_TYPE, su.is_instrumented));
            status_queue[i] = 1;
        } else {// blocked
            trans_arr[tid].is_blocked = true;
        }
    }
    
    for (int stmt_pos = 0; stmt_pos < cur_stmt_num; stmt_pos++) {
        auto tid = tid_queue[stmt_pos];
        auto su = stmt_use[stmt_pos];
        // skip the tried but still blocked transaction
        if (trans_arr[tid].is_blocked)
            continue;
        
        // skip the executed stmt
        if (status_queue[stmt_pos] == 1)
            continue;

        stmt_output output;
        auto is_executed = trans_test_unit(stmt_pos, output, debug_mode);
        // successfully execute the stmt, so label as not blocked
        if (is_executed == 1) {
            trans_arr[tid].is_blocked = false;
            status_queue[stmt_pos] = 1;
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(stmt_queue[stmt_pos]);
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(stmt_use[stmt_pos]);
            
            auto stmt = print_stmt_to_string(stmt_queue[stmt_pos]);
            auto commit_str = trans_arr[tid].dut->commit_stmt();
            auto abort_str = trans_arr[tid].dut->abort_stmt();
            auto is_commit = (stmt.find(commit_str) != string::npos) && 
                            (stmt.size() <= commit_str.size() + 3) &&
                            (stmt.size() >= commit_str.size());
            auto is_abort = (stmt.find(abort_str) != string::npos) && 
                            (stmt.size() <= abort_str.size() + 3) &&
                            (stmt.size() >= abort_str.size());
            if (is_commit || is_abort) {
                retry_block_stmt(stmt_pos, status_queue, debug_mode);
            }
        } else if (is_executed == 2) { // skipped
            trans_arr[tid].is_blocked = false;
            status_queue[stmt_pos] = 1;

            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT));
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(stmt_usage(INIT_TYPE, su.is_instrumented));
        }
        else { // still blocked
            trans_arr[tid].is_blocked = true;
        }
    }
    if (debug_mode)
        cerr << YELLOW << "retrying process end..." << RESET << endl;
}

void transaction_test::trans_test(bool debug_mode)
{
    dut_reset_to_backup(test_dbms_info);
    dut_get_content(test_dbms_info, init_db_content); // get initial database content
    
    if (debug_mode)
        cerr << YELLOW << "transaction test" << RESET << endl;
    // status_queue: 0 -> blocked, 1->executed (succeed or fail)
    int status_queue[stmt_num];
    
    for (int i = 0; i < stmt_num; i++) 
        status_queue[i] = 0;
    
    /* 
    Note: for sqlite, after using dut_reset_to_backup(), 
    original connection is broken and the database only 
    can be read. We need to reconnect to the new one.
    */
    for (int i = 0; i < trans_num; i++) 
        trans_arr[i].dut = dut_setup(test_dbms_info);
    
    for (int stmt_index = 0; stmt_index < stmt_num; stmt_index++) {
        auto tid = tid_queue[stmt_index];
        auto& stmt = stmt_queue[stmt_index];
        auto su = stmt_use[stmt_index];
        
        if (trans_arr[tid].is_blocked)
            continue;
        
        stmt_output output;
        auto is_executed = trans_test_unit(stmt_index, output, debug_mode);
        if (is_executed == 0) {
            trans_arr[tid].is_blocked = true;
            continue;
        }
        if (is_executed == 2) { // the executed stmt fail
            status_queue[stmt_index] = 1;
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT));
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(stmt_usage(INIT_TYPE, su.is_instrumented));
            continue;
        }
        status_queue[stmt_index] = 1;
        real_tid_queue.push_back(tid);
        real_stmt_queue.push_back(stmt);
        real_output_queue.push_back(output);
        real_stmt_usage.push_back(su);
        
        // after a commit or abort, retry the statement
        auto stmt_str = print_stmt_to_string(stmt);
        auto commit_str = trans_arr[tid].dut->commit_stmt();
        auto abort_str = trans_arr[tid].dut->abort_stmt();
        auto is_commit = (stmt_str.find(commit_str) != string::npos) && 
                            (stmt_str.size() <= commit_str.size() + 3) &&
                            (stmt_str.size() >= commit_str.size());
         auto is_abort = (stmt_str.find(abort_str) != string::npos) && 
                            (stmt_str.size() <= abort_str.size() + 3) &&
                            (stmt_str.size() >= abort_str.size());
        if (is_commit || is_abort) {
            retry_block_stmt(stmt_index, status_queue, debug_mode);
        }
    }

    int no_change = 0;
    int executed = 0;
    for (int i = 0; i < stmt_num; i++) {
        if (status_queue[i] == 1)
            executed++;
    }
    if (executed < stmt_num) 
        cerr << RED << "some stmt is still not executed, finish them" << RESET << endl;
    
    while (executed < stmt_num) {
        retry_block_stmt(stmt_num, status_queue, debug_mode);
        int new_executed = 0;
        for (int i = 0; i < stmt_num; i++) {
            if (status_queue[i] == 1)
                new_executed++;
        }
        if (new_executed == stmt_num)
            break;
        if (executed == new_executed) {
            no_change++;
            if (no_change > 2) {
                break;
                // throw runtime_error("Transaction deadlock found");
                // cerr << RED << "dead lock, they wait for each other, try to delete some stmt" << RESET << endl;
                // no_change = 0;
                // // get unexecuted stmt num of each txn
                // int unexecuted_stmt_txn[trans_num];
                // int last_stmt_idx_txn[trans_num];
                // for (int i = 0; i < trans_num; i++) 
                //     unexecuted_stmt_txn[i] = 0;
                // for (int i = 0; i < stmt_num; i++) {
                //     if (status_queue[i] != 1)
                //         unexecuted_stmt_txn[tid_queue[i]]++;
                //     last_stmt_idx_txn[tid_queue[i]] = i;
                // }
                // // get the txn that has least unexecuted stmts
                // int least_stmt_txn = 0;
                // int least_stmt = -1;
                // for (int i = 0; i < trans_num; i++) {
                //     // some txn has executed all stmts, we only focus on that not executed all
                //     if (unexecuted_stmt_txn[i] <= 1) // if only commit (or abort) stmt not executed, do not target on them
                //         continue;
                //     if (least_stmt == -1 || unexecuted_stmt_txn[i] < least_stmt) {
                //         least_stmt = unexecuted_stmt_txn[i];
                //         least_stmt_txn = i;
                //     }
                // }
                // // replace the unexecuted stmt with SPACE_HOLDER_STMT
                // cerr << RED << "target txn: " << least_stmt_txn << " stmt_num: " << least_stmt << RESET << endl;
                // int replace_num = 0;
                // for (int i = 0; i < stmt_num; i++) {
                //     if (tid_queue[i] != least_stmt_txn)
                //         continue;
                //     if (status_queue[i] == 1) // do not change the executed stmt
                //         continue;
                //     if (last_stmt_idx_txn[least_stmt_txn] == i) // do not change commit or abort
                //         continue;
                //     stmt_queue[i] = make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT);
                //     stmt_use[i].stmt_type = INIT_TYPE;
                //     replace_num++;
                // }
                // cerr << "replace " << replace_num << " stmts" << endl;
            }
        }
        executed = new_executed;
    }
    
    for (int i = 0; i < stmt_num; i++) {
        if (status_queue[i] == 1)
            continue;
        
        cerr << RED << "something error, some stmt is still not executed" << RESET << endl;
        throw runtime_error("some stmt is still not executed");
    }

    if (real_stmt_queue.size() != stmt_num) {
        cerr << "real_stmt_queue size: " << real_stmt_queue.size() << endl;
        cerr << "stmt_num: " << stmt_num << endl;
        cerr << "real_stmt_queue size is not equal to stmt_queue size, something wrong" << endl;
        throw runtime_error("real_stmt_queue size is not equal to stmt_queue size, something wrong");
    }

    // collect database information
    dut_get_content(test_dbms_info, trans_db_content);
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

    // save stmt use queue
    string total_stmt_use_file = dir_name + "stmt_use.txt";
    ofstream total_stmt_use_output(total_stmt_use_file);
    for (int i = 0; i < stmt_num; i++) {
        total_stmt_use_output << stmt_use[i] << endl;
    }
    total_stmt_use_output.close();

    cerr << RED << "done" << RESET << endl;
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

// cannot be called by child process
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
            if (errno == ECHILD) {
                cerr << "there is no child process to wait" << endl;
                flag = true;
                break;
            }
            else {
                cerr << "waitpid() fail: " <<  res << endl;
                throw runtime_error(string("waitpid() fail"));
            }
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

bool transaction_test::fork_if_server_closed(dbms_info& d_info)
{
    bool server_restart = false;
    auto time_begin = get_cur_time_ms();

    while (1) {
        try {
            auto dut = dut_setup(d_info);
            if (server_restart)
                sleep(3);
            break; // connect successfully, so break;
        
        } catch (exception &e) { // connect fail
            auto ret = kill(server_process_id, 0);
            if (ret != 0) { // server has die
                cerr << "testing server die, restart it" << endl;

                while (try_to_kill_server() == false) {} // just for safe
                server_process_id = fork_db_server(d_info);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }

            auto time_end = get_cur_time_ms();
            if (time_end - time_begin > WAIT_FOR_PROC_TIME_MS) {
                cerr << "testing server hang, kill it and restart" << endl;
                
                while (try_to_kill_server() == false) {}
                server_process_id = fork_db_server(d_info);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }
        }
    }

    return server_restart;
}

// Note: txn_string_stmt contains begin, commit, abort, select 1 where 0<>0 and select * from t
bool transaction_test::refine_stmt_queue(vector<stmt_id>& stmt_path, shared_ptr<dependency_analyzer>& da)
{
    // refine txn_stmt because the skipped stmt has been changed
    int stmt_pos_of_txn[trans_num];
    for (int i = 0; i < trans_num; i++) 
        stmt_pos_of_txn[i] = 0;
    for (int i = 0; i < stmt_num; i++) {
        auto casted = dynamic_pointer_cast<txn_string_stmt>(stmt_queue[i]);
        auto tid = tid_queue[i];
        if (casted && stmt_use[i] == INIT_TYPE) 
            trans_arr[tid].stmts[stmt_pos_of_txn[tid]] = casted;
        stmt_pos_of_txn[tid]++;
    }
    
    bool is_refined = false;
    // change txns that are not in the path to abort
    auto path_length = stmt_path.size();
    set<int> exist_tid;
    set<stmt_id> exist_stmt;
    for (int i = 0; i < path_length; i++) {
        exist_tid.insert(stmt_path[i].txn_id);
        exist_stmt.insert(stmt_path[i]);
    }
    for (int i = 0; i < trans_num; i++) {
        if (exist_tid.count(i) > 0)
            continue;
        if (change_txn_status(i, TXN_ABORT))
            is_refined = true;
    }

    // cerr << YELLOW << "after changing txns that are not in the path to abort" << RESET << endl;
    // if (true) {
    //     int stmt_step_of_txn[trans_num];
    //     memset(stmt_step_of_txn, 0, sizeof(int) * trans_num);
    //     for (int i = 0; i < stmt_num; i++) {
    //         auto tid = tid_queue[i];
    //         auto stmt = stmt_queue[i];
    //         auto stmt_str = print_stmt_to_string(stmt);
    //         auto show_str = stmt_str.substr(0, stmt_str.size() > SHOW_CHARACTERS ? SHOW_CHARACTERS : stmt_str.size());
    //         replace(show_str.begin(), show_str.end(), '\n', ' ');
    //         cerr << "T" << tid << " S" << stmt_step_of_txn[tid] << ": " << show_str << endl;
    //         stmt_step_of_txn[tid]++;
    //     }
    // }

    for (int i = 0; i < trans_num; i++) 
        stmt_pos_of_txn[i] = 0;
    cerr << RED << "Refining: " << RESET;
    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        if (exist_tid.count(tid) == 0) 
            continue;
        auto stmt_pos = stmt_pos_of_txn[tid];
        stmt_id stmt_idx(tid, stmt_pos);
        stmt_pos_of_txn[tid]++;
        
        if (exist_stmt.count(stmt_idx) > 0) // matched
            continue;

        // already been replaced, include commit and abort stmt
        auto casted = dynamic_pointer_cast<txn_string_stmt>(stmt_queue[i]);
        if (casted && stmt_use[i] == INIT_TYPE) 
            continue;
        
        // only change basic instrumented stmt (intrumented one will be deleted later)
        if (stmt_use[i].is_instrumented == true)
            continue;
        
        cerr << "(" << stmt_idx.txn_id << "." << stmt_idx.stmt_idx_in_txn << ") ";

        // txn in the path, and stmt not match, should change to SELECT 1 WHERE FALSE
        is_refined = true; // only mark as refined if change basic stmts
        auto change_set = da->get_instrumented_stmt_set(i);
        for (auto& stmt_idx : change_set) {
            // mark its instrumented stmt as false to prevent it from being deleted by clean_instrument, otherwise the number of stmts will change
            stmt_use[stmt_idx] = INIT_TYPE;
            // change its instrumented stmt as SPACE_HOLDER_STMT
            stmt_use[stmt_idx].is_instrumented = false; 
            stmt_queue[stmt_idx] = make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT);
        }
    }
    cerr << endl;
    
    if (is_refined == false)
        return false;
    
    clear_execution_status();
    return true;
}

void transaction_test::normal_stmt_test(vector<stmt_id>& stmt_path)
{
    cerr << RED << "normal testing" << RESET << endl;
    dut_reset_to_backup(test_dbms_info);
    auto normal_dut = dut_setup(test_dbms_info);
    int count = -1;
    for (auto& stmt_id : stmt_path) {
        count++;
        auto tid = stmt_id.txn_id;
        auto stmt_pos = stmt_id.stmt_idx_in_txn;
        auto stmt = print_stmt_to_string(trans_arr[tid].stmts[stmt_pos]);
        auto show_str = stmt.substr(0, stmt.size() > SHOW_CHARACTERS ? SHOW_CHARACTERS : stmt.size());
        replace(show_str.begin(), show_str.end(), '\n', ' ');
        stmt_output output;
        try {
            normal_dut->test(stmt, &output);
            normal_stmt_output.push_back(output);
            normal_stmt_err_info.push_back("");
            cerr << count << " T" << tid << " S" << stmt_pos << ": " << show_str << endl;
        } catch (exception &e) {
            string err = e.what();
            stmt_output empty_output;
            cerr << RED << count
                << " T" << tid << " S" << stmt_pos << ": " << show_str << ": fail, err: " 
                << err << RESET << endl;
            if (err.find("skipped") != string::npos) {
                normal_stmt_output.push_back(empty_output);
                normal_stmt_err_info.push_back("");
                continue;
            }
            normal_stmt_output.push_back(empty_output);
            normal_stmt_err_info.push_back(err);
        }
    }
    dut_get_content(test_dbms_info, normal_stmt_db_content);
}

void print_stmt_output(stmt_output& output)
{
    auto row_num = output.size();
    for (int i = 0; i < row_num; i++) {
        auto& row = output[i];
        auto col_num = row.size();
        for (int j = 0; j < col_num; j++) 
            cerr << row[j] << " ";
        cerr << endl;
    }
    cerr << endl;
}

bool transaction_test::check_normal_stmt_result(vector<stmt_id>& stmt_path, bool debug)
{
    if (!compare_content(trans_db_content, normal_stmt_db_content)) {
        cerr << "trans_db_content is not equal to normal_stmt_db_content" << endl;
        return false;
    }

    vector<stmt_output> path_txn_output;
    vector<string> path_txn_err_info;

    auto path_length = stmt_path.size();
    for (int i = 0; i < path_length; i++) {
        auto tid = stmt_path[i].txn_id;
        auto stmt_pos = stmt_path[i].stmt_idx_in_txn;
        path_txn_output.push_back(trans_arr[tid].stmt_outputs[stmt_pos]);
        path_txn_err_info.push_back(trans_arr[tid].stmt_err_info[stmt_pos]);
    }

    if (debug) {
        for (int i = 0; i < path_length; i++) {
            cerr << "txn output: " << endl;
            print_stmt_output(path_txn_output[i]);
            cerr << "normal output: " << endl;
            print_stmt_output(normal_stmt_output[i]);
        }
    }

    if (!compare_output(path_txn_output, normal_stmt_output)) {
        cerr << "txn output is not equal to normal stmt one" << endl;
        return false;
    }

    auto err_info_size = path_txn_err_info.size();
    if (err_info_size != normal_stmt_err_info.size()) {
        cerr << "txn error info size is not equal to normal stmt one" << endl;
        cerr << "path_txn_err_info: " << err_info_size << ", normal_stmt_err_info" << normal_stmt_err_info.size() << endl;
        return false;
    }
    bool err_result = true;
    for (int i = 0; i < err_info_size; i++) {
        // cerr << "err idx: " << i << endl;
        // cerr << "txn one: " << path_txn_err_info[i] << endl;
        // cerr << "normal one: " << normal_stmt_err_info[i] << endl;
        
        if (path_txn_err_info[i] != normal_stmt_err_info[i]) {
            if (path_txn_err_info[i] != "" && normal_stmt_err_info[i] != "") // both has error, the content could be different
                continue;
            cerr << "txn error info is not equal to normal stmt one, idx: " << i << endl;
            cerr << "txn one: " << path_txn_err_info[i] << endl;
            cerr << "normal one: " << normal_stmt_err_info[i] << endl;
            err_result = false;
        }
    }

    return err_result;
}

void print_stmt_path(vector<stmt_id>& stmt_path, map<pair<stmt_id, stmt_id>, set<dependency_type>>& stmt_graph)
{
    auto path_length = stmt_path.size();
    for (int i = 0; i < path_length; i++) {
        auto& stmt_i = stmt_path[i];
        cerr << "(" << stmt_i.txn_id << "." << stmt_i.stmt_idx_in_txn << ")" << "-";
        int forward_steps = 1;
        for (forward_steps = 1; i + forward_steps < path_length; forward_steps++) {
            auto j = i + forward_steps;
            auto& stmt_j = stmt_path[j];
            auto branch = make_pair<>(stmt_i, stmt_j);
            if (stmt_graph.count(branch) == 0)
                continue;
            auto& dset = stmt_graph[branch];
            bool printed = false;
            if (dset.count(WRITE_READ)) {
                cerr << RED << forward_steps << "WR|" << RESET;
                printed = true;
            }
            if (dset.count(WRITE_WRITE)) {
                cerr << RED << forward_steps << "WW|" << RESET;
                printed = true;
            }
            if (dset.count(READ_WRITE)) {
                cerr << RED << forward_steps << "RW|" << RESET;
                printed = true;
            }
            if (dset.count(VERSION_SET_DEPEND)) {
                cerr << RED << forward_steps << "VS|" << RESET;
                printed = true;
            }
            if (dset.count(OVERWRITE_DEPEND)) {
                cerr << RED << forward_steps << "OW|" << RESET;
                printed = true;
            }
            if (dset.count(INSTRUMENT_DEPEND)) {
                cerr << forward_steps << "IN|";
                printed = true;
            }
            if (printed == true)
                break;
        }
        cerr << "->";
    }
    cerr << endl;
}

void infer_instrument_after_blocking(vector<shared_ptr<prod>>& whole_before_stmt_queue,
                                    vector<int>& whole_before_tid_queue,
                                    vector<stmt_usage>& whole_before_stmt_usage,
                                    shared_ptr<dependency_analyzer>& before_da,
                                    vector<shared_ptr<prod>>& after_stmt_queue,
                                    vector<int>& after_tid_queue,
                                    vector<stmt_usage>& after_stmt_usage)
{
    vector<shared_ptr<prod>> final_after_stmt_queue;
    vector<int> final_after_tid_queue;
    vector<stmt_usage> final_after_stmt_usage;

    auto whole_before_queue_size = whole_before_stmt_queue.size();

    auto after_queue_size = after_stmt_queue.size();
    int tid_num = 0;
    for (int i = 0; i < after_queue_size; i++) {
        if (after_tid_queue[i] >= tid_num)
            tid_num = after_tid_queue[i] + 1;
    }
    int stmt_pos_of_txn[tid_num];
    for (int i = 0; i < tid_num; i++)
        stmt_pos_of_txn[i] = -1;

    for (int i = 0; i < after_queue_size; i++) {
        auto tid = after_tid_queue[i];
        auto& stmt_use = after_stmt_usage[i];
        stmt_pos_of_txn[tid]++;
        auto stmt_pos = stmt_pos_of_txn[tid];

        if (stmt_use != INIT_TYPE) {
            final_after_stmt_queue.push_back(after_stmt_queue[i]);
            final_after_tid_queue.push_back(tid);
            final_after_stmt_usage.push_back(stmt_use);
            continue;
        }
        
        // if it is INIT_TYPE
        // find the corresponding basic stmt in whole_before_queue
        int basic_stmt_pos = -1;
        int j = 0;
        for (; j < whole_before_queue_size; j++) {
            if (whole_before_tid_queue[j] != tid)
                continue;
            if (whole_before_stmt_usage[j].is_instrumented == true)
                continue;
            basic_stmt_pos++;
            if (basic_stmt_pos == stmt_pos)
                break;
        }

        if (whole_before_stmt_usage[j] == INIT_TYPE) { // both are INIT_TYPE, not need to process
            final_after_stmt_queue.push_back(after_stmt_queue[i]);
            final_after_tid_queue.push_back(tid);
            final_after_stmt_usage.push_back(stmt_use);
            continue;
        }

        // they are different, mean that this stmt has been changed to space_holder after blocking scheduling
        auto instrumented_set = before_da->get_instrumented_stmt_set(j);
        auto set_size = instrumented_set.size();
        for (int k = 0; k < set_size; k++) { // put it set_size times to make the queue size same
            final_after_stmt_queue.push_back(after_stmt_queue[i]);
            final_after_tid_queue.push_back(tid);
            final_after_stmt_usage.push_back(stmt_use);
        }
    }

    after_stmt_queue = final_after_stmt_queue;
    after_tid_queue = final_after_tid_queue;
    after_stmt_usage = final_after_stmt_usage;
}

bool transaction_test::multi_stmt_round_test()
{
    block_scheduling(); // it will make many stmts fails, we replace these failed stmts with space holder
    instrument_txn_stmts();
    trans_test(); // first run, get all dependency information
    shared_ptr<dependency_analyzer> init_da;
    if (analyze_txn_dependency(init_da)) 
        throw runtime_error("BUG: found in analyze_txn_dependency()");
    
    // record init status
    auto init_stmt_queue = stmt_queue;
    auto init_tid_queue = tid_queue;
    auto init_stmt_usage = stmt_use;
    txn_status init_txn_status[trans_num];
    vector<shared_ptr<prod>> init_txn_stmt[trans_num];
    for (int tid = 0; tid < trans_num; tid++) {
        init_txn_status[tid] = trans_arr[tid].status;
        init_txn_stmt[tid] = trans_arr[tid].stmts;
    }
    set<stmt_id> deleted_nodes;

    int round_count = 1;
    int stmt_path_empty_time = 0;
    while (1) { // until there is not statement in the stmt path
        auto longest_stmt_path = init_da->topological_sort_path(deleted_nodes);
        if (longest_stmt_path.empty())
            break;
        
        cerr << "\n\n";
        cerr << RED << "test round: " << round_count << RESET << endl;
        round_count++;
        cerr << "ideal test stmt path: ";
        print_stmt_path(longest_stmt_path, init_da->stmt_dependency_graph);

        // use the longest path to refine
        bool empty_stmt_path = false;
        shared_ptr<dependency_analyzer> tmp_da = init_da;

        while (refine_stmt_queue(longest_stmt_path, tmp_da) == true) { // will change some stmts to space holder
            cerr << YELLOW << "previous instrmented stmt_queue length: " << stmt_queue.size() << RESET << endl;
            
            auto tmp_whole_before_stmt_queue = stmt_queue;
            auto tmp_whole_before_tid_queue = tid_queue;
            auto tmp_whole_before_stmt_usage = stmt_use;
            
            clean_instrument();
            cerr << YELLOW << "cleared stmt_queue length: " << stmt_queue.size() << RESET << endl;

            block_scheduling(); // will change some stmts to space holder
            cerr << YELLOW << "scheduled stmt_queue length: " << stmt_queue.size() << RESET << endl;
            infer_instrument_after_blocking(tmp_whole_before_stmt_queue, 
                                                tmp_whole_before_tid_queue, 
                                                tmp_whole_before_stmt_usage,
                                                init_da,
                                                stmt_queue,
                                                tid_queue,
                                                stmt_use);
            cerr << YELLOW << "first instrmented stmt_queue length: " << stmt_queue.size() << RESET << endl;
            instrument_txn_stmts();
            cerr << YELLOW << "final instrmented stmt_queue length: " << stmt_queue.size() << RESET << endl;

            cerr << RED << "txn testing:" << RESET << endl;
            trans_test();
            if (analyze_txn_dependency(tmp_da)) 
                throw runtime_error("BUG: found in analyze_txn_dependency()");
            longest_stmt_path = tmp_da->topological_sort_path(deleted_nodes);

            cerr << RED << "stmt path for refining: " << RESET;
            print_stmt_path(longest_stmt_path, tmp_da->stmt_dependency_graph);
            if (longest_stmt_path.empty()) {
                empty_stmt_path = true;
                break;
            }
        }

        if (empty_stmt_path) {
            stmt_path_empty_time++;
            if (stmt_path_empty_time >= 3) {
                cerr << "generate " << stmt_path_empty_time << " times empty path, skip this graph" << endl;
                break;
            }
        }

        // normal test and check
        normal_stmt_test(longest_stmt_path);
        if (check_normal_stmt_result(longest_stmt_path) == false)
            return true;
        
        // delete stmts from the stmt_dependency_graph
        auto path_length = longest_stmt_path.size();
        auto& stmt_graph = init_da->stmt_dependency_graph;
        auto& init_da_tid_queue = init_da->f_txn_id_queue;
        cerr << "deleting node: ";
        for (int i = 0; i < path_length; i++) {
            auto& cur_sid = longest_stmt_path[i];
            cerr << "(" << cur_sid.txn_id << "." << cur_sid.stmt_idx_in_txn << ") ";
            auto queue_idx = cur_sid.transfer_2_stmt_idx(init_da_tid_queue);
            auto idx_set = init_da->get_instrumented_stmt_set(queue_idx);
            for (auto& delete_idx : idx_set) {
                auto chosen_stmt_id = stmt_id(init_da_tid_queue, delete_idx);
                deleted_nodes.insert(chosen_stmt_id);
                for (int j = 0; j < init_da->stmt_num; j++) {
                    auto another_stmt = stmt_id(init_da_tid_queue, j);
                    auto out_branch = make_pair(chosen_stmt_id, another_stmt);
                    auto in_branch = make_pair(another_stmt, chosen_stmt_id);
                    // should not delete INSTRUMENT_DEPEND edge which is needed for get_instrument_set
                    if (stmt_graph[in_branch].count(INSTRUMENT_DEPEND) > 0) {
                        stmt_graph[in_branch].clear();
                        stmt_graph[in_branch].insert(INSTRUMENT_DEPEND);
                    }
                    else
                        stmt_graph.erase(in_branch);
                    if (stmt_graph[out_branch].count(INSTRUMENT_DEPEND) > 0) {
                        stmt_graph[out_branch].clear();
                        stmt_graph[out_branch].insert(INSTRUMENT_DEPEND);
                    }
                    else
                        stmt_graph.erase(out_branch);
                }
            }
        }
        cerr << endl;
        
        stmt_queue = init_stmt_queue;
        stmt_use = init_stmt_usage;
        tid_queue = init_tid_queue;
        stmt_num = stmt_queue.size();
        for (int tid = 0; tid < trans_num; tid++) {
            trans_arr[tid].stmts = init_txn_stmt[tid];
            change_txn_status(tid, init_txn_status[tid]);
        }
    }
    return false;
}

void transaction_test::block_scheduling()
{
    cerr << endl << RED << "block scheduling" << RESET << endl;
    int round = 0;
    while (1) {
        cerr << RED << "scheduling: " << round << RESET << endl;
        trans_test(false);
        if (stmt_queue == real_stmt_queue) // no failing 
            break;
        stmt_queue = real_stmt_queue;
        stmt_use = real_stmt_usage;
        tid_queue = real_tid_queue;
        clear_execution_status();
        round++;
    }
    clear_execution_status();
    cerr << RED << "block scheduling finish" << RESET << endl << endl;
}

int transaction_test::test()
{
    try {
        assign_txn_id();
        assign_txn_status();
        gen_txn_stmts();
    } catch(exception &e) {
        cerr << RED << "Trigger a normal bugs when inializing the stmts" << RESET << endl;
        cerr << "Bug info: " << e.what() << endl;
        cerr << "Found normal bug " << record_bug_num << "!!!" << endl;
        
        string dir_name = output_path_dir + "bug_" + to_string(record_bug_num) + "_normal/"; 
        record_bug_num++;
        if (make_dir_error_exit(dir_name) == 1)
            return 255;
        
        string cmd = "mv " + string(NORMAL_BUG_FILE) + " " + dir_name;
        if (system(cmd.c_str()) == -1) {
            cerr << "system() error, return -1 in transaction_test::test!" << endl;
            return 255;
        }

        save_backup_file(dir_name, test_dbms_info); // save database
        return 1; // not need to do other transaction thing
    }
    
    try {
        if (multi_stmt_round_test() == false)
            return 0;
    } catch(exception &e) {
        string err = e.what();
        cerr << "error captured by test: " << err << endl;
        if (err.find("INSTRUMENT_ERR") != string::npos) // it is cause by: after instrumented, the scheduling change and error in txn_test happens
            return 0;
        if (err.find("still not executed") != string::npos) // cannot reproduce
            return 0;
    }

    cerr << "Found transaction bug " << record_bug_num << "!!!" << endl;
    string dir_name = output_path_dir + "bug_" + to_string(record_bug_num) + "_trans/"; 
    record_bug_num++;
    if (make_dir_error_exit(dir_name) == 1)
        return 255;

    save_backup_file(dir_name, test_dbms_info);
    save_test_case(dir_name);
    
    return 1;
}

transaction_test::transaction_test(dbms_info& d_info)
{
    trans_num = MAX_CONCURRENT_TXN_NUM * 3; // 9
    test_dbms_info = d_info;

    trans_arr = new transaction[trans_num];
    commit_num = trans_num; // all commit
    stmt_num = 0;
    for (int i = 0; i < trans_num; i++) {
        trans_arr[i].stmt_num = 4 + d6(); // 5 - 10
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
