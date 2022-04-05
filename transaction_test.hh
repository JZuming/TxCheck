#ifndef TRANSACTION_TEST_HH
#define TRANSACTION_TEST_HH

#include "config.h"

#include "dbms_info.hh"
#include "general_process.hh"
#include "instrumentor.hh"
#include "dependency_analyzer.hh"

#include <sys/time.h>
#include <sys/wait.h>

using namespace std;

enum txn_status {NOT_DEFINED, TXN_COMMIT, TXN_ABORT};

struct transaction {
    shared_ptr<dut_base> dut;
    bool is_blocked;
    
    vector<shared_ptr<prod>> stmts;
    vector<stmt_output> stmt_outputs;
    vector<string> stmt_err_info;

    vector<shared_ptr<prod>> normal_stmts;
    vector<vector<stmt_output>> possible_normal_outputs; // vector of txn output
    vector<vector<string>> possible_normal_err_info;

    transaction() {is_blocked = false; stmt_num = 0; status = NOT_DEFINED;}

    int stmt_num;
    txn_status status;
};

class transaction_test {
public:
    static int record_bug_num;
    static pid_t server_process_id;
    static bool try_to_kill_server();

    transaction* trans_arr;
    string output_path_dir;

    dbms_info test_dbms_info;
    int commit_num;

    int trans_num;
    int stmt_num;

    shared_ptr<schema> db_schema;

    vector<int> tid_queue;
    vector<shared_ptr<prod>> stmt_queue;
    vector<stmt_usage> stmt_use;

    vector<int> real_tid_queue;
    vector<shared_ptr<prod>> real_stmt_queue;
    map<string, vector<vector<string>>> trans_db_content;

    vector<vector<int>> possible_normal_trans_order;
    vector<map<string, vector<vector<string>>>> possible_normal_db_content;

    void assign_txn_id();
    void assign_txn_status();
    void gen_txn_stmts();
    void instrument_txn_stmts();
    
    bool check_commit_trans_blocked();
    void trans_test();
    void retry_block_stmt(int cur_stmt_num, shared_ptr<int[]> status_queue);
    int trans_test_unit(int stmt_pos);

    void normal_test();
    bool check_result();

    bool fork_if_server_closed();

    transaction_test(dbms_info& d_info);
    ~transaction_test();
    
    int test();

private:
    void get_possible_order();
    void execute_possible_order();
    bool check_one_order_result(int order_index);
    void save_test_case(string dir_name);
};

#endif