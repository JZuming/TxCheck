#ifndef TRANSACTION_TEST_HH
#define TRANSACTION_TEST_HH

#include "config.h"

#include "dbms_info.hh"
#include "general_process.hh"

#include <sys/time.h>
#include <sys/wait.h>

using namespace std;

enum txn_status {NOT_DEFINED, TXN_COMMIT, TXN_ABORT};

struct transaction {
    shared_ptr<dut_base> dut;
    bool is_blocked;
    
    vector<string> stmts;
    vector<vector<string>> stmt_outputs;
    vector<string> stmt_err_info;

    vector<string> normal_stmts;
    vector<vector<vector<string>>> possible_normal_outputs;
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

    int commit_num;

    dbms_info test_dbms_info;

    int trans_num;
    int stmt_num;

    vector<int> tid_queue;
    vector<string> stmt_queue;

    vector<int> real_tid_queue;
    vector<string> real_stmt_queue;
    map<string, vector<string>> trans_db_content;

    vector<vector<int>> possible_normal_trans_order;
    vector<map<string, vector<string>>> possible_normal_db_content;

    void arrage_trans_for_tid_queue();
    void assign_trans_status();
    void gen_stmt_for_each_trans();
    
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