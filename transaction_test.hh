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

#define SHOW_CHARACTERS 100
#define SPACE_HOLDER_STMT "SELECT 1 WHERE 0 <> 0"

struct transaction {
    shared_ptr<dut_base> dut;
    bool is_blocked;
    
    vector<shared_ptr<prod>> stmts;
    vector<stmt_output> stmt_outputs;
    vector<string> stmt_err_info;

    vector<shared_ptr<prod>> normal_stmts;
    vector<stmt_output> normal_outputs;
    vector<string> normal_err_info;

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
    map<string, vector<vector<string>>> init_db_content;

    vector<int> real_tid_queue;
    vector<shared_ptr<prod>> real_stmt_queue;
    vector<stmt_output> real_output_queue;
    vector<stmt_usage> real_stmt_usage;
    map<string, vector<vector<string>>> trans_db_content;

    // normal stmt test related
    vector<stmt_output> normal_stmt_output;
    vector<string> normal_stmt_err_info;
    map<string, vector<vector<string>>> normal_stmt_db_content;

    vector<int> longest_seq_txn_order;

    map<string, vector<vector<string>>> normal_db_content;

    void assign_txn_id();
    void assign_txn_status();
    void gen_txn_stmts();
    void instrument_txn_stmts();
    void clean_instrument();
    void block_scheduling();

    static vector<int> get_longest_path_from_graph(shared_ptr<dependency_analyzer>& da);
    bool change_txn_status(int tid, txn_status final_status);
    bool analyze_txn_dependency(shared_ptr<dependency_analyzer>& da); // input da is empty; output the analyzed da
    bool refine_txn_as_txn_order();
    void clear_execution_status();
    bool multi_round_test(); // true: find bugs; false: no bug
    bool multi_stmt_round_test(); // true: find bugs; false: no bug
    bool refine_stmt_queue(vector<stmt_id>& stmt_path, shared_ptr<dependency_analyzer>& da);
    void normal_stmt_test(vector<stmt_id>& stmt_path);
    bool check_normal_stmt_result(vector<stmt_id>& stmt_path, bool debug = false);
    
    void trans_test(bool debug_mode = true);
    void retry_block_stmt(int cur_stmt_num, int* status_queue, bool debug_mode = true);
    int trans_test_unit(int stmt_pos, stmt_output& output, bool debug_mode = true);

    bool check_txn_normal_result();

    static bool fork_if_server_closed(dbms_info& d_info);

    void normal_test();

    transaction_test(dbms_info& d_info);
    ~transaction_test();
    
    int test();

private:
    bool check_one_order_result(int order_index);
    void save_test_case(string dir_name);
};

void print_stmt_path(vector<stmt_id>& stmt_path, map<pair<stmt_id, stmt_id>, set<dependency_type>>& stmt_graph);

#endif