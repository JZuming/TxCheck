#ifndef DEPENDENCY_ANALYZER_HH
#define DEPENDENCY_ANALYZER_HH

#include <ostream>
#include "relmodel.hh"
#include <memory>
#include "schema.hh"

#include "prod.hh"
#include "expr.hh"
#include "grammar.hh"

#include "instrumentor.hh"
#include <vector>
#include <set>
#include <algorithm>

using namespace std;

// START_DEPEND: the begin is count by first read or write
// STRICT_START_DEPEND: the begin is count by begin statement
enum dependency_type {WRITE_READ, WRITE_WRITE, READ_WRITE, START_DEPEND, STRICT_START_DEPEND, INNER_DEPEND};

typedef vector<string> row_output; // a row consists of several field(string)
typedef vector<row_output> stmt_output; // one output consits of several rows

struct operate_unit {
    stmt_usage stmt_u;
    int write_op_id;
    int tid;
    int stmt_idx;
    int row_id;
    size_t hash;
    operate_unit(stmt_usage use, int op_id, int tid, int stmt_idx, int row_id, size_t hash):
        stmt_u(use), write_op_id(op_id), 
        tid(tid), stmt_idx(stmt_idx), 
        row_id(row_id), hash(hash) {}
};

struct row_change_history {
    int row_id;
    vector<operate_unit> row_op_list;
};

struct history {
    vector<row_change_history> change_history;
    void insert_to_history(operate_unit& oper_unit);
};

struct stmt_id {
    int txn_id;
    int stmt_idx_in_txn;
    bool operator==(const stmt_id& other_id) const {
        return this->txn_id == other_id.txn_id && 
                this->stmt_idx_in_txn == other_id.stmt_idx_in_txn;
    }

    bool operator<(const stmt_id& other_id) const {
        if (this->txn_id == other_id.txn_id) 
            return this->stmt_idx_in_txn < other_id.stmt_idx_in_txn;
        else 
            return this->txn_id < other_id.txn_id;
    }

    stmt_id(vector<int>& final_tid_queue, int stmt_idx);
    stmt_id() {txn_id = -1; stmt_idx_in_txn = -1;}
    stmt_id(int tid, int stmt_pos) {txn_id = tid; stmt_idx_in_txn = stmt_pos;}
};

struct dependency_analyzer
{
    dependency_analyzer(vector<stmt_output>& init_output,
                        vector<stmt_output>& total_output,
                        vector<int>& final_tid_queue,
                        vector<stmt_usage>& final_stmt_usage,
                        vector<txn_status>& final_txn_status,
                        int t_num,
                        int primary_key_idx,
                        int write_op_key_idx);
    ~dependency_analyzer();

    size_t hash_output(row_output& row);
    void build_WR_dependency(vector<operate_unit>& op_list, int op_idx);
    void build_RW_dependency(vector<operate_unit>& op_list, int op_idx);
    void build_WW_dependency(vector<operate_unit>& op_list, int op_idx);
    
    void build_start_dependency();
    void build_stmt_inner_dependency();
    void build_stmt_start_dependency(int prev_tid, int later_tid, dependency_type dt);

    void print_dependency_graph();
    
    // G1a: Aborted Reads. A history H exhibits phenomenon G1a if it contains an aborted
    // transaction Ti and a committed transaction Tj such that Tj has read some object
    // (maybe via a predicate) modified by Ti.
    bool check_G1a();
    // G1b: Intermediate Reads. A history H exhibits phenomenon G1b if it contains a
    // committed transaction Tj that has read a version of object x (maybe via a predicate)
    // written by transaction Ti that was not Ti’s final modification of x.
    bool check_G1b();
    // G1c: Circular Information Flow. A history H exhibits phenomenon G1c if DSG(H)
    // contains a directed cycle consisting entirely of dependency edges.
    bool check_G1c();
    // G2-item: Item Anti-dependency Cycles. A history H exhibits phenomenon G2-item
    // if DSG(H) contains a directed cycle having one or more item-anti-dependency edges.
    bool check_G2_item();

    // Snapshot Isolation:
    // G-SIa: Interference. A history H exhibits phenomenon G-SIa if SSG(H) contains a
    // read/write-dependency edge from Ti to Tj without there also being a start-dependency
    // edge from Ti to Tj.
    bool check_GSIa();
    // G-SIb: Missed Effects. A history H exhibits phenomenon G-SIb if SSG(H) contains
    // a directed cycle with exactly one anti-dependency edge.
    bool check_GSIb();

    bool check_cycle(set<dependency_type>& edge_types);
    static bool reduce_graph_indegree(int **direct_graph, int length);
    static bool reduce_graph_outdegree(int **direct_graph, int length);
    vector<int> PL2_longest_path();
    vector<int> PL3_longest_path();
    vector<int> SI_longest_path();
    vector<int> longest_path(set<dependency_type>& used_dependency_set);
    vector<int> longest_path(int **direct_graph, int length);

    history h;
    int tid_num;
    int stmt_num;
    int* tid_begin_idx;
    int* tid_strict_begin_idx;
    int* tid_end_idx;
    vector<txn_status> f_txn_status;
    vector<int> f_txn_id_queue;
    vector<int> f_txn_size;
    map<int, row_output> hash_to_output;
    set<dependency_type> **dependency_graph;

    map<pair<stmt_id, stmt_id>, set<dependency_type>> stmt_dependency_graph;
    void build_stmt_depend_from_stmt_idx(int stmt_idx1, int stmt_idx2, dependency_type dt);
    vector<stmt_id> longest_stmt_path(map<pair<stmt_id, stmt_id>, int>& stmt_dist_graph);
    vector<stmt_id> longest_stmt_path();
};

#endif