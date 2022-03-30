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

using namespace std;

enum dependency_type {WRITE_READ, WRITE_WRITE, READ_WRITE};

typedef vector<string> row_output; // a row consists of several field(string)
typedef vector<row_output> one_output; // one output consits of several rows

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

struct dependency_analyzer
{
    dependency_analyzer(vector<one_output>& init_output,
                        vector<one_output>& total_output,
                        vector<int>& final_tid_queue,
                        vector<stmt_usage>& final_stmt_usage,
                        int t_num,
                        int primary_key_idx,
                        int write_op_key_idx);
    ~dependency_analyzer();

    size_t hash_output(row_output& row);
    void build_WR_dependency(vector<operate_unit>& op_list, int op_idx);

    history h;
    int tid_num;
    vector<dependency_type> **dependency_graph;
};

#endif