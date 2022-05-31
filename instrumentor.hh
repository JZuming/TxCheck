#ifndef INSTRUMENTOR_HH
#define INSTRUMENTOR_HH

#include <ostream>
#include "relmodel.hh"
#include <memory>
#include "schema.hh"

#include "prod.hh"
#include "expr.hh"
#include "grammar.hh"

#include <vector>

using namespace std;

enum stmt_basic_type {NORMAL, BEFORE_WRITE_READ, AFTER_WRITE_READ, BEFORE_PREDICATE_JUDGE, AFTER_PREDICATE_JUDGE};

struct stmt_usage {
    stmt_basic_type stmt_type;
    int pred_source_txn_id; // for predicate judge, the predicate source
    int pred_source_stmt_pos; // for predicate judge, the predicate source
    int pred_target_txn_id; // for predicate judge, the instrumented stmt
    int pred_target_stmt_pos; // for predicate judge, the instrumented stmt

    stmt_usage(const stmt_basic_type& target_st) {
        stmt_type = target_st;
        pred_source_txn_id = -1;
        pred_source_stmt_pos = -1;
        pred_target_txn_id = -1;
        pred_target_stmt_pos = -1;
    }
    stmt_usage(const stmt_basic_type& target_st, 
                const int& source_txn_id, const int& source_stmt_pos,
                const int& target_txn_id, const int& target_stmt_pos) {
        stmt_type = target_st;
        if (stmt_type != BEFORE_PREDICATE_JUDGE && stmt_type != AFTER_PREDICATE_JUDGE) {
            pred_source_txn_id = -1;
            pred_source_stmt_pos = -1;
            pred_target_txn_id = -1;
            pred_target_stmt_pos = -1;
            return;
        }
        pred_source_txn_id = source_txn_id;
        pred_source_stmt_pos = source_stmt_pos;
        pred_target_txn_id = target_txn_id;
        pred_target_stmt_pos = target_stmt_pos;
    }
    bool operator==(const stmt_basic_type& target_st) const {
        return stmt_type == target_st;
    }
    bool operator!=(const stmt_basic_type& target_st) const {
        return stmt_type != target_st;
    }
    void operator=(const stmt_basic_type& target_st) {
        stmt_type = target_st;
    }
    friend ostream &operator<<( ostream &output, const stmt_usage &su ) { 
        output << su.stmt_type;
        return output;            
    }
};

struct instrumentor
{
    instrumentor(vector<shared_ptr<prod>>& stmt_queue,
                            vector<int>& tid_queue,
                            shared_ptr<schema> db_schema);

    vector<shared_ptr<prod>> final_stmt_queue;
    vector<int> final_tid_queue;

    vector<stmt_usage> final_stmt_usage;
};




#endif