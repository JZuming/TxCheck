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


// item-dependency related: BEFORE_WRITE_READ, AFTER_WRITE_READ
// predicate-dependency related: VERSION_SET_READ, BEFORE_OVERWRITE_JUDGE, AFTER_OVERWRITE_JUDGE
enum stmt_basic_type {INIT_TYPE, // replaced str or did not figure out yet
                    SELECT_READ,
                    UPDATE_WRITE,
                    INSERT_WRITE,
                    DELETE_WRITE,
                    BEFORE_WRITE_READ, 
                    AFTER_WRITE_READ, 
                    VERSION_SET_READ};

struct stmt_usage {
    stmt_basic_type stmt_type;

    stmt_usage(const stmt_basic_type& target_st) {
        stmt_type = target_st;
    }
    stmt_usage(const stmt_usage& target_su) {
        stmt_type = target_su.stmt_type;
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
    void operator=(const stmt_usage& target_su) {
        stmt_type = target_su.stmt_type;
    }
    friend ostream &operator<<(ostream &output, const stmt_usage &su) { 
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