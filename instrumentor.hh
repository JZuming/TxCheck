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

enum stmt_usage {NORMAL, BEFORE_WRITE_READ, AFTER_WRITE_READ};

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