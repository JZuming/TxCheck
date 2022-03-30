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

typedef vector<string> row; // a row consists of several field(string)
typedef vector<row> one_output; // one output consits of several rows

struct dependency_analyzer
{
    dependency_analyzer(vector<one_output>& total_output,
                        vector<int>& final_tid_queue,
                        vector<stmt_usage>& final_stmt_usage,
                        int t_num);
    ~dependency_analyzer();
    int tid_num;
    dependency_type **dependency_graph;
};

#endif