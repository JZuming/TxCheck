#include <dependency_analyzer.hh>

void history::insert_to_history(operate_unit& oper_unit)
{
    auto row_id = oper_unit.row_id;
    auto size = change_history.size();
    bool exist_row_id = false;
    int row_idx;
    for (int i = 0; i < size; i ++) {
        if (change_history[i].row_id == row_id) {
            exist_row_id = true;
            row_idx = i;
            break;
        }
    }

    if (exist_row_id) 
        change_history[row_idx].row_op_list.push_back(oper_unit);
    else {
        row_change_history rch;
        rch.row_id = row_id;
        rch.row_op_list.push_back(oper_unit);
        change_history.push_back(rch);
    }

    return;
}

size_t dependency_analyzer::hash_output(row_output& row)
{
    register size_t hash = 0; 
    for (auto& str:row) {
        auto size = str.size();
        for (int i = 0; i < size; i++ ) {
            size_t ch = (size_t)str[i];
            hash = hash * 131 + ch;
        }
    }
    return hash;
}

void dependency_analyzer::build_WR_dependency(vector<operate_unit>& op_list, int op_idx)
{
    auto& target_op = op_list[op_idx];
    bool find_the_write = false;
    for (int i = op_idx - 1; i >= 0; i--) {
        if (op_list[i].stmt_u != AFTER_WRITE_READ)
            continue;

        // need strict compare to check whether the write is missed
        if (op_list[i].hash != target_op.hash)
            continue;
        
        find_the_write = true;

        if (op_list[i].tid != target_op.tid) 
            dependency_graph[op_list[i].tid][target_op.tid].insert(WRITE_READ);
        
        break; // only find the nearest write
    }
    if (find_the_write == false)
        throw runtime_error("BUG: Cannot find the corresponding write");
    return;
}

void dependency_analyzer::build_RW_dependency(vector<operate_unit>& op_list, int op_idx)
{
    auto& target_op = op_list[op_idx];
    if (target_op.stmt_u != BEFORE_WRITE_READ)
        throw runtime_error("something wrong, target_op.stmt_u is not BEFORE_WRITE_READ in build_RW_dependency");

    for (int i = op_idx - 1; i >= 0; i--) {
        // need eazier compare to build more edge
        if (op_list[i].write_op_id != target_op.write_op_id)
            continue;

        if (op_list[i].tid == target_op.tid)
            continue;

        dependency_graph[op_list[i].tid][target_op.tid].insert(READ_WRITE);

        // do not break, because need to find all the read
    }
    return;
}

void dependency_analyzer::build_WW_dependency(vector<operate_unit>& op_list, int op_idx)
{
    auto& target_op = op_list[op_idx];
    if (target_op.stmt_u != BEFORE_WRITE_READ)
        throw runtime_error("something wrong, target_op.stmt_u is not BEFORE_WRITE_READ in build_WW_dependency");

    bool find_the_write = false;
    for (int i = op_idx - 1; i >= 0; i--) {
        if (op_list[i].stmt_u != AFTER_WRITE_READ)
            continue;

        // need strict compare to find miss write bug
        if (op_list[i].hash != target_op.hash)
            continue;
        
        find_the_write = true;

        if (op_list[i].tid != target_op.tid)
            dependency_graph[op_list[i].tid][target_op.tid].insert(WRITE_WRITE);

        break; // only find the nearest write
    }
    if (find_the_write == false)
        throw runtime_error("BUG: Cannot find the corresponding write");
    return;
}

dependency_analyzer::dependency_analyzer(vector<one_output>& init_output,
                        vector<one_output>& total_output,
                        vector<int>& final_tid_queue,
                        vector<stmt_usage>& final_stmt_usage,
                        vector<txn_status>& final_txn_status,
                        int t_num,
                        int primary_key_idx,
                        int write_op_key_idx):
tid_num(t_num), f_txn_status(final_txn_status)
{   
    dependency_graph = new set<dependency_type>* [tid_num];
    for (int i = 0; i < tid_num; i++) 
        dependency_graph[i] = new set<dependency_type> [tid_num];

    for (auto& each_output : init_output) {
        if (each_output.empty())
            continue;
        for (auto& row : each_output) {
            auto row_id = stoi(row[primary_key_idx]);
            auto write_op_id = stoi(row[write_op_key_idx]);
            auto hash = hash_output(row);
            operate_unit op(AFTER_WRITE_READ, write_op_id, -1, -1, row_id, hash);
            h.insert_to_history(op);
        }
    }
    
    auto stmt_num = final_tid_queue.size();
    for (int i = 0; i < stmt_num; i++) {
        auto& each_output = total_output[i];
        auto tid = final_tid_queue[i];
        auto stmt_u = final_stmt_usage[i];

        // do not analyze empty output select read;
        // write operation (insert, delete, update) will be analzye by using before/after-write read
        if (each_output.empty())
            continue;
        
        for (auto& row : each_output) {
            auto row_id = stoi(row[primary_key_idx]);
            auto write_op_id = stoi(row[write_op_key_idx]);
            auto hash = hash_output(row);
            operate_unit op(stmt_u, write_op_id, tid, i, row_id, hash);
            h.insert_to_history(op);
        }
    }

    // generate ww, wr, rw dependency
    for (auto& row_history : h.change_history) {
        auto& row_op_list = row_history.row_op_list;
        auto size = row_op_list.size();
        for (int i = 0; i < size; i++) {
            auto& op_unit = row_op_list[i];
            build_WR_dependency(row_op_list, i); // it is a read itself
            if (op_unit.stmt_u == BEFORE_WRITE_READ) {
                build_WW_dependency(row_op_list, i);
                build_RW_dependency(row_op_list, i);
            }
        }
    }

    // generate start dependency (for snapshot)
    tid_begin_idx = new int[tid_num];
    tid_end_idx = new int[tid_num];
    for (int i = 0; i < tid_num; i++) {
        tid_begin_idx[i] = -1;
        tid_end_idx[i] = -1;
    }
    for (int i = 0; i < stmt_num; i++) {
        auto tid = final_tid_queue[i];
        if (tid_begin_idx[tid] == -1)
            tid_begin_idx[tid] = i;
        if (tid_end_idx[tid] < i)
            tid_end_idx[tid] = i;
    }
    for (int i = 0; i < tid_num; i++) {
        if (f_txn_status[i] != TXN_COMMIT)
            continue;
        for (int j = 0; j < tid_num; j++) {
            if (i == j)
                continue;
            if (tid_end_idx[i] < tid_begin_idx[j])
                dependency_graph[i][j].insert(START_DEPEND);
        }
    }
}

dependency_analyzer::~dependency_analyzer()
{
    delete[] tid_end_idx;
    delete[] tid_begin_idx;
    
    for (int i = 0; i < tid_num; i++) 
        delete[] dependency_graph[i];
    delete[] dependency_graph;
}

// G1a: Aborted Reads. A history H exhibits phenomenon G1a if it contains an aborted
// transaction Ti and a committed transaction Tj such that Tj has read some object
// (maybe via a predicate) modified by Ti. Phenomenon G1a can be represented using
// the following history fragments:
// wi(xi:m) ... rj(xi:m) ... (ai and cj in any order)
// wi(xi:m) ... rj(P: xi:m, ...) ... (ai and cj in any order)
bool dependency_analyzer::check_G1a()
{
    // check whether there is a read dependency from Ti to Tj that is aborted
    for (int j = 0; j < tid_num; j++) {
        if (f_txn_status[j] != TXN_ABORT)
            continue; // txn j must be aborted
        
        for (int i = 0; i < tid_num; i++) {
            if (f_txn_status[i] != TXN_COMMIT)
                continue; // txn i must be committed
            
            auto& dependencies = dependency_graph[i][j];
            for (auto dependency : dependencies) {
                if (dependency == WRITE_READ)
                    return true;
            }
        }
    }
    return false;
}

// G1b: Intermediate Reads. A history H exhibits phenomenon G1b if it contains a
// committed transaction Tj that has read a version of object x (maybe via a predicate)
// written by transaction Ti that was not Ti’s final modification of x. The following history
// fragments represent this phenomenon:
// wi(xi:m) ... rj(xi:m) ... wi(xi:n) ... cj
// wi(xi:m) ... rj(P: xi:m, ...) ... wi(xi:n) ... cj
bool dependency_analyzer::check_G1b()
{
    enum check_stage {NONE, WRITE_STAGE, WRITE_READ_STAGE, WRITE_READ_WRITE_STAGE};
    
    for (auto& rch : h.change_history) {
        auto& op_list = rch.row_op_list;
        auto opl_size = op_list.size();
        for (int i = 0; i < opl_size; i++) {
            if (op_list[i].stmt_u != AFTER_WRITE_READ)
                continue;
            
            int wop_id = op_list[i].write_op_id;
            int tid = op_list[i].tid;
            int txn_end_idx = tid_end_idx[tid];
            bool has_read = false;
            bool has_write = false;
            check_stage stage = WRITE_STAGE;
            
            for (int j = i + 1; j < opl_size; j++) {
                if (op_list[j].stmt_idx > txn_end_idx)
                    break; // the later stmt will not contain the write from txn i
                
                // check whether the earlier version is read
                if (has_read == false && 
                        op_list[j].write_op_id == wop_id &&
                        op_list[j].tid != tid)
                    has_read = true;
                
                // check whether it will be rewrite by itself
                if (has_write == false &&
                        op_list[j].tid == tid &&
                        op_list[j].stmt_u == BEFORE_WRITE_READ)
                    has_write = true;
                
                if (has_read && has_write)
                    return true;
            }
        }
    }

    return false;
}

// recursively remove the node have 0 in-degree
// return false if graph is empty after reduction, otherwise true
bool dependency_analyzer::reduce_graph_indegree(int **direct_graph, int length)
{
    set<int> deleted_nodes;
    while (1) {
        // check whether the graph is empty
        if (deleted_nodes.size() == length)
            return false;
        
        // find a node whose in-degree is 0
        int zero_indegree_idx = -1;
        for (int i = 0; i < length; i++) {
            if (deleted_nodes.count(i) > 0)
                continue;
            
            bool has_indegree = false;
            for (int j = 0; j < length; j++) {
                if (direct_graph[j][i] > 0) {
                    has_indegree = true;
                    break;
                }
            }
            if (has_indegree == false) {
                zero_indegree_idx = i;
                break;
            }
        }
        // if all nodes have indegree, there is a cycle
        if (zero_indegree_idx == -1)
            return true;

        // delete the node and edge from node to other node
        deleted_nodes.insert(zero_indegree_idx);
        for (int j = 0; j < length; j++)
            direct_graph[zero_indegree_idx][j] = 0; 
    }
}

// recursively remove the node have 0 out-degree
// return false if graph is empty after reduction, otherwise true
bool dependency_analyzer::reduce_graph_outdegree(int **direct_graph, int length)
{
    set<int> deleted_nodes;
    while (1) {
        // check whether the graph is empty
        if (deleted_nodes.size() == length)
            return false;
        
        // find a node whose out-degree is 0
        int zero_outdegree_idx = -1;
        for (int i = 0; i < length; i++) {
            if (deleted_nodes.count(i) > 0)
                continue;
            
            bool has_outdegree = false;
            for (int j = 0; j < length; j++) {
                if (direct_graph[i][j] > 0) {
                    has_outdegree = true;
                    break;
                }
            }
            if (has_outdegree == false) {
                zero_outdegree_idx = i;
                break;
            }
        }
        // if all nodes have outdegree, there is a cycle
        if (zero_outdegree_idx == -1)
            return true;

        // delete the node and edge from other node to this node
        deleted_nodes.insert(zero_outdegree_idx);
        for (int i = 0; i < length; i++) 
            direct_graph[i][zero_outdegree_idx] = 0;
    }
}

bool dependency_analyzer::check_cycle(set<dependency_type>& edge_types)
{
    auto tmp_dgraph = new int* [tid_num];
    for (int i = 0; i < tid_num; i++) 
        tmp_dgraph[i] = new int [tid_num];
    
    // initialize tmp_dgraph
    for (int i = 0; i < tid_num; i++) {
        for (int j = 0; j < tid_num; j++) {
            set<dependency_type> res;
            set_intersection(edge_types.begin(), edge_types.end(), 
                    dependency_graph[i][j].begin(), dependency_graph[i][j].end(),
                    inserter(res, res.begin()));
            
            // have needed edges
            if (res.empty())
                tmp_dgraph[i][j] = 0;
            else
                tmp_dgraph[i][j] = 1;
        }
    }

    // topp sort
    bool have_cycle = reduce_graph_indegree(tmp_dgraph, tid_num);
    
    for (int i = 0; i < tid_num; i++)
        delete[] tmp_dgraph[i];
    delete[] tmp_dgraph;

    return have_cycle;
}

bool dependency_analyzer::check_G1c()
{
    set<dependency_type> ww_wr_set;
    ww_wr_set.insert(WRITE_WRITE);
    ww_wr_set.insert(WRITE_READ);

    auto tmp_dgraph = new int* [tid_num];
    for (int i = 0; i < tid_num; i++) 
        tmp_dgraph[i] = new int [tid_num];
    
    // initialize tmp_dgraph
    for (int i = 0; i < tid_num; i++) {
        for (int j = 0; j < tid_num; j++) {
            set<dependency_type> res;
            set_intersection(ww_wr_set.begin(), ww_wr_set.end(), 
                    dependency_graph[i][j].begin(), dependency_graph[i][j].end(),
                    inserter(res, res.begin()));
            
            // have needed edges
            if (res.empty())
                tmp_dgraph[i][j] = 0;
            else
                tmp_dgraph[i][j] = 1;
        }
    }

    bool have_cycle = reduce_graph_indegree(tmp_dgraph, tid_num);
    
    for (int i = 0; i < tid_num; i++)
        delete[] tmp_dgraph[i];
    delete[] tmp_dgraph;

    return have_cycle;
}

bool dependency_analyzer::check_GSIa()
{
    for (int i = 0; i < tid_num; i++) {
        for (int j = 0; j < tid_num; j++) {
            // check whether they have ww or wr dependency
            if (dependency_graph[i][j].count(WRITE_WRITE) == 0 &&
                    dependency_graph[i][j].count(WRITE_READ) == 0) 
                continue;
            
            // check whether they have start dependency
            if (dependency_graph[i][j].count(START_DEPEND) == 0)
                return true;
        }
    }
    return false;
}

bool dependency_analyzer::check_GSIb()
{
    set<dependency_type> target_dependency_set;
    target_dependency_set.insert(WRITE_WRITE);
    target_dependency_set.insert(WRITE_READ);
    target_dependency_set.insert(READ_WRITE);
    target_dependency_set.insert(START_DEPEND);
    
    auto tmp_dgraph = new int* [tid_num];
    for (int i = 0; i < tid_num; i++) 
        tmp_dgraph[i] = new int [tid_num];
    
    // initialize tmp_dgraph
    for (int i = 0; i < tid_num; i++) {
        for (int j = 0; j < tid_num; j++) {
            set<dependency_type> res;
            set_intersection(target_dependency_set.begin(), target_dependency_set.end(), 
                    dependency_graph[i][j].begin(), dependency_graph[i][j].end(),
                    inserter(res, res.begin()));
            
            // have needed edges
            if (res.empty())
                tmp_dgraph[i][j] = 0;
            else
                tmp_dgraph[i][j] = 1;
        }
    }

    if (reduce_graph_indegree(tmp_dgraph, tid_num) == false ||
            reduce_graph_outdegree(tmp_dgraph, tid_num) == false) {// empty
        
        for (int i = 0; i < tid_num; i++)
            delete[] tmp_dgraph[i];
        delete[] tmp_dgraph;
        return false;
    }
    
    // check which edge only have rw dependency
    vector<pair<int, int>> rw_edges;
    for (int i = 0; i < tid_num; i++) {
        for (int j = 0; j < tid_num; j++) {
            if (tmp_dgraph[i][j] == 0)
                continue;
            // if there is other depend (WR, WW, START(equal to WR or WW according to GSIa)), 
            // no need to remove it, will be report by G1c
            if (dependency_graph[i][j].size() > 1)
                continue;
            if (dependency_graph[i][j].count(READ_WRITE) == 0)
                continue;
            
            // exactly the READ_WRITE depend
            rw_edges.push_back(pair<int, int>(i, j));
            tmp_dgraph[i][j] = 0; // delete the edge
        }
    }
    
    // only leave one rw edege
    bool has_rw_cycle = false;
    for (auto& rw_edge : rw_edges) {
        // only insert 1 rw edge each time
        tmp_dgraph[rw_edge.first][rw_edge.second] = 1;
        if (reduce_graph_indegree(tmp_dgraph, tid_num)) {
            has_rw_cycle = true;
            break;
        }
        tmp_dgraph[rw_edge.first][rw_edge.second] = 0;
    }

    for (int i = 0; i < tid_num; i++)
        delete[] tmp_dgraph[i];
    delete[] tmp_dgraph;

    return has_rw_cycle;
}
