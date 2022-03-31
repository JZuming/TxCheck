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
            dependency_graph[op_list[i].tid][target_op.tid].push_back(WRITE_READ);
        
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

        dependency_graph[op_list[i].tid][target_op.tid].push_back(READ_WRITE);

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
            dependency_graph[op_list[i].tid][target_op.tid].push_back(WRITE_WRITE);

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
                        int t_num,
                        int primary_key_idx,
                        int write_op_key_idx):
tid_num(t_num)
{   
    dependency_graph = new vector<dependency_type>* [tid_num];
    for (int i = 0; i < tid_num; i++) 
        dependency_graph[i] = new vector<dependency_type> [tid_num];

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
}

dependency_analyzer::~dependency_analyzer()
{
    for (int i = 0; i < tid_num; i++) 
        delete[] dependency_graph[i];
    delete[] dependency_graph;
}