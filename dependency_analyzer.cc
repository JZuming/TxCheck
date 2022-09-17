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

stmt_id::stmt_id(vector<int>& final_tid_queue, int stmt_idx)
{
    txn_id = final_tid_queue[stmt_idx];
    stmt_idx_in_txn = -1;
    for (int i = 0; i <= stmt_idx; i++) {
        if (final_tid_queue[i] == txn_id)
            stmt_idx_in_txn++;
    }
}

// -1: error, txn_id or stmt_idx_in_txn is -1, or cannot find 
int stmt_id::transfer_2_stmt_idx(vector<int>& final_tid_queue)
{
    if (txn_id == -1 || stmt_idx_in_txn == -1)
        return -1;
    int tmp_target_txn_idx = -1;
    auto queue_size = final_tid_queue.size();
    for (int i = 0; i < queue_size; i++) {
        if (final_tid_queue[i] != txn_id)
            continue;
        tmp_target_txn_idx++;
        if (tmp_target_txn_idx == stmt_idx_in_txn)
            return i;
    }
    return -1;
}

void dependency_analyzer::build_stmt_depend_from_stmt_idx(int stmt_idx1, int stmt_idx2, dependency_type dt)
{
    auto stmt_id1 = stmt_id(f_txn_id_queue, stmt_idx1);
    auto stmt_id2 = stmt_id(f_txn_id_queue, stmt_idx2);
    auto stmt_pair = make_pair(stmt_id1, stmt_id2);
    if (stmt_dependency_graph.count(stmt_pair) > 0)
        stmt_dependency_graph[stmt_pair].insert(dt);
    else {
        set<dependency_type> d_set;
        d_set.insert(dt);
        stmt_dependency_graph[stmt_pair] = d_set;
    }
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

// for BEFORE_WRITE_READ, VERSION_SET_READ, SELECT_READ
void dependency_analyzer::build_WR_dependency(vector<operate_unit>& op_list, int op_idx)
{
    auto& target_op = op_list[op_idx];
    bool find_the_write = false;
    for (int i = op_idx - 1; i >= 0; i--) {
        if (op_list[i].stmt_u != AFTER_WRITE_READ) // only search for AFTER_WRITE_READ
            continue;
        
        // need strict compare to check whether the write is missed
        if (op_list[i].hash != target_op.hash)
            continue;
        
        find_the_write = true;

        if (op_list[i].stmt_idx >= 0 && target_op.stmt_idx >= 0) // stmts in same transaction should build dependency 
            build_stmt_depend_from_stmt_idx(op_list[i].stmt_idx, target_op.stmt_idx, WRITE_READ);

        if (op_list[i].tid != target_op.tid) 
            dependency_graph[op_list[i].tid][target_op.tid].insert(WRITE_READ);
        
        break; // only find the nearest write
    }
    if (find_the_write == false) {
        cerr << "Read stmt idx: " << target_op.stmt_idx << endl;
        cerr << "Read stmt tid: " << target_op.tid << endl;
        
        cerr << "Problem read: ";
        auto& problem_row = hash_to_output[target_op.hash];
        for (int i = 0; i < problem_row.size(); i++)
            cerr << problem_row[i] << " ";
        cerr << endl;

        for (int i = 0; i < op_list.size(); i++) {
            if (op_list[i].stmt_u != AFTER_WRITE_READ)
                continue;
            cerr << "AFTER_WRITE_READ " << i << ": ";
            auto& write_row = hash_to_output[op_list[i].hash];
            for (int i = 0; i < write_row.size(); i++)
                cerr << write_row[i] << " ";
            cerr << endl;
        }

        throw runtime_error("BUG: Cannot find the corresponding write");
    }

    return;
}

// for BEFORE_WRITE_READ
void dependency_analyzer::build_RW_dependency(vector<operate_unit>& op_list, int op_idx)
{
    auto& target_op = op_list[op_idx];
    if (target_op.stmt_u != BEFORE_WRITE_READ)
        throw runtime_error("something wrong, target_op.stmt_u is not BEFORE_WRITE_READ in build_RW_dependency");

    auto list_size = op_list.size();
    for (int i = 0; i < list_size; i++) {
        // could not build BWR -> BWR ()
        // if BWR -> BWR is build (RW), then AWR -> BWR is also built (WW), so missing it is fine
        if (op_list[i].stmt_u != SELECT_READ && op_list[i].stmt_u != AFTER_WRITE_READ) 
            continue; // only search for SELECT_READ, AFTER_WRITE_READ

        // need eazier compare to build more edge
        if (op_list[i].write_op_id != target_op.write_op_id)
            continue;

        if (op_list[i].stmt_idx >= 0 && target_op.stmt_idx >= 0) 
            build_stmt_depend_from_stmt_idx(op_list[i].stmt_idx, target_op.stmt_idx, READ_WRITE);

        if (op_list[i].tid != target_op.tid)
            dependency_graph[op_list[i].tid][target_op.tid].insert(READ_WRITE);

        // do not break, because need to find all the read
    }

    return;
}

// for BEFORE_WRITE_READ
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

        if (op_list[i].stmt_idx >= 0 && target_op.stmt_idx >= 0) 
                build_stmt_depend_from_stmt_idx(op_list[i].stmt_idx, target_op.stmt_idx, WRITE_WRITE);

        if (op_list[i].tid != target_op.tid) 
            dependency_graph[op_list[i].tid][target_op.tid].insert(WRITE_WRITE);

        break; // only find the nearest write
    }
    if (find_the_write == false)
        throw runtime_error("BUG: Cannot find the corresponding write");
    
    return;
}

// should be used after build_start_dependency
void dependency_analyzer::build_VS_dependency()
{
    if (tid_begin_idx == NULL || tid_strict_begin_idx == NULL || tid_end_idx == NULL) {
        cerr << "you should not use build_VS_dependency before build_start_dependency" << endl;
        throw runtime_error("you should not use build_VS_dependency before build_start_dependency");
    }
    for (int i = 0; i < stmt_num; i++) {
        auto& i_stmt_u = f_stmt_usage[i];
        if (i_stmt_u != VERSION_SET_READ)
            continue;
        auto& i_tid = f_txn_id_queue[i];
        auto& i_output = f_stmt_output[i];

        set<pair<int, int>> i_pv_pair_set; // primary_key, version_key
        set<int> i_primary_set; // primary_key
        for (auto& row : i_output) {
            auto row_id = stoi(row[primary_key_index]);
            auto version_id = stoi(row[version_key_index]);
            pair<int, int> p(row_id, version_id);
            i_pv_pair_set.insert(p);
            i_primary_set.insert(row_id);
        }

        for (int j = 0; j < stmt_num; j++) {
            auto& j_tid = f_txn_id_queue[j];
            if (i_tid == j_tid)
                continue;
            // skip if they donot interleave
            if (dependency_graph[i_tid][j_tid].count(STRICT_START_DEPEND) > 0)
                continue;
            if (dependency_graph[j_tid][i_tid].count(STRICT_START_DEPEND) > 0)
                continue;
            
            auto& j_stmt_u = f_stmt_usage[j];
            if (j_stmt_u == UPDATE_WRITE || j_stmt_u == INSERT_WRITE) {
                auto after_write_idx = j + 1;
                if (f_stmt_usage[after_write_idx] != AFTER_WRITE_READ) {
                    auto err_info = "[INSTRUMENT_ERR] build_VS_dependency: after_write_idx is not AFTER_WRITE_READ, after_write_idx = " + to_string(after_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                auto& after_write_output = f_stmt_output[after_write_idx];
                set<pair<int, int>> after_write_pv_pair_set; // primary_key, version_key
                for (auto& row:after_write_output) {
                    auto row_id = stoi(row[primary_key_index]);
                    auto version_id = stoi(row[version_key_index]);
                    pair<int, int> p(row_id, version_id);
                    after_write_pv_pair_set.insert(p);
                }

                set<pair<int, int>> res;
                set_intersection(i_pv_pair_set.begin(), i_pv_pair_set.end(), 
                    after_write_pv_pair_set.begin(), after_write_pv_pair_set.end(),
                    inserter(res, res.begin()));

                if (!res.empty()) { // if it is not empty, the changed version is seen in version read
                    dependency_graph[j_tid][i_tid].insert(VERSION_SET_DEPEND);
                    build_stmt_depend_from_stmt_idx(after_write_idx, i, VERSION_SET_DEPEND);
                    // update/insert -> AFTER_WRITE_READ -> VERSION_SET_READ -> target_one
                }
            }
            else if (j_stmt_u == DELETE_WRITE) {
                auto before_write_idx = j - 1;
                if (f_stmt_usage[before_write_idx] != BEFORE_WRITE_READ) {
                    auto err_info = "[INSTRUMENT_ERR] build_VS_dependency: before_write_idx is not BEFORE_WRITE_READ, before_write_idx = " + to_string(before_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                // skip if they donot handle the same table
                if (f_stmt_usage[before_write_idx].target_table == "" || i_stmt_u.target_table == "") {
                    auto err_info = "[INSTRUMENT_ERR] build_VS_dependency: target_table is not initialized, before_write_idx = " + to_string(before_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                if (f_stmt_usage[before_write_idx].target_table != i_stmt_u.target_table)
                    continue;
                
                auto& before_write_output = f_stmt_output[before_write_idx];
                if (before_write_output.empty()) // delete nothing, skip
                    continue;

                set<int> before_write_primary_set; // primary_key, version_key
                for (auto& row:before_write_output) {
                    auto row_id = stoi(row[primary_key_index]);
                    before_write_primary_set.insert(row_id);
                }

                set<int> res;
                set_intersection(i_primary_set.begin(), i_primary_set.begin(),
                    before_write_primary_set.begin(), before_write_primary_set.begin(),
                    inserter(res, res.begin()));
                if (res.empty()) { // if it is emtpy, the row is deleted
                    dependency_graph[j_tid][i_tid].insert(VERSION_SET_DEPEND);
                    build_stmt_depend_from_stmt_idx(j, i, VERSION_SET_DEPEND);
                    // BEFORE_WRITE_READ-> delete -> VERSION_SET_READ -> target_one
                }
            }
        }
    }
}

// should be used after build_start_dependency
void dependency_analyzer::build_OW_dependency()
{
    if (tid_begin_idx == NULL || tid_strict_begin_idx == NULL || tid_end_idx == NULL) {
        cerr << "you should not use build_VS_dependency before build_start_dependency" << endl;
        throw runtime_error("you should not use build_VS_dependency before build_start_dependency");
    }

    for (int i = 0; i < stmt_num; i++) {
        auto& i_stmt_u = f_stmt_usage[i];
        if (i_stmt_u != VERSION_SET_READ)
            continue;
        auto& i_tid = f_txn_id_queue[i];
        auto& i_output = f_stmt_output[i];

        set<pair<int, int>> i_pv_pair_set; // primary_key, version_key
        set<int> i_primary_set; // primary_key
        for (auto& row : i_output) {
            auto row_id = stoi(row[primary_key_index]);
            auto version_id = stoi(row[version_key_index]);
            pair<int, int> p(row_id, version_id);
            i_pv_pair_set.insert(p);
            i_primary_set.insert(row_id);
        }

        int orginal_index = -1;
        for (int j = i + 1; j < stmt_num; j++) {
            if (f_stmt_usage[j] == SELECT_READ ||
                    f_stmt_usage[j] == UPDATE_WRITE ||
                    f_stmt_usage[j] == DELETE_WRITE ||
                    f_stmt_usage[j] == INSERT_WRITE) {
                orginal_index = j;
                break;
            }
        }
        if (orginal_index == -1) {
            auto err_info = "[INSTRUMENT_ERR] cannot find the orginal_index in build_OW_dependency";
            cerr << err_info << endl;
            throw runtime_error(err_info);
        }
        if (f_stmt_usage[orginal_index] == UPDATE_WRITE ||
                f_stmt_usage[orginal_index] == INSERT_WRITE) {
            orginal_index++; // use after_write_read (SELECT_READ and DELETE_WRITE donot have awr)
            if (f_stmt_usage[orginal_index] != AFTER_WRITE_READ) {
                auto err_info = "[INSTRUMENT_ERR] build_OW_dependency: orginal_index + 1 is not AFTER_WRITE_READ, orginal_index = " + to_string(orginal_index);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }
        }
            
        for (int j = 0; j < stmt_num; j++) {
            auto& j_tid = f_txn_id_queue[j];
            if (i_tid == j_tid)
                continue;
            // skip if they donot interleave
            if (dependency_graph[i_tid][j_tid].count(STRICT_START_DEPEND) > 0)
                continue;
            if (dependency_graph[j_tid][i_tid].count(STRICT_START_DEPEND) > 0)
                continue;
            
            auto& j_stmt_u = f_stmt_usage[j];
            if (j_stmt_u == UPDATE_WRITE || j_stmt_u == DELETE_WRITE) {
                auto before_write_idx = j - 1;
                if (f_stmt_usage[before_write_idx] != BEFORE_WRITE_READ) {
                    auto err_info = "[INSTRUMENT_ERR] build_OW_dependency: before_write_idx is not BEFORE_WRITE_READ, before_write_idx = " + to_string(before_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                auto& before_write_output = f_stmt_output[before_write_idx];
                set<pair<int, int>> before_write_pv_pair_set; // primary_key, version_key
                for (auto& row:before_write_output) {
                    auto row_id = stoi(row[primary_key_index]);
                    auto version_id = stoi(row[version_key_index]);
                    pair<int, int> p(row_id, version_id);
                    before_write_pv_pair_set.insert(p);
                }

                set<pair<int, int>> res;
                set_intersection(i_pv_pair_set.begin(), i_pv_pair_set.end(), 
                    before_write_pv_pair_set.begin(), before_write_pv_pair_set.end(),
                    inserter(res, res.begin()));
                if (!res.empty()) {
                    dependency_graph[i_tid][j_tid].insert(OVERWRITE_DEPEND);
                    build_stmt_depend_from_stmt_idx(orginal_index, before_write_idx, OVERWRITE_DEPEND);
                    // version_set read -> target_one -> before_read -> update/delete
                }
            }
            else if (j_stmt_u == INSERT_WRITE) {
                auto after_write_idx = j + 1;
                if (f_stmt_usage[after_write_idx] != AFTER_WRITE_READ) {
                    auto err_info = "[INSTRUMENT_ERR] build_OW_dependency: after_write_idx is not AFTER_WRITE_READ, after_write_idx = " + to_string(after_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                // skip if they donot handle the same table
                if (f_stmt_usage[after_write_idx].target_table == "" || i_stmt_u.target_table == "") {
                    auto err_info = "[INSTRUMENT_ERR] build_OW_dependency: target_table is not initialized, after_write_idx = " + to_string(after_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                if (f_stmt_usage[after_write_idx].target_table != i_stmt_u.target_table)
                    continue;
                
                auto& after_write_output = f_stmt_output[after_write_idx];
                if (after_write_output.empty()) // insert nothing, skip
                    continue;
                set<int> after_write_primary_set;
                for (auto& row : after_write_output) {
                    auto row_id = stoi(row[primary_key_index]);
                    after_write_primary_set.insert(row_id);
                }
                set<int> res;
                set_intersection(i_primary_set.begin(), i_primary_set.begin(),
                    after_write_primary_set.begin(), after_write_primary_set.begin(),
                    inserter(res, res.begin()));
                if (res.empty()) { // if it is emtpy, the row is not inserted yet
                    dependency_graph[i_tid][j_tid].insert(OVERWRITE_DEPEND);
                    build_stmt_depend_from_stmt_idx(orginal_index, j, OVERWRITE_DEPEND);
                    // version_set read -> target_one -> insert -> after_read
                }
            }
        }
    }
}

void dependency_analyzer::build_start_dependency()
{
    // count the second stmt as begin stmt, because some dbms donot use snapshot unless it read or write something
    auto tid_has_used_begin = new bool[tid_num];
    tid_strict_begin_idx = new int[tid_num];
    tid_begin_idx = new int[tid_num];
    tid_end_idx = new int[tid_num];
    for (int i = 0; i < tid_num; i++) {
        tid_strict_begin_idx[i] = -1;
        tid_begin_idx[i] = -1;
        tid_end_idx[i] = -1;
        tid_has_used_begin[i] = false;
    }
    for (int i = 0; i < stmt_num; i++) {
        auto tid = f_txn_id_queue[i];
        // skip the first stmt (i.e. start transaction)
        if (tid_has_used_begin[tid] == false) {
            tid_has_used_begin[tid] = true;
            tid_strict_begin_idx[tid] = i;
            continue;
        }
        if (tid_begin_idx[tid] == -1)
            tid_begin_idx[tid] = i;
        if (tid_end_idx[tid] < i)
            tid_end_idx[tid] = i;
    }
    for (int i = 0; i < tid_num; i++) {
        for (int j = 0; j < tid_num; j++) {
            if (i == j)
                continue;
            if (tid_end_idx[i] < tid_begin_idx[j]) {
                dependency_graph[i][j].insert(START_DEPEND);
                build_stmt_start_dependency(i, j, START_DEPEND);
            }
            if (tid_end_idx[i] < tid_strict_begin_idx[j]) {
                dependency_graph[i][j].insert(STRICT_START_DEPEND);
                build_stmt_start_dependency(i, j, STRICT_START_DEPEND);
            }
        }
    }
    delete[] tid_has_used_begin;
}

void dependency_analyzer::build_stmt_instrument_dependency()
{
    for (int i = 0; i < stmt_num; i++) {
        auto cur_usage = f_stmt_usage[i];
        auto cur_tid = f_txn_id_queue[i];
        
        if (cur_usage == BEFORE_WRITE_READ) {
            if (i + 1 >= stmt_num) {
                auto err_info = "[INSTRUMENT_ERR] i = BEFORE_WRITE_READ, i + 1 >= stmt_num, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }
                
            auto next_tid = f_txn_id_queue[i + 1];
            auto next_usage = f_stmt_usage[i + 1];
            if (next_tid != cur_tid) {
                auto err_info = "[INSTRUMENT_ERR] BEFORE_WRITE_READ: next_tid != cur_tid, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            if (next_usage != UPDATE_WRITE && next_usage != DELETE_WRITE) {
                auto err_info = "[INSTRUMENT_ERR] BEFORE_WRITE_READ: next_usage != UPDATE_WRITE && next_usage != DELETE_WRITE, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            build_stmt_depend_from_stmt_idx(i, i + 1, INSTRUMENT_DEPEND);
        }
        else if (cur_usage == AFTER_WRITE_READ) {
            if (i - 1 < 0) {
                auto err_info = "[INSTRUMENT_ERR] i = AFTER_WRITE_READ, i - 1 < 0, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            auto prev_tid = f_txn_id_queue[i - 1];
            auto prev_usage = f_stmt_usage[i - 1];
            if (prev_tid != cur_tid) {
                auto err_info = "[INSTRUMENT_ERR] AFTER_WRITE_READ: prev_tid != cur_tid, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            if (prev_usage != UPDATE_WRITE && prev_usage != INSERT_WRITE) {
                auto err_info = "[INSTRUMENT_ERR] AFTER_WRITE_READ: prev_tid != UPDATE_WRITE && prev_tid != INSERT_WRITE, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            build_stmt_depend_from_stmt_idx(i - 1, i, INSTRUMENT_DEPEND);
        }
        else if (cur_usage == VERSION_SET_READ) {
            int normal_pos = i + 1;
            while (normal_pos < stmt_num) {
                auto next_tid = f_txn_id_queue[normal_pos];
                auto next_usage = f_stmt_usage[normal_pos];
                if (next_tid != cur_tid) {
                    auto err_info = "[INSTRUMENT_ERR] VERSION_SET_READ: next_tid != cur_tid, cur: " + to_string(i) + " next: " + to_string(normal_pos);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                if (next_usage == SELECT_READ || 
                        next_usage == UPDATE_WRITE || 
                        next_usage == DELETE_WRITE || 
                        next_usage == INSERT_WRITE)
                    break;
                normal_pos ++;
            }

            if (normal_pos == stmt_num) {
                auto err_info = "[INSTRUMENT_ERR] VERSION_SET_READ: cannot find the normal one, cur: " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            build_stmt_depend_from_stmt_idx(i, normal_pos, INSTRUMENT_DEPEND);
        }
    }
}

set<int> dependency_analyzer::get_instrumented_stmt_set(int queue_idx)
{
    set<int> init_idx_set;
    set<int> processed_idx_set;
    init_idx_set.insert(queue_idx);
    while (!init_idx_set.empty()) {
        auto select_idx = *init_idx_set.begin();
        init_idx_set.erase(select_idx);
        processed_idx_set.insert(select_idx);

        auto stmt_id1 = stmt_id(f_txn_id_queue, select_idx);
        for (int i = 0; i < stmt_num; i++) {
            if (processed_idx_set.count(i) > 0) // has been processed
                continue;
            auto stmt_id2 = stmt_id(f_txn_id_queue, i);
            pair<stmt_id, stmt_id> instrument_pair;
            if (i < select_idx)
                instrument_pair = make_pair<>(stmt_id2, stmt_id1);
            else
                instrument_pair = make_pair<>(stmt_id1, stmt_id2);
            if (stmt_dependency_graph[instrument_pair].count(INSTRUMENT_DEPEND) > 0)
                init_idx_set.insert(i);
        }
    }
    return processed_idx_set;
}

void dependency_analyzer::build_stmt_inner_dependency()
{
    for (int i = 0; i < stmt_num; i++) {
        auto tid = f_txn_id_queue[i];
        for (int j = 0; j < i; j++) {
            auto prev_tid = f_txn_id_queue[j];
            if (prev_tid == tid) 
                build_stmt_depend_from_stmt_idx(j, i, INNER_DEPEND);
        }
    }
}

void dependency_analyzer::build_stmt_start_dependency(int prev_tid, int later_tid, dependency_type dt)
{
    for (int i = 0; i < stmt_num; i++) {
        auto i_tid = f_txn_id_queue[i];
        if (i_tid != prev_tid)
            continue;
        for (int j = i + 1; j < stmt_num; j++) {
            auto j_tid = f_txn_id_queue[j];
            if (j_tid != later_tid)
                continue;
            
            build_stmt_depend_from_stmt_idx(i, j, dt);
        }
    }
}

void dependency_analyzer::print_dependency_graph()
{
    cerr << "  ";
    for (int i = 0; i < tid_num; i++) {
        if (i < 10)
            cerr << "|     " << i;
        else
            cerr << "|    " << i;
    }
    cerr << "|" << endl;
    for (int i = 0; i < tid_num; i++) {
        if (i < 10)
            cerr << " " << i;
        else
            cerr << i;
        for (int j = 0; j < tid_num; j++) {
            cerr << "|";
            if (dependency_graph[i][j].count(WRITE_READ))
                cerr << "0";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(WRITE_WRITE))
                cerr << "1";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(READ_WRITE))
                cerr << "2";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(VERSION_SET_DEPEND))
                cerr << "3";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(OVERWRITE_DEPEND))
                cerr << "4";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(STRICT_START_DEPEND))
                cerr << "5";
            else
                cerr << " ";
        }
        cerr << "|" << endl;
    }
}

dependency_analyzer::dependency_analyzer(vector<stmt_output>& init_output,
                        vector<stmt_output>& total_output,
                        vector<int>& final_tid_queue,
                        vector<stmt_usage>& final_stmt_usage,
                        vector<txn_status>& final_txn_status,
                        int t_num,
                        int primary_key_idx,
                        int write_op_key_idx):
tid_num(t_num + 1),  // add 1 for init txn
tid_begin_idx(NULL),
tid_strict_begin_idx(NULL),
tid_end_idx(NULL), 
primary_key_index(primary_key_idx),
version_key_index(write_op_key_idx),
f_txn_status(final_txn_status),
f_txn_id_queue(final_tid_queue),
f_stmt_usage(final_stmt_usage),
f_stmt_output(total_output)
{   
    if (f_stmt_output.size() != f_txn_id_queue.size() || f_stmt_output.size() != f_stmt_usage.size()) {
        cerr << "dependency_analyzer: total_output, final_tid_queue and final_stmt_usage size are not equal" << endl;
        throw runtime_error("dependency_analyzer: total_output, final_tid_queue and final_stmt_usage size are not equal");
    }
    stmt_num = f_stmt_output.size();
    
    f_txn_status.push_back(TXN_COMMIT); // for init txn;

    for (int txn_id = 0; txn_id < tid_num; txn_id++) {
        int txn_stmt_num = 0;
        for (int i = 0; i < stmt_num; i++) {
            if (f_txn_id_queue[i] == txn_id)
                txn_stmt_num++;
        }
        f_txn_size.push_back(txn_stmt_num);
    }
    
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
            hash_to_output[hash] = row;
            operate_unit op(stmt_usage(AFTER_WRITE_READ, false), write_op_id, tid_num - 1, -1, row_id, hash);
            h.insert_to_history(op);
        }
    }
    
    for (int i = 0; i < stmt_num; i++) {
        auto& each_output = f_stmt_output[i];
        auto tid = f_txn_id_queue[i];
        auto stmt_u = f_stmt_usage[i];

        // do not analyze empty output select read;
        // write operation (insert, delete, update) will be analzye by using before/after-write read
        if (each_output.empty())
            continue;
        
        for (auto& row : each_output) {
            auto row_id = stoi(row[primary_key_idx]);
            auto write_op_id = stoi(row[write_op_key_idx]);
            auto hash = hash_output(row);
            hash_to_output[hash] = row;
            operate_unit op(stmt_u, write_op_id, tid, i, row_id, hash);
            h.insert_to_history(op);
        }
    }

    // first build instrument dependency, make sure that the instrument is correct
    build_stmt_instrument_dependency();

    // generate ww, wr, rw dependency
    for (auto& row_history : h.change_history) {
        auto& row_op_list = row_history.row_op_list;
        auto size = row_op_list.size();
        for (int i = 0; i < size; i++) {
            auto& op_unit = row_op_list[i];
            if (op_unit.tid == tid_num - 1) // init txn do not depend on others
                continue;
            if (op_unit.stmt_u == AFTER_WRITE_READ)
                continue;
            build_WR_dependency(row_op_list, i); // it is a read itself
            if (op_unit.stmt_u == BEFORE_WRITE_READ) {
                build_WW_dependency(row_op_list, i);
                build_RW_dependency(row_op_list, i);
            }
        }
    }

    // // generate start dependency (for snapshot)
    build_start_dependency();

    // build version_set depend and overwrite depend that should be build after start depend
    build_VS_dependency();
    build_OW_dependency();

    // generate stmt inner depend
    build_stmt_inner_dependency();
    
    // // print dependency graph
    // print_dependency_graph();
}

dependency_analyzer::~dependency_analyzer()
{
    delete[] tid_end_idx;
    delete[] tid_begin_idx;
    delete[] tid_strict_begin_idx;

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
            
            auto& dependencies = dependency_graph[j][i]; // j(abort) -> WR -> i(commit) [i wr depend on j]
            if (dependencies.count(WRITE_READ) > 0) {
                cerr << "abort txn: " << j << endl;
                cerr << "commit txn: " << i << endl;
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
    for (auto& rch : h.change_history) {
        auto& op_list = rch.row_op_list;
        auto opl_size = op_list.size();
        for (int i = 0; i < opl_size; i++) {
            if (op_list[i].stmt_u != AFTER_WRITE_READ)
                continue;
            
            int wop_id = op_list[i].write_op_id;
            int tid = op_list[i].tid;
            int txn_end_idx = tid_end_idx[tid];
            int other_read_idx = -1;
            int second_write_idx = -1;
            
            for (int j = i + 1; j < opl_size; j++) {
                if (op_list[j].stmt_idx > txn_end_idx)
                    break; // the later stmt will not contain the write from txn i
                
                // check whether the earlier version is read
                if (other_read_idx == -1 && 
                        op_list[j].write_op_id == wop_id &&
                        op_list[j].tid != tid)
                    other_read_idx = j;
                
                // check whether it will be rewrite by itself
                if (second_write_idx == -1 &&
                        op_list[j].tid == tid &&
                        op_list[j].stmt_u == BEFORE_WRITE_READ)
                    second_write_idx = j;
                
                if (other_read_idx >= 0 && second_write_idx >= 0) {
                    cerr << "first_write_idx: " << i << endl;
                    cerr << "tid: " << tid << endl;
                    cerr << "outpout: " << endl;
                    auto& first_write_row = hash_to_output[op_list[i].hash];
                    for (int e = 0; e < first_write_row.size(); e++)
                        cerr << first_write_row[e] << " ";
                    cerr << endl;
                    
                    cerr << "other_read_idx: " << other_read_idx << endl;
                    cerr << "tid: " << op_list[other_read_idx].tid << endl;
                    cerr << "outpout: " << endl;
                    auto& read_row = hash_to_output[op_list[other_read_idx].hash];
                    for (int e = 0; e < read_row.size(); e++)
                        cerr << read_row[e] << " ";
                    cerr << endl;

                    cerr << "second_write_idx: " << second_write_idx << endl;
                    cerr << "tid: " << op_list[second_write_idx].tid << endl;
                    
                    return true;
                }
                    
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

    return false;
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

    return false;
}

bool dependency_analyzer::check_G1c()
{
    set<dependency_type> ww_wr_set;
    ww_wr_set.insert(WRITE_WRITE);
    ww_wr_set.insert(WRITE_READ);

    auto tmp_dgraph = new int* [tid_num];
    for (int i = 0; i < tid_num; i++) 
        tmp_dgraph[i] = new int [tid_num];
    for (int i = 0; i < tid_num; i++) {
        for (int j = 0; j < tid_num; j++) {
            tmp_dgraph[i][j] = 0;
        }
    }
    
    // initialize tmp_dgraph
    for (int i = 0; i < tid_num; i++) {
        if (f_txn_status[i] != TXN_COMMIT)
            continue;
        for (int j = 0; j < tid_num; j++) {
            if (f_txn_status[j] != TXN_COMMIT)
                continue;
            set<dependency_type> res;
            set_intersection(ww_wr_set.begin(), ww_wr_set.end(), 
                    dependency_graph[i][j].begin(), dependency_graph[i][j].end(),
                    inserter(res, res.begin()));
            
            // have needed edges
            if (res.empty() == false)
                tmp_dgraph[i][j] = 1;
        }
    }

    reduce_graph_indegree(tmp_dgraph, tid_num);
    bool have_cycle = reduce_graph_outdegree(tmp_dgraph, tid_num);
    if (have_cycle) {
        cerr << "have cycle in G1c" << endl;
        for (int i = 0; i < tid_num; i++) {
            for (int j = 0; j < tid_num; j++) {
                if (tmp_dgraph[i][j] == 1)
                    cerr << i << " " << j << endl;
            }
        }
    }
    
    for (int i = 0; i < tid_num; i++)
        delete[] tmp_dgraph[i];
    delete[] tmp_dgraph;

    return have_cycle;
}

// G2-item: Item Anti-dependency Cycles. A history H exhibits phenomenon G2-item
// if DSG(H) contains a directed cycle having one or more item-anti-dependency edges.
bool dependency_analyzer::check_G2_item()
{
    set<dependency_type> ww_wr_rw_set;
    ww_wr_rw_set.insert(WRITE_WRITE);
    ww_wr_rw_set.insert(WRITE_READ);
    ww_wr_rw_set.insert(READ_WRITE);

    auto tmp_dgraph = new int* [tid_num];
    for (int i = 0; i < tid_num; i++) 
        tmp_dgraph[i] = new int [tid_num];
    for (int i = 0; i < tid_num; i++) {
        for (int j = 0; j < tid_num; j++) {
            tmp_dgraph[i][j] = 0;
        }
    }
    
    // initialize tmp_dgraph
    for (int i = 0; i < tid_num; i++) {
        if (f_txn_status[i] != TXN_COMMIT)
            continue;
        for (int j = 0; j < tid_num; j++) {
            if (f_txn_status[j] != TXN_COMMIT)
                continue;
            set<dependency_type> res;
            set_intersection(ww_wr_rw_set.begin(), ww_wr_rw_set.end(), 
                    dependency_graph[i][j].begin(), dependency_graph[i][j].end(),
                    inserter(res, res.begin()));
            
            // have needed edges
            if (res.empty() == false)
                tmp_dgraph[i][j] = 1;
        }
    }

    reduce_graph_indegree(tmp_dgraph, tid_num);
    bool have_cycle = reduce_graph_outdegree(tmp_dgraph, tid_num);
    if (have_cycle) {
        cerr << "have cycle in G2_item" << endl;
        for (int i = 0; i < tid_num; i++) {
            for (int j = 0; j < tid_num; j++) {
                if (tmp_dgraph[i][j] == 1) {
                    cerr << i << " " << j << ": ";
                    for (auto& dependency:dependency_graph[i][j])
                        cerr << dependency << " ";
                    cerr << endl;
                }
            }
        }
    }
    
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
            if (dependency_graph[i][j].count(START_DEPEND) == 0) {
                cerr << "txn i: " << i <<endl;
                cerr << "txn j: " << j << endl;
                return true;
            }
                
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
    target_dependency_set.insert(STRICT_START_DEPEND);
    
    auto tmp_dgraph = new int* [tid_num];
    for (int i = 0; i < tid_num; i++) 
        tmp_dgraph[i] = new int [tid_num];
    for (int i = 0; i < tid_num; i++) {
        for (int j = 0; j < tid_num; j++) {
            tmp_dgraph[i][j] = 0;
        }
    }
    
    // initialize tmp_dgraph
    for (int i = 0; i < tid_num; i++) {
        if (f_txn_status[i] != TXN_COMMIT)
            continue;
        for (int j = 0; j < tid_num; j++) {
            if (f_txn_status[j] != TXN_COMMIT)
                continue;
            set<dependency_type> res;
            set_intersection(target_dependency_set.begin(), target_dependency_set.end(), 
                    dependency_graph[i][j].begin(), dependency_graph[i][j].end(),
                    inserter(res, res.begin()));
            
            // have needed edges
            if (res.empty() == false)
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

    if (has_rw_cycle) {
        cerr << "have cycle in GSIb" << endl;
        for (int i = 0; i < tid_num; i++) {
            for (int j = 0; j < tid_num; j++) {
                if (tmp_dgraph[i][j] == 1) {
                    cerr << i << " " << j << ": ";
                    for (auto& dependency:dependency_graph[i][j])
                        cerr << dependency << " ";
                    cerr << endl;
                }
            }
        }
    }

    for (int i = 0; i < tid_num; i++)
        delete[] tmp_dgraph[i];
    delete[] tmp_dgraph;

    return has_rw_cycle;
}

// stmt_dist_graph may have cycle
vector<stmt_id> dependency_analyzer::longest_stmt_path(
    map<pair<stmt_id, stmt_id>, int>& stmt_dist_graph)
{
    map<stmt_id, stmt_id> dad_stmt;
    map<stmt_id, int> dist_length;
    set<stmt_id> real_deleted_node; // to delete cycle
    for (int i = 0; i < stmt_num; i++) {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        dist_length[stmt_i] = 0;
    }
    set<stmt_id> all_stmt_set;
    for (int i = 0; i < stmt_num; i++) {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        all_stmt_set.insert(stmt_i);
    }

    auto tmp_stmt_graph = stmt_dist_graph;
    set<stmt_id> delete_node;
    while (delete_node.size() + real_deleted_node.size() < stmt_num) {
        int zero_indegree_idx = -1;
        // --- find zero-indegree statement ---
        for (int i = 0; i < stmt_num; i++) {
            auto stmt_i = stmt_id(f_txn_id_queue, i);
            if (delete_node.count(stmt_i) > 0) // has been deleted from tmp_stmt_graph
                continue;
            if (real_deleted_node.count(stmt_i) > 0) // // has been really deleted (for decycle)
                continue;
            bool has_indegree = false;
            for (int j = 0; j < stmt_num; j++) {
                auto stmt_j = stmt_id(f_txn_id_queue, j);
                if (tmp_stmt_graph.count(make_pair(stmt_j, stmt_i)) > 0) {
                    has_indegree = true;
                    break;
                }
            }
            if (has_indegree == false) {
                zero_indegree_idx = i;
                break;
            }
        }
        // ------------------------------------
        
        // if do not has zero-indegree statement, so there is a cycle
        if (zero_indegree_idx == -1) {
            // cerr << "There is a cycle in longest_stmt_path(), delete one node: ";
            // select one node to delete
            auto tmp_stmt_set = all_stmt_set;
            for (auto& node : delete_node) 
                tmp_stmt_set.erase(node);
            for (auto& node : real_deleted_node)
                tmp_stmt_set.erase(node);
            auto r = rand() % tmp_stmt_set.size();
            auto select_one_it = tmp_stmt_set.begin();
            advance(select_one_it, r);

            // delete its set (version_set, before_read, itself, after_read)
            auto select_stmt_id = *select_one_it;
            auto select_queue_idx = select_stmt_id.transfer_2_stmt_idx(f_txn_id_queue);
            auto select_idx_set = get_instrumented_stmt_set(select_queue_idx);
            for (auto chosen_idx : select_idx_set) {
                auto chosen_stmt_id = stmt_id(f_txn_id_queue, chosen_idx);
                real_deleted_node.insert(chosen_stmt_id);
                for (int i = 0; i < stmt_num; i++) {
                    auto out_branch = make_pair(chosen_stmt_id, stmt_id(f_txn_id_queue, i));
                    auto in_branch = make_pair(stmt_id(f_txn_id_queue, i), chosen_stmt_id);
                    tmp_stmt_graph.erase(out_branch);
                    tmp_stmt_graph.erase(in_branch);
                }
                // cerr << chosen_stmt_id.txn_id << "." << chosen_stmt_id.stmt_idx_in_txn << ", ";
            }
            // cerr << endl;
            continue;
        }
        // ------------------------------------
        
        // if do has zero-indegree statement
        int cur_max_length = 0;
        stmt_id cur_max_dad;
        auto stmt_zero_idx = stmt_id(f_txn_id_queue, zero_indegree_idx);
        for (int i = 0; i < stmt_num; i++) {
            auto stmt_i = stmt_id(f_txn_id_queue, i);
            if (real_deleted_node.count(stmt_i) > 0) // // has been really deleted (for decycle)
                continue;
            auto branch = make_pair(stmt_i, stmt_zero_idx);
            if (stmt_dist_graph.count(branch) == 0)
                continue;
            if (dist_length[stmt_i] + stmt_dist_graph[branch] > cur_max_length) {
                cur_max_length = dist_length[stmt_i] + stmt_dist_graph[branch];
                cur_max_dad = stmt_i;
            }
        }
        dist_length[stmt_zero_idx] = cur_max_length;
        dad_stmt[stmt_zero_idx] = cur_max_dad; // the first one is (-1, -1): no dad

        delete_node.insert(stmt_zero_idx);
        for (int j = 0; j < stmt_num; j++) {
            auto branch = make_pair(stmt_zero_idx, stmt_id(f_txn_id_queue, j));
            tmp_stmt_graph.erase(branch);
        }
    }

    vector<stmt_id> longest_path;
    int longest_dist = 0;
    stmt_id longest_dist_stmt;
    for (int i = 0; i < stmt_num; i++) {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        if (real_deleted_node.count(stmt_i) > 0) // // has been really deleted (for decycle)
                continue;
        auto path_length = dist_length[stmt_i];
        if (path_length > longest_dist) {
            longest_dist = path_length;
            longest_dist_stmt = stmt_i;
        }
    }

    while (longest_dist_stmt.txn_id != -1) { // default
        longest_path.insert(longest_path.begin(), longest_dist_stmt);
        longest_dist_stmt = dad_stmt[longest_dist_stmt];
    }

    cerr << "stmt path length: " << longest_dist << endl;
    return longest_path;
}

vector<stmt_id> dependency_analyzer::longest_stmt_path()
{
    map<pair<stmt_id, stmt_id>, int> stmt_dist_graph;
    for (int i = 0; i < stmt_num; i++) {
        if (f_txn_status[f_txn_id_queue[i]] != TXN_COMMIT)
            continue;
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        for (int j = 0; j < stmt_num; j++) {
            if (f_txn_status[f_txn_id_queue[j]] != TXN_COMMIT)
                continue;
            auto stmt_j = stmt_id(f_txn_id_queue, j);
            auto branch = make_pair(stmt_i, stmt_j);
            if (stmt_dependency_graph.count(branch) == 0)
                continue;
            auto depend_set = stmt_dependency_graph[branch];
            depend_set.erase(START_DEPEND);
            depend_set.erase(INSTRUMENT_DEPEND);
            if (depend_set.empty()) 
                continue;
            if (depend_set.count(INNER_DEPEND) > 0 && depend_set.size() == 1)
                stmt_dist_graph[branch] = 1;
            else if (depend_set.count(STRICT_START_DEPEND) > 0 && depend_set.size() == 1)
                stmt_dist_graph[branch] = 10;
            else if (depend_set.count(STRICT_START_DEPEND) > 0 || depend_set.count(INNER_DEPEND) > 0)
                stmt_dist_graph[branch] = 100; // contain STRICT_START_DEPEND or INNER_DEPEND, and other
            else if (depend_set.count(WRITE_WRITE) > 0 || 
                     depend_set.count(WRITE_READ) > 0)
                stmt_dist_graph[branch] = 100000; // contain WRITE_READ or WRITE_WRITE, but do not contain start and inner
            else if (depend_set.count(VERSION_SET_DEPEND) > 0 || 
                     depend_set.count(OVERWRITE_DEPEND) > 0 ||
                     depend_set.count(READ_WRITE) > 0)
                stmt_dist_graph[branch] = 10000; // only contain VERSION_SET_DEPEND, OVERWRITE_DEPEND and READ_WRITE
        }
    }

    auto path = longest_stmt_path(stmt_dist_graph);
    auto path_size = path.size();
    for (int i = 0; i < path_size; i++) {
        auto txn_id = path[i].txn_id;
        auto stmt_pos = path[i].stmt_idx_in_txn;
        if (stmt_pos != 0 && f_txn_size[txn_id] != stmt_pos + 1)
            continue;
        // if it is the first one (begin), or last one (commit), delete it
        path.erase(path.begin() + i);
        path_size--;
        i--;
    }

    // delete replaced stmt
    for (int i = 0; i < path_size; i++) {
        auto queue_idx = path[i].transfer_2_stmt_idx(f_txn_id_queue);
        if (f_stmt_usage[queue_idx] != INIT_TYPE)
            continue;
        path.erase(path.begin() + i);
        path_size--;
        i--;
    }

    return path;
}

vector<stmt_id> dependency_analyzer::topological_sort_path(set<stmt_id> deleted_nodes)
{
    vector<stmt_id> path;
    auto tmp_stmt_dependency_graph = stmt_dependency_graph;
    set<stmt_id> outputted_node; // the node that has been outputted from graph
    set<stmt_id> all_stmt_set; // record all stmts in the graph 
    for (int i = 0; i < stmt_num; i++) {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        all_stmt_set.insert(stmt_i);
    }

    // deleted_nodes include: 
    //  1) the nodes that have been deleted for decycle, 
    //  2) the nodes in abort stmt
    //  3) the nodes that have been deleted in transaction_test::multi_stmt_round_test
    // delete node that in abort txn
    for (int i = 0; i < stmt_num; i++) {
        auto txn_id = f_txn_id_queue[i];
        if (f_txn_status[txn_id] == TXN_COMMIT)
            continue;
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        deleted_nodes.insert(stmt_i); 
        for (int j = 0; j < stmt_num; j++) {
            auto stmt_j = stmt_id(f_txn_id_queue, j);
            auto out_branch = make_pair(stmt_i, stmt_j);
            auto in_branch = make_pair(stmt_j, stmt_i);
            tmp_stmt_dependency_graph.erase(out_branch);
            tmp_stmt_dependency_graph.erase(in_branch);
        }
    }

    while (outputted_node.size() + deleted_nodes.size() < stmt_num) {
        int zero_indegree_idx = -1;
        
        // --- find zero-indegree statement ---
        for (int i = stmt_num - 1; i >= 0; i--) { // use reverse order as possible
            auto stmt_i = stmt_id(f_txn_id_queue, i);
            if (outputted_node.count(stmt_i) > 0) // has been outputted from tmp_stmt_graph
                continue;
            if (deleted_nodes.count(stmt_i) > 0) // has been really deleted (for decycle)
                continue;
            bool has_indegree = false;
            // check whether the node has indegree
            for (int j = 0; j < stmt_num; j++) {
                if (i == j)
                    continue;
                auto stmt_j = stmt_id(f_txn_id_queue, j);
                if (tmp_stmt_dependency_graph.count(make_pair(stmt_j, stmt_i)) > 0) {
                    has_indegree = true;
                    break;
                }
            }
            if (has_indegree == false) {
                zero_indegree_idx = i;
                break;
            }
        }
        // ------------------------------------
        
        // if do not has zero-indegree statement, so there is a cycle
        if (zero_indegree_idx == -1) {
            // cerr << "There is a cycle in topological_sort_path(), delete one node: ";
            // select one node to delete
            auto tmp_stmt_set = all_stmt_set;
            for (auto& node : outputted_node) // cannot delete outputted node
                tmp_stmt_set.erase(node);
            for (auto& node : deleted_nodes) // skip the node that has been deleted
                tmp_stmt_set.erase(node);
            auto r = rand() % tmp_stmt_set.size();
            auto select_one_it = tmp_stmt_set.begin();
            advance(select_one_it, r);

            // delete its set (version_set, before_read, itself, after_read)
            auto select_stmt_id = *select_one_it;
            auto select_queue_idx = select_stmt_id.transfer_2_stmt_idx(f_txn_id_queue);
            auto select_idx_set = get_instrumented_stmt_set(select_queue_idx);
            for (auto chosen_idx : select_idx_set) {
                auto chosen_stmt_id = stmt_id(f_txn_id_queue, chosen_idx);
                deleted_nodes.insert(chosen_stmt_id);
                for (int i = 0; i < stmt_num; i++) {
                    auto out_branch = make_pair(chosen_stmt_id, stmt_id(f_txn_id_queue, i));
                    auto in_branch = make_pair(stmt_id(f_txn_id_queue, i), chosen_stmt_id);
                    tmp_stmt_dependency_graph.erase(out_branch);
                    tmp_stmt_dependency_graph.erase(in_branch);
                }
                // cerr << chosen_stmt_id.txn_id << "." << chosen_stmt_id.stmt_idx_in_txn << ", ";
            }
            // cerr << endl;
            continue;
        }
        // ------------------------------------
        
        // if do has zero-indegree statement, push the stmt to the path
        auto stmt_zero_idx = stmt_id(f_txn_id_queue, zero_indegree_idx);
        path.push_back(stmt_zero_idx);
        
        // mark the outputted node, and delete its edges.
        outputted_node.insert(stmt_zero_idx);
        for (int j = 0; j < stmt_num; j++) {
            auto branch = make_pair(stmt_zero_idx, stmt_id(f_txn_id_queue, j));
            tmp_stmt_dependency_graph.erase(branch);
        }
    }

    auto path_size = path.size();
    // delete begin stmts and commit/abort stmts
    for (int i = 0; i < path_size; i++) {
        auto txn_id = path[i].txn_id;
        auto stmt_pos = path[i].stmt_idx_in_txn;
        if (stmt_pos != 0 && f_txn_size[txn_id] != stmt_pos + 1)
            continue;
        // if it is the first one (begin), or last one (commit), delete it
        path.erase(path.begin() + i);
        path_size--;
        i--;
    }

    // delete replaced stmts
    for (int i = 0; i < path_size; i++) {
        auto queue_idx = path[i].transfer_2_stmt_idx(f_txn_id_queue);
        if (f_stmt_usage[queue_idx] != INIT_TYPE)
            continue;
        path.erase(path.begin() + i);
        path_size--;
        i--;
    }

    return path;
}