#include "instrumentor.hh"

string print_stmt_to_string(shared_ptr<prod> stmt);

set<string> extract_words_begin_with(const string str, const string begin_str)
{
    set<string> words;
    auto pos = str.find(begin_str, 0);
    while (pos != string::npos) {
        if (pos >= 1 &&
                str[pos - 1] != '(' &&
                str[pos - 1] != ' ' && 
                str[pos - 1] != '\n') { // not begin
            pos = str.find(begin_str, pos + 1);
            continue;
        }

        // find the interval
        auto interval_pos = pos + 1;
        while (interval_pos < str.size() &&
                str[interval_pos] != ')' && 
                str[interval_pos] != '.' &&  
                str[interval_pos] != ' ' && 
                str[interval_pos] != '\n' &&
                str[interval_pos] != ';') // not end
            interval_pos++;
        
        auto word = str.substr(pos, interval_pos - pos);
        words.insert(word);
        
        if (interval_pos == str.size()) // the last one
            return words;

        pos = str.find(begin_str, interval_pos + 1);
    }
    return words;
}

instrumentor::instrumentor(vector<shared_ptr<prod>>& stmt_queue,
                            vector<int>& tid_queue,
                            shared_ptr<schema> db_schema)
{
    cerr << "instrumenting the statement ... ";
    int stmt_num = stmt_queue.size();
    scope used_scope;
    db_schema->fill_scope(used_scope);

    for (int i = 0; i < stmt_num; i++) {
        auto stmt = stmt_queue[i];
        auto tid = tid_queue[i];

        auto update_statement = dynamic_pointer_cast<update_stmt>(stmt);
        if (update_statement) { // is a update statement
            used_scope.new_stmt(); // for before_write_select_stmt
            auto before_write_select_stmt = make_shared<query_spec>((struct prod *)0, &used_scope, 
                                    update_statement->victim, update_statement->search);
            
            used_scope.new_stmt(); // for after_write_select_stmt
            // get wkey idex
            auto table = update_statement->victim;
            int wkey_idx = -1;
            auto& columns = table->columns();
            int t_size = columns.size();
            for (int i = 0; i < t_size; i++) {
                if (columns[i].name == "wkey") {
                    wkey_idx = i;
                    break;
                }
            }
            if (wkey_idx == -1) {
                cerr << "problem stmt:\n" << print_stmt_to_string(update_statement) << endl;
                throw runtime_error("intrument update statement: cannot find wkey");
            }
            
            // get wkey value
            int wkey_set_idx = -1;
            auto names_size = update_statement->set_list->names.size();
            for (int i = 0; i < names_size; i++) {
                if (update_statement->set_list->names[i] == "wkey") {
                    wkey_set_idx = i;
                    break;
                }
            }
            if (wkey_set_idx == -1)
                throw runtime_error("intrument update statement: cannot find wkey = expr");

            auto wkey_value = update_statement->set_list->value_exprs[wkey_set_idx];

            // init compare op
            op *equal_op = NULL;
            for (auto& op : db_schema->operators) {
                if (op.name == "=") {
                    equal_op = &op;
                    break;
                }
            }
            if (equal_op == NULL) 
                throw runtime_error("intrument update statement: cannot find = operator");
            
            // init column reference
            auto wkey_column = make_shared<column_reference>((struct prod *)0, columns[wkey_idx].type, columns[wkey_idx].name, table->name);
            // init the select
            auto after_write_select_stmt = make_shared<query_spec>((struct prod *)0, &used_scope, table, equal_op, wkey_column, wkey_value);
            
            /*---- version_set select (select * from t where 1=1) ---*/
            auto involved_tables = extract_words_begin_with(print_stmt_to_string(stmt), "t_");
            for (auto& table_str:involved_tables) {
                auto version_set_select_stmt = make_shared<txn_string_stmt>((prod *)0, "SELECT * FROM " + table_str);
                final_tid_queue.push_back(tid); // version_set select, build predicate-WW
                final_stmt_queue.push_back(version_set_select_stmt);
                final_stmt_usage.push_back(stmt_usage(VERSION_SET_READ, true, table_str));
            }
            
            final_tid_queue.push_back(tid); // get the ealier value to build RW and WW dependency
            final_tid_queue.push_back(tid);
            final_tid_queue.push_back(tid); // get the changed value for later WW and WR dependency

            final_stmt_queue.push_back(before_write_select_stmt);
            final_stmt_queue.push_back(stmt);
            final_stmt_queue.push_back(after_write_select_stmt);

            auto target_table_str = table->ident();
            final_stmt_usage.push_back(stmt_usage(BEFORE_WRITE_READ, true, target_table_str));
            final_stmt_usage.push_back(stmt_usage(UPDATE_WRITE, false, target_table_str));
            final_stmt_usage.push_back(stmt_usage(AFTER_WRITE_READ, true, target_table_str));
            
            continue;
        }

        auto delete_statement = dynamic_pointer_cast<delete_stmt>(stmt);
        if (delete_statement) {
            used_scope.new_stmt(); // for select_stmt
            auto select_stmt = make_shared<query_spec>((struct prod *)0, &used_scope,
                                    delete_statement->victim, delete_statement->search);
            
            /*---- version_set select (select * from t where 1=1) ---*/
            auto involved_tables = extract_words_begin_with(print_stmt_to_string(stmt), "t_");
            for (auto& table_str:involved_tables) {
                auto version_set_select_stmt = make_shared<txn_string_stmt>((prod *)0, "SELECT * FROM " + table_str);
                final_tid_queue.push_back(tid); // version_set select, build predicate-WW
                final_stmt_queue.push_back(version_set_select_stmt);
                final_stmt_usage.push_back(stmt_usage(VERSION_SET_READ, true, table_str));
            }

            final_tid_queue.push_back(tid); // get the ealier value to build RW and WW dependency
            final_tid_queue.push_back(tid);
            // item is deleted, so no need for later dependency
            
            final_stmt_queue.push_back(select_stmt);
            final_stmt_queue.push_back(stmt);

            auto target_table_str = delete_statement->victim->ident();
            final_stmt_usage.push_back(stmt_usage(BEFORE_WRITE_READ, true, target_table_str));
            final_stmt_usage.push_back(stmt_usage(DELETE_WRITE, false, target_table_str));

            continue;
        }

        auto insert_statement = dynamic_pointer_cast<insert_stmt>(stmt);
        if (insert_statement) {
            used_scope.new_stmt(); // for select_stmt
            // get wkey idex
            auto table = insert_statement->victim;
            int wkey_idx = -1;
            auto& columns = table->columns();
            int t_size = columns.size();
            for (int i = 0; i < t_size; i++) {
                if (columns[i].name == "wkey") {
                    wkey_idx = i;
                    break;
                }
            }
            if (wkey_idx == -1) {
                cerr << "problem stmt:\n" << print_stmt_to_string(insert_statement) << endl;
                throw runtime_error("intrument insert statement: cannot find wkey");
            }

            // get wkey value
            auto& items = insert_statement->value_exprs_vector.front();
            auto wkey_value = items[wkey_idx];

            // init compare op
            op *equal_op = NULL;
            for (auto& op : db_schema->operators) {
                if (op.name == "=") {
                    equal_op = &op;
                    break;
                }
            }
            if (equal_op == NULL) 
                throw runtime_error("intrument insert statement: cannot find = operator");
            
            // init column reference
            auto wkey_column = make_shared<column_reference>((struct prod *)0, columns[wkey_idx].type, columns[wkey_idx].name, table->name);
            // init the select
            auto select_stmt = make_shared<query_spec>((struct prod *)0, &used_scope, table, equal_op, wkey_column, wkey_value);
            
            /*---- version_set select (select * from t where 1=1) ---*/
            // the inserted value may be determined by other table's row.
            auto involved_tables = extract_words_begin_with(print_stmt_to_string(stmt), "t_");
            involved_tables.erase(table->ident());
            for (auto& table_str:involved_tables) {
                auto version_set_select_stmt = make_shared<txn_string_stmt>((prod *)0, "SELECT * FROM " + table_str);
                final_tid_queue.push_back(tid); // version_set select, build predicate-WW
                final_stmt_queue.push_back(version_set_select_stmt);
                final_stmt_usage.push_back(stmt_usage(VERSION_SET_READ, true, table_str));
            }
            
            // item does not exist, so no need for ealier dependency
            final_tid_queue.push_back(tid);
            final_tid_queue.push_back(tid); // get the later value to build WR and WW dependency

            final_stmt_queue.push_back(stmt);
            final_stmt_queue.push_back(select_stmt);

            auto target_table_str = table->ident();
            final_stmt_usage.push_back(stmt_usage(INSERT_WRITE, false, target_table_str));
            final_stmt_usage.push_back(stmt_usage(AFTER_WRITE_READ, true, target_table_str));

            continue;
        }

        auto string_stmt = dynamic_pointer_cast<txn_string_stmt>(stmt);
        // begin, commit, abort, SELECT 1 WHERE 0 <> 0, but should not include SELECT * FROM t
        if (string_stmt && print_stmt_to_string(string_stmt).find("SELECT * FROM") == string::npos) { 
            final_tid_queue.push_back(tid);
            final_stmt_queue.push_back(stmt);
            final_stmt_usage.push_back(stmt_usage(INIT_TYPE, false));
            continue;
        }

        // normal select (with cte) query
        auto involved_tables = extract_words_begin_with(print_stmt_to_string(stmt), "t_");
        for (auto& table_str:involved_tables) {
            auto version_set_select_stmt = make_shared<txn_string_stmt>((prod *)0, "SELECT * FROM " + table_str);
            final_tid_queue.push_back(tid); // version_set select, build predicate-WW
            final_stmt_queue.push_back(version_set_select_stmt);
            final_stmt_usage.push_back(stmt_usage(VERSION_SET_READ, true, table_str));
        }
        final_tid_queue.push_back(tid);
        final_stmt_queue.push_back(stmt);
        final_stmt_usage.push_back(stmt_usage(SELECT_READ, false));
    }

    cerr << "done" << endl;
}