#include "instrumentor.hh"

instrumentor::instrumentor(vector<shared_ptr<prod>>& stmt_queue,
                            vector<int>& tid_queue,
                            shared_ptr<schema> db_schema)
{
    int stmt_num = stmt_queue.size();
    scope used_scope;
    db_schema->fill_scope(used_scope);

    for (int i = 0; i < stmt_num; i++) {
        auto stmt = stmt_queue[i];
        auto tid = tid_queue[i];

        auto update_statement = dynamic_pointer_cast<update_stmt>(stmt);
        if (update_statement) { // is a update statement
            auto before_write_select_stmt = make_shared<query_spec>((struct prod *)0, &used_scope, 
                                    update_statement->victim, update_statement->search);
            
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
            if (wkey_idx == -1)
                throw runtime_error("intrument update statement: cannot find wkey");
            
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
                throw runtime_error("intrument insert statement: cannot find = operator");
            
            // init column reference
            auto wkey_column = make_shared<column_reference>((struct prod *)0, columns[wkey_idx].type, columns[wkey_idx].name, table);
            // init where clause
            auto where_search = make_shared<comparison_op>((struct prod *)0, equal_op, wkey_column, wkey_value);
            // init the select
            auto after_write_select_stmt = make_shared<query_spec>((struct prod *)0, used_scope,
                                    table, where_search);

            final_tid_queue.push_back(tid); // get the ealier value to build RW and WW dependency
            final_tid_queue.push_back(tid);
            final_tid_queue.push_back(tid); // get the changed value for later WW and WR dependency

            final_stmt_queue.push_back(before_write_select_stmt);
            final_stmt_queue.push_back(stmt);
            final_stmt_queue.push_back(after_write_select_stmt);

            final_stmt_usage.push_back(BEFORE_WRITE_READ);
            final_stmt_usage.push_back(NORMAL);
            final_stmt_usage.push_back(AFTER_WRITE_READ);
            
            continue;
        }

        auto delete_statement = dynamic_pointer_cast<delete_stmt>(stmt);
        if (delete_statement) {
            auto select_stmt = make_shared<query_spec>((struct prod *)0, &used_scope,
                                    delete_statement->victim, delete_statement->search);
            final_tid_queue.push_back(tid); // get the ealier value to build RW and WW dependency
            final_tid_queue.push_back(tid);
            // item is deleted, so no need for later dependency

            final_stmt_queue.push_back(select_stmt);
            final_stmt_queue.push_back(stmt);

            final_stmt_usage.push_back(BEFORE_WRITE_READ);
            final_stmt_usage.push_back(NORMAL);

            continue;
        }

        auto insert_statement = dynamic_pointer_cast<insert_stmt>(stmt);
        if (insert_statement) {
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
            if (wkey_idx == -1)
                throw runtime_error("intrument insert statement: cannot find wkey");
            
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
            auto wkey_column = make_shared<column_reference>((struct prod *)0, columns[wkey_idx].type, columns[wkey_idx].name, table);
            // init where clause
            auto where_search = make_shared<comparison_op>((struct prod *)0, equal_op, wkey_column, wkey_value);
            // init the select
            auto select_stmt = make_shared<query_spec>((struct prod *)0, used_scope,
                                    table, where_search);
            
            // item does not exist, so no need for ealier dependency
            final_tid_queue.push_back(tid);
            final_tid_queue.push_back(tid); // get the later value to build WR and WW dependency

            final_stmt_queue.push_back(stmt);
            final_stmt_queue.push_back(select_stmt);

            final_stmt_usage.push_back(NORMAL);
            final_stmt_usage.push_back(AFTER_WRITE_READ);

            continue;
        }


    }
}