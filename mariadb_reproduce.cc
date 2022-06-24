/* This program is used to check whether the same transaction query will produce different result */

#include <mysql/mysql.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <sys/time.h> // for gettimeofday
#include <set>

using namespace std;

#define MYSQL_STMT_BLOCK_MS 100

static unsigned long long get_cur_time_ms(void) {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

void read_stmt_tid_file(string stmt_file_name, string tid_file_name, vector<string>& stmt_queue, vector<int>& tid_queue)
{
    // read the stmts
    ifstream stmt_file(stmt_file_name);
    stringstream buffer;
    buffer << stmt_file.rdbuf();
    stmt_file.close();
    string stmts(buffer.str());
    int old_off = 0;
    while (1) {
        int new_off = stmts.find(";\n\n", old_off);
        if (new_off == string::npos) 
            break;
        
        auto each_sql = stmts.substr(old_off, new_off - old_off); // not include ;\n\n
        old_off = new_off + string(";\n\n").size();

        stmt_queue.push_back(each_sql + ";");
    }

    // read the txn id
    ifstream tid_file(tid_file_name);
    int tid;
    while (tid_file >> tid) 
        tid_queue.push_back(tid);
    tid_file.close();
}

void connect_and_use_testdb(MYSQL& handler)
{
    MYSQL_RES *res;

    if (!mysql_init(&handler))
        throw std::runtime_error("Cannot init handler!!");

    if (!mysql_real_connect(&handler, "127.0.0.1", "root", NULL, NULL, 3306, NULL, 0))
        throw runtime_error("Cannot connect to local server!!");
        
    string create_testdb = "create database if not exists testdb;";
    if (mysql_real_query(&handler, create_testdb.c_str(), create_testdb.size()))
        throw runtime_error("Cannot create database testdb!!");
    res = mysql_store_result(&handler);
    mysql_free_result(res);
    
    string use_testdb = "use database testdb";
    if (mysql_real_query(&handler, use_testdb.c_str(), use_testdb.size()))
        throw runtime_error("Cannot use database testdb!!");
    res = mysql_store_result(&handler);
    mysql_free_result(res);

    return;
}

bool check_whether_block(MYSQL& check_handler, MYSQL& exec_handler)
{
    auto checked_thread_id = mysql_thread_id(&exec_handler);
    string get_block_tid = "SELECT waiting_pid FROM sys.innodb_lock_waits;";
    vector<string> output;
    
    mysql_real_query(&check_handler, get_block_tid.c_str(), get_block_tid.size());
    auto result = mysql_store_result(&check_handler);
    if (result) {
        auto column_num = mysql_num_fields(result);
        while (auto row = mysql_fetch_row(result)) {
            for (int i = 0; i < column_num; i++) {
                string str;
                if (row[i] == NULL)
                    str = "NULL";
                else
                    str = row[i];
                output.push_back(str);
            }
            output.push_back("\n");
        }
    }
    mysql_free_result(result);

    // check output
    string tid_str = to_string(checked_thread_id);
    auto output_size = output.size();
    for (int i = 0; i < output_size; i++) {
        if (tid_str == output[i])
            return true;
    }

    return false;
}

// true if the stmt is executed
// false if the stmt is blocked
bool test_one_stmt(MYSQL& exec_handler, MYSQL& check_handler, string& stmt, vector<vector<string>>& stmt_output, int& query_status)
{
    stmt_output.clear();
    int err;
    if (query_status == 0) {
        query_status = mysql_real_query_start(&err, &exec_handler, stmt.c_str(), stmt.size());
        if (mysql_errno(&exec_handler) != 0) {
            string err = mysql_error(&exec_handler);
            throw std::runtime_error("mysql_real_query_start fails: " + err); 
        }
    }

    auto begin_time = get_cur_time_ms();
    while (1) {
        query_status = mysql_real_query_cont(&err, &exec_handler, query_status);
        if (mysql_errno(&exec_handler) != 0) {
            string err = mysql_error(&exec_handler);
            throw std::runtime_error("mysql_real_query_cont fails: " + err); 
        }
        if (query_status == 0) 
                break;
        
        auto cur_time = get_cur_time_ms();
        if (cur_time - begin_time >= MYSQL_STMT_BLOCK_MS) {
            auto blocked = check_whether_block(check_handler, exec_handler);
            if (blocked == true)
                return false;
            begin_time = cur_time;
        }
    }

    auto result = mysql_store_result(&exec_handler);
    if (mysql_errno(&exec_handler) != 0) {
        string err = mysql_error(&exec_handler);
        throw std::runtime_error("mysql_store_result fails: " + err); 
    }
    
    if (result) {
        auto column_num = mysql_num_fields(result);
        while (auto row = mysql_fetch_row(result)) {
            vector<string> row_output;
            for (int i = 0; i < column_num; i++) {
                string str;
                if (row[i] == NULL)
                    str = "NULL";
                else
                    str = row[i];
                row_output.push_back(str);
            }
            stmt_output.push_back(row_output);
        }
    }
    mysql_free_result(result);
    return true;
}

void retry_block_queue(MYSQL* handlers, MYSQL& check_handler, int* query_status,
                    vector<string>& stmt_queue,
                    vector<int>& tid_queue,
                    int cur_stmt_pos,
                    int *status_queue,
                    int *txn_block_flag)
{
    // firstly try the first stmt of each blocked transaction
    set<int> first_tried_tid;
    for (int i = 0; i < cur_stmt_pos; i++) {
        if (status_queue[i] == 1) // the stmt is executed
            continue;
        
        auto tid = tid_queue[i];
        if (txn_block_flag[tid] == 0)
            continue;

        if (first_tried_tid.count(tid) != 0) // have tried
            continue;
        
        first_tried_tid.insert(tid);
        vector<vector<string>> stmt_output;
        auto is_executed = test_one_stmt(handlers[tid], check_handler, stmt_queue[i], stmt_output, query_status[tid]);
        if (is_executed == true) { // executed
            txn_block_flag[tid] = 0;
            status_queue[i] = 1;
        } else {// blocked
            txn_block_flag[tid] = 1;
        }
    }
    
    for (int stmt_pos = 0; stmt_pos < cur_stmt_pos; stmt_pos++) {
        auto tid = tid_queue[stmt_pos];
        // skip the tried but still blocked transaction
        if (txn_block_flag[tid] == 1)
            continue;
        
        // skip the executed stmt
        if (status_queue[stmt_pos] == 1)
            continue;

        vector<vector<string>> stmt_output;
        auto is_executed = test_one_stmt(handlers[tid], check_handler, stmt_queue[stmt_pos], stmt_output, query_status[tid]);
        // successfully execute the stmt, so label as not blocked
        if (is_executed == 1) {
            txn_block_flag[tid] = 0;
            status_queue[stmt_pos] = 1;
            
            // if it is a commit or rollback stmt, check whether the blocked txn is still blocked
            if (stmt_queue[stmt_pos].find("COMMIT") != string::npos || stmt_queue[stmt_pos].find("ROLLBACK") != string::npos)
                retry_block_queue(handlers, check_handler, query_status, stmt_queue, tid_queue, stmt_pos, status_queue, txn_block_flag);
        }
        else { // still blocked
            txn_block_flag[tid] = 1;
        }
    }
}

int main(int argc, char **argv)
{
    cerr << "enter reproduce mode" << endl;
    vector<string> stmt_queue;
    vector<int> tid_queue;

    read_stmt_tid_file("stmt.txt", "tid.txt", stmt_queue, tid_queue);

    auto queue_size = tid_queue.size();
    int max_tid = -1;
    for (int i = 0; i < queue_size; i++) {
        if (tid_queue[i] > max_tid)
            max_tid = tid_queue[i];
    }
    
    int txn_num = max_tid + 1;
    MYSQL handlers[txn_num];
    MYSQL check_handler;
    int query_status[txn_num];
    int test_time = 10;
    vector<int> txn_ouput_size[test_time];
    
    // test dbms
    for (int k = 0; k < test_time; k++) { // repeat the test k times
        // reset to backup
        string mysql_source = "/usr/local/mysql/bin/mysql -uroot -Dtestdb < mysql_bk.sql";
        if (system(mysql_source.c_str()) == -1) 
            throw std::runtime_error("system() error, return -1");
        
        // each handler connects to the server
        for (int i = 0; i < txn_num; i++) {
            connect_and_use_testdb(handlers[i]);
            query_status[i] = 0;
        }
        connect_and_use_testdb(check_handler);

        // begin to test with blocking
        int status_queue[queue_size];
        for (int i = 0; i < queue_size; i++) 
            status_queue[i] = 0;
        int txn_block_flag[txn_num];
        for (int i = 0; i < txn_num; i++)
            txn_block_flag[i] = 0;
        
        for (int i = 0; i < queue_size; i++) {
            auto tid = tid_queue[i];
            auto& stmt = stmt_queue[i];

            if (txn_block_flag[tid] == 1)
                continue; // the txn is blocked

            vector<vector<string>> stmt_output;
            auto is_executed = test_one_stmt(handlers[tid], check_handler, stmt, stmt_output, query_status[tid]);
            if (is_executed == false) { // blocked
                txn_block_flag[tid] = 1;
                continue;
            }
            
            // is executed
            txn_ouput_size[k].push_back(stmt_output.size());
            status_queue[i] = 1; // mean that the stmt is executed
            
            // if it is a commit or rollback stmt, check whether the blocked txn is still blocked
            if (stmt.find("COMMIT") != string::npos || stmt.find("ROLLBACK") != string::npos) { 
                retry_block_queue(handlers, check_handler, query_status, stmt_queue, tid_queue, i, status_queue, txn_block_flag);
            }
        }

        // close the connection
        for (int i = 0; i < txn_num; i++) 
            mysql_close(&handlers[i]);
        mysql_close(&check_handler);

        // check whether they output different size
        if (k > 0 && txn_ouput_size[k] != txn_ouput_size[k - 1]) {
            cerr << "test result different: " << endl;
            cerr << "one: " << endl;
            for (int j = 0; j < txn_ouput_size[k - 1].size(); j++)
                cerr << txn_ouput_size[k - 1][j] << " " << endl;
            cerr << "two: " << endl;
            for (int j = 0; j < txn_ouput_size[k].size(); j++)
                cerr << txn_ouput_size[k][j] << " " << endl;
        }
    }
    
    
}