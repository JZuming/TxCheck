#include "config.h"

#include <iostream>
#include <chrono>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

#include <thread>
#include <typeinfo>

#include "random.hh"
#include "grammar.hh"
#include "relmodel.hh"
#include "schema.hh"
#include "gitrev.h"

#include "log.hh"
#include "dump.hh"
#include "impedance.hh"
#include "dut.hh"

#include <sys/time.h>
#include <sys/wait.h>

using namespace std;

using namespace std::chrono;

extern "C" {
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
}

#include "transaction_test.hh"

#define NORMAL_EXIT 0
#define FIND_BUG_EXIT 7
#define MAX_TIMEOUT_TIME 3
#define MAX_SETUP_TRY_TIME 3

pthread_mutex_t mutex_timeout;  
pthread_cond_t  cond_timeout;

int child_pid = 0;
bool child_timed_out = false;

extern int write_op_id;

void kill_process_signal(int signal)  
{  
    if(signal != SIGALRM) {  
        printf("unexpect signal %d\n", signal);  
        exit(1);  
    }

    if (child_pid > 0) {
        printf("child pid timeout, kill it\n"); 
        child_timed_out = true;
		kill(child_pid, SIGKILL);
        // also kill server process to restart
        while (transaction_test::try_to_kill_server() == false) {}
	}

    cerr << "get SIGALRM, stop the process" << endl;
    return;  
}

int fork_for_generating_database(dbms_info& d_info)
{
    static itimerval itimer;
    transaction_test::fork_if_server_closed(d_info);
    
    write_op_id = 0;
    child_pid = fork();
    if (child_pid == 0) { // in child process
        generate_database(d_info);
        ofstream output_wkey("wkey.txt");
        output_wkey << write_op_id << endl;
        output_wkey.close();
        exit(NORMAL_EXIT);
    }

    itimer.it_value.tv_sec = TRANSACTION_TIMEOUT;
    itimer.it_value.tv_usec = 0; // us limit
    setitimer(ITIMER_REAL, &itimer, NULL);

    int status;
    auto res = waitpid(child_pid, &status, 0);
    if (res <= 0) {
        cerr << "waitpid() fail: " <<  res << endl;
        throw runtime_error(string("waitpid() fail"));
    }

    if (!WIFSTOPPED(status)) 
        child_pid = 0;
    
    itimer.it_value.tv_sec = 0;
    itimer.it_value.tv_usec = 0;
    setitimer(ITIMER_REAL, &itimer, NULL);
    
    if (WIFEXITED(status)) {
        auto exit_code =  WEXITSTATUS(status); // only low 8 bit (max 255)
        // cerr << "exit code: " << exit_code << endl;
        if (exit_code == FIND_BUG_EXIT) {
            cerr << RED << "a bug is found in fork process" << RESET << endl;
            transaction_test::record_bug_num++;
            exit(-1);
        }
        if (exit_code == 255)
            exit(-1);
    }

    if (WIFSIGNALED(status)) {
        auto killSignal = WTERMSIG(status);
        if (child_timed_out && killSignal == SIGKILL) {
            // cerr << "timeout in generating stmt, reset the seed" << endl;
            // transaction_test::try_to_kill_server();
            // auto just_check_server = make_shared<transaction_test>(d_info);
            // auto restart = just_check_server->fork_if_server_closed();
            // if (restart)
            //     throw runtime_error(string("restart server")); // need to generate database again
            
            // smith::rng.seed(time(NULL));
            throw runtime_error(string("transaction test timeout"));
        }
        else {
            cerr << RED << "find memory bug" << RESET << endl;
            cerr << "killSignal: " << killSignal << endl;
            throw runtime_error(string("memory bug"));
        }
    }

    ifstream input_wkey("wkey.txt");
    input_wkey >> write_op_id;
    input_wkey.close();

    write_op_id++;
    // cerr << "updating write_op_id: "<< write_op_id << endl;

    return 0;
}

int fork_for_transaction_test(dbms_info& d_info)
{
    static itimerval itimer;

    transaction_test::fork_if_server_closed(d_info);
    
    child_pid = fork();
    if (child_pid == 0) { // in child process
        try {
            // cerr << "write_op_id: " << write_op_id << endl;
            transaction_test tt(d_info);
            auto ret = tt.test();
            if (ret == 1) {
                cerr << RED << "Find a bug !!!" << RESET << endl;
                exit(FIND_BUG_EXIT);
            }
        } catch(std::exception &e) { // ignore runtime error
            cerr << "in test: " << e.what() << endl;
        }
        exit(NORMAL_EXIT);
    }

    itimer.it_value.tv_sec = TRANSACTION_TIMEOUT;
    itimer.it_value.tv_usec = 0; // us limit
    setitimer(ITIMER_REAL, &itimer, NULL);

    int status;
    auto res = waitpid(child_pid, &status, 0);
    if (res <= 0) {
        cerr << "waitpid() fail: " <<  res << endl;
        throw runtime_error(string("waitpid() fail"));
    }

    if (!WIFSTOPPED(status)) 
        child_pid = 0;
    
    itimer.it_value.tv_sec = 0;
    itimer.it_value.tv_usec = 0;
    setitimer(ITIMER_REAL, &itimer, NULL);
    
    if (WIFEXITED(status)) {
        auto exit_code =  WEXITSTATUS(status); // only low 8 bit (max 255)
        // cerr << "exit code: " << exit_code << endl;
        if (exit_code == FIND_BUG_EXIT) {
            cerr << RED << "a bug is found in fork process" << RESET << endl;
            transaction_test::record_bug_num++;
            // abort();
        }
        if (exit_code == 255)
            abort();
    }

    if (WIFSIGNALED(status)) {
        auto killSignal = WTERMSIG(status);
        if (child_timed_out && killSignal == SIGKILL) {
            // cerr << "timeout in generating stmt, reset the seed" << endl;
            // smith::rng.seed(time(NULL));
            throw runtime_error(string("transaction test timeout"));
        }
        else {
            cerr << RED << "find memory bug" << RESET << endl;
            cerr << "killSignal: " << killSignal << endl;
            abort();
            // throw runtime_error(string("memory bug"));
        }
    }

    return 0;
}

int random_test(dbms_info& d_info)
{   
    random_device rd;
    auto rand_seed = rd();
    // rand_seed = 1069001986;
    cerr << "random seed: " << rand_seed << " -> ";
    smith::rng.seed(rand_seed);
    
    // reset the target DBMS to initial state
    int setup_try_time = 0;
    while (1) {
        if (setup_try_time > MAX_SETUP_TRY_TIME) {
            kill_process_with_SIGTERM(transaction_test::server_process_id);
            setup_try_time = 0;
        }

        try {
            // donot fork, so that the static schema can be used in each test case
            transaction_test::fork_if_server_closed(d_info);
            generate_database(d_info);
            
            // fork_for_generating_database(d_info);
            break;
        } catch(std::exception &e) {
            cerr << e.what() << " in setup stage" << endl;
            setup_try_time++;
        }
    } 

    int i = TEST_TIME_FOR_EACH_DB;
    while (i--) {  
        try {
            fork_for_transaction_test(d_info);
        } catch (exception &e) {
            string err = e.what();
            cerr << "ERROR in random_test: " << err << endl;
            if (err == "restart server")
                break;
            else if (err == "transaction test timeout") {
                break; // break the test and begin a new test
                // after killing and starting a new server, created tables might be lost
                // so it needs to begin a new test to generate tables
            }
            else {
                cerr << "the exception cannot be handled" << endl;
                throw e;
            }
        }
    }
    
    return 0;
}

int main(int argc, char *argv[])
{
    // analyze the options
    map<string,string> options;
    regex optregex("--\
(help|min|postgres|sqlite|monetdb|random-seed|\
postgres-db|postgres-port|\
tidb-db|tidb-port|\
mysql-db|mysql-port|\
mariadb-db|mariadb-port|\
oceanbase-db|oceanbase-port|\
monetdb-db|monetdb-port|\
cockroach-db|cockroach-port|\
output-or-affect-num|\
check-txn-cycle|\
txn-decycle|\
check-topo-sort|\
reproduce-sql|reproduce-tid|reproduce-usage)(?:=((?:.|\n)*))?");
  
    for(char **opt = argv + 1 ;opt < argv + argc; opt++) {
        smatch match;
        string s(*opt);
        if (regex_match(s, match, optregex)) {
            options[string(match[1])] = match[2];
        } else {
            cerr << "Cannot parse option: " << *opt << endl;
            options["help"] = "";
        }
    }

    if (options.count("help")) {
        cerr <<
            "    --postgres=connstr   postgres database to send queries to" << endl <<
            "    --postgres-db=connstr  Postgres database to send queries to, should used with --postgres-port" <<endl <<
            "    --postgres-port=int    Postgres server port number, , should used with --postgres-port" <<endl <<
            #ifdef HAVE_LIBSQLITE3
            "    --sqlite=URI         SQLite database to send queries to" << endl <<
            #endif
            #ifdef HAVE_MONETDB
            "    --monetdb-db=connstr  MonetDB database to send queries to" <<endl <<
            "    --monetdb-port=int    MonetDB server port number" <<endl <<
            #endif
            #ifdef HAVE_LIBMYSQLCLIENT
            #ifdef HAVE_TIDB
            "    --tidb-db=constr   tidb database name to send queries to (should used with" << endl << 
            "    --tidb-port=int    tidb server port number" << endl << 
            #endif
            #ifdef HAVE_MARIADB
            "    --mariadb-db=constr   mariadb database name to send queries to (should used with" << endl << 
            "    --mariadb-port=int    mariadb server port number" << endl <<
            #endif
            #ifdef HAVE_OCEANBASE
            "    --oceanbase-db=constr   oceanbase database name to send queries to (should used with" << endl << 
            "    --oceanbase-port=int    oceanbase server port number" << endl <<
            #endif
            #ifdef HAVE_MYSQL
            "    --mysql-db=constr  mysql database name to send queries to (should used with" << endl << 
            "    --mysql-port=int   mysql server port number" << endl << 
            #endif
            #endif
            "    --cockroach-db=constr  cockroach database name to send queries to (should used with" << endl << 
            "    --cockroach-port=int   cockroach server port number" << endl << 
            "    --output-or-affect-num=int  generating statement that output num rows or affect num rows"
            "    --random-seed=filename    random file for dynamic query interaction" << endl <<
            "    --reproduce-sql=filename    sql file to reproduce the problem" << endl <<
            "    --reproduce-tid=filename    tid file to reproduce the problem" << endl <<
            "    --reproduce-usage=filename    stmt usage file to reproduce the problem" << endl <<
            "    --min                  minimize the reproduce test case (should be used with --reproduce-sql, --reproduce-tid, and --reproduce-usage)" << endl <<
            "    --check-txn-cycle      check whether the test case has transactional cycles (should be used with --reproduce-sql, --reproduce-tid, and --reproduce-usage)" << endl <<
            "    --txn-decycle          perform transactional decycling, and check whether still trigger the bug (should be used with --reproduce-sql, --reproduce-tid, and --reproduce-usage)" << endl <<
            "    --check-topo-sort      check whether all topological sorting results can trigger the bug (should be used with --reproduce-sql, --reproduce-tid, and --reproduce-usage)" << endl <<
            "    --help                 print available command line options and exit" << endl;
        return 0;
    } else if (options.count("version")) {
        return 0;
    }

    // set timeout action
    struct sigaction action;  
    memset(&action, 0, sizeof(action));  
    sigemptyset(&action.sa_mask);  
    action.sa_flags = 0;  
    action.sa_handler = user_signal;  
    if (sigaction(SIGUSR1, &action, NULL)) {
        cerr << "sigaction error" << endl;
        exit(1);
    }

    // set timeout action for fork
    struct sigaction sa;  
    memset(&sa, 0, sizeof(sa));  
    sigemptyset(&sa.sa_mask);  
    sa.sa_flags = SA_RESTART; 
    sa.sa_handler = kill_process_signal;  
    if (sigaction(SIGALRM, &sa, NULL)) {
        cerr << "sigaction error" << endl;
        exit(1);
    }

    // init the lock
    pthread_mutex_init(&mutex_timeout, NULL);  
    pthread_cond_init(&cond_timeout, NULL);

    dbms_info d_info(options);

    cerr << "-------------Test Info------------" << endl;
    cerr << "Test DBMS: " << d_info.dbms_name << endl;
    cerr << "Test database: " << d_info.test_db << endl;
    cerr << "Test port: " << d_info.test_port << endl;
    cerr << "Can trigger error in transaction: " << d_info.can_trigger_error_in_txn << endl;
    cerr << "Output or affect num: " << d_info.ouput_or_affect_num << endl;
    cerr << "----------------------------------" << endl;

    if (options.count("reproduce-sql")) {
        cerr << "enter reproduce mode" << endl;
        if (!options.count("reproduce-tid")) {
            cerr << "should also provide tid file" << endl;
            return 0;
        }

        // get stmt queue
        vector<shared_ptr<prod>> stmt_queue;
        ifstream stmt_file(options["reproduce-sql"]);
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

            stmt_queue.push_back(make_shared<txn_string_stmt>((prod *)0, each_sql));
        }
	    
        // get tid queue
        vector<int> tid_queue;
        ifstream tid_file(options["reproduce-tid"]);
        int tid;
        int max_tid = -1;
        while (tid_file >> tid) {
            tid_queue.push_back(tid);
            if (tid > max_tid)
                max_tid = tid;
        }
        tid_file.close();

        // get stmt use queue
        vector<stmt_usage> stmt_usage_queue;
        ifstream stmt_usage_file(options["reproduce-usage"]);
        int use;
        while (stmt_usage_file >> use) {
            switch (use) {
            case 0:
                stmt_usage_queue.push_back(stmt_usage(INIT_TYPE, false, "t_***"));
                break;
            case 1:
                stmt_usage_queue.push_back(stmt_usage(SELECT_READ, false, "t_***"));
                break;
            case 2:
                stmt_usage_queue.push_back(stmt_usage(UPDATE_WRITE, false, "t_***"));
                break;
            case 3:
                stmt_usage_queue.push_back(stmt_usage(INSERT_WRITE, false, "t_***"));
                break;
            case 4:
                stmt_usage_queue.push_back(stmt_usage(DELETE_WRITE, false, "t_***"));
                break;
            case 5:
                stmt_usage_queue.push_back(stmt_usage(BEFORE_WRITE_READ, true, "t_***"));
                break;
            case 6:
                stmt_usage_queue.push_back(stmt_usage(AFTER_WRITE_READ, true, "t_***"));
                break;
            case 7:
                stmt_usage_queue.push_back(stmt_usage(VERSION_SET_READ, true, "t_***"));
                break;
            default:
                cerr << "unknown stmt usage: " << use << endl;
                exit(-1);
                break;
            }
        }
        stmt_usage_file.close();

        if (options.count("min"))
            minimize_testcase(d_info, stmt_queue, tid_queue, stmt_usage_queue);
        else if (options.count("check-txn-cycle"))
            check_txn_cycle(d_info, stmt_queue, tid_queue, stmt_usage_queue);
        else if (options.count("txn-decycle")) {
            int succeed_time = 0;
            int all_time = 0;
            vector<int> delete_nodes;
            txn_decycle_test(d_info, stmt_queue, tid_queue, stmt_usage_queue, succeed_time, all_time, delete_nodes);
            cerr << "succeed time: " << succeed_time << endl;
            cerr << "all time: " << all_time << endl;
        }
        else if (options.count("check-topo-sort")) {
            int succeed_time = 0;
            int all_time = 0;
            check_topo_sort(d_info, stmt_queue, tid_queue, stmt_usage_queue, succeed_time, all_time);
            cerr << "succeed time: " << succeed_time << endl;
            cerr << "all time: " << all_time << endl;
        }
        else {
            string empty_str;
            reproduce_routine(d_info, stmt_queue, tid_queue, stmt_usage_queue, empty_str);
        }
            
        return 0;
    }
    
    while (1) {
        random_test(d_info);
    }

    return 0;
}
