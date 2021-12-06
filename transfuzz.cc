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

#ifdef HAVE_LIBSQLITE3
#include "sqlite.hh"
#endif

#ifdef HAVE_LIBMYSQLCLIENT
#include "mysql.hh"
#endif

#ifdef HAVE_MONETDB
#include "monetdb.hh"
#endif

#include "postgres.hh"

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
#define RESTART_SERVER_EXIT 8
#define MAX_TIMEOUT_TIME 3

pthread_mutex_t mutex_timeout;  
pthread_cond_t  cond_timeout;

int child_pid = 0;
bool child_timed_out = false;

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
	}

    cerr << "get SIGALRM, stop the process" << endl;
    return;  
}

int fork_for_transaction_test(map<string,string>& options,
                            file_random_machine* random_file,
                            bool is_serilizable)
{
    static itimerval itimer;

    child_pid = fork();
    if (child_pid == 0) { // in child process
        try {
            transaction_test tt(options, random_file, false);
            auto restart = tt.fork_if_server_closed();
            if (restart)
                exit(RESTART_SERVER_EXIT); // need to generate database again
            
            auto ret = tt.test();
            if (ret == 1)
                cerr << RED << "find a bug" << RESET << endl;

        } catch(std::exception &e) { // ignore runtime error
            cerr << e.what() << endl;
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
        cerr << "exit code " << exit_code << endl;
        if (exit_code == RESTART_SERVER_EXIT) {
            cerr << RED << "server is restarted, should generate db again" << RESET << endl;
            throw runtime_error(string("restart server"));
        }
    }

    if (WIFSIGNALED(status)) {
        auto killSignal = WTERMSIG(status);
        if (child_timed_out && killSignal == SIGKILL) {
            cerr << "timeout in generating stmt, reset the seed" << endl;
            smith::rng.seed(time(NULL));
            throw runtime_error(string("transaction test timeout"));
        }
        else {
            cerr << RED << "find memory bug" << RESET << endl;
            throw runtime_error(string("memory bug"));
        }
    }

    return 0;
}

int random_test(map<string,string>& options)
{
    struct file_random_machine* random_file;
    if (options.count("random-seed")) {
        cerr << "random seed is " << options["random-seed"] << endl;
        random_file = file_random_machine::get(options["random-seed"]);
        file_random_machine::use_file(options["random-seed"]);
    }
    else 
        random_file = NULL;
    
    if (random_file == NULL) {
        cerr << YELLOW << "initial seed as time(NULL)" << RESET << endl;
        smith::rng.seed(time(NULL));
    }
    
    // reset the target DBMS to initial state
    while (1) {
        try {
            transaction_test just_setup(options, random_file, false);
            just_setup.fork_if_server_closed();
            
            dut_reset(options);
            generate_database(options, random_file);
            break;
        } catch(std::exception &e) {
            cerr << e.what() << "in setup stage" << endl;
        }
    } 

    int i = TEST_TIME_FOR_EACH_DB;
    int timeout_time = 0;
    while (i--) {
        // try {
        //     transaction_test tt(options, random_file, false);
        //     auto restart = tt.fork_if_server_closed();
        //     if (restart)
        //         break;
        //     auto ret = tt.test();
        //     //auto ret = old_transaction_test(options, random_file);
        //     if (ret == 1) {
        //         cerr << RED << "find a bug" << RESET << endl;
        //     }
        // } catch(std::exception &e) { // ignore runtime error
        //     cerr << e.what() << endl;
        // } 
        
        if (timeout_time >= MAX_TIMEOUT_TIME) {
            kill_process_with_SIGTERM(transaction_test::server_process_id);
            timeout_time = 0;
        }
        
        try {
            fork_for_transaction_test(options, random_file, false);
        } catch (exception &e) {
            string err = e.what();
            cerr << "ERROR in random_test: " << err << endl;
            if (err == "restart server")
                break;
            else if (err == "transaction test timeout") 
                timeout_time++;
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
    regex optregex("--(help|postgres|sqlite|monetdb|random-seed|mysql-db|mysql-port|reproduce-sql|reproduce-tid)(?:=((?:.|\n)*))?");
  
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
            #ifdef HAVE_LIBSQLITE3
            "    --sqlite=URI         SQLite database to send queries to" << endl <<
            #endif
            #ifdef HAVE_MONETDB
            "    --monetdb=connstr    MonetDB database to send queries to" <<endl <<
            #endif
            #ifdef HAVE_LIBMYSQLCLIENT
            "    --mysql-db=constr    mysql database name to send queries to (should used with" << endl << 
            "    --mysql-port=int     mysql server port number" << endl << 
            #endif
            "    --random-seed=filename    random file for dynamic query interaction" << endl <<
            "    --reproduce-sql=filename    sql file to reproduce the problem" << endl <<
            "    --reproduce-tid=filename    tid file to reproduce the problem" << endl <<
            "    --help               print available command line options and exit" << endl;
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

    // static itimerval itimer;
    // random_test(options);

    if (options.count("reproduce-sql")) {
        cerr << "enter reproduce mode" << endl;
        if (!options.count("reproduce-tid"))
            cerr << "should also provide tid file" << endl;

        transaction_test re_test(options, NULL, false);

        // get stmt queue
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

            re_test.stmt_queue.push_back(each_sql + ";");
        }
	    
        // get tid queue
        set<int> tid_set;
        ifstream tid_file(options["reproduce-tid"]);
        int tid;
        while (tid_file >> tid) {
            re_test.tid_queue.push_back(tid);
            tid_set.insert(tid);
        }
        tid_file.close();

        re_test.stmt_num = re_test.tid_queue.size();
        re_test.trans_num = tid_set.size();
        delete[] re_test.trans_arr;
        re_test.trans_arr = new transaction[re_test.trans_num];
	
        cerr << re_test.trans_num << " " << re_test.stmt_num<< " " << re_test.stmt_queue.size() << endl;
        for (int i = 0; i < re_test.stmt_num; i++) {
            auto tid = re_test.tid_queue[i];
            re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
        }
        
        for (int tid = 0; tid < re_test.trans_num; tid++) {
            re_test.trans_arr[tid].dut = dut_setup(options);
            re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
            if (re_test.trans_arr[tid].stmts.back().find("COMMIT") != string::npos)
                re_test.trans_arr[tid].status = 1;
            else
                re_test.trans_arr[tid].status = 2;
        }
	
        re_test.trans_test();
        re_test.normal_test();
        if (!re_test.check_result()) {
            cerr << "reproduce successfully" << endl;
        }

        return 0;
    }
    
    while (1) {
        // child_timed_out = false;

        // cerr << RED << "New Test Database --------------------------" << RESET << endl;
        // child_pid = fork();
        // if (child_pid == 0) {
            random_test(options);
        //     exit(0);
        // }

        // // timeout is ms
        // itimer.it_value.tv_sec = DATABASE_TIMEOUT;
        // itimer.it_value.tv_usec = 0; // us limit
        // setitimer(ITIMER_REAL, &itimer, NULL);

        // cerr << "begin waiting" << endl;

        // // wait for the tests
        // int status;
        // auto res = waitpid(child_pid, &status, 0);
        // if (res <= 0) {
        //     cerr << "waitpid() fail: " <<  res << endl;
        //     exit(-1);
        // }

        // // disable HandleTimeout
        // if (!WIFSTOPPED(status)) 
        //     child_pid = 0;
        
        // itimer.it_value.tv_sec = 0;
        // itimer.it_value.tv_usec = 0;
        // setitimer(ITIMER_REAL, &itimer, NULL);

        // if (WIFSIGNALED(status)) {
        //     auto killSignal = WTERMSIG(status);
        //     if (child_timed_out && killSignal == SIGKILL) {
        //         cerr << "just timeout" << endl;
        //         continue;
        //     }
        //     else {
        //         cerr << RED << "find memory bug" << RESET << endl;
        //         return -1;
        //     }
        // }

        // if (WIFEXITED(status)) {
        //     auto exit_code =  WEXITSTATUS(status); // only low 8 bit (max 255)
        //     cerr << "exit code " << exit_code << endl;
        //     if (exit_code == 166) {
        //         cerr << RED << "find correctness bug" << RESET << endl;
        //         return -1;
        //     }
        // }
    }

    return 0;
}
