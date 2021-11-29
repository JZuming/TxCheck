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
// #define __DEBUG_MODE__

pthread_mutex_t mutex_timeout;  
pthread_cond_t  cond_timeout;

int child_pid = 0;
bool child_timed_out = false;

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
    dut_reset(options); 
    generate_database(options, random_file);

    int i = TEST_TIME_FOR_EACH_DB;
    while (i--) {
        try {
            transaction_test tt(options, random_file, false);
            auto ret = tt.test();
            //auto ret = old_transaction_test(options, random_file);
            if (ret == 1) {
                exit(166);
            }
        } catch(std::exception &e) { // ignore runtime error
            cerr << e.what() << endl;
        } 
    }
    
    return 0;
}

int main(int argc, char *argv[])
{
    // analyze the options
    map<string,string> options;
    regex optregex("--(help|postgres|sqlite|monetdb|random-seed|mysql-db|mysql-port)(?:=((?:.|\n)*))?");
  
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

    static itimerval itimer;
    // random_test(options);
    
    while (1) {
        child_timed_out = false;

        cerr << RED << "New Test Database --------------------------" << RESET << endl;
        child_pid = fork();
        if (child_pid == 0) {
            random_test(options);
            exit(0);
        }

        // timeout is ms
        itimer.it_value.tv_sec = DATABASE_TIMEOUT;
        itimer.it_value.tv_usec = 0; // us limit
        setitimer(ITIMER_REAL, &itimer, NULL);

        cerr << "begin waiting" << endl;

        // wait for the tests
        int status;
        auto res = waitpid(child_pid, &status, 0);
        if (res <= 0) {
            cerr << "waitpid() fail: " <<  res << endl;
            exit(-1);
        }

        // disable HandleTimeout
        if (!WIFSTOPPED(status)) 
            child_pid = 0;
        
        itimer.it_value.tv_sec = 0;
        itimer.it_value.tv_usec = 0;
        setitimer(ITIMER_REAL, &itimer, NULL);

        if (WIFSIGNALED(status)) {
            auto killSignal = WTERMSIG(status);
            if (child_timed_out && killSignal == SIGKILL) {
                cerr << "just timeout" << endl;
                continue;
            }
            else {
                cerr << RED << "find memory bug" << RESET << endl;
                return -1;
            }
        }

        if (WIFEXITED(status)) {
            auto exit_code =  WEXITSTATUS(status); // only low 8 bit (max 255)
            cerr << "exit code " << exit_code << endl;
            if (exit_code == 166) {
                cerr << RED << "find correctness bug" << RESET << endl;
                return -1;
            }
        }
    }

    return 0;
}