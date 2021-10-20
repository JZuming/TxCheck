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

#ifdef HAVE_MONETDB
#include "monetdb.hh"
#endif

#include "postgres.hh"

using namespace std;

using namespace std::chrono;

extern "C" {
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
}

// argv[0]: itself
// argv[1]: dbms (supported: mysql)

int main(int argc, char *argv[])
{
    map<string,string> options;
    regex optregex("--(help|postgres|sqlite|monetdb|random-seed)(?:=((?:.|\n)*))?");
  
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
            "    --random-seed=filename    random file for dynamic query interaction" << endl <<
            "    --help               print available command line options and exit" << endl;
        return 0;
    } else if (options.count("version")) {
        return 0;
    }

    struct file_random_machine* random_file;
    if (options.count("random-seed")) {
        cerr << options["random-seed"] << endl;
        random_file = file_random_machine::get(options["random-seed"]);
    }

    int i = 0;
    while (1) {
        shared_ptr<schema> schema;
        if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
	        schema = make_shared<schema_sqlite>(options["sqlite"], true);
#else
	        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
	        return 1;
#endif
        } else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB
	        schema = make_shared<schema_monetdb>(options["monetdb"]);
#else
	        cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
	        return 1;
#endif
        } else if(options.count("postgres")) 
	        schema = make_shared<schema_pqxx>(options["postgres"], true);
        else {
            cerr << "Sorry,  you should specify a dbms and its database" << endl;
            return 1;
        }

        scope scope;
        long queries_generated = 0;
        schema->fill_scope(scope);

        shared_ptr<prod> gen = statement_factory(&scope);
        ostringstream s;
	    gen->out(s);
        
        cerr << "======= " << i << " ======= " << endl;
        // cerr << s.str() << ";" << endl;
        i++;
        
//         shared_ptr<dut_base> dut;
//         if (options.count("sqlite")) {
// #ifdef HAVE_LIBSQLITE3
// 	        dut = make_shared<dut_sqlite>(options["sqlite"]);
// #else
// 	        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
// 	        return 1;
// #endif
//         } else if(options.count("monetdb")) {
// #ifdef HAVE_MONETDB	   
// 	        dut = make_shared<dut_monetdb>(options["monetdb"]);
// #else
// 	        cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
// 	        return 1;
// #endif
//         } else if(options.count("postgres")) 
// 	        dut = make_shared<dut_libpq>(options["postgres"]);
//         else {
//             cerr << "Sorry,  you should specify a dbms and its database" << endl;
//             return 1;
//         }

//         dut->test(s.str());
        if (random_file->read_byte > random_file->end_pos)
            break;
    }
}