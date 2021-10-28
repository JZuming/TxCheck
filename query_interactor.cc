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

shared_ptr<schema> get_schema(map<string,string>& options)
{
    shared_ptr<schema> schema;
    if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
        schema = make_shared<schema_sqlite>(options["sqlite"], true);
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
        throw runtime_error("Does not support SQLite");
#endif
    } else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB
        schema = make_shared<schema_monetdb>(options["monetdb"]);
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
        throw runtime_error("Does not support MonetDB");
#endif
    } else if(options.count("postgres")) 
        schema = make_shared<schema_pqxx>(options["postgres"], true);
    else {
        cerr << "Sorry,  you should specify a dbms and its database" << endl;
        throw runtime_error("Does not define target dbms and db");
    }
    return schema;
}

shared_ptr<dut_base> dut_setup(map<string,string>& options)
{
    shared_ptr<dut_base> dut;
    if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
        dut = make_shared<dut_sqlite>(options["sqlite"]);
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
        throw runtime_error("Does not support SQLite");
#endif
    } else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB	   
        dut = make_shared<dut_monetdb>(options["monetdb"]);
#else
        cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
        throw runtime_error("Does not support MonetDB");
#endif
    } else if(options.count("postgres")) 
        dut = make_shared<dut_libpq>(options["postgres"]);
    else {
        cerr << "Sorry,  you should specify a dbms and its database" << endl;
        throw runtime_error("Does not define target dbms and db");
    }

    return dut;
}

void dut_test(map<string,string>& options, const string& stmt)
{
    auto dut = dut_setup(options);
    dut->test(stmt);
}

void dut_reset(map<string,string>& options)
{
    auto dut = dut_setup(options);
    dut->reset();
}

int main(int argc, char *argv[])
{
    // analyze the options
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
    else
        random_file = NULL;

    // reset the target DBMS to initial state
    dut_reset(options); 

    // set up basic shared schema for two transaction


    while (1) {
        auto schema = get_schema(options);

        scope scope;
        schema->fill_scope(scope);

        shared_ptr<prod> gen = statement_factory(&scope);
        ostringstream s;
	    gen->out(s);
        
        // cerr << s.str() << ";" << endl; 
        
        try {
            dut_test(options, s.str());
        } catch(std::exception &e) {
        }
        
        if (random_file != NULL && random_file->read_byte > random_file->end_pos)
            break;
    }
}