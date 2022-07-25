#include <stdexcept>
#include <cassert>
#include "monetdb.hh"
#include <iostream>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

extern "C" {
#include <mapi.h>
#include <unistd.h>
}

#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

// connect montetdb
monetdb_connection::monetdb_connection(string db, unsigned int port)
{
	test_db = db;
	test_port = port;
	dbh = mapi_connect(NULL, port, "monetdb", "monetdb", "sql", db.c_str());
	auto err = mapi_error(dbh);
	if (err) {
		string err_msg = mapi_error_str(dbh);
		if (err_msg.find("no such database") != string::npos) {
			string create_cmd = "monetdb create " + test_db;
			string start_cmd = "monetdb start " + test_db;
			if (system(create_cmd.c_str()) != 0) {
				std::cerr << "create monetdb database fail" << endl;
				exit(-1);
			}
			if (system(start_cmd.c_str()) != 0) {
				std::cerr << "start monetdb database fail" << endl;
				exit(-1);
			}
			dbh = mapi_connect(NULL, port, "monetdb", "monetdb", "sql", db.c_str());
			err = mapi_error(dbh);
		}
    }
	if (err) {
		if (dbh != NULL) {
        	mapi_explain(dbh, stderr);
            mapi_destroy(dbh);
    	} else {
            fprintf(stderr, "command failed\n");
    	}
        exit(-1);
	}

	mapi_reconnect(dbh);
	if (mapi_error(dbh)) {
		mapi_explain(dbh, stderr);
		mapi_destroy(dbh);
        exit(-1);
    }
}

// execute queries on MonetDB
void monetdb_connection::q(const char* query)
{
	MapiHdl hdl = mapi_query(dbh, query);
	if (mapi_result_error(hdl) != NULL)
		mapi_explain_result(hdl, stderr);
	mapi_close_handle(hdl);
}

// disconnect MonetDB
monetdb_connection::~monetdb_connection()
{
	mapi_destroy(dbh);
}

//load schema from MonetDB
schema_monetdb::schema_monetdb(string db, unsigned int port):monetdb_connection(db, port)
{
	// cerr << "init booltype, inttype, internaltype, arraytype here" << endl;
	booltype = sqltype::get("boolean");
	inttype = sqltype::get("int");
	internaltype = sqltype::get("internal");
	arraytype = sqltype::get("ARRAY");

    realtype = sqltype::get("double");
    texttype = sqltype::get("text");

	cerr << "Loading tables from database: " << db << endl;
	string qry = "select t.name, s.name, t.system, t.type from sys.tables t,  sys.schemas s where t.schema_id=s.id and t.system=false";
	// string qry = "select t.name, s.name, t.system, t.type from sys.tables t,  sys.schemas s where t.schema_id=s.id ";
	MapiHdl hdl = mapi_query(dbh,qry.c_str());
	while (mapi_fetch_row(hdl)) {
		tables.push_back(table(mapi_fetch_field(hdl,0),mapi_fetch_field(hdl,1),strcmp(mapi_fetch_field(hdl,2),"false")==0 ? true : false , atoi(mapi_fetch_field(hdl,3))==0 ? false : true));
	}
	mapi_close_handle(hdl);
	cerr << " done." << endl;

	cerr << "Loading columns and constraints...";
	for (auto t = tables.begin(); t!=tables.end(); t++) {
		string q("select col.name,"
			" col.type "
			" from sys.columns col, sys.tables tab"
			" where tab.name= '");
		q += t->name;
		q += "' and tab.id = col.table_id";

		hdl = mapi_query(dbh,q.c_str());
		while (mapi_fetch_row(hdl)) {
			column c(mapi_fetch_field(hdl,0), sqltype::get(mapi_fetch_field(hdl,1)));
			t->columns().push_back(c);
		}
		mapi_close_handle(hdl);
	}
	// TODO: confirm with Martin or Stefan about column
	// constraints in MonetDB
	cerr << " done." << endl;

	cerr << "Loading operators...";
	string opq("select f.func, a.type, b.type, c.type"
		" from sys.functions f, sys.args a, sys.args b, sys.args c"
                "  where f.id=a.func_id and f.id=b.func_id and f.id=c.func_id and a.name='arg_1' and b.name='arg_2' and c.number=0");
	hdl = mapi_query(dbh,opq.c_str());
	while (mapi_fetch_row(hdl)) {
		op o(mapi_fetch_field(hdl,0),sqltype::get(mapi_fetch_field(hdl,1)),sqltype::get(mapi_fetch_field(hdl,2)),sqltype::get(mapi_fetch_field(hdl,3)));
		register_operator(o);
	}
	mapi_close_handle(hdl);
	cerr << " done." << endl;

	cerr << "Loading routines...";
	string routq("select s.name, f.id, a.type, f.name from sys.schemas s, sys.args a, sys.types t, sys.functions f where f.schema_id = s.id and f.id=a.func_id and a.number=0 and a.type = t.sqlname and f.mod<>'aggr'");
	hdl = mapi_query(dbh,routq.c_str());
	while (mapi_fetch_row(hdl)) {
		routine proc(mapi_fetch_field(hdl,0),mapi_fetch_field(hdl,1),sqltype::get(mapi_fetch_field(hdl,2)),mapi_fetch_field(hdl,3));
		register_routine(proc);
	}
	mapi_close_handle(hdl);
	cerr << " done." << endl;

	cerr << "Loading routine parameters...";
	for (auto &proc : routines) {
		string routpq ("select a.type from sys.args a,"
			       " sys.functions f "
			       " where f.id = a.func_id and a.number <> 0 and f.id = '");
		routpq += proc.specific_name;
		routpq += "'";
		hdl = mapi_query(dbh,routpq.c_str());
		while (mapi_fetch_row(hdl)) {
			proc.argtypes.push_back(sqltype::get(mapi_fetch_field(hdl,0)));
		}
		mapi_close_handle(hdl);
	}
	cerr << " done."<< endl;

	cerr << "Loading aggregates...";
	string aggq("select s.name, f.id, a.type, f.name from sys.schemas s, sys.args a, sys.types t, sys.functions f where f.schema_id = s.id and f.id=a.func_id and a.number=0 and a.type = t.sqlname and f.mod='aggr'");

	hdl = mapi_query(dbh,aggq.c_str());
	while (mapi_fetch_row(hdl)) {
		routine proc(mapi_fetch_field(hdl,0),mapi_fetch_field(hdl,1),sqltype::get(mapi_fetch_field(hdl,2)),mapi_fetch_field(hdl,3));
		register_aggregate(proc);
	}
	mapi_close_handle(hdl);
	cerr << " done." << endl;

	cerr << "Loading aggregates parameters...";
	for (auto &proc: aggregates) {
		string aggpq ("select a.type from sys.args a, sys.functions f "
			      "where f.id = a.func_id and a.number <> 0 and f.id = '");
		aggpq += proc.specific_name;
		aggpq += "'";
		hdl = mapi_query(dbh,aggpq.c_str());
		while (mapi_fetch_row(hdl)) {
			proc.argtypes.push_back(sqltype::get(mapi_fetch_field(hdl,0)));
		}
		mapi_close_handle(hdl);
	}
	cerr << " done."<< endl;

	// mapi_destroy(dbh);
	generate_indexes();
}

void schema_monetdb::update_schema()
{
	tables.clear();
	cerr << "Loading tables from database: " << test_db << "...";
	string qry = "select t.name, s.name, t.system, t.type from sys.tables t,  sys.schemas s where t.schema_id=s.id and t.system=false";
	// string qry = "select t.name, s.name, t.system, t.type from sys.tables t,  sys.schemas s where t.schema_id=s.id ";
	MapiHdl hdl = mapi_query(dbh,qry.c_str());
	while (mapi_fetch_row(hdl)) {
		string table_name = mapi_fetch_field(hdl,0);
		string table_schema = mapi_fetch_field(hdl,1);
		auto insertable = strcmp(mapi_fetch_field(hdl,2),"false")==0 ? true : false;
		auto base_table = atoi(mapi_fetch_field(hdl,3))==0 ? false : true;
		// cerr << "table: " << table_name << " " << table_schema << " " << insertable << " " << base_table << endl;
		tables.push_back(table(table_name, table_schema, insertable, base_table));
	}
	mapi_close_handle(hdl);
	cerr << " done." << endl;

	cerr << "Loading columns and constraints...";
	for (auto t = tables.begin(); t!=tables.end(); t++) {
		string q("select col.name,"
			" col.type "
			" from sys.columns col, sys.tables tab"
			" where tab.name= '");
		q += t->name;
		q += "' and tab.id = col.table_id";

		hdl = mapi_query(dbh,q.c_str());
		while (mapi_fetch_row(hdl)) {
			column c(mapi_fetch_field(hdl,0), sqltype::get(mapi_fetch_field(hdl,1)));
			t->columns().push_back(c);
		}
		mapi_close_handle(hdl);
	}
	// TODO: confirm with Martin or Stefan about column
	// constraints in MonetDB
	cerr << " done." << endl;

	return;
}

dut_monetdb::dut_monetdb(string db, unsigned int port):monetdb_connection(db, port)
{
	//build connection
}

void dut_monetdb::test(const string &stmt, vector<vector<string>>* output, int* affected_row_num)
{
	// MapiHdl hdl = mapi_query(dbh,"CALL sys.settimeout(1)");
	// mapi_close_handle(hdl);
	auto hdl = mapi_query(dbh, stmt.c_str());
	if (mapi_error(dbh) != MOK) {
		auto err_str = mapi_result_error(hdl);
		if (!err_str)
		    err_str = "unknown error";
		string err_string = err_str;
		
		if (stmt.find("COMMIT") != string::npos)
            throw runtime_error("fail to commit, can only rollback: " + err_string); // commit cannot be block. if so, just rollback
		if (err_string.find("conflict with another transaction") != string::npos)
            throw std::runtime_error("blocked: " + err_string);
		throw std::runtime_error("mapi_query fails(skipped): " + err_string); 
	}

	if (affected_row_num)
        *affected_row_num = mapi_rows_affected(hdl);

	if (output) {
		auto column_num = mapi_get_field_count(hdl);
		while (mapi_fetch_row(hdl)) {
			vector<string> row_output;
			for (int i = 0; i < column_num; i++) {
                string str;
				auto char_str = mapi_fetch_field(hdl, i);
                if (char_str == NULL)
                    str = "NULL";
                else
                    str = char_str;
                row_output.push_back(str);
            }
            output->push_back(row_output);
		}
	}
	mapi_close_handle(hdl);
	return;
}

void dut_monetdb::reset(void)
{
    mapi_destroy(dbh);

	string stop_cmd = "monetdb stop " + test_db;
	string destroy_cmd = "monetdb destroy -f " + test_db;
	string create_cmd = "monetdb create " + test_db;
	string start_cmd = "monetdb start " + test_db;
	
	system(stop_cmd.c_str()); // may fail when no database started
	system(destroy_cmd.c_str()); // may fail when no database created
	if (system(create_cmd.c_str()) != 0) {
        std::cerr << "create monetdb database fail" << endl;
        throw std::runtime_error("create monetdb database fail"); 
    }
	if (system(start_cmd.c_str()) != 0) {
        std::cerr << "start monetdb database fail" << endl;
        throw std::runtime_error("start monetdb database fail"); 
    }

	dbh = mapi_connect(NULL, test_port, "monetdb", "monetdb", "sql", test_db.c_str());
	return;
}

void dut_monetdb::backup(void)
{
	string echo_cmd = "echo 'user=monetdb\npassword=monetdb' > .monetdb"; // do not need -e because the "-n" will be transferred automatically
	if (system(echo_cmd.c_str()) != 0) {
        std::cerr << "backup (echo_cmd) monetdb fail" << endl;
        throw std::runtime_error("backup (echo_cmd) monetdb fail"); 
    }
	string dump_cmd = "msqldump -q " + test_db + " > /tmp/monetdb_bk.sql";
    if (system(dump_cmd.c_str()) != 0) {
        std::cerr << "backup (dump_cmd) monetdb fail" << endl;
        throw std::runtime_error("backup (dump_cmd) monetdb fail"); 
    }
	return;
}

void dut_monetdb::reset_to_backup(void)
{
    reset();
    string bk_file = "/tmp/monetdb_bk.sql";
    if (access(bk_file.c_str(), F_OK ) == -1) 
        return;
    
    string echo_cmd = "echo 'user=monetdb\npassword=monetdb' > .monetdb";
	if (system(echo_cmd.c_str()) != 0) {
        std::cerr << "reset_to_backup (echo_cmd) monetdb fail" << endl;
        throw std::runtime_error("reset_to_backup (echo_cmd) monetdb fail"); 
    }
	string monetdb_source = "mclient -d " + test_db + "/tmp/monetdb_bk.sql";
    if (system(monetdb_source.c_str()) != 0) 
        throw std::runtime_error("reset_to_backup (monetdb_source) error, return -1");
}

string dut_monetdb::begin_stmt() {
    return "START TRANSACTION ISOLATION LEVEL READ COMMITTED";
}

string dut_monetdb::commit_stmt() {
    return "COMMIT";
}

string dut_monetdb::abort_stmt() {
    return "ROLLBACK";
}

void dut_monetdb::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table:tables_name) {
        vector<vector<string>> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";

		auto hdl = mapi_query(dbh, query.c_str());
		if (mapi_error(dbh) != MOK) {
            string err = mapi_result_error(hdl);
            cerr << "Cannot get content of " + table << endl;
            cerr << "Error: " + err << endl;
            continue;
        }

		auto column_num = mapi_get_field_count(hdl);
		while (mapi_fetch_row(hdl)) {
			vector<string> row_output;
			for (int i = 0; i < column_num; i++) {
                string str;
				auto char_str = mapi_fetch_field(hdl, i);
                if (char_str == NULL)
                    str = "NULL";
                else
                    str = char_str;
                row_output.push_back(str);
            }
            table_content.push_back(row_output);
		}
		mapi_close_handle(hdl);
        content[table] = table_content;
    }
	return;
}

pid_t dut_monetdb::fork_db_server()
{
    pid_t child = fork();
    if (child < 0) {
        throw std::runtime_error(string("Fork monetdb server fail") + "\nLocation: " + debug_info);
    }

    if (child == 0) {
        char *server_argv[128];
        int i = 0;
        server_argv[i++] = (char *)"/usr/local/bin/monetdbd"; // path of tiup
        server_argv[i++] = (char *)"start";
        server_argv[i++] = (char *)"-n ";
        server_argv[i++] = (char *)"monetdb_farm";
        server_argv[i++] = NULL;
        execv(server_argv[0], server_argv);
        cerr << "fork mysql server fail \nLocation: " + debug_info << endl; 
    }
    
    sleep(3);
    cout << "server pid: " << child << endl;
    return child;
}

int dut_monetdb::save_backup_file(string path)
{
    string cp_cmd = "cp /tmp/monetdb_bk.sql " + path;
    return system(cp_cmd.c_str());
}
