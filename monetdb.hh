/// @file
/// @brief schema and dut classes for MonetDB


#ifndef MONETDB_HH
#define MONETDB_HH

#include "dut.hh"
#include "relmodel.hh"
#include "schema.hh"
#include <string.h>

#include <mapi.h>

struct monetdb_connection {
	Mapi dbh;
	string test_db;
	unsigned int test_port;
	monetdb_connection(string db, unsigned int port);
	void q(const char* query);
	~monetdb_connection();
};

struct schema_monetdb : schema, monetdb_connection {
	schema_monetdb(string db, unsigned int port);
	virtual std::string quote_name(const std::string &id) {
		return id;
	}
};

struct dut_monetdb : dut_base, monetdb_connection {
	virtual void test(const string &stmt, vector<vector<string>>* output = NULL, int* affected_row_num = NULL);
    virtual void reset(void);
    virtual void backup(void);
    virtual void reset_to_backup(void);
    virtual string commit_stmt();
    virtual string abort_stmt();
    virtual string begin_stmt();
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    
	static pid_t fork_db_server();
	static int save_backup_file(string path);
	
	dut_monetdb(string db, unsigned int port);
};

#endif
