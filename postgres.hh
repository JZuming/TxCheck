/// @file
/// @brief schema and dut classes for PostgreSQL

#ifndef POSTGRES_HH
#define POSTGRES_HH

#include "dut.hh"
#include "relmodel.hh"
#include "schema.hh"

#include <pqxx/pqxx>

extern "C" {
#include <libpq-fe.h>
}

#include <sys/time.h>

#define POSTGRES_STMT_BLOCK_MS 200

#define OID long

struct pg_type : sqltype {
    OID oid_;
    char typdelim_;
    OID typrelid_;
    OID typelem_;
    OID typarray_;
    char typtype_;
    pg_type(string name,
        OID oid,
        char typdelim,
        OID typrelid,
        OID typelem,
        OID typarray,
        char typtype)
        : sqltype(name), oid_(oid), typdelim_(typdelim), typrelid_(typrelid),
          typelem_(typelem), typarray_(typarray), typtype_(typtype) { }

    virtual bool consistent(struct sqltype *rvalue);
    bool consistent_(sqltype *rvalue);
};


struct schema_pqxx : public schema {
    pqxx::connection c;
    map<OID, pg_type*> oid2type;
    map<string, pg_type*> name2type;

    virtual std::string quote_name(const std::string &id) {
        return c.quote_name(id);
    }
    schema_pqxx(std::string &conninfo, bool no_catalog);
};

struct dut_pqxx : dut_base {
    pqxx::connection c;
    virtual void test(const std::string &stmt);
    dut_pqxx(std::string conninfo);
};

struct dut_libpq : dut_base {
    PGconn *conn = 0;
    // std::string conninfo_;
    string test_db;
    unsigned int test_port;

    virtual void test(const std::string &stmt, vector<vector<string>>* output = NULL, int* affected_row_num = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);    

    virtual string commit_stmt();
    virtual string abort_stmt();
    virtual string begin_stmt();

    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    
    static int save_backup_file(string path);
    static pid_t fork_db_server();
    
    bool has_sent_sql;
    string sent_sql;
    int process_id;

    bool check_whether_block();
    void block_test(const std::string &stmt, std::vector<std::string>* output = NULL, int* affected_row_num = NULL);

    void command(const std::string &stmt);
    void connect(string db, unsigned int port);
    dut_libpq(string db, unsigned int port);
    ~dut_libpq();
};

#endif
