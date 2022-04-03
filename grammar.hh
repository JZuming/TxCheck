/// @file
/// @brief grammar: Top-level and unsorted grammar productions

#ifndef GRAMMAR_HH
#define GRAMMAR_HH

#include <ostream>
#include "relmodel.hh"
#include <memory>
#include "schema.hh"

#include "prod.hh"
#include "expr.hh"

#include <set>
using std::shared_ptr;

struct table_ref : prod {
  vector<shared_ptr<named_relation> > refs;
  static shared_ptr<table_ref> factory(prod *p);
  table_ref(prod *p) : prod(p) { }
  virtual ~table_ref() { }
};

struct table_or_query_name : table_ref {
    virtual void out(std::ostream &out);
    table_or_query_name(prod *p);
    table_or_query_name(prod *p, table *target_table);
    virtual ~table_or_query_name() { }
    named_relation *t;
};

struct target_table : table_ref {
  virtual void out(std::ostream &out);
  target_table(prod *p, table *victim = 0);
  virtual ~target_table() { }
  table *victim_;
};

struct table_sample : table_ref {
  virtual void out(std::ostream &out);
  table_sample(prod *p);
  virtual ~table_sample() { }
  struct table *t;
private:
  string method;
  double percent;
};

struct table_subquery : table_ref {
  bool is_lateral;
  virtual void out(std::ostream &out);
  shared_ptr<struct query_spec> query;
  table_subquery(prod *p, bool lateral = false);
  virtual ~table_subquery();
  virtual void accept(prod_visitor *v);
};

struct lateral_subquery : table_subquery {
  lateral_subquery(prod *p)
    : table_subquery(p, true) {  }
};

struct join_cond : prod {
     static shared_ptr<join_cond> factory(prod *p, table_ref &lhs, table_ref &rhs);
     join_cond(prod *p, table_ref &lhs, table_ref &rhs)
	  : prod(p) { (void) lhs; (void) rhs;}
};

struct simple_join_cond : join_cond {
     std::string condition;
     simple_join_cond(prod *p, table_ref &lhs, table_ref &rhs);
     virtual void out(std::ostream &out);
};

struct expr_join_cond : join_cond {
     struct scope joinscope;
     shared_ptr<bool_expr> search;
     expr_join_cond(prod *p, table_ref &lhs, table_ref &rhs);
     virtual void out(std::ostream &out);
     virtual void accept(prod_visitor *v) {
	  search->accept(v);
	  v->visit(this);
     }
};

struct joined_table : table_ref {
    virtual void out(std::ostream &out);  
    joined_table(prod *p);
    std::string type;
    std::string alias;
    virtual std::string ident() { return alias; }
    shared_ptr<table_ref> lhs;
    shared_ptr<table_ref> rhs;
    shared_ptr<join_cond> condition;
    virtual ~joined_table() {
    }
    virtual void accept(prod_visitor *v) {
        lhs->accept(v);
        rhs->accept(v);
        if (type == "inner" || type == "left outer")
            condition->accept(v);
        v->visit(this);
    }
};

struct from_clause : prod {
    std::vector<shared_ptr<table_ref> > reflist;
    virtual void out(std::ostream &out);
    from_clause(prod *p, bool only_table = false);
    from_clause(prod *p, table *from_table);
    ~from_clause() { }
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        for (auto p : reflist)
            p->accept(v);
    }
};

struct select_list : prod {
    std::vector<shared_ptr<value_expr> > value_exprs;
    std::vector<shared_ptr<named_relation> > *prefer_refs;
    relation derived_table;
    int columns = 0;
    select_list(prod *p, 
              std::vector<shared_ptr<named_relation> > *refs = 0, 
              vector<sqltype *> *pointed_type = NULL,
              bool select_all = false);
    virtual void out(std::ostream &out);
    ~select_list() { }
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        for (auto p : value_exprs)
            p->accept(v);
    }
};

struct group_clause: prod {
    struct scope myscope;
    shared_ptr<struct select_list> modified_select_list;
    group_clause(prod *p, struct scope *s, 
            shared_ptr<struct select_list> select_list,
            std::vector<shared_ptr<named_relation> > *from_refs);
    string target_ref;
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
    }
};

struct named_window : prod {
    struct scope myscope;
    virtual void out(std::ostream &out);
    virtual ~named_window() { }
    named_window(prod *p, struct scope *s);
    string window_name;
    vector<shared_ptr<value_expr> > partition_by;
    vector<shared_ptr<value_expr> > order_by;

    virtual void accept(prod_visitor *v) {
        v->visit(this);
        for (auto p : partition_by)
            p->accept(v);
        for (auto p : order_by)
            p->accept(v);
    }
};

struct query_spec : prod {
    std::string set_quantifier;
    shared_ptr<struct from_clause> from_clause;
    shared_ptr<struct select_list> select_list;
    shared_ptr<bool_expr> search;
    
    bool has_group;
    shared_ptr<struct group_clause> group_clause;

    bool has_window;
    shared_ptr<struct named_window> window_clause;

    bool has_order;
    vector<shared_ptr<value_expr> > order_clause;
    
    bool has_limit;
    int limit_num;

    struct scope myscope;
    virtual void out(std::ostream &out);

    query_spec(prod *p, struct scope *s,
                bool lateral = 0, 
                vector<sqltype *> *pointed_type = NULL,
                bool txn_mode = false);
    
    query_spec(prod *p, struct scope *s,
              table *from_table, 
              shared_ptr<bool_expr> where_search);
    
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        select_list->accept(v);
        from_clause->accept(v);
        search->accept(v);
        if (has_group)
            group_clause->accept(v);
        if (has_window)
            window_clause->accept(v);
    }
};

struct select_for_update : query_spec {
  const char *lockmode;
  virtual void out(std::ostream &out);
  select_for_update(prod *p, struct scope *s, bool lateral = 0);
};

struct prepare_stmt : prod {
  query_spec q;
  static long seq;
  long id;
  virtual void out(std::ostream &out) {
    out << "prepare prep" << id << " as " << q;
  }
  prepare_stmt(prod *p) : prod(p), q(p, scope) {
    id = seq++;
  }
  virtual void accept(prod_visitor *v) {
    v->visit(this);
    q.accept(v);
  }
};

struct modifying_stmt : prod {
    table *victim;
    struct scope myscope;
    modifying_stmt(prod *p, struct scope *s, struct table *victim = 0);
//   shared_ptr<modifying_stmt> modifying_stmt::factory(prod *p, struct scope *s);
    virtual void pick_victim();
};

struct delete_stmt : modifying_stmt {
    shared_ptr<bool_expr> search;
    delete_stmt(prod *p, struct scope *s, table *v = 0);
    virtual ~delete_stmt() { }
    virtual void out(std::ostream &out) {
        out << "delete from " << victim->ident();
        indent(out);
        out << "where " << std::endl << *search;
    }
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        search->accept(v);
    }
};

struct delete_returning : delete_stmt {
  shared_ptr<struct select_list> select_list;
  delete_returning(prod *p, struct scope *s, table *victim = 0);
  virtual void out(std::ostream &out) {
    delete_stmt::out(out);
    out << std::endl << "returning " << *select_list;
  }
  virtual void accept(prod_visitor *v) {
    v->visit(this);
    search->accept(v);
    select_list->accept(v);
  }
};

struct insert_stmt : modifying_stmt {
    vector<vector<shared_ptr<value_expr> > > value_exprs_vector;
    // vector<shared_ptr<value_expr> > value_exprs;
    insert_stmt(prod *p, struct scope *s, table *victim = 0, bool only_const = false);
    virtual ~insert_stmt() {  }
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        for (auto &value_exprs: value_exprs_vector)
            for (auto p : value_exprs) 
                p->accept(v);
    }
};

struct set_list : prod {
    vector<shared_ptr<value_expr> > value_exprs;
    vector<string> names;
    set_list(prod *p, table *target);
    virtual ~set_list() {  }
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        for (auto p : value_exprs) 
            p->accept(v);
    }
};

struct upsert_stmt : insert_stmt {
  shared_ptr<struct set_list> set_list;
  string constraint;
  shared_ptr<bool_expr> search;
  upsert_stmt(prod *p, struct scope *s, table *v = 0);
  virtual void out(std::ostream &out) {
    insert_stmt::out(out);
    out << " on conflict on constraint " << constraint << " do update ";
    out << *set_list << " where " << *search;
  }
  virtual void accept(prod_visitor *v) {
    insert_stmt::accept(v);
    set_list->accept(v);
    search->accept(v);
  }
  virtual ~upsert_stmt() {  }
};

struct update_stmt : modifying_stmt {
    shared_ptr<bool_expr> search;
    shared_ptr<struct set_list> set_list;
    update_stmt(prod *p, struct scope *s, table *victim = 0);
    virtual ~update_stmt() {  }
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        search->accept(v);
    }
};

struct when_clause : prod {
  bool matched;
  shared_ptr<bool_expr> condition;  
//   shared_ptr<prod> merge_action;
  when_clause(struct merge_stmt *p);
  virtual ~when_clause() { }
  static shared_ptr<when_clause> factory(struct merge_stmt *p);
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v);
};

struct when_clause_update : when_clause {
  shared_ptr<struct set_list> set_list;
  struct scope myscope;
  when_clause_update(struct merge_stmt *p);
  virtual ~when_clause_update() { }
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v);
};

struct when_clause_insert : when_clause {
  vector<shared_ptr<value_expr> > exprs;
  when_clause_insert(struct merge_stmt *p);
  virtual ~when_clause_insert() { }
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v);
};

struct merge_stmt : modifying_stmt {
  merge_stmt(prod *p, struct scope *s, table *victim = 0);
  shared_ptr<table_ref> target_table_;
  shared_ptr<table_ref> data_source;
  shared_ptr<join_cond> join_condition;
  vector<shared_ptr<when_clause> > clauselist;
  virtual ~merge_stmt() {  }
  virtual void out(std::ostream &out);
  virtual void accept(prod_visitor *v);
};

struct update_returning : update_stmt {
  shared_ptr<struct select_list> select_list;
  update_returning(prod *p, struct scope *s, table *victim = 0);
  virtual void out(std::ostream &out) {
    update_stmt::out(out);
    out << std::endl << "returning " << *select_list;
  }
  virtual void accept(prod_visitor *v) {
    v->visit(this);
    search->accept(v);
    set_list->accept(v);
    select_list->accept(v);
  }
};

struct common_table_expression : prod {
    vector<shared_ptr<prod> > with_queries;
    shared_ptr<prod> query;
    vector<shared_ptr<named_relation> > refs;
    struct scope myscope;
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v);
    common_table_expression(prod *parent, struct scope *s, bool txn_mode = false);
};

struct create_table_stmt: prod {
    shared_ptr<struct table> created_table;
    struct scope myscope;

    vector<string> constraints;
    bool has_check;
    shared_ptr<struct bool_expr> check_expr;
    virtual void out(std::ostream &out);
    create_table_stmt(prod *parent, struct scope *s);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        if (has_check)
            check_expr->accept(v);
    }
};

struct create_table_select_stmt: prod {
    string tatble_name;
    shared_ptr<struct query_spec> subquery;
    struct scope myscope;
    bool is_base_table;
    virtual void out(std::ostream &out);
    create_table_select_stmt(prod *parent, struct scope *s, int is_base = -1);
    virtual void accept(prod_visitor *v) {
        subquery->accept(v);
        v->visit(this);
    }
};

struct alter_table_stmt: prod {
    // shared_ptr<struct table> created_table;
    struct scope myscope;
    int stmt_type; // 0: rename table, 1: rename column, 2: add column, 3: drop column
    string stmt_string;
    virtual void out(std::ostream &out);
    alter_table_stmt(prod *parent, struct scope *s);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
    }
};

struct drop_table_stmt: prod {
    // shared_ptr<struct table> created_table;
    struct scope myscope;
    string stmt_string;
    virtual void out(std::ostream &out);
    drop_table_stmt(prod *parent, struct scope *s);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
    }
};

struct create_index_stmt: prod {
    struct scope myscope;
    string index_name;
    string table_name;
    vector<string> indexed_columns; 
    bool is_unique;
    create_index_stmt(prod *parent, struct scope *s);
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
    }
};

struct create_trigger_stmt: prod {
    struct scope myscope;
    string trigger_name;
    string trigger_time; // after or before
    string trigger_event; // delete, insert or update
    string table_name;
    vector<shared_ptr<struct modifying_stmt>> doing_stmts;

    virtual void out(std::ostream &out);
    create_trigger_stmt(prod *parent, struct scope *s);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        for (auto &stmt : doing_stmts)
            stmt->accept(v); 
    }
};

struct unioned_query : prod { //can be used as same as query_spec
    struct scope myscope;
    shared_ptr<query_spec> lhs;
    shared_ptr<query_spec> rhs;
    string type; // " union " or " union all "
    virtual void out(std::ostream &out);  
    unioned_query(prod *p, struct scope *s, bool lateral = 0, vector<sqltype *> *pointed_type = NULL);
    virtual ~unioned_query() {}
    virtual void accept(prod_visitor *v) {
        lhs->accept(v);
        rhs->accept(v);
        v->visit(this);
    }
};

struct insert_select_stmt : modifying_stmt {
    shared_ptr<struct query_spec> target_subquery;
    insert_select_stmt(prod *p, struct scope *s, table *victim = 0);
    virtual ~insert_select_stmt() {  }
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v) {
        v->visit(this);
        target_subquery->accept(v);
    }
};

struct txn_string_stmt : prod {
  string stmt;
  txn_string_stmt(prod *p, string bs) : 
    prod(p), stmt(bs) {}
  virtual ~txn_string_stmt() {}
  virtual void out(std::ostream &out) {out << stmt;}
  virtual void accept(prod_visitor *v) {
    v->visit(this);
  }
};

shared_ptr<prod> statement_factory(struct scope *s);
shared_ptr<prod> ddl_statement_factory(struct scope *s);
shared_ptr<prod> basic_dml_statement_factory(struct scope *s);
shared_ptr<prod> txn_statement_factory(struct scope *s);

#endif
