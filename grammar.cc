#include <typeinfo>
#include <numeric>
#include <algorithm>
#include <stdexcept>
#include <cassert>

#include "random.hh"
#include "relmodel.hh"
#include "grammar.hh"
#include "schema.hh"
#include "impedance.hh"

using namespace std;

int use_group = 2; // 0->no group, 1->use group, 2->to_be_define
int in_update_set_list = 0;
int in_in_clause = 0; // 0-> not in "in" clause, 1-> in "in" clause
int in_check_clause = 0; // 0-> not in "check" clause, 1-> in "check" clause
set<string> update_used_column_ref;

int write_op_id = 0;
static int row_id = 0;

static void exclude_tables(
    table *victim,
    vector<named_relation *> &target_tables,
    multimap<sqltype*, table*> &target_t_with_c_of_type,
    vector<named_relation *> &excluded_tables,
    vector<pair<sqltype*, table*>> &excluded_t_with_c_of_type)
{
    for (std::size_t i = 0; i < target_tables.size(); i++) {
        if (target_tables[i]->ident() == victim->ident()) {
            excluded_tables.push_back(target_tables[i]);
            target_tables.erase(target_tables.begin() + i);
            i--;
        }
    }

    for (auto iter = target_t_with_c_of_type.begin(); iter != target_t_with_c_of_type.end();) {
        if (iter->second->ident() == victim->ident()) {
            excluded_t_with_c_of_type.push_back(*iter);
            iter = target_t_with_c_of_type.erase(iter);
        }
        else
            iter++;
    }
}

static void recover_tables(
    vector<named_relation *> &target_tables,
    multimap<sqltype*, table*> &target_t_with_c_of_type,
    vector<named_relation *> &excluded_tables,
    vector<pair<sqltype*, table*>> &excluded_t_with_c_of_type)
{
    for (std::size_t i = 0; i < excluded_tables.size(); i++) {
        target_tables.push_back(excluded_tables[i]);
    }
    for (auto iter = excluded_t_with_c_of_type.begin(); iter != excluded_t_with_c_of_type.end(); iter++) {
        target_t_with_c_of_type.insert(*iter);
    }
}

shared_ptr<table_ref> table_ref::factory(prod *p) {
    try {
        if (p->level < 3 + d6()) {
            if (d6() > 3 && p->level < d6())
	            return make_shared<table_subquery>(p);
            if (d6() > 3)
	            return make_shared<joined_table>(p);
        }
        return make_shared<table_or_query_name>(p);
    } catch (runtime_error &e) {
        p->retry();
    }
    return factory(p);
}

table_or_query_name::table_or_query_name(prod *p) : table_ref(p) {
    t = random_pick(scope->tables);
    refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("ref"), t));
}

table_or_query_name::table_or_query_name(prod *p, table *target_table) : table_ref(p) {
    // in this case, do not use aliase
    t = target_table;
    refs.push_back(make_shared<aliased_relation>(t->ident(), t));
}

void table_or_query_name::out(std::ostream &out) {
    if (refs[0]->ident() != t->ident())
        out << t->ident() << " as " << refs[0]->ident();
    else
        out << t->ident();
}

target_table::target_table(prod *p, table *victim) : table_ref(p)
{
  while (! victim
	 || victim->schema == "pg_catalog"
	 || !victim->is_base_table
	 || !victim->columns().size()) {
    struct named_relation *pick = random_pick(scope->tables);
    victim = dynamic_cast<table *>(pick);
    retry();
  }
  victim_ = victim;
  refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("target"), victim));
}

void target_table::out(std::ostream &out) {
  out << victim_->ident() << " as " << refs[0]->ident();
}

table_sample::table_sample(prod *p) : table_ref(p) {
  match();
  retry_limit = 1000; /* retries are cheap here */
  do {
    auto pick = random_pick(scope->schema->base_tables);
    t = dynamic_cast<struct table*>(pick);
    retry();
  } while (!t || !t->is_base_table);
  
  refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("sample"), t));
  percent = 0.1 * d100();
  method = (d6() > 2) ? "system" : "bernoulli";
}

void table_sample::out(std::ostream &out) {
  out << t->ident() <<
    " as " << refs[0]->ident() <<
    " tablesample " << method <<
    " (" << percent << ") ";
}

table_subquery::table_subquery(prod *p, bool lateral)
  : table_ref(p), is_lateral(lateral) {
    query = make_shared<query_spec>(this, scope, lateral);
    string alias = scope->stmt_uid("subq");
    relation *aliased_rel = &query->select_list->derived_table;
    refs.push_back(make_shared<aliased_relation>(alias, aliased_rel));
}

table_subquery::~table_subquery() { }

void table_subquery::accept(prod_visitor *v) {
    query->accept(v);
    v->visit(this);
}

shared_ptr<join_cond> join_cond::factory(prod *p, table_ref &lhs, table_ref &rhs)
{
    try {
#if (!defined TEST_CLICKHOUSE) && (!defined TEST_TIDB)
    if (d6() < 6)
        return make_shared<expr_join_cond>(p, lhs, rhs);
    else
#endif
        return make_shared<simple_join_cond>(p, lhs, rhs);
    } catch (runtime_error &e) {
        p->retry();
    }
    return factory(p, lhs, rhs);
}

simple_join_cond::simple_join_cond(prod *p, table_ref &lhs, table_ref &rhs)
     : join_cond(p, lhs, rhs)
{
retry:
  named_relation *left_rel = &*random_pick(lhs.refs);
  
  if (!left_rel->columns().size())
    { retry(); goto retry; }

  named_relation *right_rel = &*random_pick(rhs.refs);

  column &c1 = random_pick(left_rel->columns());

  for (auto c2 : right_rel->columns()) {
    if (c1.type == c2.type) {
      condition +=
	left_rel->ident() + "." + c1.name + " = " + right_rel->ident() + "." + c2.name + " ";
      break;
    }
  }
  if (condition == "") {
    retry(); goto retry;
  }
}

void simple_join_cond::out(std::ostream &out) {
     out << condition;
}

expr_join_cond::expr_join_cond(prod *p, table_ref &lhs, table_ref &rhs)
     : join_cond(p, lhs, rhs), joinscope(p->scope)
{
    joinscope.refs.clear(); // only use the refs in lhs and rhs
    scope = &joinscope;
    for (auto ref: lhs.refs)
        joinscope.refs.push_back(&*ref);
    for (auto ref: rhs.refs)
        joinscope.refs.push_back(&*ref);
    search = bool_expr::factory(this);
}

void expr_join_cond::out(std::ostream &out) {
     out << *search;
}

joined_table::joined_table(prod *p) : table_ref(p) {
    lhs = table_ref::factory(this);
    rhs = table_ref::factory(this);
#ifdef TEST_CLICKHOUSE
    while (auto ret = dynamic_pointer_cast<joined_table>(lhs)) {
        lhs.reset();
        lhs = table_ref::factory(this);
    }
    while (auto ret = dynamic_pointer_cast<joined_table>(rhs)) {
        rhs.reset();
        rhs = table_ref::factory(this);
    }
#endif

    auto choice = d6();
    if (choice <= 2) 
        type = "cross";
    else if (choice <= 4)
        type = "inner";
    else 
        type = "left outer";

    auto tmp_group = use_group;
    use_group = 0;
    if (type == "inner" || type == "left outer")
        condition = join_cond::factory(this, *lhs, *rhs);
    use_group = tmp_group;

    for (auto ref: lhs->refs)
        refs.push_back(ref);
    for (auto ref: rhs->refs)
        refs.push_back(ref);
}

void joined_table::out(std::ostream &out) {
#if (!defined TEST_MONETDB) && (!defined TEST_CLICKHOUSE)
    out << "(";
#endif
    out << *lhs;
    indent(out);
    out << type << " join " << *rhs;
    indent(out);
    if (type == "inner" || type == "left outer")
        out << "on (" << *condition << ")";
#if (!defined TEST_MONETDB) && (!defined TEST_CLICKHOUSE)
    out << ")";
#endif
}

void table_subquery::out(std::ostream &out) {
    if (is_lateral)
        out << "lateral ";
    out << "(" << *query << ") as " << refs[0]->ident();
}

void from_clause::out(std::ostream &out) {
    if (! reflist.size())
        return;
    out << "from ";

    for (auto r = reflist.begin(); r < reflist.end(); r++) {
        indent(out);
        out << **r;
        if (r + 1 != reflist.end())
            out << ",";
    }
}

from_clause::from_clause(prod *p, bool only_table) : prod(p) {
    if (only_table)
        reflist.push_back(make_shared<table_or_query_name>(this));
    else
        reflist.push_back(table_ref::factory(this));
    for (auto r : reflist.back()->refs)
        scope->refs.push_back(&*r);
}

from_clause::from_clause(prod *p, table *from_table) : prod(p) {
    reflist.push_back(make_shared<table_or_query_name>(this, from_table));
    for (auto r : reflist.back()->refs)
        scope->refs.push_back(&*r);
}

select_list::select_list(prod *p, 
                    vector<shared_ptr<named_relation> > *refs, 
                    vector<sqltype *> *pointed_type,
                    bool select_all) :
 prod(p), prefer_refs(refs)
{
    if (select_all && pointed_type != NULL) 
        throw std::runtime_error("select_all and pointed_type cannot be used at the same time");
    
    if (select_all && refs == NULL)
        throw std::runtime_error("select_all and refs should be used at the same time");
    
    if (select_all) { // pointed_type is null and prefer_refs is not null
        auto r = &*random_pick(*prefer_refs);
        for (auto& col : r->columns()) {
            auto expr = make_shared<column_reference>(this, (sqltype *)0, prefer_refs);
            expr->type = col.type;
            expr->reference = r->ident() + "." + col.name;
            expr->table_ref = r->ident();
            
            value_exprs.push_back(expr);
            ostringstream name;
            name << "c" << columns++;
            sqltype *t = expr->type;
            derived_table.columns().push_back(column(name.str(), t));
        }
        return;
    }

    if (pointed_type == NULL) {
        do {
            shared_ptr<value_expr> e = value_expr::factory(this, (sqltype *)0, prefer_refs);
            value_exprs.push_back(e);
            ostringstream name;
            name << "c" << columns++;
            sqltype *t = e->type;
            assert(t);
            derived_table.columns().push_back(column(name.str(), t));
        } while (d6() > 1);

        return;
    }

    // pointed_type is not null
    for (size_t i = 0; i < pointed_type->size(); i++) {
        shared_ptr<value_expr> e = value_expr::factory(this, (*pointed_type)[i], prefer_refs);
        value_exprs.push_back(e);
        ostringstream name;
        name << "c" << columns++;
        sqltype *t = e->type;
        assert(t);
        derived_table.columns().push_back(column(name.str(), t));
    }
}

void select_list::out(std::ostream &out)
{
    int i = 0;
    for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++) {
        indent(out);
        out << **expr << " as " << derived_table.columns()[i].name;
        i++;
        if (expr+1 != value_exprs.end())
            out << ", ";
    }
}

void query_spec::out(std::ostream &out) {
    out << "select " << set_quantifier << " "
        << *select_list;
    indent(out);
    out << *from_clause;
    indent(out);
    out << "where ";
    out << *search;

    if (has_group) {
        indent(out);
        out << "group by ";
        out << *group_clause;
    }

    if (has_window) {
        indent(out);
        out << *window_clause;
    }

    if (has_order) {
        indent(out);
        out << "order by ";
        auto &selected_columns = select_list->derived_table.columns();
        auto select_list_size = selected_columns.size();
        for (std::size_t i = 0; i < select_list_size; i++) {
            out << selected_columns[i].name;
            if (i + 1 < select_list_size)
                out << ", ";
            else
                out << " ";
        }

        if (asc) 
            out << "asc";
        else
            out << "desc";
    }

    if (has_limit)
        out << " limit " << limit_num;
}

struct for_update_verify : prod_visitor {
  virtual void visit(prod *p) {
    if (dynamic_cast<window_function*>(p))
      throw("window function");
    joined_table* join = dynamic_cast<joined_table*>(p);
    if (join && join->type != "inner")
      throw("outer join");
    query_spec* subquery = dynamic_cast<query_spec*>(p);
    if (subquery)
      subquery->set_quantifier = "";
    table_or_query_name* tab = dynamic_cast<table_or_query_name*>(p);
    if (tab) {
      table *actual_table = dynamic_cast<table*>(tab->t);
      if (actual_table && !actual_table->is_insertable)
	throw("read only");
      if (actual_table->name.find("pg_"))
	throw("catalog");
    }
    table_sample* sample = dynamic_cast<table_sample*>(p);
    if (sample) {
      table *actual_table = dynamic_cast<table*>(sample->t);
      if (actual_table && !actual_table->is_insertable)
	throw("read only");
      if (actual_table->name.find("pg_"))
	throw("catalog");
    }
  } ;
};


select_for_update::select_for_update(prod *p, struct scope *s, bool lateral)
  : query_spec(p,s,lateral)
{
  static const char *modes[] = {
    "update",
    "share",
    "no key update",
    "key share",
  };

  try {
    for_update_verify v1;
    this->accept(&v1);

  } catch (const char* reason) {
    lockmode = 0;
    return;
  }
  lockmode = modes[d6()%(sizeof(modes)/sizeof(*modes))];
  set_quantifier = ""; // disallow distinct
}

void select_for_update::out(std::ostream &out) {
  query_spec::out(out);
  if (lockmode) {
    indent(out);
    out << " for " << lockmode;
  }
}

query_spec::query_spec(prod *p, 
                    struct scope *s, 
                    bool lateral, 
                    vector<sqltype *> *pointed_type,
                    bool txn_mode) :
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;
    has_group = false;
    has_window = false;
    has_order = false;
    has_limit = false;

    if (txn_mode == true)
        use_group = 0;

    if (use_group == 2) { // confirm whether use group
        if (d6() == 1) use_group = 1;
        else use_group = 0;
    }

    if (lateral)
        scope->refs = s->refs;
    
    int tmp_group = use_group; // store use_group temporarily
    
    // from clause can use "group by" or not.
    use_group = 2; 
    // txn testing: need to know which rows are read, so just from a table
    from_clause = make_shared<struct from_clause>(this, txn_mode);

    use_group = 0; // cannot use "group by" in "where" and "select" clause.
    search = bool_expr::factory(this); 

    // txn testing: need to know all info of columns so select * from table_name where
    select_list = make_shared<struct select_list>(this, &from_clause->reflist.back()->refs, pointed_type, txn_mode);
    
    set_quantifier = (d9() == 1) ? "distinct" : "";
    use_group = tmp_group; // recover use_group

    if (use_group == 1) {
        group_clause = make_shared<struct group_clause>(this, this->scope, select_list, &from_clause->reflist.back()->refs);
        has_group = true;
    }

    int only_allow_level_0 = false;
#ifdef TEST_MONETDB
    only_allow_level_0 = true;
#endif

    if (only_allow_level_0 && this->level != 0)
        return;

#ifndef TEST_SQLITE
    if (txn_mode == false && has_group == false && d9() == 1) {
        has_window = true;
        window_clause = make_shared<named_window>(this, this->scope);
        auto &select_exprs = select_list->value_exprs;
        auto size = select_exprs.size();
        
        for (size_t i = 0; i < size; i++) {
            if (!dynamic_pointer_cast<window_function>(select_exprs[i]))
                continue;
            if (d6() > 3) // 50%
                continue;
            auto new_expr = make_shared<win_func_using_exist_win>(this, select_exprs[i]->type, window_clause->window_name);
            select_exprs.erase(select_exprs.begin() + i);
            select_exprs.insert(select_exprs.begin() + i, new_expr);
            select_list->derived_table.columns()[i].type = new_expr->type;
        }
    }
#endif

    if (has_group == false && d6() == 1) {
        has_order = true;
        if (d6() > 3)
            asc = true;
        else
            asc = false;
    }

    // if (in_in_clause == 0 && d6() < 3) { // the subquery in clause cannot use limit (mysql) 
    //     has_limit = true;
    //     limit_num = d100() + d100();
    // }
}

query_spec::query_spec(prod *p, struct scope *s,
              table *from_table, 
              shared_ptr<bool_expr> where_search) :
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;
    has_group = false;
    has_window = false;
    has_order = false;
    has_limit = false;
    use_group = 0;

    from_clause = make_shared<struct from_clause>(this, from_table);
    search = where_search; 
    select_list = make_shared<struct select_list>(this, &from_clause->reflist.back()->refs, (vector<sqltype *> *)NULL, true);
}

query_spec::query_spec(prod *p, struct scope *s,
              table *from_table, 
              op *target_op, 
              shared_ptr<value_expr> left_operand,
              shared_ptr<value_expr> right_operand) :
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;
    has_group = false;
    has_window = false;
    has_order = false;
    has_limit = false;
    use_group = 0;

    from_clause = make_shared<struct from_clause>(this, from_table);
    search = make_shared<struct comparison_op>(this, target_op, left_operand, right_operand);
    select_list = make_shared<struct select_list>(this, &from_clause->reflist.back()->refs, (vector<sqltype *> *)NULL, true);
}

long prepare_stmt::seq;

void modifying_stmt::pick_victim()
{
  do {
      struct named_relation *pick = random_pick(scope->tables);
      victim = dynamic_cast<struct table*>(pick);
      retry();
    } while (! victim
	   || victim->schema == "pg_catalog"
	   || !victim->is_base_table
	   || !victim->columns().size());
}

modifying_stmt::modifying_stmt(prod *p, struct scope *s, table *victim)
  : prod(p), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    if (!victim)
        pick_victim();
}


delete_stmt::delete_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p,s,v) {
    scope->refs.push_back(victim);
    write_op_id++;
    
    // dont select the target table
    vector<named_relation *> excluded_tables;
    vector<pair<sqltype*, table*>> excluded_t_with_c_of_type;
    exclude_tables(victim, scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);

    search = bool_expr::factory(this);
    
    recover_tables(scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);
}

delete_returning::delete_returning(prod *p, struct scope *s, table *victim)
  : delete_stmt(p, s, victim) {
  match();
  select_list = make_shared<struct select_list>(this);
}

insert_stmt::insert_stmt(prod *p, struct scope *s, table *v, bool only_const)
  : modifying_stmt(p, s, v)
{
    match();
    write_op_id++;

    // dont select the target table
    vector<named_relation *> excluded_tables;
    vector<pair<sqltype*, table*>> excluded_t_with_c_of_type;
    exclude_tables(victim, scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);
    
    auto insert_num = d6();
    for (auto i = 0; i < insert_num; i++) {
        vector<shared_ptr<value_expr> > value_exprs;
        for (auto col : victim->columns()) {
            if (col.name == "wkey") {
                auto  expr = make_shared<const_expr>(this, col.type);
                assert(expr->type == col.type);
                expr->expr = to_string(write_op_id); // use write_op_id
                value_exprs.push_back(expr);
                continue;
            }

            if (col.name == "pkey") {
                row_id++;
                auto  expr = make_shared<const_expr>(this, col.type);
                assert(expr->type == col.type);
                expr->expr = to_string(row_id); // use write_op_id
                value_exprs.push_back(expr);
                continue;
            }

            if (only_const) {
                auto  expr = make_shared<const_expr>(this, col.type);
                assert(expr->type == col.type);
                value_exprs.push_back(expr);
            }
            else {
                auto expr = value_expr::factory(this, col.type);
                assert(expr->type == col.type);
                value_exprs.push_back(expr);
            }
        }
        value_exprs_vector.push_back(value_exprs);
    }

    recover_tables(scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);
}

void insert_stmt::out(std::ostream &out)
{
    out << "insert into " << victim->ident() << " ";

    if (!value_exprs_vector.size()) {
        out << "default values";
        return;
    }

    out << "values ";

    for (auto value_exprs = value_exprs_vector.begin();
      value_exprs != value_exprs_vector.end(); value_exprs++) {
        indent(out);
        out << "(";
        for (auto expr = value_exprs->begin();
          expr != value_exprs->end(); expr++) {
            out << **expr;
            if (expr + 1 != value_exprs->end())
                out << ", ";
        }
        out << ")";
        if (value_exprs + 1 != value_exprs_vector.end()) 
            out << ", ";
    }
}

set_list::set_list(prod *p, table *target) : prod(p)
{
    auto tmp_update_set = in_update_set_list;
    in_update_set_list = 1;
    update_used_column_ref.clear();
    do {
        for (auto col : target->columns()) {
            if (name_set.count(col.name) != 0)
                continue;
            
            if (col.name == "wkey") {
                update_used_column_ref.insert(target->ident() + "." + col.name);
                auto  expr = make_shared<const_expr>(this, col.type);
                assert(expr->type == col.type);
                expr->expr = to_string(write_op_id); // use write_op_id
                
                value_exprs.push_back(expr);
                names.push_back(col.name);
                name_set.insert(col.name);
                continue;
            }

            if (col.name == "pkey")
                continue;
            
            if (d6() < 4)
	            continue;
            
            update_used_column_ref.insert(target->ident() + "." + col.name);

            auto expr = value_expr::factory(this, col.type);
            value_exprs.push_back(expr);
            names.push_back(col.name);
            name_set.insert(col.name);
        }
    } while (names.size() < 2);
    in_update_set_list = tmp_update_set;
}

void set_list::out(std::ostream &out)
{
    assert(names.size());
    out << " set ";
    for (size_t i = 0; i < names.size(); i++) {
        indent(out);
        out << names[i] << " = " << *value_exprs[i];
        if (i + 1 != names.size())
            out << ", ";
    }
}

update_stmt::update_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p, s, v) {
    scope->refs.push_back(victim);
    write_op_id++;

    // dont select the target table
    vector<named_relation *> excluded_tables;
    vector<pair<sqltype*, table*>> excluded_t_with_c_of_type;
    exclude_tables(victim, scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);

    search = bool_expr::factory(this);
    set_list = make_shared<struct set_list>(this, victim);

    recover_tables(scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);
}

void update_stmt::out(std::ostream &out)
{
    out << "update " << victim->ident() << *set_list;
    indent(out);
    out << "where ";
    out << *search;
}

update_returning::update_returning(prod *p, struct scope *s, table *v)
  : update_stmt(p, s, v) {
  match();

  select_list = make_shared<struct select_list>(this);
}


upsert_stmt::upsert_stmt(prod *p, struct scope *s, table *v)
  : insert_stmt(p,s,v)
{
  match();

  if (!victim->constraints.size())
    fail("need table w/ constraint for upsert");
    
  set_list = std::make_shared<struct set_list>(this, victim);
  search = bool_expr::factory(this);
  constraint = random_pick(victim->constraints);
}

void common_table_expression::accept(prod_visitor *v)
{
  v->visit(this);
  for(auto q : with_queries)
    q->accept(v);
  query->accept(v);
}

common_table_expression::common_table_expression(prod *parent, struct scope *s, bool txn_mode)
  : prod(parent), myscope(s)
{
    scope = &myscope;
    do {
        shared_ptr<query_spec> query = make_shared<query_spec>(this, s, false, (vector<sqltype *> *)NULL, txn_mode);
        with_queries.push_back(query);
        string alias = scope->stmt_uid("cte");
        relation *relation = &query->select_list->derived_table;
        auto aliased_rel = make_shared<aliased_relation>(alias, relation);
        refs.push_back(aliased_rel);
        scope->tables.push_back(&*aliased_rel);

    } while (d6() > 2);

retry:
    do {
        auto pick = random_pick(s->tables);
        scope->tables.push_back(pick);
    } while (d6() > 3);
    try {
        query = make_shared<query_spec>(this, scope, false, (vector<sqltype *> *)NULL, txn_mode);
    } catch (runtime_error &e) {
        retry();
        goto retry;
    }

}

void common_table_expression::out(std::ostream &out)
{
    out << "WITH " ;
    for (size_t i = 0; i < with_queries.size(); i++) {
        indent(out);
        out << refs[i]->ident() << " AS " << "(" << *with_queries[i] << ")";
        if (i+1 != with_queries.size())
            out << ", ";
        indent(out);
    }
    out << *query;
    indent(out);
}

merge_stmt::merge_stmt(prod *p, struct scope *s, table *v)
     : modifying_stmt(p,s,v) {
  match();
  target_table_ = make_shared<target_table>(this, victim);
  data_source = table_ref::factory(this);
//   join_condition = join_cond::factory(this, *target_table_, *data_source);
  join_condition = make_shared<simple_join_cond>(this, *target_table_, *data_source);


  /* Put data_source into scope but not target_table.  Visibility of
     the latter varies depending on kind of when clause. */
//   for (auto r : data_source->refs)
//     scope->refs.push_back(&*r);

  clauselist.push_back(when_clause::factory(this));
  while (d6()>4)
    clauselist.push_back(when_clause::factory(this));
}

void merge_stmt::out(std::ostream &out)
{
     out << "MERGE INTO " << *target_table_;
     indent(out);
     out << "USING " << *data_source;
     indent(out);
     out << "ON " << *join_condition;
     indent(out);
     for (auto p : clauselist) {
       out << *p;
       indent(out);
     }
}

void merge_stmt::accept(prod_visitor *v)
{
  v->visit(this);
  target_table_->accept(v);
  data_source->accept(v);
  join_condition->accept(v);
  for (auto p : clauselist)
    p->accept(v);
    
}

when_clause::when_clause(merge_stmt *p)
  : prod(p)
{
  condition = bool_expr::factory(this);
  matched = d6() > 3;
}

void when_clause::out(std::ostream &out)
{
  out << (matched ? "WHEN MATCHED " : "WHEN NOT MATCHED");
  indent(out);
  out << "AND " << *condition;
  indent(out);
  out << " THEN ";
  out << (matched ? "DELETE" : "DO NOTHING");
}

void when_clause::accept(prod_visitor *v)
{
  v->visit(this);
  condition->accept(v);
}

when_clause_update::when_clause_update(merge_stmt *p)
  : when_clause(p), myscope(p->scope)
{
  myscope.tables = scope->tables;
  myscope.refs = scope->refs;
  scope = &myscope;
  scope->refs.push_back(&*(p->target_table_->refs[0]));
  
  set_list = std::make_shared<struct set_list>(this, p->victim);
}

void when_clause_update::out(std::ostream &out) {
  out << "WHEN MATCHED AND " << *condition;
  indent(out);
  out << " THEN UPDATE " << *set_list;
}

void when_clause_update::accept(prod_visitor *v)
{
  v->visit(this);
  set_list->accept(v);
}


when_clause_insert::when_clause_insert(struct merge_stmt *p)
  : when_clause(p)
{
  for (auto col : p->victim->columns()) {
    auto expr = value_expr::factory(this, col.type);
    assert(expr->type == col.type);
    exprs.push_back(expr);
  }
}

void when_clause_insert::out(std::ostream &out) {
  out << "WHEN NOT MATCHED AND " << *condition;
  indent(out);
  out << " THEN INSERT VALUES ( ";

  for (auto expr = exprs.begin();
       expr != exprs.end();
       expr++) {
    out << **expr;
    if (expr+1 != exprs.end())
      out << ", ";
  }
  out << ")";

}

void when_clause_insert::accept(prod_visitor *v)
{
  v->visit(this);
  for (auto p : exprs)
    p->accept(v);
}

shared_ptr<when_clause> when_clause::factory(struct merge_stmt *p)
{
  try {
    switch(d6()) {
    case 1:
    case 2:
      return make_shared<when_clause_insert>(p);
    case 3:
    case 4:
      return make_shared<when_clause_update>(p);
    default:
      return make_shared<when_clause>(p);
    }
  } catch (runtime_error &e) {
    p->retry();
  }
  return factory(p);
}

string upper_translate(string str)
{
    string ret;
    for (std::size_t i = 0; i < str.length(); i++) {
        if(str[i] >= 'a' && str[i] <= 'z') {
            ret.push_back(str[i] + 'A' - 'a');
            continue;
        }
        
        ret.push_back(str[i]);
    }
    return ret;
}

string create_unique_column_name(void)
{
    static std::set<string> created_names;

    std::string table_name = "c_" + random_identifier_generate();
    while (created_names.count(upper_translate(table_name))) {
        table_name += "_2";
    }

    created_names.insert(upper_translate(table_name));
    return table_name;
}

string unique_table_name(scope *s)
{
    static set<string> exist_table_name;
    static bool init = false;

    if (init == false) {
        for (auto t : s->tables) {
            exist_table_name.insert(upper_translate(t->ident()));
        }
        init = true;
    }

    auto new_table_name = "t_" + random_identifier_generate();
    while (exist_table_name.count(upper_translate(new_table_name))) {
            new_table_name = new_table_name + "_2";
    }
    return new_table_name;
}

string unique_index_name(scope *s)
{
    static set<string> exist_index_name;
    static bool init = false;

    if (init == false) {
        for (auto i : s->indexes) {
            exist_index_name.insert(upper_translate(i));
        }
        init = true;
    }

    auto new_index_name = "t_" + random_identifier_generate();
    while (exist_index_name.count(upper_translate(new_index_name))) {
            new_index_name = new_index_name + "_2";
    }
    return new_index_name;
}

create_table_stmt::create_table_stmt(prod *parent, struct scope *s)
: prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    // create table
    string table_name;
    table_name = unique_table_name(scope);
    created_table = make_shared<struct table>(table_name, "main", true, true);

    // generate write_key (identify different write operateions)
    column wkey("wkey", scope->schema->inttype);
    constraints.push_back(""); // no constraint
    created_table->columns().push_back(wkey);
    
    // generate primary_key (identify different rows)
    column primary_key("pkey", scope->schema->inttype);
    constraints.push_back("PRIMARY KEY UNIQUE NOT NULL");
    created_table->columns().push_back(primary_key);

    // generate other columns
    int column_num = d9();
    for (int i = 0; i < column_num; i++) {
        string column_name = create_unique_column_name();
        vector<sqltype *> enable_type;
        enable_type.push_back(scope->schema->inttype);
        enable_type.push_back(scope->schema->texttype);
        enable_type.push_back(scope->schema->realtype);
        auto type = random_pick<>(enable_type);

        column create_column(column_name, type);
        created_table->columns().push_back(create_column);
        string constraint_str = "";
#ifdef USE_CONSTRAINT
        if (d6() == 1)
            constraint_str += "NOT NULL ";
        if (d6() == 1)
            constraint_str += "UNIQUE ";
#endif
        constraints.push_back(constraint_str);
    }

    // check clause
    has_check = false;
#ifdef USE_CONSTRAINT
#if (!defined TEST_MONETDB) && (!defined TEST_CLICKHOUSE)
    if (d6() == 1) {
        has_check = true;
        scope->refs.push_back(&(*created_table));

        auto check_state = in_check_clause;
        in_check_clause = 1;
        check_expr = bool_expr::factory(this);
        in_check_clause = check_state;
    }
#endif
#endif
}

void create_table_stmt::out(std::ostream &out)
{
    out << "create table ";
    out << created_table->name << " ( ";
    indent(out);

    auto columns_in_table = created_table->columns();
    int column_num = columns_in_table.size();
    for (int i = 0; i < column_num; i++) {
        out << columns_in_table[i].name << " ";
        out << columns_in_table[i].type->name << " ";
        out << constraints[i];
        if (i != column_num - 1)
            out << ",";
        indent(out);
    }

    if (has_check) {
        out << ",";
        indent(out);
        out << "check(";
        out << *check_expr;
        out << ")";
    }
    indent(out);
    out << ")";
}

create_table_select_stmt::create_table_select_stmt(prod *parent, struct scope *s, int is_base)
: prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    if (is_base == 1)
        is_base_table = true;
    else if (is_base == 0)
        is_base_table = false;
    else if (d6() <= 3)
        is_base_table = true;
    else
        is_base_table = false;

    // remove the "BOOLEAN" columns
    subquery = make_shared<struct query_spec>(this, scope);
    auto &columns = subquery->select_list->derived_table.columns();
    auto &exprs = subquery->select_list->value_exprs;
    int size = columns.size();

    for (int i = 0; i < size; i++) {
        if (scope->schema->booltype != columns[i].type)
            continue;
        
        columns.erase(columns.begin() + i);
        size--;
        exprs.erase(exprs.begin() + i);
        i--;
    }

    if (columns.size() == 0) {
        auto e = make_shared<column_reference>(this);
        exprs.push_back(e);
        ostringstream name;
        name << "c1";
        sqltype *t = e->type;
        assert(t);
        columns.push_back(column(name.str(), t));
    }

    tatble_name = unique_table_name(scope);
}

void create_table_select_stmt::out(std::ostream &out)
{
    out << "CREATE ";
    out << (is_base_table ? "TABLE " : "VIEW ");
    out << tatble_name << " AS ";
    indent(out);

    out << *subquery;
}

group_clause::group_clause(prod *p, struct scope *s, 
            shared_ptr<struct select_list> select_list,
            std::vector<shared_ptr<named_relation> > *from_refs)
: prod(p), myscope(s), modified_select_list(select_list)
{
    scope = &myscope;
    scope->tables = s->tables;

    auto& select_exprs = modified_select_list->value_exprs;
    auto& select_columns = modified_select_list->derived_table.columns();

    auto size = select_columns.size();
    size_t chosen_index = dx(size) - 1;

    // target_ref = modified_select_list->derived_table.columns()[chosen_index].name; // cannot use alias

    size_t try_time = 0;
    while (1) { // only choose column_reference as target ref
        if (auto columns_ref = dynamic_pointer_cast<column_reference>(select_exprs[chosen_index])) {
            bool suitable_one = false;
            
            for (auto &r : *from_refs) {
                if (columns_ref->table_ref == r->ident()) {// the table from from_clause
                    suitable_one = true;
                    break;
                }
            }

            if (suitable_one == true)
                break;
        }
        
        chosen_index = (chosen_index + 1) % size;
        try_time++;
        if (try_time >= size) {
            throw std::runtime_error(std::string("excessive retries in ")
			   + typeid(*this).name());
        }
    }

    std::stringstream strout;
    strout << *select_exprs[chosen_index];
    target_ref = strout.str();

    int tmp_group = use_group;
    use_group = 0; // cannot use aggregate function in aggregate function
    for (size_t i = 0; i < size; i++) {
        if (i == chosen_index)
            continue;
        
        auto new_expr = make_shared<funcall>(p, (sqltype *)0, true);
        select_exprs.erase(select_exprs.begin() + i);
        select_exprs.insert(select_exprs.begin() + i, new_expr);
        select_columns[i].type = new_expr->type;
    }
    use_group = tmp_group;
}

void group_clause::out(std::ostream &out)
{
    out << target_ref;
}

alter_table_stmt::alter_table_stmt(prod *parent, struct scope *s):
prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    int type_chosen = d9();
    if (type_chosen <= 3)
        stmt_type = 0;
    else if (type_chosen <= 6)
        stmt_type = 1;
    else 
        stmt_type = 2;
    // else
    //     stmt_type = 3;

    // choose the base table (not view)
    int size = scope->tables.size();
    int chosen_table_idx = dx(size) - 1;
    named_relation *table_ref = NULL;
    table * real_table = NULL;
    while (1) {
        table_ref = scope->tables[chosen_table_idx];
        real_table = dynamic_cast<table *>(table_ref);
        if (real_table && real_table->is_base_table) {
            break;
        }
        chosen_table_idx = (chosen_table_idx + 1) % size;
    };
    
    set<string> exist_column_name;
    for (auto &c : table_ref->columns()) {
        exist_column_name.insert(upper_translate(c.name));
    }

    if (stmt_type == 0) { // rename table
        auto new_table_name = unique_table_name(scope);
        stmt_string = "alter table " + table_ref->ident() + " rename to " + new_table_name;
    }
    else if (stmt_type == 1) { // rename column
        column *column_ref;
        while (1) {
            column_ref = &random_pick(table_ref->columns());
            // do not alter pkey and wkey
            if (column_ref->name != "pkey" && column_ref->name != "wkey")
                break;
        }
        auto new_column_name = "c_" + random_identifier_generate();
        while (exist_column_name.count(upper_translate(new_column_name))) {
            new_column_name = new_column_name + "_2";
        }

        stmt_string = "alter table " + table_ref->ident() + " rename column " + column_ref->name
                        + " to " + new_column_name;
    }
    else if (stmt_type == 2) { // add column
        auto new_column_name = "c_" + random_identifier_generate();
        while (exist_column_name.count(upper_translate(new_column_name))) {
            new_column_name = new_column_name + "_2";
        }

        vector<sqltype *> enable_type;
        enable_type.push_back(scope->schema->inttype);
        enable_type.push_back(scope->schema->realtype);
        enable_type.push_back(scope->schema->texttype);
        auto type = random_pick<>(enable_type);

        stmt_string = "alter table " + table_ref->ident() + " add column " + new_column_name 
                        + " " + type->name;
    }
    // else if (stmt_type == 3){ // drop column
    //     auto& column_ref = random_pick(table_ref->columns());
    //     stmt_string = "alter table " + table_ref->ident() + " drop column " + column_ref.name;
    // }
}

void alter_table_stmt::out(std::ostream &out)
{
    out << stmt_string;
}

drop_table_stmt::drop_table_stmt(prod *parent, struct scope *s):
prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    // choose the base table (not view)
    int size = scope->tables.size();
    int chosen_table_idx = dx(size) - 1;
    named_relation *table_ref = NULL;
    table * real_table = NULL;
    while (1) {
        table_ref = scope->tables[chosen_table_idx];
        real_table = dynamic_cast<table *>(table_ref);
        if (real_table && real_table->is_base_table) {
            break;
        }
        chosen_table_idx = (chosen_table_idx + 1) % size;
    };

    stmt_string = "drop table if exists " + table_ref->ident();
}

void drop_table_stmt::out(std::ostream &out)
{
    out << stmt_string;
}

create_index_stmt::create_index_stmt(prod *parent, struct scope *s)
: prod(parent), myscope(s)
{
    scope = &myscope;
    scope->indexes = s->indexes;

    is_unique = false;
#ifndef TEST_CLICKHOUSE
    is_unique = (d6() == 1);
#endif

    index_name = unique_index_name(scope);

    // only choose base table
    auto tables_size = s->tables.size();
    size_t chosen_one = dx(tables_size) - 1;
    while (1) {
        auto chosen_table = dynamic_cast<struct table*>(s->tables[chosen_one]);
        if (chosen_table && chosen_table->is_base_table)
            break;
        chosen_one++;
        if (chosen_one == tables_size)
            chosen_one = 0;
    }
    auto target_table = s->tables[chosen_one];
    table_name = target_table->ident();

    auto target_columns = target_table->columns();
    for (auto &col : target_columns) {
        if (col.type == scope->schema->texttype)
            continue;
        indexed_columns.push_back(col.name);
    }
    size_t indexed_num = dx(target_columns.size());
    while (indexed_columns.size() > indexed_num) {
        indexed_columns.erase(indexed_columns.begin() + dx(indexed_columns.size()) -1);
    }
}

void create_index_stmt::out(std::ostream &out)
{
    out << "create" << (is_unique ? " unique " : " ")
        << "index " << index_name << " on " << table_name << " (";
    
    int size = indexed_columns.size();
    for (int i = 0; i < size; i++) {
        out << indexed_columns[i];
        if (i + 1 < size)
            out << ", ";
    }
    out << ")";
}

create_trigger_stmt::create_trigger_stmt(prod *parent, struct scope *s)
: prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    trigger_name = unique_table_name(scope);
    trigger_time = d6() <= 4 ? "after" : "before";
    
    auto choose = d9();
    if (choose <= 3)
        trigger_event = "insert";
    else if (choose <= 6)
        trigger_event = "update";
    else
        trigger_event = "delete";
    
    // only choose base table
    auto tables_size = s->tables.size();
    size_t chosen_one = dx(tables_size) - 1;
    while (1) {
        auto chosen_table = dynamic_cast<struct table*>(s->tables[chosen_one]);
        if (chosen_table && chosen_table->is_base_table)
            break;
        chosen_one++;
        if (chosen_one == tables_size)
            chosen_one = 0;
    }
    table_name = s->tables[chosen_one]->ident();

    int stmts_num = (d6() + 1) / 2; // 1 - 3
    for (int i = 0; i < stmts_num; i++) {
        shared_ptr<struct modifying_stmt> doing_stmt;
        choose = d9();
        if (choose <= 3)
            doing_stmt = make_shared<insert_stmt>(this, scope);
        else if (choose <= 6)
            doing_stmt = make_shared<update_stmt>(this, scope);
        else
            doing_stmt = make_shared<delete_stmt>(this, scope);
        
        doing_stmts.push_back(doing_stmt);
    }
    
}

void create_trigger_stmt::out(std::ostream &out)
{
    out << "create trigger " << trigger_name << " ";
    out << trigger_time << " ";
    out << trigger_event << " on " << table_name;
    indent(out);
    out << "for each row";
    indent(out);
    out << "begin ";
    indent(out);
    for (auto &stmt : doing_stmts) {
        out << *stmt << "; ";
        indent(out);
    }
    out << "end";
}

named_window::named_window(prod *p, struct scope *s):
 prod(p), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    window_name = "w_" + random_identifier_generate();

    int partition_num = d6() > 4 ? 2 : 1;
    while (partition_num > 0) {
        auto partition_expr = value_expr::factory(this);
        if (!dynamic_pointer_cast<const_expr>(partition_expr)) {
            partition_by.push_back(partition_expr);
            partition_num--;
        }
    }

    int order_num = d6() > 4 ? 2 : 1;
    while (order_num > 0) {
        auto order_expr = value_expr::factory(this);
        if (!dynamic_pointer_cast<const_expr>(order_expr)) {
            order_by.push_back(order_expr);
            order_num--;
        }
    }

    if (d6() > 3)
        asc = true;
    else
        asc = false;
}

void named_window::out(std::ostream &out)
{
    out << "window " << window_name << " as ( partition by ";
    for (auto ref = partition_by.begin(); ref != partition_by.end(); ref++) {
        out << **ref;
        if (ref + 1 != partition_by.end())
            out << ",";
    }

    out << " order by ";
    for (auto ref = order_by.begin(); ref != order_by.end(); ref++) {
        out << **ref;
        if (ref + 1 != order_by.end())
            out << ",";
    }
    if (asc) 
        out << " asc";
    else
        out << " desc";
    out << ")";
}

unioned_query::unioned_query(prod *p, struct scope *s, bool lateral, vector<sqltype *> *pointed_type):
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;
    if (lateral)
        scope->refs = s->refs;
    
    lhs = make_shared<query_spec>(this, this->scope, lateral, pointed_type);
    vector<sqltype *> tmp_type_vec;
    auto &lhs_exprs = lhs->select_list->value_exprs;
    for (auto iter = lhs_exprs.begin(); iter != lhs_exprs.end(); iter++) {
        tmp_type_vec.push_back((*iter)->type);
    }
    rhs = make_shared<query_spec>(this, this->scope, lateral, &tmp_type_vec);

    lhs->has_order = false;
    rhs->has_order = false;
    lhs->has_limit = false;
    rhs->has_limit = false;

#if (!defined TEST_CLICKHOUSE)
    if (d6() == 1)
#endif
        type = "union all";
#if (!defined TEST_CLICKHOUSE)
    else
        type = "union";
#endif
}

void unioned_query::out(std::ostream &out) {
    out << *lhs;
    indent(out);
    out << type;
    indent(out);
    out << *rhs;
}

insert_select_stmt::insert_select_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p, s, v)
{
    match();

    // dont select the target table
    vector<named_relation *> excluded_tables;
    vector<pair<sqltype*, table*>> excluded_t_with_c_of_type;
    exclude_tables(victim, scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);
    
    vector<sqltype *> pointed_type;
    for (auto &col : victim->columns()) {
        pointed_type.push_back(col.type);
    }
    target_subquery = make_shared<query_spec>(this, scope, false, &pointed_type);

    recover_tables(scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);
}

void insert_select_stmt::out(std::ostream &out)
{
    out << "insert into " << victim->ident();
    indent(out);
    out << *target_subquery;
}

shared_ptr<prod> statement_factory(struct scope *s)
{
    try {
        s->new_stmt();
        // if less than 2 tables, update_stmt will easily enter a dead loop.
        if (s->tables.size() < 2) { 
            #ifndef TEST_CLICKHOUSE
            if (s->tables.empty() || d6() > 3)
                return make_shared<create_table_stmt>((struct prod *)0, s);
            else
                return make_shared<create_table_select_stmt>((struct prod *)0, s);
            #else
            return make_shared<create_table_stmt>((struct prod *)0, s);
            #endif
        }

        auto choice = d20();
        if (s->tables.empty() || choice == 1)
            return make_shared<create_table_stmt>((struct prod *)0, s);
        #ifndef TEST_CLICKHOUSE
        #ifndef TEST_TIDB
        if (choice == 2)
            return make_shared<create_table_select_stmt>((struct prod *)0, s);
        #endif
        if (choice == 3)
            return make_shared<alter_table_stmt>((struct prod *)0, s);
        if (choice == 18)
            return make_shared<drop_table_stmt>((struct prod *)0, s);
        if (choice == 4)
            return make_shared<delete_stmt>((struct prod *)0, s);
        if (choice == 5) 
            return make_shared<update_stmt>((struct prod *)0, s);
        if (choice == 6)
            return make_shared<create_index_stmt>((struct prod *)0, s);
        #else
        if (choice >= 2 && choice <= 5)
            return make_shared<drop_table_stmt>((struct prod *)0, s);
        #endif
        
        #if (!defined TEST_MONETDB) && (!defined TEST_PGSQL) && (!defined TEST_CLICKHOUSE)
        if (choice == 7)
            return make_shared<create_trigger_stmt>((struct prod *)0, s);
        #endif
        if (choice == 8)
            return make_shared<insert_stmt>((struct prod *)0, s);
        if (choice == 9)
            return make_shared<insert_select_stmt>((struct prod *)0, s);
        if (choice >= 10 && choice <= 12)
            return make_shared<common_table_expression>((struct prod *)0, s);
        if (choice >= 13 && choice <= 15)
            return make_shared<unioned_query>((struct prod *)0, s);
        return make_shared<query_spec>((struct prod *)0, s);
    } catch (runtime_error &e) {
        cerr << "catch a runtime error" << endl;
        return statement_factory(s);
    }
}

shared_ptr<prod> ddl_statement_factory(struct scope *s)
{
    try {
        s->new_stmt();
        // if less than 2 tables, update_stmt will easily enter a dead loop.
        if (s->tables.size() < 2) { 
            #if (!defined TEST_CLICKHOUSE) && (!defined TEST_TIDB)
            if (s->tables.empty() || d6() > 3)
                return make_shared<create_table_stmt>((struct prod *)0, s);
            else
                return make_shared<create_table_select_stmt>((struct prod *)0, s);
            #else
            return make_shared<create_table_stmt>((struct prod *)0, s);
            #endif
        }

        auto choice = d6();
        #ifndef TEST_CLICKHOUSE
        #ifndef TEST_TIDB
        if (choice == 1)
            return make_shared<create_table_select_stmt>((struct prod *)0, s);
        #else
            // do not use view because it will prevent write operation
            // return make_shared<create_table_select_stmt>((struct prod *)0, s, 0);
        #endif
        
        if (choice == 2)
            return make_shared<alter_table_stmt>((struct prod *)0, s);
        
        if (choice == 3)
            return make_shared<create_index_stmt>((struct prod *)0, s);
        #endif
        
        // database has at least 2 tables in case dml statements are used
        if (choice == 4 && s->tables.size() >= 3) 
            return make_shared<drop_table_stmt>((struct prod *)0, s);

        #if (!defined TEST_MONETDB) && (!defined TEST_PGSQL) && (!defined TEST_CLICKHOUSE) && (!defined TEST_TIDB)
        if (choice == 5)
            return make_shared<create_trigger_stmt>((struct prod *)0, s);
        #endif

        if (choice == 6)
            return make_shared<create_table_stmt>((struct prod *)0, s);
        
        return ddl_statement_factory(s);

    } catch (runtime_error &e) {
        cerr << "catch a runtime error in " << __FUNCTION__  << endl;
        return ddl_statement_factory(s);
    }
}

shared_ptr<prod> basic_dml_statement_factory(struct scope *s)
{
    try { // only use insert_stmt to add data to target table
        s->new_stmt();
        return make_shared<insert_stmt>((struct prod *)0, s, (table *)0, true);

    } catch (runtime_error &e) {
        cerr << "catch a runtime error in " << __FUNCTION__  << endl;
        return basic_dml_statement_factory(s);
    }
}

shared_ptr<prod> txn_statement_factory(struct scope *s, int choice)
{
    try {
        s->new_stmt();
        if (choice == -1)
            choice = d12();
        // should not have ddl statement, which will auto commit in tidb;
#ifndef TEST_CLICKHOUSE
        if (choice == 1)
            return make_shared<delete_stmt>((struct prod *)0, s);
        if (choice == 8 || choice == 9 || choice == 10 || choice == 11 || choice == 12) 
            return make_shared<update_stmt>((struct prod *)0, s);
#endif
        if (choice == 4 || choice == 5 || choice == 6 || choice == 7)
            return make_shared<insert_stmt>((struct prod *)0, s);
        if (choice == 2)
            return make_shared<common_table_expression>((struct prod *)0, s, true);
        if (choice == 3)
            return make_shared<query_spec>((struct prod *)0, s, false, (vector<sqltype *> *)NULL, true);
        
        return txn_statement_factory(s);
    } catch (runtime_error &e) {
        string err = e.what();
        cerr << "catch a runtime error: " << err << endl;
        return txn_statement_factory(s, choice);
    }
}