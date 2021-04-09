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
set<string> update_used_column_ref;

shared_ptr<table_ref> table_ref::factory(prod *p) {
    try {
        if (p->level < 3 + d6()) {
            if (d6() > 3 && p->level < d6())
	            return make_shared<table_subquery>(p);
            if (d6() > 3)
	            return make_shared<joined_table>(p);
        }
        // if (d6() > 3)
            return make_shared<table_or_query_name>(p);
        // else
        //     return make_shared<table_sample>(p);
    } catch (runtime_error &e) {
        p->retry();
    }
    return factory(p);
}

table_or_query_name::table_or_query_name(prod *p) : table_ref(p) {
    t = random_pick(scope->tables);
    refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("ref"), t));
}

void table_or_query_name::out(std::ostream &out) {
  out << t->ident() << " as " << refs[0]->ident();
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
	  if (d6() < 6)
	       return make_shared<expr_join_cond>(p, lhs, rhs);
	  else
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
    out << "(";
    out << *lhs;
    indent(out);
    out << type << " join " << *rhs;
    indent(out);
    if (type == "inner" || type == "left outer")
        out << "on (" << *condition << ")";
    out << ")";
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

from_clause::from_clause(prod *p) : prod(p) {
    reflist.push_back(table_ref::factory(this));
    for (auto r : reflist.back()->refs)
        scope->refs.push_back(&*r);

//   while (d6() > 5) {
//     // add a lateral subquery
//     if (!impedance::matched(typeid(lateral_subquery)))
//       break;
//     reflist.push_back(make_shared<lateral_subquery>(this));
//     for (auto r : reflist.back()->refs)
//       scope->refs.push_back(&*r);
//   }
}

select_list::select_list(prod *p, vector<shared_ptr<named_relation> > *refs) : prod(p), prefer_refs(refs)
{
    do {
        shared_ptr<value_expr> e = value_expr::factory(this, 0, prefer_refs);
        value_exprs.push_back(e);
        ostringstream name;
        name << "c" << columns++;
        sqltype *t = e->type;
        assert(t);
        derived_table.columns().push_back(column(name.str(), t));
    } while (d6() > 1);
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
    if (limit_clause.length()) {
        indent(out);
        out << limit_clause;
    }
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

query_spec::query_spec(prod *p, struct scope *s, bool lateral) :
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;

    if (use_group == 2) { // confirm whether use group
        if (d6() == 1) use_group = 1;
        else use_group = 0;
    }

    if (lateral)
        scope->refs = s->refs;
    
    int tmp_group = use_group;
    use_group = 2; // from clause can use group or not.
    from_clause = make_shared<struct from_clause>(this);
    use_group = tmp_group;

    tmp_group = use_group;
    use_group = 0; // cannot use aggregate in later son select statement

    select_list = make_shared<struct select_list>(this, &from_clause->reflist.back()->refs);
    
    set_quantifier = (d100() == 1) ? "distinct" : "";
    
    search = bool_expr::factory(this); // cannot use group in where

    use_group = tmp_group;

    has_group = false;
    if (use_group == 1) {
        group_clause = make_shared<struct group_clause>(this, this->scope, select_list, &from_clause->reflist.back()->refs);
        has_group = true;
    }

    if (has_group == false && d6() > 2) {
        ostringstream cons;
        
        cons << "order by ";
        auto &selected_columns = select_list->derived_table.columns();
        auto select_list_size = selected_columns.size();
        for (std::size_t i = 0; i < select_list_size; i++) {
            cons << selected_columns[i].name;
            if (i + 1 < select_list_size)
                cons << ", ";
            else
                cons << " ";
        }

        if (d6() > 3) 
            cons << "asc ";
        else
            cons << "desc ";

        cons << "limit " << d100() + d100();
        limit_clause = cons.str();
    }
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
    
    // dont select the target table
    vector<named_relation *> exclude_tables;
    for (std::size_t i = 0; i < scope->tables.size(); i++) {
        if (scope->tables[i]->ident() == victim->ident()) {
            exclude_tables.push_back(scope->tables[i]);
            scope->tables.erase(scope->tables.begin() + i);
            i--;
        }
    }

    search = bool_expr::factory(this);
    
    for (std::size_t i = 0; i < exclude_tables.size(); i++) {
        scope->tables.push_back(exclude_tables[i]);
    }
}

delete_returning::delete_returning(prod *p, struct scope *s, table *victim)
  : delete_stmt(p, s, victim) {
  match();
  select_list = make_shared<struct select_list>(this);
}

insert_stmt::insert_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p, s, v)
{
    match();

    // dont select the target table
    vector<named_relation *> exclude_tables;
    for (std::size_t i = 0; i < scope->tables.size(); i++) {
        if (scope->tables[i]->ident() == victim->ident()) {
            exclude_tables.push_back(scope->tables[i]);
            scope->tables.erase(scope->tables.begin() + i);
            i--;
        }
    }

    for (auto col : victim->columns()) {
        auto expr = value_expr::factory(this, col.type);
        assert(expr->type == col.type);
        value_exprs.push_back(expr);
    }

    for (std::size_t i = 0; i < exclude_tables.size(); i++) {
        scope->tables.push_back(exclude_tables[i]);
    }
}

void insert_stmt::out(std::ostream &out)
{
    out << "insert into " << victim->ident() << " ";

    if (!value_exprs.size()) {
        out << "default values";
        return;
    }

    out << "values (";
  
    for (auto expr = value_exprs.begin();
      expr != value_exprs.end();
      expr++) {
        indent(out);
        out << **expr;
        if (expr + 1 != value_exprs.end())
            out << ", ";
    }
    out << ")";
}

set_list::set_list(prod *p, table *target) : prod(p)
{
    in_update_set_list = 1;
    update_used_column_ref.clear();
    do {
        for (auto col : target->columns()) {
            if (d6() < 4)
	            continue;
            
            update_used_column_ref.insert(target->ident() + "." + col.name);

            auto expr = value_expr::factory(this, col.type);
            value_exprs.push_back(expr);
            names.push_back(col.name);
        }
    } while (!names.size());
    in_update_set_list = 0;
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

    // dont select the target table
    vector<named_relation *> exclude_tables;
    for (std::size_t i = 0; i < scope->tables.size(); i++) {
        if (scope->tables[i]->ident() == victim->ident()) {
            exclude_tables.push_back(scope->tables[i]);
            scope->tables.erase(scope->tables.begin() + i);
            i--;
        }
    }

    search = bool_expr::factory(this);
    set_list = make_shared<struct set_list>(this, victim);

    for (std::size_t i = 0; i < exclude_tables.size(); i++) {
        scope->tables.push_back(exclude_tables[i]);
    }
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

shared_ptr<prod> statement_factory(struct scope *s)
{
    try {
        s->new_stmt();
        auto choice = d20();
        if (s->tables.empty() || choice == 1)
            return make_shared<create_table_stmt>((struct prod *)0, s);
        if (choice == 2)
            return make_shared<create_table_select_stmt>((struct prod *)0, s);
        if (choice == 3)
            return make_shared<alter_table_stmt>((struct prod *)0, s);
        if (choice == 4)
            return make_shared<delete_stmt>((struct prod *)0, s);
        if (choice == 5) 
            return make_shared<update_stmt>((struct prod *)0, s);
        if (choice <= 9) // have more chance to insert data
            return make_shared<insert_stmt>((struct prod *)0, s);
        if (choice <= 12)
            return make_shared<common_table_expression>((struct prod *)0, s);
        return make_shared<query_spec>((struct prod *)0, s);
        /* TODO:
        if (d42() == 1)
            return make_shared<merge_stmt>((struct prod *)0, s);
        else if (d42() == 1)
            return make_shared<delete_returning>((struct prod *)0, s);
        if (d42() == 1) 
            return make_shared<upsert_stmt>((struct prod *)0, s);
        else if (d42() == 1)
            return make_shared<update_returning>((struct prod *)0, s);
        if (d6() > 4)
            return make_shared<select_for_update>((struct prod *)0, s);
        */
    } catch (runtime_error &e) {
        cerr << "catch a runtime error" << endl;
        return statement_factory(s);
    }
}

void common_table_expression::accept(prod_visitor *v)
{
  v->visit(this);
  for(auto q : with_queries)
    q->accept(v);
  query->accept(v);
}

common_table_expression::common_table_expression(prod *parent, struct scope *s)
  : prod(parent), myscope(s)
{
    scope = &myscope;
    do {
        shared_ptr<query_spec> query = make_shared<query_spec>(this, s);
        with_queries.push_back(query);
        string alias = scope->stmt_uid("jennifer");
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
        query = make_shared<query_spec>(this, scope);
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

    std::string table_name = random_identifier_generate();
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

    auto new_table_name = random_identifier_generate();
    while (exist_table_name.count(upper_translate(new_table_name))) {
            new_table_name = new_table_name + "_2";
    }
    return new_table_name;
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

    std::vector<string> created_names;
    
    // create its columns
    string key_column_name = create_unique_column_name();
    auto key_type = sqltype::get("INTEGER");
    column key_column(key_column_name, key_type);
    created_table->columns().push_back(key_column);

    int column_num = dx(10);
    for (int i = 0; i < column_num; i++) {
        string column_name = create_unique_column_name();
        int type_idx = dx(sqltype::typemap.size()) - 1;
        auto type_ptr = sqltype::typemap.begin();
        for (int j = 0; j < type_idx; j++) 
            type_ptr++;
        
        while (type_ptr->first == "internal" || 
               type_ptr->first == "ARRAY" ||
               type_ptr->first == "NUM") {
            type_ptr++;
            if (type_ptr == sqltype::typemap.end())
                type_ptr = sqltype::typemap.begin();
        }

        auto type = type_ptr->second;

        column create_column(column_name, type);
        created_table->columns().push_back(create_column);
    }

    // primary key
    key_idx = 0;
}

void create_table_stmt::out(std::ostream &out)
{
    out << "CREATE TABLE ";
    out << created_table->name << " ( ";
    indent(out);

    auto columns_in_table = created_table->columns();
    int column_num = columns_in_table.size();
    for (int i = 0; i < column_num; i++) {
        out << columns_in_table[i].name << " " << columns_in_table[i].type->name << ", ";
        indent(out);
    }

    out << "PRIMARY KEY (" << columns_in_table[key_idx].name << ")";
    indent(out);

    out << ")";
}

create_table_select_stmt::create_table_select_stmt(prod *parent, struct scope *s)
: prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    if (d6() <= 4)
        is_base_table = true;
    else
        is_base_table = false;

    subquery = make_shared<struct query_spec>(this, scope);

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

    int type_chosen = d6();
    if (type_chosen <= 2)
        stmt_type = 0;
    else if (type_chosen <= 4)
        stmt_type = 1;
    else
        stmt_type = 2;

    // choose the base table (not view)
    int size = scope->tables.size();
    int chosen_table_idx = dx(size) - 1;
    named_relation *table_ref = NULL;
    table * real_table = NULL;
    while (1) {
        table_ref = scope->tables[chosen_table_idx];
        real_table = dynamic_cast<table *>(table_ref);
        if (real_table)
            break;
        
        chosen_table_idx = (chosen_table_idx + 1) % size;
    };
    
    set<string> exist_column_name;
    for (auto &c : table_ref->columns()) {
        exist_column_name.insert(upper_translate(c.name));
    }

    if (stmt_type == 0) {
        auto new_table_name = unique_table_name(scope);
        stmt_string = "alter table " + table_ref->ident() + " rename to " + new_table_name;
    }
    else if (stmt_type == 1) {
        auto& column_ref = random_pick(table_ref->columns());
        auto new_column_name = random_identifier_generate();
        while (exist_column_name.count(upper_translate(new_column_name))) {
            new_column_name = new_column_name + "_2";
        }

        stmt_string = "alter table " + table_ref->ident() + " rename column " + column_ref.name
                        + " to " + new_column_name;
    }
    else {
        auto new_column_name = random_identifier_generate();
        while (exist_column_name.count(upper_translate(new_column_name))) {
            new_column_name = new_column_name + "_2";
        }

        int type_idx = dx(sqltype::typemap.size()) - 1;
        auto type_ptr = sqltype::typemap.begin();
        for (int j = 0; j < type_idx; j++) 
            type_ptr++;
        
        while (type_ptr->first == "internal" || 
            type_ptr->first == "ARRAY") {
            type_ptr++;
            if (type_ptr == sqltype::typemap.end())
                type_ptr = sqltype::typemap.begin();
        }

        auto type = type_ptr->second;
        stmt_string = "alter table " + table_ref->ident() + " add column " + new_column_name 
                        + " " + type->name;
    }
}

void alter_table_stmt::out(std::ostream &out)
{
    out << stmt_string;
}