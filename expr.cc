#include <numeric>
#include <algorithm>
#include <stdexcept>
#include <memory>
#include <cassert>

#include "random.hh"
#include "relmodel.hh"
#include "grammar.hh"
#include "schema.hh"
#include "impedance.hh"
#include "expr.hh"

using namespace std;
using impedance::matched;

extern int in_update_set_list;
extern set<string> update_used_column_ref;
extern int use_group; // 0->no group, 1->use group, 2->to_be_define
extern int in_in_clause; // 0-> not in "in" clause, 1-> in "in" clause (cannot use limit)
extern int in_check_clause; // 0-> not in "check" clause, 1-> in "check" clause (cannot use subquery)

shared_ptr<value_expr> value_expr::factory(prod *p, sqltype *type_constraint, 
vector<shared_ptr<named_relation> > *prefer_refs)
{
    try {
        if (sqltype::get("BOOLEAN") == type_constraint)
            return bool_expr::factory(p); 
        
        if (p->level < d6()) {
            auto choice = d42();
#ifndef TEST_CLICKHOUSE
            if ((choice <= 2) && window_function::allowed(p))
                return make_shared<window_function>(p, type_constraint);
#endif
            if (choice == 3)
                return make_shared<coalesce>(p, type_constraint);
            if (choice == 4)
                return make_shared<nullif>(p, type_constraint);
            if (choice <= 11)
                return make_shared<funcall>(p, type_constraint);
            if (choice <= 16)
                return make_shared<case_expr>(p, type_constraint);
            if (choice <= 21)
                return make_shared<binop_expr>(p, type_constraint);
        }
        auto choice = d42();
#ifndef TEST_MONETDB // monetdb dont allow limit in subquery, which make subselect return more than one row
        if (!in_check_clause && !in_in_clause && choice <= 4)
            return make_shared<atomic_subselect>(p, type_constraint);
#endif
        if (p->scope->refs.size() && choice <= 40)
            return make_shared<column_reference>(p, type_constraint, prefer_refs);
        return make_shared<const_expr>(p, type_constraint);
    } catch (runtime_error &e) {
    }
    p->retry();
    return factory(p, type_constraint);
}

extern string upper_translate(string str);
string cast_type_name_wrapper(string origin_type_name)
{
#ifdef TEST_MYSQL
    string integer_ret = "SIGNED"; // use SIGNED in mysql, use INTEGER in pgsql
    string boolean_ret = "UNSIGNED"; // use UNSIGNED in mysql, use BOOLEAN in pgsql
#else
    string integer_ret = "INTEGER"; // use SIGNED in mysql, use INTEGER in pgsql
    string boolean_ret = "BOOLEAN"; // use UNSIGNED in mysql, use BOOLEAN in pgsql
#endif
    
    string cast_type_name;
    if (upper_translate(origin_type_name) == "NUMERIC")
        cast_type_name = integer_ret; 
    if (upper_translate(origin_type_name) == "NUM")
        cast_type_name = integer_ret;
    else if (upper_translate(origin_type_name) == "INTEGER")
        cast_type_name = integer_ret; 
    else if (upper_translate(origin_type_name) == "INT")
        cast_type_name = integer_ret;
    else if (origin_type_name == "")
        cast_type_name = integer_ret;
    else if (upper_translate(origin_type_name) == "BOOLEAN")
        cast_type_name = boolean_ret;
    else if (upper_translate(origin_type_name) == "REAL")
        cast_type_name = integer_ret;
    else if (upper_translate(origin_type_name) == "TEXT")
        cast_type_name = "CHAR";
    else
        cast_type_name = origin_type_name;
    
    return cast_type_name;
}

case_expr::case_expr(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
  condition = bool_expr::factory(this);
  true_expr = value_expr::factory(this, type_constraint);
  false_expr = value_expr::factory(this, true_expr->type);

  if(false_expr->type != true_expr->type) {
       /* Types are consistent but not identical.  Try to find a more
	  concrete one for a better match. */
       if (true_expr->type->consistent(false_expr->type))
	    true_expr = value_expr::factory(this, false_expr->type);
       else 
	    false_expr = value_expr::factory(this, true_expr->type);
  }
  type = true_expr->type;
}

void case_expr::out(std::ostream &out)
{
  out << "case when " << *condition;
  out << " then " << *true_expr;
  out << " else " << *true_expr;
  out << " end";
  indent(out);
}

void case_expr::accept(prod_visitor *v)
{
  v->visit(this);
  condition->accept(v);
  true_expr->accept(v);
  false_expr->accept(v);
}

column_reference::column_reference(prod *p, sqltype *type_constraint, 
vector<shared_ptr<named_relation> > *prefer_refs) : 
value_expr(p)
{
    if (type_constraint) {
        while (1) {
            auto pairs = scope->refs_of_type(type_constraint);
            auto picked = random_pick(pairs);
            reference = picked.first->ident()
                        + "." + picked.second.name;
            table_ref = picked.first->ident();
            type = picked.second.type;
            assert(type_constraint->consistent(type));

            // in update_stmt, the column_ref cannot be used twice.
            if (in_update_set_list == 0)
                break;
            if (update_used_column_ref.count(reference) == 0) {
                update_used_column_ref.insert(reference);
                break;
            }
            retry();
        }
    } else {
        while (1) {
            named_relation *r;
            if (prefer_refs != 0 && d6() > 2) 
                r = &*random_pick(*prefer_refs);
            else 
                r = random_pick(scope->refs);
            
            column &c = random_pick(r->columns());
            type = c.type;
            reference = r->ident() + "." + c.name;
            table_ref = r->ident();

            // in update_stmt, the column_ref cannot be used twice.
            if (in_update_set_list == 0)
                break;
            if (update_used_column_ref.count(reference) == 0) {
                update_used_column_ref.insert(reference);
                break;
            }
            retry();
        }
    }
}

shared_ptr<bool_expr> bool_expr::factory(prod *p)
{
    try {
        if (p->level > d100())
            return make_shared<truth_value>(p);
        
        auto choose = d42();
        if(choose <= 15)
            return make_shared<comparison_op>(p);
        else if (choose <= 21)
            return make_shared<bool_term>(p);
        else if (choose <= 24)
            return make_shared<null_predicate>(p);
        else if (choose <= 27)
            return make_shared<truth_value>(p);
        else if (choose <= 30)
            return make_shared<between_op>(p);
        else if (choose <= 33)
            return make_shared<like_op>(p);
        else if (choose <= 36)
            return make_shared<in_op>(p);
        else if (!in_check_clause && choose <= 39)
            return make_shared<comp_subquery>(p);
#if (!defined TEST_CLICKHOUSE)
        else if (!in_check_clause)
            return make_shared<exists_predicate>(p);
#endif
        //     return make_shared<distinct_pred>(q);
    } catch (runtime_error &e) {
    }
    p->retry();
    return factory(p);
}

exists_predicate::exists_predicate(prod *p) : bool_expr(p)
{
  subquery = make_shared<query_spec>(this, scope);
}

void exists_predicate::accept(prod_visitor *v)
{
  v->visit(this);
  subquery->accept(v);
}

void exists_predicate::out(std::ostream &out)
{
  out << "EXISTS (";
  indent(out);
  out << *subquery << ")";
}

distinct_pred::distinct_pred(prod *p) : bool_binop(p)
{
  lhs = make_shared<column_reference>(this);
  rhs = make_shared<column_reference>(this, lhs->type);
}

comparison_op::comparison_op(prod *p) : bool_binop(p)
{
  auto &idx = p->scope->schema->operators_returning_type;

  auto iters = idx.equal_range(scope->schema->booltype);
  oper = random_pick<>(iters)->second;

  lhs = value_expr::factory(this, oper->left);
  rhs = value_expr::factory(this, oper->right);

  if (oper->left == oper->right
	 && lhs->type != rhs->type) {

    if (lhs->type->consistent(rhs->type))
      lhs = value_expr::factory(this, rhs->type);
    else
      rhs = value_expr::factory(this, lhs->type);
  }
}

coalesce::coalesce(prod *p, sqltype *type_constraint, const char *abbrev)
     : value_expr(p), abbrev_(abbrev)
{
    auto first_expr = value_expr::factory(this, type_constraint);
    auto second_expr = value_expr::factory(this, first_expr->type);

    retry_limit = 20;
    while(first_expr->type != second_expr->type) {
        retry();
        if (first_expr->type->consistent(second_expr->type))
            first_expr = value_expr::factory(this, second_expr->type);
        else 
            second_expr = value_expr::factory(this, first_expr->type);
    }
    type = second_expr->type;

  value_exprs.push_back(first_expr);
  value_exprs.push_back(second_expr);
}
 
void coalesce::out(std::ostream &out)
{
#ifndef TEST_CLICKHOUSE
    out << "cast(" ;
#endif
    out << abbrev_ << "(";
    for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++) {
    out << **expr;
    if (expr+1 != value_exprs.end())
        out << ",", indent(out);
    }
    out << ")";
#ifndef TEST_CLICKHOUSE
    out << " as " << cast_type_name_wrapper(type->name) << ")";
#endif
}

const_expr::const_expr(prod *p, sqltype *type_constraint)
    : value_expr(p), expr("")
{
    type = type_constraint ? type_constraint : scope->schema->inttype;

    if (type == scope->schema->inttype) {
        // if (d100() == 1)
        //     expr = "9223372036854775808";
        // else
            expr = to_string(d100());
        
        return;
    }

   if (d9() == 1) {
#ifndef TEST_CLICKHOUSE
        expr = "cast(null as " + cast_type_name_wrapper(type->name) + ")";
#else
        expr = "null";
#endif
        return;
    }

    if (type == sqltype::get("REAL")) {
        if (d100() == 1)
            expr = "2147483648.100000";
        else 
            expr = to_string(d100()) + "." +  to_string(d100());
    }
    else if (type == scope->schema->booltype)
        expr += (d6() > 3) ? scope->schema->true_literal : scope->schema->false_literal;
    else if (type == sqltype::get("TEXT")) 
        expr = "'" + random_identifier_generate() + "'";
    else
#ifndef TEST_CLICKHOUSE
        expr += "cast(null as " + cast_type_name_wrapper(type->name) + ")";
#else
        expr += "null";
#endif
}

funcall::funcall(prod *p, sqltype *type_constraint, bool agg)
  : value_expr(p), is_aggregate(agg)
{
    if (type_constraint == scope->schema->internaltype)
        fail("cannot call functions involving internal type");

    auto &idx = agg ? p->scope->schema->aggregates_returning_type : 
        (4 < d6()) ? p->scope->schema->routines_returning_type
        : p->scope->schema->parameterless_routines_returning_type;
        
retry:
    
    if (!type_constraint) {
        proc = random_pick(idx.begin(), idx.end())->second;
    } 
    else {
        auto iters = idx.equal_range(type_constraint);
        proc = random_pick<>(iters)->second;
        if (proc && !type_constraint->consistent(proc->restype)) {
            retry();
            goto retry;
        }
    }

    if (!proc) {
        retry();
        goto retry;
    }

    if (type_constraint)
        type = type_constraint;
    else
        type = proc->restype;

    if (type == scope->schema->internaltype) {
        retry();
        goto retry;
    }

    for (auto type : proc->argtypes)
        if (type == scope->schema->internaltype
	        || type == scope->schema->arraytype) {
            retry();
            goto retry;
        }
  
    for (auto argtype : proc->argtypes) {
        assert(argtype);
        auto expr = value_expr::factory(this, argtype);
        parms.push_back(expr);
    }
}

void funcall::out(std::ostream &out)
{
    out << proc->ident() << "(";
    for (auto expr = parms.begin(); expr != parms.end(); expr++) {
        indent(out);
#ifndef TEST_CLICKHOUSE
        out << "cast(";
#endif
        out << **expr;
#ifndef TEST_CLICKHOUSE
        out << " as " << cast_type_name_wrapper((*expr)->type->name) << ")";
#endif
        if (expr+1 != parms.end())
            out << ",";
    }

    if (is_aggregate && (parms.begin() == parms.end()))
        out << "*";
    
    out << ")";
}

atomic_subselect::atomic_subselect(prod *p, sqltype *type_constraint)
  : value_expr(p), offset((d6() == 6) ? d100() : d6())
{
    match();
    if (d6() < 3) {
        if (type_constraint) {
            auto idx = scope->schema->aggregates_returning_type;
            auto iters = idx.equal_range(type_constraint);
            agg = random_pick<>(iters)->second;
        } else {
            agg = &random_pick<>(scope->schema->aggregates);
        }
        if (agg->argtypes.size() != 1)
            agg = 0;
        else
            type_constraint = agg->argtypes[0];
    } else {
        agg = 0;
    }

    if (type_constraint) {
        auto idx = scope->schema->tables_with_columns_of_type;
        col = 0;
        auto iters = idx.equal_range(type_constraint);
        tab = random_pick<>(iters)->second;

        for (auto &cand : tab->columns()) {
            if (type_constraint->consistent(cand.type)) {
	            col = &cand;
	            break;
            }
        }
        assert(col);
    } else {
        tab = random_pick<>(scope->tables);
        col = &random_pick<>(tab->columns());
    }

    type = agg ? agg->restype : col->type;
}

void atomic_subselect::out(std::ostream &out)
{
    out << "(select ";
    if (agg) 
        out << agg->ident() << "(" << col->name << ")";
    else
        out << col->name;
    
    out << " from " << tab->ident();

    if (!agg)
        out << " order by " << col->name << " limit 1 offset " << offset;
    
    out << ")";
    indent(out);
}

void window_function::out(std::ostream &out)
{
  indent(out);
  out << *aggregate << " over (partition by ";
    
  for (auto ref = partition_by.begin(); ref != partition_by.end(); ref++) {
    out << **ref;
    if (ref+1 != partition_by.end())
      out << ",";
  }

  out << " order by ";
    
  for (auto ref = order_by.begin(); ref != order_by.end(); ref++) {
    out << **ref;
    if (ref+1 != order_by.end())
      out << ",";
  }

  out << ")";
}

window_function::window_function(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
    match();
    while (1) {
        aggregate = make_shared<win_funcall>(this, type_constraint);
        if (aggregate->proc->name != "zipfile")
            break;
        aggregate.reset();
    }
  
    type = aggregate->type;
    partition_by.push_back(make_shared<column_reference>(this));
    while(d6() > 4)
        partition_by.push_back(make_shared<column_reference>(this));

    order_by.push_back(make_shared<column_reference>(this));
    while(d6() > 4)
        order_by.push_back(make_shared<column_reference>(this));
    }

bool window_function::allowed(prod *p)
{
  if (dynamic_cast<select_list *>(p))
    return dynamic_cast<query_spec *>(p->pprod) ? true : false;
  if (dynamic_cast<window_function *>(p))
    return false;
  if (dynamic_cast<value_expr *>(p))
    return allowed(p->pprod);
  return false;
}

binop_expr::binop_expr(prod *p, sqltype *type_constraint) : value_expr(p)
{
    auto &idx = p->scope->schema->operators_returning_type;
retry:
    if (type_constraint) {
        auto iters = idx.equal_range(type_constraint);
        oper = random_pick<>(iters)->second;
        if (oper && !type_constraint->consistent(oper->result)) {
            retry();
            goto retry;
        }
    }
    else {
        oper = random_pick(idx.begin(), idx.end())->second;
    }
    type = oper->result;

    lhs = value_expr::factory(this, oper->left);
    rhs = value_expr::factory(this, oper->right);

    if (oper->left == oper->right && lhs->type != rhs->type) {
        if (lhs->type->consistent(rhs->type))
            lhs = value_expr::factory(this, rhs->type);
        else
            rhs = value_expr::factory(this, lhs->type);
    }
}

between_op::between_op(prod *p) : bool_expr(p)
{
    mhs = value_expr::factory(this, scope->schema->inttype);
    lhs = value_expr::factory(this, scope->schema->inttype);
    rhs = value_expr::factory(this, scope->schema->inttype);
}

like_op::like_op(prod *p) : bool_expr(p)
{
    lhs = value_expr::factory(this, sqltype::get("TEXT"));
    
    like_format = random_identifier_generate();
    auto scope = like_format.size() / 3;
    scope = scope == 0 ? 1 : scope;
    auto change_time = dx(scope);
    for (; change_time > 0; change_time--) {
        auto pos = dx(like_format.size()) - 1;
        if (d6() < 4)
            like_format[pos] = '%';
        else
            like_format[pos] = '_';
    }
    like_format = "'" + like_format + "'";

    if (d6() < 4)
        like_operator = " like ";
    else
        like_operator = " not like ";
}

in_op::in_op(prod *p) : bool_expr(p), myscope(scope)
{
    myscope.tables = scope->tables;
    scope = &myscope;
    
    lhs = value_expr::factory(this);
    if (d6() < 4)
        in_operator = " in ";
    else
        in_operator = " not in ";
    
    if (!in_check_clause && d6() < 6)
        use_query = true;
    else
        use_query = false;
    
    auto tmp_in_state = in_in_clause;
    in_in_clause = 1;
    if (!use_query) {
        auto vec_size = dx(5);
        for (; vec_size > 0; vec_size--) {
            auto e = value_expr::factory(this, lhs->type);
            expr_vec.push_back(e);
        }
    }
    else {
        auto tmp_use_group = use_group;
        use_group = 0;
        scope->refs.clear(); // dont use the ref of parent select
        vector<sqltype *> pointed_type;
        pointed_type.push_back(lhs->type);
        if (d6() < 4)
            in_subquery = make_shared<unioned_query>(this, scope, false, &pointed_type);
        else 
            in_subquery = make_shared<query_spec>(this, scope, false, &pointed_type);
        use_group = tmp_use_group;
    }
    in_in_clause = tmp_in_state;
}

void in_op::out(std::ostream &out) {
    out << *lhs << in_operator << "(";
    indent(out);
    if (!use_query) {
        for (size_t i = 0; i < expr_vec.size(); i++) {
            out << *expr_vec[i];
            if (i < expr_vec.size() - 1)
                out << ", ";
        }
    }
    else {
        out << *in_subquery;
    }
    out << ")";
}

void win_func_using_exist_win::out(std::ostream &out)
{
  indent(out);
  out << *aggregate << " over " + exist_window;
}

win_func_using_exist_win::win_func_using_exist_win(prod *p, sqltype *type_constraint, string exist_win)
  : value_expr(p)
{
  match();
    while (1) {
        aggregate = make_shared<win_funcall>(this, type_constraint);
        type = type_constraint;
        if (aggregate->proc->name != "zipfile")
            break;
        aggregate.reset();
    }
    exist_window = exist_win;
}

win_funcall::win_funcall(prod *p, sqltype *type_constraint)
  : value_expr(p)
{
    if (type_constraint == scope->schema->internaltype)
        fail("cannot call functions involving internal type");

    multimap<sqltype*, routine*> *idx;
    if (d6() > 4)
        idx = &p->scope->schema->aggregates_returning_type;
    else
        idx = &p->scope->schema->windows_returning_type;
        
retry:
    
    if (!type_constraint) {
        proc = random_pick(idx->begin(), idx->end())->second;
    } 
    else {
        auto iters = idx->equal_range(type_constraint);
        proc = random_pick<>(iters)->second;
        if (proc && !type_constraint->consistent(proc->restype)) {
            retry();
            goto retry;
        }
    }

    if (!proc) {
        retry();
        goto retry;
    }

    if (type_constraint)
        type = type_constraint;
    else
        type = proc->restype;

    if (type == scope->schema->internaltype) {
        retry();
        goto retry;
    }

    for (auto type : proc->argtypes)
        if (type == scope->schema->internaltype
	        || type == scope->schema->arraytype) {
            retry();
            goto retry;
        }
  
    for (auto argtype : proc->argtypes) {
        assert(argtype);
        auto expr = value_expr::factory(this, argtype);
        parms.push_back(expr);
    }
}

void win_funcall::out(std::ostream &out)
{
    out << proc->ident() << "(";
    for (auto expr = parms.begin(); expr != parms.end(); expr++) {
        indent(out);
#ifndef TEST_CLICKHOUSE
        out << "cast(";
#endif
        out << **expr;
#ifndef TEST_CLICKHOUSE
        out << " as " << cast_type_name_wrapper((*expr)->type->name) << ")";
#endif
        if (expr+1 != parms.end())
            out << ",";
    }

    if (proc->ident() == "count" && (parms.begin() == parms.end()))
        out << "*";
    
    out << ")";
}

comp_subquery::comp_subquery(prod *p) : bool_expr(p), myscope(scope)
{
    myscope.tables = scope->tables;
    scope = &myscope;
    
    lhs = value_expr::factory(this);

    auto chosen_comp = d6();// =  >  <  >=  <=  <>
    switch (chosen_comp)
    {
        case 1:
            comp_op = "=";
            break;
        case 2:
            comp_op = "<>";
            break;
        case 3:
            comp_op = ">";
            break;
        case 4:
            comp_op = "<";
            break;
        case 5:
            comp_op = ">=";
            break;
        case 6:
            comp_op = "<=";
            break;
        default:
            comp_op = "<>";
            break;
    }

    vector<sqltype *> pointed_type;
    pointed_type.push_back(lhs->type);
#ifdef TEST_CLICKHOUSE
    scope->refs.clear();
#endif
    if (d6() < 4) {
        target_subquery = make_shared<unioned_query>(this, scope, false, &pointed_type);
    }
    else {
        target_subquery = make_shared<query_spec>(this, scope, false, &pointed_type);
        // auto subquery = dynamic_pointer_cast<query_spec>(target_subquery);
        // subquery->has_limit = true;
        // subquery->limit_num = 1;
    }
}

void comp_subquery::out(std::ostream &out) {
    out << *lhs << " " << comp_op << " ( ";
    indent(out);
    out << *target_subquery << ")";
}