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

shared_ptr<value_expr> value_expr::factory(prod *p, sqltype *type_constraint, 
vector<shared_ptr<named_relation> > *prefer_refs)
{
    try {
        if (p->level < d6()) {
            auto choice = d42();
            if ((choice <= 2) && window_function::allowed(p))
                return make_shared<window_function>(p, type_constraint);
            if (choice == 3)
                return make_shared<coalesce>(p, type_constraint);
            if (choice == 4)
                return make_shared<nullif>(p, type_constraint);
            if (choice <= 11)
                return make_shared<funcall>(p, type_constraint);
            if (choice <= 16)
                return make_shared<case_expr>(p, type_constraint);
        }
        auto choice = d42();
        if (choice <= 4)
            return make_shared<atomic_subselect>(p, type_constraint);
        if (p->scope->refs.size() && choice <= 40)
            return make_shared<column_reference>(p, type_constraint, prefer_refs);
        return make_shared<const_expr>(p, type_constraint);
    } catch (runtime_error &e) {
    }
    p->retry();
    return factory(p, type_constraint);
}

string cast_type_name_wrapper(string origin_type_name)
{
    string cast_type_name;
    if (origin_type_name == "NUMERIC")
        cast_type_name = "SIGNED"; // use SIGNED in mysql, use integer in pgsql
    else if (origin_type_name == "INTEGER")
        cast_type_name = "SIGNED"; // use SIGNED in mysql, use integer in pgsql
    else if (origin_type_name == "INT")
        cast_type_name = "SIGNED"; // use SIGNED in mysql, use integer in pgsql
    else if (origin_type_name == "")
        cast_type_name = "SIGNED"; // use SIGNED in mysql, use integer in pgsql
    else if (origin_type_name == "REAL")
        cast_type_name = "SIGNED"; // use SIGNED in mysql, use integer in pgsql
    else if (origin_type_name == "TEXT")
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
       if(d6() < 4)
	    return make_shared<comparison_op>(p);
       else if (d6() < 4)
	    return make_shared<bool_term>(p);
       else if (d6() < 4)
	    return make_shared<null_predicate>(p);
       else if (d6() < 4)
	    return make_shared<truth_value>(p);
       else
	    return make_shared<exists_predicate>(p);
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
    out << "cast(" << abbrev_ << "(";
    for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++) {
    out << **expr;
    if (expr+1 != value_exprs.end())
        out << ",", indent(out);
    }
    out << ")";
    out << " as " << cast_type_name_wrapper(type->name) << ")";
}

const_expr::const_expr(prod *p, sqltype *type_constraint)
    : value_expr(p), expr("")
{
    type = type_constraint ? type_constraint : scope->schema->inttype;
      
    if (type == scope->schema->inttype)
        expr = to_string(d100());
    else if (type == scope->schema->booltype)
        expr += (d6() > 3) ? scope->schema->true_literal : scope->schema->false_literal;
//   else if (dynamic_cast<insert_stmt*>(p) && (d6() > 3))
//     expr += "default";
    else
        expr += "cast(null as " + cast_type_name_wrapper(type->name) + ")";
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
        out << "cast(" << **expr << " as " << cast_type_name_wrapper((*expr)->type->name) << ")";
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
        out << " limit 1 offset " << offset;
    
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
  aggregate = make_shared<funcall>(this, type_constraint, true);
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
