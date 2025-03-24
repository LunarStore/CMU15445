#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::GetRightLeftKeyForHashJoin(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef>& l_key_exprs_out, 
  std::vector<AbstractExpressionRef>& r_key_exprs_out) -> bool {
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
    column_value_expr != nullptr) {
    
    if (column_value_expr->GetTupleIdx() == 0) {
      l_key_exprs_out.emplace_back(std::make_shared<ColumnValueExpression>(0, column_value_expr->GetColIdx(), column_value_expr->GetReturnType()));

    } else {
      BUSTUB_ENSURE(column_value_expr->GetTupleIdx() == 1, "tuple_idx cannot be value other than 1");
      r_key_exprs_out.emplace_back(std::make_shared<ColumnValueExpression>(1, column_value_expr->GetColIdx(), column_value_expr->GetReturnType()));
    }

    return true;
  }

  int ret = false;

  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
    logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
  
    for (const auto &child : expr->GetChildren()) {
      if (!GetRightLeftKeyForHashJoin(child, l_key_exprs_out, r_key_exprs_out)) {
        return false;
      }
    }
    ret = true;
  }

  if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
    cmp_expr != nullptr && cmp_expr->comp_type_ == ComparisonType::Equal) {
  
    for (const auto &child : expr->GetChildren()) {
      if (!GetRightLeftKeyForHashJoin(child, l_key_exprs_out, r_key_exprs_out)) {
        return false;
      }
    }
    ret = true;
  }

  return ret;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly tow child
    BUSTUB_ENSURE(optimized_plan->children_.size() == 2, "NestedLoopJoin should have exactly 2 children.");

    std::vector<AbstractExpressionRef> l_key_exprs;
    std::vector<AbstractExpressionRef> r_key_exprs;
    if (GetRightLeftKeyForHashJoin(nlj_plan.Predicate(), l_key_exprs, r_key_exprs)) {
      return std::make_shared<HashJoinPlanNode> (nlj_plan.output_schema_,
        nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
        l_key_exprs, r_key_exprs,
        nlj_plan.GetJoinType());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
