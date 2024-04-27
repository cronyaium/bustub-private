#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    auto left_plan = nlj_plan.GetLeftPlan();
    if (left_plan->GetType() == PlanType::NestedLoopJoin) {
      left_plan = OptimizeNLJAsHashJoin(left_plan);
    }
    auto right_plan = nlj_plan.GetRightPlan();
    if (right_plan->GetType() == PlanType::NestedLoopJoin) {
      right_plan = OptimizeNLJAsHashJoin(right_plan);
    }
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;

    std::function<void(AbstractExpressionRef, std::vector<AbstractExpressionRef>&, std::vector<AbstractExpressionRef>&)>
    dfs = [&](AbstractExpressionRef expr, std::vector<AbstractExpressionRef> &left, std::vector<AbstractExpressionRef> &right) -> void {
      auto comp_expr = dynamic_cast<ComparisonExpression *>(expr.get());
      if (comp_expr != nullptr) {
        auto ch = comp_expr->GetChildAt(0);
        auto column = dynamic_cast<ColumnValueExpression *>(ch.get());
        BUSTUB_ASSERT(column != nullptr, "dynamic_cast to ColumnValueExpression * failed");
        if (column->GetTupleIdx() == 0) {
          left.push_back(ch);
        } else {
          right.push_back(ch);
        }
        
        ch = comp_expr->GetChildAt(1);
        column = dynamic_cast<ColumnValueExpression *>(ch.get());
        BUSTUB_ASSERT(column != nullptr, "dynamic_cast to ColumnValueExpression * failed");
        if (column->GetTupleIdx() == 0) {
          left.push_back(ch);
        } else {
          right.push_back(ch);
        }
        return;
      }
      for (auto& subexpr : expr->GetChildren()) {
        dfs(subexpr, left, right);
      }
    };
    dfs(nlj_plan.Predicate(), left_key_expressions, right_key_expressions);

    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, std::move(left_plan), std::move(right_plan), std::move(left_key_expressions), std::move(right_key_expressions), nlj_plan.GetJoinType());
  }
  return optimized_plan;
}

}  // namespace bustub
