#include <memory>
#include <vector>
#include "execution/plans/filter_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/index_scan_plan.h"

#include "optimizer/optimizer.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {

auto Optimizer::OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
      if (seq_scan_plan.filter_predicate_ == nullptr) {
        auto comp = dynamic_cast<const ComparisonExpression *>(filter_plan.GetPredicate().get());
        if (comp != nullptr) {
          auto lhs = dynamic_cast<const ColumnValueExpression*>(comp->children_[0].get());
          auto rhs = dynamic_cast<ConstantValueExpression*>(comp->children_[1].get());
          if (lhs != nullptr && rhs != nullptr) {
            auto index_info = MatchIndex(seq_scan_plan.table_name_, lhs->GetColIdx());
            if (index_info != std::nullopt) {
              return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, std::get<0>(index_info.value()), filter_plan.GetPredicate(), rhs);
            }
          }
          auto lhs2 = dynamic_cast<const ColumnValueExpression*>(comp->children_[1].get());
          auto rhs2 = dynamic_cast<ConstantValueExpression*>(comp->children_[0].get());
          if (lhs2 != nullptr && rhs2 != nullptr) {
            auto index_info = MatchIndex(seq_scan_plan.table_name_, lhs2->GetColIdx());
            if (index_info != std::nullopt) {
              return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, std::get<0>(index_info.value()), filter_plan.GetPredicate(), rhs2);
            }
          }
        }
        return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                 seq_scan_plan.table_name_, filter_plan.GetPredicate());
      }
      
    }
  }

  return optimized_plan;
}

}  // namespace bustub
