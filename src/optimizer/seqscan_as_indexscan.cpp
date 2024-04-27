#include "optimizer/optimizer.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    auto comp = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
    if (comp != nullptr) {
      auto lhs = dynamic_cast<const ColumnValueExpression*>(comp->children_[0].get());
      auto rhs = dynamic_cast<ConstantValueExpression*>(comp->children_[1].get());
      if (lhs != nullptr && rhs != nullptr) {
        auto index_info = MatchIndex(seq_scan_plan.table_name_, lhs->GetColIdx());
        if (index_info != std::nullopt) {
          return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, std::get<0>(index_info.value()), seq_scan_plan.filter_predicate_, rhs);
        }
      }
      auto lhs2 = dynamic_cast<const ColumnValueExpression*>(comp->children_[1].get());
      auto rhs2 = dynamic_cast<ConstantValueExpression*>(comp->children_[0].get());
      if (lhs2 != nullptr && rhs2 != nullptr) {
        auto index_info = MatchIndex(seq_scan_plan.table_name_, lhs2->GetColIdx());
        if (index_info != std::nullopt) {
          return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, std::get<0>(index_info.value()), seq_scan_plan.filter_predicate_, rhs2);
        }
      }
    }
  }
  
  return optimized_plan;
}

}  // namespace bustub
