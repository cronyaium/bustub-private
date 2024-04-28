#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    result_.emplace_back(tuple, rid);
  }
  std::function<bool(const std::pair<Tuple, RID> &, const std::pair<Tuple, RID>)> kp =
      [&](const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) -> bool {
    for (const auto &[tp, expr] : plan_->GetOrderBy()) {
      auto va = expr->Evaluate(&a.first, child_executor_->GetOutputSchema());
      auto vb = expr->Evaluate(&b.first, child_executor_->GetOutputSchema());
      if (va.CompareEquals(vb) == CmpBool::CmpTrue) {
        continue;
      }
      if (tp == OrderByType::ASC || tp == OrderByType::DEFAULT) {
        return va.CompareLessThan(vb) == CmpBool::CmpTrue;
      }
      return va.CompareGreaterThan(vb) == CmpBool::CmpTrue;
    }
    return true;
  };
  std::sort(result_.begin(), result_.end(), kp);
  // std::reverse(result_.begin(), result_.end());
  result_iter_ = result_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iter_ != result_.end()) {
    *tuple = result_iter_->first;
    *rid = result_iter_->second;
    ++result_iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
