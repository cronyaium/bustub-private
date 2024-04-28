#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
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
  std::priority_queue<std::pair<Tuple, RID>, std::vector<std::pair<Tuple, RID>>, decltype(kp)> pq(kp);

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    pq.emplace(tuple, rid);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
  while (!pq.empty()) {
    result_.emplace_back(pq.top());
    pq.pop();
  }
  std::reverse(result_.begin(), result_.end());
  result_iter_ = result_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iter_ != result_.end()) {
    *tuple = result_iter_->first;
    *rid = result_iter_->second;
    ++result_iter_;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return 0; };

}  // namespace bustub
