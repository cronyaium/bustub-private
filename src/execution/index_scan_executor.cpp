//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())) {}

void IndexScanExecutor::Init() {
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  std::vector<Value> values{plan_->pred_key_->val_};
  auto key_schema = Schema({Column("index", values[0].GetTypeId())});
  auto key = Tuple(std::move(values), &key_schema);
  htable->ScanKey(key, &ret_, exec_ctx_->GetTransaction());
  iter_ = ret_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != ret_.end()) {
    *rid = *iter_;
    *tuple = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(*rid).second;
    auto filter = plan_->filter_predicate_;
    if (filter) {
      auto value = filter->Evaluate(tuple, this->GetOutputSchema());
      if (!value.GetAs<bool>()) {
        continue;
      }
    }
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
