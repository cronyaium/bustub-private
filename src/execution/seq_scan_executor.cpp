//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator()) {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_.IsEnd()) {
    return false;
  }
  while (!iter_.IsEnd()) {
    auto meta = iter_.GetTuple().first;    
    *rid = iter_.GetRID();
    auto base_tuple = iter_.GetTuple().second;
    auto txn = exec_ctx_->GetTransaction();
    auto txn_ts = txn->GetReadTs();

    if (meta.ts_ == txn->GetTransactionId() || meta.ts_ <= txn_ts) {
      if (meta.is_deleted_) {
        ++iter_;
        continue;
      }
      *tuple = base_tuple;
      ++iter_;
      return true;
    }
    
    std::vector<UndoLog>undo_logs;
    auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(*rid);
    bool flag = false;
    while (undo_link.has_value() && undo_link->IsValid()) {
      auto undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(undo_link.value());
      undo_logs.emplace_back(undo_log);
      if (undo_log.ts_ <= txn_ts) {
        flag = true;
        break;
      }
      undo_link = std::make_optional<UndoLink>(undo_log.prev_version_);
    }
    if (!flag) {
      ++iter_;
      continue;
    }
    auto ret = ReconstructTuple(&GetOutputSchema(), base_tuple, meta, std::move(undo_logs));
    if (ret.has_value()) {
      *tuple = *ret;
      ++iter_;
      return true;
    }
    ++iter_;

    /*
    auto filter = plan_->filter_predicate_;
    if (filter) {
      auto value = filter->Evaluate(tuple, this->GetOutputSchema());
      if (!value.GetAs<bool>()) {
        ++iter_;
        continue;
      }
    }
    ++iter_;
    return true;
    */
  }
  // iter_.IsEnd() == true
  return false;
}

}  // namespace bustub
