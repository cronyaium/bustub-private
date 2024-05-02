//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!is_done_) {
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    auto &table = table_info->table_;
    Tuple scan_tuple;
    RID scan_rid;
    int32_t num = 0;
    std::vector<std::pair<Tuple, RID>> buffer;
    while (child_executor_->Next(&scan_tuple, &scan_rid)) {
      buffer.emplace_back(scan_tuple, scan_rid);
    }
    const int64_t mask = 1ULL << 62;
    std::vector<std::tuple<TupleMeta, Tuple, RID>> updates;
    for (auto &[child_tuple, child_rid] : buffer) {
      auto child_meta = table->GetTupleMeta(child_rid);
      if ((child_meta.ts_ >= mask && child_meta.ts_ != exec_ctx_->GetTransaction()->GetTransactionId()) ||
          (child_meta.ts_ < mask && child_meta.ts_ > exec_ctx_->GetTransaction()->GetReadTs())) {
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("Error: detect a write-write conflict");
      }

      // child_meta.ts_ = exec_ctx_->GetTransaction()->GetTransactionTempTs();
      child_meta.is_deleted_ = true;
      updates.emplace_back(child_meta, child_tuple, child_rid);

      num++;
      /*
      auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (auto &idx_info : table_indexes) {
        auto &index = idx_info->index_;
        index->DeleteEntry(child_tuple.KeyFromTuple(table_info->schema_, idx_info->key_schema_, index->GetKeyAttrs()),
                           child_rid, exec_ctx_->GetTransaction());
      }
      */
    }
    for (auto &[u_meta, u_tuple, u_rid] : updates) {
      // If the tuple is newly-inserted, no undo log needs to be created
      if (u_meta.ts_ != exec_ctx_->GetTransaction()->GetTransactionTempTs()) {
        std::vector<bool> modified_fields(child_executor_->GetOutputSchema().GetColumnCount(), true);
        auto head = exec_ctx_->GetTransactionManager()->GetUndoLink(u_rid);
        UndoLog undo_log = {false, std::move(modified_fields), u_tuple, u_meta.ts_,
                            head.has_value() ? head.value() : UndoLink()};
        auto new_head = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
        exec_ctx_->GetTransactionManager()->UpdateUndoLink(u_rid, new_head);
      }
      u_meta.ts_ = exec_ctx_->GetTransaction()->GetTransactionTempTs();
      table->UpdateTupleInPlace(u_meta, u_tuple, u_rid);
      exec_ctx_->GetTransaction()->AppendWriteSet(plan_->GetTableOid(), u_rid);
    }

    std::vector<Value> values{{TypeId::INTEGER, num}};
    *tuple = Tuple(std::move(values), &this->GetOutputSchema());
    is_done_ = true;
    return true;
  }

  return false;
}

}  // namespace bustub
