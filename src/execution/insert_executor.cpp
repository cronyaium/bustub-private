//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!is_done_) {
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    auto &table = table_info->table_;
    Tuple child_tuple;
    RID child_rid;
    int32_t num = 0;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      bool flag = true;
      auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (auto &idx_info : table_indexes) {
        auto &index = idx_info->index_;
        // std::cerr << index->ToString() << std::endl;
        if (index->GetMetadata()->IsPrimaryKey()) {
          auto key = child_tuple.KeyFromTuple(table_info->schema_, idx_info->key_schema_, index->GetKeyAttrs());

          // auto val = key.GetValue(&idx_info->key_schema_, 0);
          std::vector<RID> result;
          index->ScanKey(key, &result, exec_ctx_->GetTransaction());
          if (!result.empty()) {
            flag = false;
            break;
          }
        }
      }
      if (!flag) {
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("Error: detect write-write conflict");
      }

      /** Meta: the ts / txn_id of this tuple. In project 3, simply set it to 0. */
      auto ret = table->InsertTuple({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, child_tuple,
                                    exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->GetTableOid());
      if (ret != std::nullopt) {
        num++;
        for (auto &idx_info : table_indexes) {
          auto &index = idx_info->index_;
          if (!index->InsertEntry(
                  child_tuple.KeyFromTuple(table_info->schema_, idx_info->key_schema_, index->GetKeyAttrs()), *ret,
                  exec_ctx_->GetTransaction())) {
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("Error: detect write-write conflict");
          }
        }
        exec_ctx_->GetTransaction()->AppendWriteSet(plan_->GetTableOid(), ret.value());
      }
    }
    std::vector<Value> values{{TypeId::INTEGER, num}};
    *tuple = Tuple(std::move(values), &this->GetOutputSchema());
    is_done_ = true;
    return true;
  }

  return false;
}

}  // namespace bustub
