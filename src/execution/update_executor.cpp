//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!is_done_) {
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    auto &table = table_info->table_;
    Tuple child_tuple;
    RID child_rid;
    int32_t num = 0;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      /** Meta: the ts / txn_id of this tuple. In project 3, simply set it to 0. */
      table->UpdateTupleMeta({0, true}, child_rid);

      std::vector<Value> values;
      for (const auto &target_expr : plan_->target_expressions_) {
        values.push_back(target_expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
      }
      auto new_tuple = Tuple(std::move(values), &child_executor_->GetOutputSchema());

      auto ret = table->InsertTuple({0, false}, new_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
                                    plan_->GetTableOid());
      if (ret != std::nullopt) {
        num++;
        auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
        for (auto &idx_info : table_indexes) {
          auto &index = idx_info->index_;
          index->DeleteEntry(child_tuple.KeyFromTuple(table_info->schema_, idx_info->key_schema_, index->GetKeyAttrs()),
                             child_rid, exec_ctx_->GetTransaction());
          index->InsertEntry(new_tuple.KeyFromTuple(table_info->schema_, idx_info->key_schema_, index->GetKeyAttrs()),
                             *ret, exec_ctx_->GetTransaction());
        }
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
