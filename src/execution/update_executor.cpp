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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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

      updates.emplace_back(child_meta, child_tuple, child_rid);
    }
    for (auto &[u_meta, u_tuple, u_rid] : updates) {
      // table->UpdateTupleInPlace({u_meta.ts_, true}, u_tuple, u_rid);
      std::vector<Value> values;
      for (const auto &target_expr : plan_->target_expressions_) {
        values.push_back(target_expr->Evaluate(&u_tuple, child_executor_->GetOutputSchema()));
      }
      if (u_meta.ts_ != exec_ctx_->GetTransaction()->GetTransactionTempTs()) {
        std::vector<bool> modified_fields(child_executor_->GetOutputSchema().GetColumnCount());
        std::vector<uint32_t> attrs;
        std::vector<Value> undo_values;
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
          auto u_value = u_tuple.GetValue(&child_executor_->GetOutputSchema(), i);
          if (values[i].CompareExactlyEquals(u_value)) {
            modified_fields[i] = false;
          } else {
            modified_fields[i] = true;
            attrs.emplace_back(i);
            undo_values.emplace_back(u_value);
          }
        }
        size_t cnt = std::count_if(modified_fields.begin(), modified_fields.end(), [](const auto x) { return x; });
        if (cnt != 0) {
          auto undo_schema = Schema::CopySchema(&child_executor_->GetOutputSchema(), attrs);
          auto head = exec_ctx_->GetTransactionManager()->GetUndoLink(u_rid);
          UndoLog undo_log = {false, std::move(modified_fields), Tuple(std::move(undo_values), &undo_schema),
                              u_meta.ts_, head.has_value() ? head.value() : UndoLink()};
          auto new_head = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
          exec_ctx_->GetTransactionManager()->UpdateUndoLink(u_rid, new_head);
        }
      } else {
        // update the same tuple
        auto op_link = exec_ctx_->GetTransactionManager()->GetUndoLink(u_rid);
        if (op_link.has_value() && op_link->IsValid()) {
          auto origin_undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(op_link.value());
          auto origin_tuple =
              ReconstructTuple(&child_executor_->GetOutputSchema(), u_tuple, u_meta, {origin_undo_log}).value();
          std::vector<bool> modified_fields(child_executor_->GetOutputSchema().GetColumnCount());
          std::vector<uint32_t> attrs;
          std::vector<Value> undo_values;
          for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            auto u_value = origin_tuple.GetValue(&child_executor_->GetOutputSchema(), i);
            if (values[i].CompareExactlyEquals(u_value) && !origin_undo_log.modified_fields_[i]) {
              modified_fields[i] = false;
            } else {
              modified_fields[i] = true;
              attrs.emplace_back(i);
              undo_values.emplace_back(u_value);
            }
          }
          size_t cnt = std::count_if(modified_fields.begin(), modified_fields.end(), [](const auto x) { return x; });
          if (cnt != 0) {
            auto undo_schema = Schema::CopySchema(&child_executor_->GetOutputSchema(), attrs);
            UndoLog undo_log = {false, std::move(modified_fields), Tuple(std::move(undo_values), &undo_schema),
                                origin_undo_log.ts_, origin_undo_log.prev_version_};
            exec_ctx_->GetTransaction()->ModifyUndoLog(op_link->prev_log_idx_, undo_log);
          }
        }
        /*else {
          std::vector<bool>modified_fields(child_executor_->GetOutputSchema().GetColumnCount());
          std::vector<uint32_t>attrs;
          std::vector<Value>undo_values;
          for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            auto u_value = u_tuple.GetValue(&child_executor_->GetOutputSchema(), i);
            if (values[i].CompareExactlyEquals(u_value)) {
              modified_fields[i] = false;
            } else {
              modified_fields[i] = true;
              attrs.emplace_back(i);
              undo_values.emplace_back(u_value);
            }
          }
          size_t cnt = std::count_if(modified_fields.begin(), modified_fields.end(), [](const auto x) { return x; });
          if (cnt != 0) {
            auto undo_schema = Schema::CopySchema(&child_executor_->GetOutputSchema(), std::move(attrs));
            UndoLog undo_log = {false, std::move(modified_fields), Tuple(std::move(undo_values), &undo_schema),
        exec_ctx_->GetTransaction()->GetReadTs(), }; auto head = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
            exec_ctx_->GetTransactionManager()->UpdateUndoLink(u_rid, head);
          }
        }
        */
      }
      auto new_tuple = Tuple(std::move(values), &child_executor_->GetOutputSchema());

      u_meta.ts_ = exec_ctx_->GetTransaction()->GetTransactionTempTs();
      table->UpdateTupleInPlace(u_meta, new_tuple, u_rid);
      num++;
      exec_ctx_->GetTransaction()->AppendWriteSet(plan_->GetTableOid(), u_rid);
      /*
      auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (auto &idx_info : table_indexes) {
        auto &index = idx_info->index_;
        index->DeleteEntry(child_tuple.KeyFromTuple(table_info->schema_, idx_info->key_schema_, index->GetKeyAttrs()),
                            child_rid, exec_ctx_->GetTransaction());
        index->InsertEntry(new_tuple.KeyFromTuple(table_info->schema_, idx_info->key_schema_, index->GetKeyAttrs()),
                            *ret, exec_ctx_->GetTransaction());
      }
      */
    }
    std::vector<Value> values{{TypeId::INTEGER, num}};
    *tuple = Tuple(std::move(values), &this->GetOutputSchema());
    is_done_ = true;
    return true;
  }

  return false;
}

}  // namespace bustub
