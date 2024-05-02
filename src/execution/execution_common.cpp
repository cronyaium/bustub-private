#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  Tuple new_tuple = base_tuple;
  TupleMeta new_meta = base_meta;
  for (const auto &undo : undo_logs) {
    if (undo.is_deleted_) {
      // new_tuple = Tuple::Empty();
      new_meta.is_deleted_ = true;
      continue;
    }
    new_meta.is_deleted_ = false;

    // construct schema
    std::vector<uint32_t> attrs;
    for (decltype(undo.modified_fields_.size()) i = 0; i < undo.modified_fields_.size(); i++) {
      if (undo.modified_fields_[i]) {
        attrs.emplace_back(static_cast<uint32_t>(i));
      }
    }
    auto undo_tuple_schema = Schema::CopySchema(schema, attrs);

    uint32_t ii = 0;
    std::vector<Value> values(undo.modified_fields_.size());
    for (decltype(undo.modified_fields_.size()) i = 0; i < undo.modified_fields_.size(); i++) {
      if (!undo.modified_fields_[i]) {
        values[i] = new_tuple.GetValue(schema, i);
      } else {
        values[i] = undo.tuple_.GetValue(&undo_tuple_schema, ii++);
      }
    }
    new_tuple = Tuple(std::move(values), schema);
  }
  if (new_meta.is_deleted_) {
    return std::nullopt;
  }
  return new_tuple;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  /*
  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");
  */
  auto table_iter = table_heap->MakeIterator();
  while (!table_iter.IsEnd()) {
    auto [meta, tuple] = table_iter.GetTuple();
    auto rid = table_iter.GetRID();

    const int64_t mask = 1ULL << 62;
    if ((meta.ts_ & mask) != 0) {
      fmt::println(stderr, "RID={}/{} ts=txn{} tuple={}", rid.GetPageId(), rid.GetSlotNum(), meta.ts_ ^ mask,
                   tuple.ToString(&table_info->schema_));
    } else {
      fmt::println(stderr, "RID={}/{} ts={} tuple={}", rid.GetPageId(), rid.GetSlotNum(), meta.ts_,
                   tuple.ToString(&table_info->schema_));
    }
    auto undo_link = txn_mgr->GetUndoLink(rid);

    while (undo_link.has_value() && undo_link->IsValid()) {
      auto undo_log = txn_mgr->GetUndoLogOptional(undo_link.value());
      if (!undo_log.has_value()) {
        break;
      }
      // undo_log.is_deleted_ ? "<Del>" : undo_log.tuple_.ToString(&table_info->schema_)
      if (undo_log->is_deleted_) {
        fmt::println(stderr, "txn{}@{} {} ts={}", undo_link->prev_txn_ ^ mask, undo_link->prev_log_idx_, "<Del>",
                     undo_log->ts_);
      } else {
        std::vector<uint32_t> attrs;
        for (uint32_t i = 0; i < undo_log->modified_fields_.size(); i++) {
          if (undo_log->modified_fields_[i]) {
            attrs.emplace_back(i);
          }
        }
        auto schema = Schema::CopySchema(&table_info->schema_, attrs);
        fmt::println(stderr, "txn{}@{} {} ts={}", undo_link->prev_txn_ ^ mask, undo_link->prev_log_idx_,
                     undo_log->tuple_.ToString(&schema), undo_log->ts_);
      }

      undo_link = std::make_optional<UndoLink>(undo_log->prev_version_);
    }
    ++table_iter;
  }
  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
