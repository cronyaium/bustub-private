//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  // this txn can read anything affected by any commited txn
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  txn->commit_ts_ = last_commit_ts_.load();

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  txn->commit_ts_ = ++last_commit_ts_;
  for (const auto &[table_oid, rid_set] : txn->GetWriteSets()) {
    auto &table = catalog_->GetTable(table_oid)->table_;
    for (const auto &rid : rid_set) {
      auto [meta, tuple] = table->GetTuple(rid);
      meta.ts_ = txn->commit_ts_;
      table->UpdateTupleInPlace(meta, tuple, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  auto wm = running_txns_.watermark_;

  std::unordered_map<txn_id_t, std::vector<int>> invisible;
  for (const auto &table_name : catalog_->GetTableNames()) {
    auto table_iter = catalog_->GetTable(table_name)->table_->MakeIterator();
    while (!table_iter.IsEnd()) {
      auto [meta, tuple] = table_iter.GetTuple();
      auto rid = table_iter.GetRID();
      auto undo_link = GetUndoLink(rid);

      // 找到最大的 ts<wm 的log
      bool flag = meta.ts_ <= wm;
      if (!flag) {
        while (undo_link.has_value() && undo_link->IsValid()) {
          auto undo_log = GetUndoLogOptional(undo_link.value());
          if (!undo_log.has_value()) {
            break;
          }
          undo_link = std::make_optional<UndoLink>(undo_log->prev_version_);
          if (undo_log->ts_ < wm) {
            flag = true;
            break;
          }
        }
      }
      if (flag) {
        while (undo_link.has_value() && undo_link->IsValid()) {
          auto undo_log = GetUndoLogOptional(undo_link.value());
          if (!undo_log.has_value()) {
            break;
          }
          auto [txn_id, log_id] = undo_link.value();
          invisible[txn_id].emplace_back(log_id);
          undo_link = std::make_optional<UndoLink>(undo_log->prev_version_);
        }
      }
      ++table_iter;
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (const auto &[txn_id, inv_vec] : invisible) {
    if (txn_map_.find(txn_id) != txn_map_.end()) {
      auto txn = txn_map_.at(txn_id);
      if ((txn->GetTransactionState() == TransactionState::COMMITTED ||
           txn->GetTransactionState() == TransactionState::ABORTED) &&
          txn->GetUndoLogNum() == inv_vec.size()) {
        txn_map_.erase(txn_id);
      }
    }
  }
  std::vector<txn_id_t> removes;
  for (const auto &[txn_id, txn] : txn_map_) {
    if ((txn->GetTransactionState() == TransactionState::COMMITTED ||
         txn->GetTransactionState() == TransactionState::ABORTED) &&
        txn->GetUndoLogNum() == 0) {
      removes.emplace_back(txn_id);
    }
  }
  for (const auto &txn_id : removes) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
