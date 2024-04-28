//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/** HJKey represents a key in an hash-join operation */
struct HJKey {
  /** The key_ values */
  std::vector<Value> key_;

  /**
   * Compares two HJKey keys for equality.
   * @param other the other HJKey key to be compared with
   * @return `true` if both HJKey keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const HJKey &other) const -> bool {
    for (uint32_t i = 0; i < other.key_.size(); i++) {
      if (key_[i].CompareEquals(other.key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** HJValue represents a value for each of the running hash-join */
struct HJValue {
  /** The tuple values */
  std::vector<Tuple> value_;
};

struct HJKeyHash {
  auto operator()(const HJKey &hj_key) const -> std::size_t {
    std::size_t curr_hash = 0;
    for (const auto &key : hj_key.key_) {
      if (!key.IsNull()) {
        curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;

  std::unique_ptr<AbstractExecutor> right_child_;

  std::vector<Tuple> result_{};

  decltype(result_.begin()) result_iter_{result_.begin()};

  std::unordered_map<HJKey, HJValue, HJKeyHash> hash_{};

  /** @return The tuple as an LeftHJKey */
  auto MakeLeftHJKey(const Tuple *tuple) -> HJKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an RightHJKey */
  auto MakeRightHJKey(const Tuple *tuple) -> HJKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
    }
    return {keys};
  }
};

}  // namespace bustub
