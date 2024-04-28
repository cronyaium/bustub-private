//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  std::vector<Tuple> right;
  Tuple right_tuple;
  RID right_rid;
  while (right_child_->Next(&right_tuple, &right_rid)) {
    hash_[MakeRightHJKey(&right_tuple)].value_.emplace_back(std::move(right_tuple));
  }

  Tuple left_tuple;
  RID left_rid;
  while (left_child_->Next(&left_tuple, &left_rid)) {
    right_child_->Init();
    auto left_key = MakeLeftHJKey(&left_tuple);
    if (hash_.find(left_key) != hash_.end()) {
      for (const auto &tuple : hash_[left_key].value_) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(tuple.GetValue(&right_child_->GetOutputSchema(), i));
        }
        result_.emplace_back(values, &this->GetOutputSchema());
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      result_.emplace_back(values, &this->GetOutputSchema());
    }
  }
  result_iter_ = result_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iter_ != result_.end()) {
    *tuple = *result_iter_;
    ++result_iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
