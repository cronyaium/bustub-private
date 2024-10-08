//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  std::vector<Tuple> right;
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    right.push_back(right_tuple);
  }

  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    // Assertion `(casted_right_executor->GetInitCount() + 1 >= casted_left_executor->GetNextCount()) && ("nlj check
    // failed, are you initialising the right executor every time when there is a left tuple? " "(off-by-one is okay)")'
    // failed.
    right_executor_->Init();
    bool match = false;
    for (const auto &right_tuple : right) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }

      auto value = plan_->predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        result_.emplace_back(values, &this->GetOutputSchema());
        match = true;
      }
    }
    if (!match && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      result_.emplace_back(values, &this->GetOutputSchema());
    }
  }
  result_iter_ = result_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iter_ != result_.end()) {
    *tuple = *result_iter_;
    ++result_iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
