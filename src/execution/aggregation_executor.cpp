//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), aht_(plan->GetAggregates(), plan->GetAggregateTypes()), aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
    child_executor_->Init();
    Tuple child_tuple = Tuple::Empty();
    RID child_rid;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
        aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
    }
    if (plan_->GetGroupBys().empty() && aht_.Begin() == aht_.End()) {
        aht_.Init(MakeAggregateKey(&child_tuple));
    }
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (aht_iterator_ != aht_.End()) {
        auto key = aht_iterator_.Key();
        auto value = aht_iterator_.Val();
        std::vector<Value>output;
        output.reserve(key.group_bys_.size() + value.aggregates_.size());
        output.insert(output.end(), std::make_move_iterator(key.group_bys_.begin()), std::make_move_iterator(key.group_bys_.end()));
        output.insert(output.end(), std::make_move_iterator(value.aggregates_.begin()), std::make_move_iterator(value.aggregates_.end()));
        *tuple = Tuple(std::move(output), &this->GetOutputSchema());
        ++aht_iterator_;
        return true;
    }
    return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
