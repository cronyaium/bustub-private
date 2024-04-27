//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
    for (size_t i = 0; i < plan_->GetLimit(); i++) {
        Tuple tuple;
        RID rid;
        if (child_executor_->Next(&tuple, &rid)) {
            result_.emplace_back(tuple, rid);
        } else {
            break;
        }
    }
    result_iter_ = result_.begin();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (result_iter_ != result_.end()) {
        *tuple = result_iter_->first;
        *rid = result_iter_->second;
        ++result_iter_;
        return true;
    }
    return false;
}

}  // namespace bustub
