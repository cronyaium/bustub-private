//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan), iter_(std::move(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator())) { }

void SeqScanExecutor::Init() { }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
	if (iter_.IsEnd()) {
		return false;
	}
	while (!iter_.IsEnd()) {
		if (iter_.GetTuple().first.is_deleted_) {
			++iter_;
			continue;
		}
		*tuple = iter_.GetTuple().second;
		*rid = iter_.GetRID();
		auto filter = plan_->filter_predicate_;
		if (filter) {
			auto value = filter->Evaluate(tuple, this->GetOutputSchema());
			if (!value.GetAs<bool>()) {
				++iter_;
				continue;
			}
		}
		++iter_;
		return true;
	}
	return false;
}

}  // namespace bustub
