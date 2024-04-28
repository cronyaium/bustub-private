#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  std::vector<std::pair<Tuple, RID>> child_tuple;
  while (child_executor_->Next(&tuple, &rid)) {
    child_tuple.emplace_back(tuple, rid);
  }
  for (const auto &[_, wf] : plan_->window_functions_) {
    if (!wf.order_by_.empty()) {
      std::function<bool(const std::pair<Tuple, RID> &, const std::pair<Tuple, RID>)> kp =
          [&wf = wf, &child_executor = child_executor_](const std::pair<Tuple, RID> &a,
                                                        const std::pair<Tuple, RID> &b) -> bool {
        for (const auto &[tp, expr] : wf.order_by_) {
          auto va = expr->Evaluate(&a.first, child_executor->GetOutputSchema());
          auto vb = expr->Evaluate(&b.first, child_executor->GetOutputSchema());
          if (va.CompareEquals(vb) == CmpBool::CmpTrue) {
            continue;
          }
          if (tp == OrderByType::ASC || tp == OrderByType::DEFAULT) {
            return va.CompareLessThan(vb) == CmpBool::CmpTrue;
          }
          return va.CompareGreaterThan(vb) == CmpBool::CmpTrue;
        }
        return true;
      };
      std::sort(child_tuple.begin(), child_tuple.end(), kp);
      break;
    }
  }

  // winidx -> my_hash_idx
  std::unordered_map<uint32_t, uint32_t> hash;
  uint32_t hash_idx = 0;
  std::vector<std::vector<Value>> values(plan_->window_functions_.size());
  for (const auto &[winidx, wf] : plan_->window_functions_) {
    if (hash.find(winidx) == hash.end()) {
      hash[winidx] = hash_idx++;
    }
    ht_.clear();

    std::unordered_map<AggregateKey, std::pair<Value, Value>> pre_input{};
    for (const auto &[tuple, rid] : child_tuple) {
      auto key = MakeAggregateKey(&tuple, wf.partition_by_);
      if (ht_.find(key) == ht_.end()) {
        // InsertInitValue
        switch (wf.type_) {
          case WindowFunctionType::CountStarAggregate:
            // Count start starts at zero.
            ht_[key] = ValueFactory::GetIntegerValue(0);
            break;
          case WindowFunctionType::CountAggregate:
          case WindowFunctionType::SumAggregate:
          case WindowFunctionType::MinAggregate:
          case WindowFunctionType::MaxAggregate:
          case WindowFunctionType::Rank:
            // Others starts at null.
            ht_[key] = ValueFactory::GetNullValueByType(TypeId::INTEGER);
            break;
        }
      }
      auto input = wf.function_->Evaluate(&tuple, child_executor_->GetOutputSchema());
      switch (wf.type_) {
        case WindowFunctionType::CountStarAggregate:
          ht_[key] = ht_[key].Add(ValueFactory::GetIntegerValue(1));
          break;
        case WindowFunctionType::CountAggregate:
          if (!input.IsNull()) {
            if (ht_[key].IsNull()) {
              ht_[key] = ValueFactory::GetIntegerValue(1);
            } else {
              ht_[key] = ht_[key].Add(ValueFactory::GetIntegerValue(1));
            }
          }
          break;
        case WindowFunctionType::SumAggregate:
          if (!input.IsNull()) {
            if (ht_[key].IsNull()) {
              ht_[key] = input;
            } else {
              ht_[key] = ht_[key].Add(input);
            }
          }
          break;
        case WindowFunctionType::MinAggregate:
          if (!input.IsNull()) {
            if (ht_[key].IsNull()) {
              ht_[key] = input;
            } else {
              ht_[key] = ht_[key].Min(input);
            }
          }
          break;
        case WindowFunctionType::MaxAggregate:
          if (!input.IsNull()) {
            if (ht_[key].IsNull()) {
              ht_[key] = input;
            } else {
              ht_[key] = ht_[key].Max(input);
            }
          }
          break;
        case WindowFunctionType::Rank:
          // order by clause is mandatory for rank function
          input = wf.order_by_[0].second->Evaluate(&tuple, child_executor_->GetOutputSchema());
          if (!input.IsNull()) {
            if (ht_[key].IsNull()) {
              ht_[key] = ValueFactory::GetIntegerValue(1);
              pre_input[key].second = ValueFactory::GetIntegerValue(1);
            } else {
              if (input.CompareEquals(pre_input.at(key).first) == CmpBool::CmpTrue) {
                pre_input[key].second = pre_input[key].second.Add(ValueFactory::GetIntegerValue(1));
              } else {
                ht_[key] = ht_[key].Add(pre_input[key].second);
                pre_input[key].second = ValueFactory::GetIntegerValue(1);
              }
            }
            pre_input[key].first = input;
          }
          break;
      }
      if (!wf.order_by_.empty()) {
        values[hash[winidx]].emplace_back(ht_.at(key));
      }
    }
    if (wf.order_by_.empty()) {
      for (const auto &[tuple, rid] : child_tuple) {
        auto key = MakeAggregateKey(&tuple, wf.partition_by_);
        values[hash[winidx]].emplace_back(ht_.at(key));
      }
    }
  }

  // values array
  // first dimension: my_hash_idx
  // second dimension: tuple_idx
  int jj = 0;
  for (const auto &[tuple, rid] : child_tuple) {
    std::vector<Value> tuple_values;
    int ii = 0;
    for (const auto &expr : plan_->columns_) {
      auto column_expr = dynamic_cast<ColumnValueExpression *>(expr.get());
      if (column_expr->GetColIdx() == static_cast<uint32_t>(-1)) {
        tuple_values.emplace_back(values[hash[ii]][jj]);
        ++ii;
      } else {
        tuple_values.emplace_back(column_expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
      }
    }
    result_.emplace_back(Tuple(std::move(tuple_values), &this->GetOutputSchema()));
    ++jj;
  }
  result_iter_ = result_.begin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iter_ != result_.end()) {
    *tuple = *result_iter_;
    ++result_iter_;
    return true;
  }
  return false;
}
}  // namespace bustub
