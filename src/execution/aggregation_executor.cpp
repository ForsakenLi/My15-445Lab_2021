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
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  //! prepare aggregation hash table
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  auto aggr_key = aht_iterator_.Key();
  auto aggr_val = aht_iterator_.Val();
  ++aht_iterator_;
  //! judge having predicate
  if (plan_->GetHaving() == nullptr ||
      plan_->GetHaving()->EvaluateAggregate(aggr_key.group_bys_, aggr_val.aggregates_).GetAs<bool>()) {
    std::vector<Value> output;
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
      output.emplace_back(column.GetExpr()->EvaluateAggregate(aggr_key.group_bys_, aggr_val.aggregates_));
    }
    *tuple = Tuple(output, plan_->OutputSchema());
    return true;
  }
  // check next match having condition or not
  return Next(tuple, rid);
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
