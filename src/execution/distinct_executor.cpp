//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), iter_(map_.begin()) {}

void DistinctExecutor::Init() {
  if (!map_.empty()) {
    map_.clear();
  }
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    // todo: figure out why can not use emplace here
    map_.insert({MakeDistinctKey(&tuple), tuple});
  }
  iter_ = map_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == map_.end()) {
    return false;
  }
  *tuple = iter_->second;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}

DistinctKey DistinctExecutor::MakeDistinctKey(const Tuple *tuple) {
  std::vector<Value> keys;
  for (uint64_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
    keys.emplace_back(tuple->GetValue(plan_->OutputSchema(), i));
  }
  return {keys};
}

}  // namespace bustub
