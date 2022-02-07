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

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple outer_tuple;
  RID outer_rid;
  //! prepare for outer table hash
  while (left_child_->Next(&outer_tuple, &outer_rid)) {
    my_map_.insert(GetMyJoinKey(&outer_tuple, true), outer_tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!loop_res_.empty()) {
    *tuple = loop_res_.front();
    loop_res_.pop();
    return true;
  }

  Tuple inner_tuple;
  RID inner_rid;
  // inner table advance 1
  if (!right_child_->Next(&inner_tuple, &inner_rid)) {
    return false;
  }

  if (!my_map_.exist(GetMyJoinKey(&inner_tuple, false))) {
    return Next(tuple, rid);
  }

  auto match_tuples =
      my_map_[GetMyJoinKey(&inner_tuple, false)];  // those tuples have the same hash value with the inner_tuple
  for (const auto &match_outer_tuple : match_tuples) {
    std::vector<Value> output;
    // evaluate match_outer_tuple whether can join with inner_tuple or not
    for (const auto &column : GetOutputSchema()->GetColumns()) {
      output.emplace_back(column.GetExpr()->EvaluateJoin(&match_outer_tuple, left_child_->GetOutputSchema(),
                                                         &inner_tuple, right_child_->GetOutputSchema()));
    }
    loop_res_.emplace(Tuple(output, GetOutputSchema()));
  }

  return Next(tuple, rid);
}

MyHashKey HashJoinExecutor::GetMyJoinKey(const Tuple *tuple, bool isLeft) {
  MyHashKey res;
  if (isLeft) {
    res.val_ = plan_->LeftJoinKeyExpression()->Evaluate(tuple, left_child_->GetOutputSchema());
  } else {
    res.val_ = plan_->RightJoinKeyExpression()->Evaluate(tuple, right_child_->GetOutputSchema());
  }
  return res;
}

}  // namespace bustub
