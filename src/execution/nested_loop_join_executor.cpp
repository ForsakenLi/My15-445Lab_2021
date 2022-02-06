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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() { left_executor_->Init(); }

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // if no more loop res, start a new loop from a new outer table line
  if (!loop_res_.empty()) {
    *tuple = loop_res_.front();
    loop_res_.pop();
    return true;
  }
  RID outer_rid;
  Tuple outer_tuple;
  //! outer table advance 1
  if (!left_executor_->Next(&outer_tuple, &outer_rid)) {
    // LOG_DEBUG("no more outer line");
    return false;
  }
  RID inner_rid;
  Tuple inner_tuple;
  //! every time loop, loop inner table from start
  right_executor_->Init();
  while (right_executor_->Next(&inner_tuple, &inner_rid)) {
    if (plan_->Predicate() == nullptr || plan_->Predicate()
                                             ->EvaluateJoin(&outer_tuple, left_executor_->GetOutputSchema(),
                                                            &inner_tuple, right_executor_->GetOutputSchema())
                                             .GetAs<bool>()) {
      std::vector<Value> output;
      for (const auto &column : GetOutputSchema()->GetColumns()) {
        output.emplace_back(column.GetExpr()->EvaluateJoin(&outer_tuple, left_executor_->GetOutputSchema(),
                                                           &inner_tuple, right_executor_->GetOutputSchema()));
      }
      loop_res_.emplace(Tuple(output, GetOutputSchema()));
    }
  }

  return Next(tuple, rid);
}

}  // namespace bustub
