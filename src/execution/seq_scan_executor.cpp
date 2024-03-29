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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      schema_(&exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->schema_),
      table_heap_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()),
      iter_(table_heap_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() { iter_ = table_heap_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == table_heap_->End()) {
    return false;
  }

  *tuple = *iter_;
  // The output of sequential scan is a copy of each matched tuple and its original record identifier (RID)
  *rid = tuple->GetRid();
  LockManager *lock_manager = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && !lock_manager->LockShared(txn, *rid)) {
    // if isolation level require s_lock but failed to get
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  std::vector<Value> values;
  for (size_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
    values.emplace_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(tuple, schema_));
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  ++iter_;

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !lock_manager->Unlock(txn, *rid)) {
    // for read committed, when read finish we should release the s_lock
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  //! 使用predicate判断该tuple是否应该输出，不满足再次发起Next调用
  const AbstractExpression *predict = plan_->GetPredicate();
  if (predict != nullptr && !predict->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
    return Next(tuple, rid);
  }

  return true;
}

}  // namespace bustub
