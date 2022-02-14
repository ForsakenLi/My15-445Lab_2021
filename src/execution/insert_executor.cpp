//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(
    ExecutorContext *exec_ctx, const InsertPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)  // 此处为右值引用应使用move，如为通用引用则应使用forward
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      catalog_(exec_ctx->GetCatalog()),
      table_info_(catalog_->GetTable(plan->TableOid())),
      table_heap_(table_info_->table_.get()) {}

void InsertExecutor::Init() {
  if (plan_->IsRawInsert()) {  // 直接插入
    iter_ = plan_->RawValues().begin();
  } else {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  std::vector<Tuple> tuples;

  LockManager *lock_manager = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();

  if (!plan_->IsRawInsert()) {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
  } else {
    // Insert from plan local
    if (iter_ == plan_->RawValues().end()) {  // not Exist value
      return false;
    }
    *tuple = Tuple(*iter_, &table_info_->schema_);
    iter_++;
  }

  //! 这个rid此时不应该被locked，原因在table_page.cpp:77
  if (!table_heap_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
    LOG_DEBUG("INSERT FAIL");
    return false;
  }

  // if have s_lock, upgrade it to x_lock, else apply x_lock
  if (txn->IsSharedLocked(*rid)) {
    if (!lock_manager->LockUpgrade(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  } else {
    if (!lock_manager->LockExclusive(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  // because insert index may cause the index bucket page split(for extendible hash table), so it must be x_lock
  for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {  // 一张表可以建立多个索引
    index->index_->InsertEntry(
        tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());

    txn->GetIndexWriteSet()->emplace_back(
        IndexWriteRecord(*rid, table_info_->oid_, WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
  }

  if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ && !lock_manager->Unlock(txn, *rid)) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  return Next(tuple, rid);
}

}  // namespace bustub
