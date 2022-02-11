//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

/**
 * 1. 如果事务已经处于ABORT状态，返回false
 * 2. 如果处于2PL的非growth状态，或事务隔离级别为read_uncommitted, abort该事务，返回false
 * 3. 如果该事务已经有S_Lock, 返回true
 * 4. 添加锁到request_queue_和事务的shared_lock_set_中
 * 5. 尝试等待获取锁, 在成功后将锁的状态设为granted
 */
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() != TransactionState::GROWING || txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> mu(latch_);
  auto &lock_req_queue = lock_table_[rid];
  lock_req_queue.request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  txn->GetSharedLockSet()->emplace(rid);

  while (Spin(txn, &lock_req_queue)) {
    lock_req_queue.cv_.wait(mu);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  for (auto &iter : lock_req_queue.request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.granted_ = true;
      break;
    }
  }
  txn->SetState(TransactionState::GROWING);
  return true;
}

/**
 * 1. 如果事务已经处于ABORT状态，返回false
 * 2. 如果处于2PL的非growth状态，abort该事务，返回false
 * 3. 如果该事务已经有X_Lock, 返回true
 * 4. 添加锁到request_queue_和事务的x_lock_set_中
 * 5. 尝试等待获取锁, 在成功后将锁的状态设为granted
 */
bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> mu(latch_);
  auto &lock_req_queue = lock_table_[rid];
  lock_req_queue.request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
  txn->GetExclusiveLockSet()->emplace(rid);

  while (ExclusiveSpin(txn, &lock_req_queue)) {
    lock_req_queue.cv_.wait(mu);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  for (auto &iter : lock_req_queue.request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.granted_ = true;
      break;
    }
  }
  txn->SetState(TransactionState::GROWING);
  return true;
}

/**
 * txn试图将rid的共享锁升级为独占锁。这应该被block当被granted时应该返回true
 * 如果txn被回滚(abort)，则返回false
 * 如果另一个事务已经在等待upgrade他们的锁，这也应该abort该事务并返回false
 */
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> mu(latch_);
  auto &lock_req_queue = lock_table_[rid];

  while (UpgradeSpin(txn, &lock_req_queue)) {
    // 原子地解锁mu，阻塞当前执行线程，并将它添加到于*this上等待的线程列表。线程将在执行 notify_all() 或 notify_one()
    // 时被解除阻塞。 解阻塞时，mu再次锁定且wait退出。
    lock_req_queue.cv_.wait(mu);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }
  // success: set granted, change lock set, change lock_mode
  for (auto &iter : lock_req_queue.request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.granted_ = true;
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->emplace(rid);
      iter.lock_mode_ = LockMode::EXCLUSIVE;
      break;
    }
  }
  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid)) {
    return false;
  }
  std::unique_lock<std::mutex> mu(latch_);
  auto &lock_req_queue = lock_table_[rid];

  bool removed = false;
  for (auto iter = lock_req_queue.request_queue_.begin(); iter != lock_req_queue.request_queue_.end(); iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      removed = true;
      lock_req_queue.request_queue_.erase(iter);
      lock_req_queue.cv_.notify_all();
      break;
    }
  }

  if (!removed) {
    return false;
  }

  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

bool LockManager::Spin(Transaction *txn, LockRequestQueue *lock_req_queue) {
  auto first_iter = lock_req_queue->request_queue_.begin();
  if (first_iter->lock_mode_ == LockMode::SHARED) {
    // because x_lock only get granted when it's the head of queue, so now s_lock can be granted immediately
    return false;
  }
  bool have_abort = false;
  bool need_spin = false;
  for (auto iter = lock_req_queue->request_queue_.begin(); iter->txn_id_ != txn->GetTransactionId(); ++iter) {
    if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
      if (txn->GetTransactionId() < iter->txn_id_) {
        // if a younger txn want x_lock than it should be aborted to avoid deadlock
        auto *younger = TransactionManager::GetTransaction(iter->txn_id_);
        if (younger->GetState() != TransactionState::ABORTED) {
          younger->SetState(TransactionState::ABORTED);
          have_abort = true;
        }
      } else {
        // we can't abort a older lock, so we should wait it get the lock and unlocked
        need_spin = true;
      }
    }
  }

  if (have_abort) {
    // let those txn have been aborted awake and return
    lock_req_queue->cv_.notify_all();
  }
  return need_spin;
}

bool LockManager::UpgradeSpin(Transaction *txn, LockRequestQueue *lock_req_queue) {
  bool have_abort = false;
  bool need_spin = false;
  // Wound-Wait: young wait for old
  // abort every txn younger than me, until no more txn ahead of me
  for (auto iter = lock_req_queue->request_queue_.begin(); iter->txn_id_ != txn->GetTransactionId(); ++iter) {
    if (txn->GetTransactionId() < iter->txn_id_)  // younger txn have bigger txn_id
    {
      auto *younger = TransactionManager::GetTransaction(iter->txn_id_);
      if (younger->GetState() != TransactionState::ABORTED) {
        younger->SetState(TransactionState::ABORTED);
        have_abort = true;
      }
    } else {
      need_spin = true;  // if exist some older txn, we have to spin again to wait it unlock
    }
  }
  if (have_abort) {
    // let those txn have been aborted awake and return
    lock_req_queue->cv_.notify_all();
  }
  return need_spin;
}

bool LockManager::ExclusiveSpin(Transaction *txn, LockRequestQueue *lock_req_queue) {
  auto first_iter = lock_req_queue->request_queue_.begin();
  if (first_iter->txn_id_ == txn->GetTransactionId()) {
    return false;
  }
  bool have_abort = false;
  bool need_spin = false;
  for (auto iter = lock_req_queue->request_queue_.begin(); iter->txn_id_ != txn->GetTransactionId(); ++iter) {
    if (txn->GetTransactionId() < iter->txn_id_)  // younger txn have bigger txn_id
    {
      // abort every txn younger than me
      auto *younger = TransactionManager::GetTransaction(iter->txn_id_);
      if (younger->GetState() != TransactionState::ABORTED) {
        younger->SetState(TransactionState::ABORTED);
        have_abort = true;
      }
    } else {
      need_spin = true;
    }
  }
  if (have_abort) {
    // let those txn have been aborted awake and return
    lock_req_queue->cv_.notify_all();
  }
  return need_spin;
}

}  // namespace bustub
