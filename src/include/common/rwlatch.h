//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwmutex.h
//
// Identification: src/include/common/rwlatch.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <climits>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT

#include "common/macros.h"

namespace bustub {

/**
 * Reader-Writer latch backed by std::mutex.
 */
class ReaderWriterLatch {
  using mutex_t = std::mutex;
  using cond_t = std::condition_variable;
  static const uint32_t MAX_READERS = UINT_MAX;

 public:
  ReaderWriterLatch() = default;
  ~ReaderWriterLatch() { std::lock_guard<mutex_t> guard(mutex_); }  // wait for every function end

  DISALLOW_COPY(ReaderWriterLatch);

  /**
   * Acquire a write latch.
   */
  void WLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_) {  // 如有其他写者占用需要等待前一个写者释放
      reader_.wait(latch);
    }
    writer_entered_ = true;
    while (reader_count_ > 0) {  // 如有其他读者也需等待，同时writer_entered=true意味着不会有新的读者
      writer_.wait(latch);  // 写者条件变量仅在所有读者退出时(reader_count == 0)才会结束等待
    }
  }

  //! 写者条件变量解决的是reader_count == 0的busy-wait问题，因为仅在reader_count == 0时写者才能拿到写锁
  //
  //! 读者条件变量解决的是reader_count_ < MAX_READERS和writer_entered_ == false两个的busy_wait
  //! 仅在这两个条件满足时才能获取到读锁

  /**
   * Release a write latch.
   */
  void WUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    writer_entered_ = false;
    reader_.notify_all(); // 写锁释放的是reader_的条件变量
  }

  /**
   * Acquire a read latch.
   */
  void RLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_ || reader_count_ == MAX_READERS) {
      reader_.wait(latch);
    }
    reader_count_++;
  }

  /**
   * Release a read latch.
   */
  void RUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    reader_count_--;
    if (writer_entered_) {
      if (reader_count_ == 0) {
        writer_.notify_one();  // 当读者数目为0即可通知写者
      }
    } else {
      if (reader_count_ == MAX_READERS - 1) {
        reader_.notify_one();  // 先前读者数目为满在自己release后-1即可通知还在等待的读者
      }
    }
  }

 private:
  mutex_t mutex_;
  cond_t writer_;
  cond_t reader_;
  uint32_t reader_count_{0};
  bool writer_entered_{false};
};

}  // namespace bustub
