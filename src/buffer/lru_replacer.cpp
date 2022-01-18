//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { num_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

// 将一个LRU尾部的页表从Replacer中换出
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (page_list_.empty()) {
    *frame_id = INVALID_PAGE_ID;
    return false;
  }
  *frame_id = page_list_.back();  // end是获取尾部迭代器, back是最后一个值
  page_list_.pop_back();
  cache_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (!InLRUReplacer(frame_id)) {
    return;
  }
  page_list_.erase(cache_[frame_id]);
  cache_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (InLRUReplacer(frame_id)) {
    return;
  }
  page_list_.push_front(frame_id);
  cache_[frame_id] = page_list_.begin();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock_guard(latch_);
  return page_list_.size();
}

bool LRUReplacer::InLRUReplacer(frame_id_t frame_id) { return cache_.count(frame_id) > 0; }

}  // namespace bustub
