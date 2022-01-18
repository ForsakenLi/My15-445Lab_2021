//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);
  // next_page_id_.store(0, std::__1::memory_order_relaxed);
  //  Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));  // emplace: 就地构造元素
    // init page metadata
    pages_[i].pin_count_ = 0;
    pages_[i].is_dirty_ = false;
    pages_[i].page_id_ = INVALID_PAGE_ID;
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  // 仅需析构未含unused标记的指针类型成员
  delete[] pages_;
  delete replacer_;
}

// For FetchPgImp, you should return NULL if no page is available in the free list and all other pages are currently
// pinned. FlushPgImp should flush a page regardless of its pin status.
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  FlushPageToDisk(page_id);
  pages_[iter->second].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (auto &page : page_table_) {
    FlushPgImp(page.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t frame_id = FetchFreeFrame();
  if (frame_id == -1) {  // fetch fail
    return nullptr;
  }
  *page_id = AllocatePage();  // 通过指针返回分配的pageID...
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_++;
  page_table_[pages_[frame_id].page_id_] = frame_id;
  // zero out memory
  memset(pages_[frame_id].GetData(), 0, PAGE_SIZE);
  return &pages_[frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    replacer_->Pin(iter->second);
    pages_[iter->second].pin_count_++;
    return &pages_[iter->second];
  }
  frame_id_t frame_id = FetchFreeFrame();  // 1.2 && 2 && 3
  if (frame_id == -1) {
    return nullptr;
  }
  page_table_[page_id] = frame_id;
  replacer_->Pin(page_table_[page_id]);
  pages_[frame_id].pin_count_++;
  pages_[frame_id].page_id_ = page_id;
  // read in the page content from disk
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  return &pages_[frame_id];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  DeallocatePage(page_id);  // ?? attr: unused, 0 ask to call it
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  if (pages_[iter->second].pin_count_ != 0) {
    // someone is using the page
    return false;
  }
  // Remove P from the page table, reset its metadata and return it to the free list.
  free_list_.push_back(iter->second);
  page_table_.erase(page_id);
  pages_[iter->second].is_dirty_ = false;
  pages_[iter->second].page_id_ = INVALID_PAGE_ID;
  memset(pages_[iter->second].GetData(), 0, PAGE_SIZE);
  return true;
}

/**
 * Pin and Unpin within the contexts of the LRUReplacer and the BufferPoolManagerInstance have inverse meanings.
 * Within the context of the LRUReplacer, pinning a page implies that we shouldn't evict the page because it is in use.
 * This means we should remove it from the LRUReplacer. On the other hand, pinning a page in the
 * BufferPoolManagerInstance implies that we want to use a page, and that it should not be removed from the buffer pool.
 * */

// For UnpinPgImp, the is_dirty parameter keeps track of whether a page was modified while it was pinned.
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  if (pages_[iter->second].pin_count_ == 0) {
    return false;
  }
  pages_[iter->second].is_dirty_ = is_dirty;
  pages_[iter->second].pin_count_--;
  // 如果一个page的pin数目降为0, 说明其可以被Replacer按照规则换入换出
  if (pages_[iter->second].pin_count_ == 0) {
    replacer_->Unpin(iter->second);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

void BufferPoolManagerInstance::FlushPageToDisk(page_id_t page_id) {
  auto iter = page_table_.find(page_id);
  frame_id_t fid = -1;
  if (iter != page_table_.end()) {
    fid = iter->second;
  }
  if (fid == -1) {
    return;
  }
  if (pages_[fid].IsDirty()) {  // 非dirty则无需写回
    disk_manager_->WritePage(page_id, pages_[fid].GetData());
  }
}

frame_id_t BufferPoolManagerInstance::FetchFreeFrame() {
  if (!free_list_.empty()) {  // 首先从free_list头部寻找
    frame_id_t res = free_list_.front();
    free_list_.pop_front();
    return res;
  }
  frame_id_t res;
  if (replacer_->Victim(&res)) {  // 如果free_list里没有，从replacer中移除一个page
    page_id_t page_id = pages_[res].page_id_;
    // 将之从内存中清除出去
    FlushPageToDisk(page_id);
    page_table_.erase(page_id);
    pages_[res].is_dirty_ = false;
    return res;
  }
  return -1;
}

}  // namespace bustub
