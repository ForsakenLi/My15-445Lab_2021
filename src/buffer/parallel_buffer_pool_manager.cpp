//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : bp_instances_{num_instances}, pool_size_(pool_size) {
  // Allocate and create individual BufferPoolManagerInstances
  for (uint32_t index = 0; index < num_instances; index++) {
    bp_instances_[index] = new BufferPoolManagerInstance(pool_size, num_instances, index, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (auto &instance : bp_instances_) {
    delete instance;  // 基类的析构函数是虚函数, 虚函数表在子类构造时已被覆盖
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return bp_instances_.size() * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return bp_instances_[page_id % bp_instances_.size()];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FetchPage(page_id);  // FetchPage call FetchPgImp
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called

  // 这里传入的page_id是一个地址，其id由我们的BufferPoolManagerInstance实现写入
  // 而真正的page号是由BufferPoolManagerInstance的AllocatePage方法完成分配的，因此可以保证分配的page_id
  // 在取余后能被hash到同样的BufferPoolManagerInstance上

  uint32_t start_index = next_instance_index_;
  do {
    auto page = bp_instances_[next_instance_index_ % bp_instances_.size()]->NewPage(page_id);
    if (page != nullptr) {
      next_instance_index_ = (next_instance_index_ + 1) % bp_instances_.size();
      return page;
    }
    next_instance_index_ = (next_instance_index_ + 1) % bp_instances_.size();
  } while (next_instance_index_ != start_index);
  next_instance_index_ = 0;
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto &instance : bp_instances_) {
    instance->FlushAllPages();
  }
}

}  // namespace bustub
