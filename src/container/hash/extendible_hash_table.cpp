//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/hash_table_bucket_page.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();  // least-significant bits
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *res;
  // avoid concurrency to create repeated directory page.
  mu_.lock();
  if (directory_page_id_ == INVALID_PAGE_ID) {
    // renew
    LOG_DEBUG("create new directory, before %d", directory_page_id_);
    page_id_t tmp_page_id;
    res =
        reinterpret_cast<HashTableDirectoryPage *>(AssertPage(buffer_pool_manager_->NewPage(&tmp_page_id))->GetData());
    directory_page_id_ = tmp_page_id;
    res->SetPageId(directory_page_id_);
    assert(directory_page_id_ != INVALID_PAGE_ID);
    LOG_DEBUG("create new directory %d", directory_page_id_);
    // renew an initial bucket 0
    page_id_t bucket_page_id = INVALID_PAGE_ID;
    AssertPage(buffer_pool_manager_->NewPage(&bucket_page_id));
    res->SetBucketPageId(0, bucket_page_id);
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  }
  mu_.unlock();

  // re-fetch from buffer
  assert(directory_page_id_ != INVALID_PAGE_ID);
  res = reinterpret_cast<HashTableDirectoryPage *>(
      AssertPage(buffer_pool_manager_->FetchPage(directory_page_id_))->GetData());
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return AssertPage(buffer_pool_manager_->FetchPage(bucket_page_id));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::RetrieveBucketPage(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *bucket_page = FetchBucketPage(bucket_page_id);

  bucket_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucketPage(bucket_page);
  bool res = bucket->GetValue(key, comparator_, result);
  bucket_page->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 * 当写入时仅需为dictionary加入读锁，为插入的目标bucket加入写锁
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *page = FetchBucketPage(bucket_page_id);
  page->WLatch();

  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucketPage(page);
  if (!bucket->IsFull()) {
    // not full, insert it directly.
    bool res = bucket->Insert(key, value, comparator_);
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    return res;
  }

  // do SplitInsert
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

/** SplitInsert 主要逻辑
 * 如果需要插入的页已经Full了：
 *    如果插入的页的local_depth已经等于global_depth:
 *        global depth++
 *        在global depth增加的同时进行绑定操作:
 *          对于所有桶，基于本桶当前的前缀xxx，生成一个1xxx前缀，将这个1xxx在directory中链接到本桶
 *          在扩容时其实并非所有桶都是需要增加depth的，真正需要增加depth的只有本轮需要分裂的那个桶
 *          但新增的绑定1xxx的编号并不会影响对这个无需扩容的桶的索引，无论是hash(key)值低位为1xxx还是0xxx的，都会索引到这个还无需扩容的桶上
 *    找到需要扩容的bucket page(称为桶A)，进行扩容操作:
 *        - 假设桶A前缀为xxx, 新建一个前缀为1xxx的bucket page(称为桶B)
 *        - 将桶A的local depth++，相当于此时桶A的前缀变为了0xxx
 *        - 迭代桶A中的kv对，将前缀匹配1xxx的kv对移动到桶B
 *
 *        🤔一个比较极端的情况，如果以0为末尾的hash(key)值很少，在以1为末尾的桶已经翻倍扩容了3次的情况下
 *        虽然directory中的索引表已经有4位了(即global_depth)，所有xxx0格式的前缀还是指向一个bucket
 *
 *        - 基于上面的情况，我们需要额外增加一个操作，就是在A[00] B[10]桶创建完成后，额外的将所有dir_page中的以
 *        00和10为前缀的编号分别链接到A B桶上
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  // 找到需要扩容的bucket page
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  int64_t split_bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_bucket_depth = dir_page->GetLocalDepth(split_bucket_index);

  if (split_bucket_depth >= 9) {
    // can't split
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return false;
  }

  //! 如果插入的页的local_depth已经等于global_depth ==> growth
  if (split_bucket_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  page_id_t split_bucket_page_id = KeyToPageId(key, dir_page);
  Page *split_page = FetchBucketPage(split_bucket_page_id);
  split_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *split_bucket = RetrieveBucketPage(split_page);
  MappingType *origin_array = split_bucket->GetArrayCopy();
  uint32_t origin_array_size = split_bucket->NumReadable();
  split_bucket->Clear();  // 将分裂前的桶清空

  page_id_t image_bucket_page;
  HASH_TABLE_BUCKET_TYPE *image_bucket =
      RetrieveBucketPage(AssertPage(buffer_pool_manager_->NewPage(&image_bucket_page)));

  // inr local depth before get split image
  dir_page->IncrLocalDepth(split_bucket_index);
  uint32_t split_image_bucket_index = dir_page->GetSplitImageIndex(split_bucket_index);  // 1xxx
  dir_page->SetLocalDepth(split_image_bucket_index, dir_page->GetLocalDepth(split_bucket_index));

  // 绑定注册新的B桶
  dir_page->SetBucketPageId(split_image_bucket_index, image_bucket_page);

  //! 绑定所有以0xxx为前缀的编号到0xxx桶-[A桶]
  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_index);
  for (uint32_t i = split_bucket_index; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    if (i < diff) {
      // avoid negative
      break;
    }
  }
  for (uint32_t i = split_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }
  //! 绑定所有以1xxx为前缀的编号到1xxx桶-[B桶]
  for (uint32_t i = split_image_bucket_index; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, image_bucket_page);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    if (i < diff) {
      // avoid negative
      break;
    }
  }
  for (uint32_t i = split_image_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, image_bucket_page);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }

  //! 将原先的数据分发到A B桶中
  uint32_t mask = dir_page->GetLocalDepthMask(split_bucket_index);
  for (uint32_t i = 0; i < origin_array_size; i++) {
    MappingType tmp = origin_array[i];
    uint32_t target_bucket_index = Hash(tmp.first) & mask;
    page_id_t target_bucket_index_page = dir_page->GetBucketPageId(target_bucket_index);
    assert(target_bucket_index_page == split_bucket_page_id || target_bucket_index_page == image_bucket_page);
    if (target_bucket_index_page == split_bucket_page_id) {
      assert(split_bucket->Insert(tmp.first, tmp.second, comparator_));
    } else {
      assert(image_bucket->Insert(tmp.first, tmp.second, comparator_));
    }
  }
  // release
  delete[] origin_array;
  split_page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(image_bucket_page, true));

  //  dir_page->PrintDirectory();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();  // 需要先释放写锁再Insert以免出现死锁

  // re-insert original k-v
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  Page *page = FetchBucketPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucketPage(page);
  bool res = bucket->Remove(key, value, comparator_);
  //! 当一个bucket为空时才进行合并
  if (bucket->IsEmpty()) {
    // go merge
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    Merge(transaction, bucket_index);
    return res;
  }
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, uint32_t target_bucket_index) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (target_bucket_index >= dir_page->Size()) {
    // used to check to auto merge
    table_latch_.WUnlock();
    return;
  }

  page_id_t target_bucket_page_id = dir_page->GetBucketPageId(target_bucket_index);
  //! 对于0xxx, 返回1xxx, 对于1xxx, 返回0xxx, 即和本page合并的对象
  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(target_bucket_index);

  //! review something to find whether can execute merge
  uint32_t local_depth = dir_page->GetLocalDepth(target_bucket_index);
  if (local_depth == 0) {
    // can't merge because of depth is 0
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  // check the same local depth with split image
  if (local_depth != dir_page->GetLocalDepth(image_bucket_index)) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  // check empty target bucket index
  Page *target_page = FetchBucketPage(target_bucket_page_id);
  target_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *target_bucket = RetrieveBucketPage(target_page);
  if (!target_bucket->IsEmpty()) {
    // bucket is not empty
    target_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }
  //! review something end

  // 删除目标
  target_page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(target_bucket_page_id));

  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_index);
  //! 将被删除的bucket index和合并目标桶绑定
  dir_page->SetBucketPageId(target_bucket_index, image_bucket_page_id);
  dir_page->DecrLocalDepth(target_bucket_index);
  dir_page->DecrLocalDepth(image_bucket_index);
  assert(dir_page->GetLocalDepth(target_bucket_index) == dir_page->GetLocalDepth(image_bucket_index));

  //! 如果被删除的桶和合并的目标桶还有其他额外链接index，将它们链接到合并后的目标桶，并更新它们的index
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == target_bucket_page_id || dir_page->GetBucketPageId(i) == image_bucket_page_id) {
      dir_page->SetBucketPageId(i, image_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(target_bucket_index));
    }
  }

  // 如果所有的local_depth都小于global即可收缩全局深度
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  // unpin
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *ExtendibleHashTable<KeyType, ValueType, KeyComparator>::AssertPage(Page *page) {
  assert(page != nullptr);
  return page;
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
