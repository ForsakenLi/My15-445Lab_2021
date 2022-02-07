//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <vector>
#include <unordered_map>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "common/util/hash_util.h"



namespace bustub {

struct MyHashKey {
  bool operator==(const MyHashKey &o) const { return this->val_.CompareEquals(o.val_) == CmpBool::CmpTrue; }

  Value val_;
};
}  // namespace bustub

namespace std {
// 类似aggregation_plan.h::123
// 需要使用仿函数函数全特化的方式覆盖std::hash<MyHashKey>
template <>
struct hash<bustub::MyHashKey> {
  std::size_t operator()(const bustub::MyHashKey &key) const {
    if (key.val_.IsNull()) {
      return 0;
    }
    return bustub::HashUtil::CombineHashes(0, bustub::HashUtil::HashValue(&key.val_));
  }
};
}  // namespace std

namespace bustub {

class MyHashTable {
 public:
  bool exist(const MyHashKey &key) {
    return map_.find(key) != map_.cend();
  }

  void insert(const MyHashKey &key, const Tuple &tuple) {
    if (exist(key)) {
      map_[key].emplace_back(tuple);
      return;
    }
    map_.insert({key, {tuple}});
  }

  std::vector<Tuple> operator[](const MyHashKey &key) { return map_[key]; }

 private:
  // have to save every tuples whose hash values are same, put those tuple into a vector
  std::unordered_map<MyHashKey, std::vector<Tuple>> map_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  MyHashKey GetMyJoinKey(const Tuple *tuple, bool isLeft);

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  const std::unique_ptr<AbstractExecutor> left_child_;
  const std::unique_ptr<AbstractExecutor> right_child_;
  std::queue<Tuple> loop_res_{};
  MyHashTable my_map_;
};

}  // namespace bustub

