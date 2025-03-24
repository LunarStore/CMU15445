//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include <queue>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct SortKey {
  // true increase, false decrease;
  std::vector<std::pair<bool, Value>> sort_keys_;
  Tuple tuple_;
  RID rid_;

  auto operator()(SortKey lhs, SortKey rhs) const -> bool {
      for (int i = 0; i < (int)rhs.sort_keys_.size(); i++) {
        const auto& [ order, l_value ] = lhs.sort_keys_[i];
        const Value& r_value = rhs.sort_keys_[i].second;

        if (order) {
          if (l_value.CompareGreaterThan(r_value) == CmpBool::CmpTrue) {
            return true;
          } else if (l_value.CompareLessThan(r_value) == CmpBool::CmpTrue) {
            return false;
          }
        } else {

          if (l_value.CompareLessThan(r_value) == CmpBool::CmpTrue) {
            return true;
          } else if (l_value.CompareGreaterThan(r_value) == CmpBool::CmpTrue) {
            return false;
          }
        }
      }

      return false;
  }
};

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

private:
  /** @return The tuple as an SortKey */
  auto MakeSortKey(const Tuple *tuple, const RID* rid) -> SortKey {

    std::vector<std::pair<bool, Value>> keys;
    for (const auto &expr : plan_->GetOrderBy()) {
      bool order = false;
      if (expr.first == OrderByType::DEFAULT || expr.first == OrderByType::ASC) {
        order = true;   // 小根堆
      } else if (expr.first == OrderByType::DESC) {
        order = false;  // 大根堆
      } else {
        BUSTUB_ASSERT(false, "invalid order!");
      }
      keys.emplace_back(std::make_pair(order, 
        expr.second->Evaluate(tuple, child_executor_->GetOutputSchema())));
    }
    return {keys, *tuple, *rid};
  }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::priority_queue<SortKey, 
    std::vector<SortKey>, SortKey> data_;

};
}  // namespace bustub
