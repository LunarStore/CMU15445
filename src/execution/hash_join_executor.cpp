//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

#define UNUSED(val) ((void)val)

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan),
    left_executor_(std::move(left_child)), right_executor_(std::move(right_child)),
    citer_(ht_.cbegin()), l_offset_(-1),
    r_offset_(0), need_extra_(false) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  
  Tuple tp;
  RID rid;

  left_executor_->Init();
  right_executor_->Init();
  ht_.clear();
  l_offset_ = -1;
  r_offset_ = 0;
  need_extra_ = false;
  while(left_executor_->Next(&tp, &rid)) {
    ht_[MakeLeftJoinKey(&tp)].first.push_back(tp);
  }

  while(right_executor_->Next(&tp, &rid)) {
    ht_[MakeRightJoinKey(&tp)].second.push_back(tp);
  }

  citer_ = ht_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto& l_schema = left_executor_->GetOutputSchema();
  const auto& r_schema = right_executor_->GetOutputSchema();
  while (true) {
    if (r_offset_ == 0) {
      if (need_extra_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        const auto& l_cols = l_schema.GetColumns();
        const auto& r_cols = r_schema.GetColumns();
        const auto& l_tp = citer_->second.first[l_offset_];
        for (int i = 0; i < (int)l_cols.size(); i++) {
          values.push_back(l_tp.GetValue(&l_schema, i));
        }

        for (int i = 0; i < (int)r_cols.size(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(r_cols[i].GetType()));
        }

        need_extra_ = false;

        *tuple = Tuple(values, &GetOutputSchema());
        return true;
      }

      l_offset_ = l_offset_ + 1;

      while(l_offset_ >= (int)citer_->second.first.size()) {
        citer_++;
        if (citer_ == ht_.end()) {
          return false;
        }
        l_offset_ = 0;
        // r_offset_ = 0;
      }

      need_extra_ = true;
    }

    if (r_offset_ < (int)citer_->second.second.size()) {
      std::vector<Value> values;

      const auto& l_tp = citer_->second.first[l_offset_];
      const auto& r_tp = citer_->second.second[r_offset_];
      const auto& l_cols = l_schema.GetColumns();
      const auto& r_cols = r_schema.GetColumns();

      r_offset_ = (r_offset_ + 1) % citer_->second.second.size();
      for (int i = 0; i < (int)l_cols.size(); i++) {
        values.push_back(l_tp.GetValue(&l_schema, i));
      }

      for (int i = 0; i < (int)r_cols.size(); i++) {
        values.push_back(r_tp.GetValue(&r_schema, i));
      }

      *tuple = Tuple(values, &GetOutputSchema());
      UNUSED(rid);
      need_extra_ = false;
      return true;
    }
  }

  return false;
}
}  // namespace bustub
