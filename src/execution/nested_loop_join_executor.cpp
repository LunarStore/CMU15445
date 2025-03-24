//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

#define UNUSED(val) ((void)val)
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
    left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)),
    right_offset_(0), need_extra_(false) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {

  left_executor_->Init();
  right_executor_->Init();

  Tuple tp;
  RID rid;
  // while(left_executor_->Next(&tp, &rid)) {
  //   left_table_.push_back(tp);
  // }
  right_offset_ = 0;
  right_table_.clear();
  need_extra_ = false;
  while(right_executor_->Next(&tp, &rid)) {
    right_table_.push_back(tp);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto nlj_expr = plan_->Predicate();
  const auto& l_schema = left_executor_->GetOutputSchema();
  const auto& r_schema = right_executor_->GetOutputSchema();
  // Tuple l_cur_tp;
  RID l_rid;
  Tuple r_cur_tp;
  while (true) {
    if (right_offset_ == 0) {

      if (need_extra_ && plan_->GetJoinType() == JoinType::LEFT) {

        std::vector<Value> values;
        const auto& l_cols = l_schema.GetColumns();
        const auto& r_cols = r_schema.GetColumns();
        for (int i = 0; i < (int)l_cols.size(); i++) {
          values.push_back(left_cur_tuple_.GetValue(&l_schema, i));
        }

        for (int i = 0; i < (int)r_cols.size(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(r_cols[i].GetType()));
        }

        need_extra_ = false;

        *tuple = Tuple(values, &GetOutputSchema());
        return true;
      }
    // Get the next tuple
      const auto status = left_executor_->Next(&left_cur_tuple_, &l_rid);

      if (!status) {
        return false;
      }

      right_executor_->Init();
      need_extra_ = true;
    }


    if (right_table_.empty()) continue;
    r_cur_tp = right_table_[right_offset_];
    auto value = nlj_expr->EvaluateJoin(&left_cur_tuple_, l_schema,
      &r_cur_tp, r_schema);
    right_offset_ = (right_offset_ + 1) % right_table_.size();

    if (!value.IsNull() && value.GetAs<bool>()) {
      std::vector<Value> values;

      const auto& l_cols = l_schema.GetColumns();
      const auto& r_cols = r_schema.GetColumns();
      for (int i = 0; i < (int)l_cols.size(); i++) {
        values.push_back(left_cur_tuple_.GetValue(&l_schema, i));
      }

      for (int i = 0; i < (int)r_cols.size(); i++) {
        values.push_back(r_cur_tp.GetValue(&r_schema, i));
      }

      *tuple = Tuple(values, &GetOutputSchema());
      UNUSED(rid);
      need_extra_ = false;
      return true;
    }
  }
}

}  // namespace bustub
