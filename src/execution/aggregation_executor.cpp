//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

#define UNUSED(val) ((void)val)

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan),  child_(std::move(child)),
    aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
    aht_iterator_(aht_.Begin()), is_empty_(false){}

void AggregationExecutor::Init() {
    Tuple tp;
    RID rid;

    aht_.Clear();
    child_->Init();
    while(child_->Next(&tp, &rid)) {
        AggregateKey agg_key;
        AggregateValue agg_value;

        agg_key = MakeAggregateKey(&tp);
        agg_value = MakeAggregateValue(&tp);

        aht_.InsertCombine(agg_key, agg_value);
    }

    aht_iterator_ = aht_.Begin();

    is_empty_ = aht_iterator_ == aht_.End();
}

// 1、已知agg_key、agg_value，那么如何将他们序列化成GetOutputSchema()格式的tuple？
// 2、rid如何产生？如何赋值？是否有意义？
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 

    if (is_empty_ && plan_->GetGroupBys().empty()) {    // for empty table
        *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema());
        is_empty_ = false;
        return true;
    }
    if (aht_iterator_ == aht_.End()) {
        return false;
    }

    // 根据AggregationPlanNode::InferAggSchema可以推测GetOutputSchema的Value组成
    const AggregateKey & agg_key = aht_iterator_.Key();
    const AggregateValue & agg_value = aht_iterator_.Val();

    std::vector<Value> values;
    values.insert(values.end(), agg_key.group_bys_.cbegin(), 
        agg_key.group_bys_.cend());
    values.insert(values.end(), agg_value.aggregates_.cbegin(), 
        agg_value.aggregates_.cend());
    
    *tuple = Tuple(values, &GetOutputSchema());

    ++aht_iterator_;
    UNUSED(rid);

    return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
