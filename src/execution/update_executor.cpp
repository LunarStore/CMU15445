//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
        table_info_(AbstractExecutor::exec_ctx_->GetCatalog()->GetTable(
            plan_->TableOid())),
        child_executor_(std::move(child_executor)),
        indexs_(AbstractExecutor::exec_ctx_->GetCatalog()->GetTableIndexes(
            table_info_->name_)),
        update_ok_(false)  {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { 
    child_executor_->Init();
    update_ok_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    Tuple   upd_tp;
    RID     upd_rid;
    int upd_cnt = 0;

    if (update_ok_) return false;

    while (child_executor_->Next(&upd_tp, &upd_rid)) {
        auto old_meta_tuple = table_info_->table_->GetTuple(upd_rid);

        if (old_meta_tuple.first.is_deleted_) {
            continue;
        }
        TupleMeta meta = {
            .insert_txn_id_ = INVALID_TXN_ID,
            .delete_txn_id_ = INVALID_TXN_ID,
            .is_deleted_ = false
        };

        table_info_->table_->UpdateTupleMeta(TupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}), upd_rid);

        std::vector<Value> values;

        for (const auto &expr : plan_->target_expressions_) {
            values.push_back(expr->Evaluate(&upd_tp, child_executor_->GetOutputSchema()));
        }

        upd_tp = Tuple(values, &child_executor_->GetOutputSchema());
        *rid = table_info_->table_->InsertTuple(meta, upd_tp).value();

        for (auto it : indexs_) {

            auto key = upd_tp.KeyFromTuple(table_info_->schema_, 
                *it->index_->GetKeySchema(),
                it->index_->GetKeyAttrs()
            );
            it->index_->DeleteEntry(key,  
                upd_rid,
                nullptr
            );
            it->index_->InsertEntry(key,  
                *rid,
                nullptr
            );
        }
        upd_cnt++;
    }

    *tuple = Tuple({
        std::vector<Value>({
                Value(GetOutputSchema().GetColumn(0).GetType(), upd_cnt)
            }), 
        &GetOutputSchema()
    });
    update_ok_ = true;
    return true;
}

}  // namespace bustub
