//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
    child_executor_(std::move(child_executor)),
    table_info_(AbstractExecutor::exec_ctx_->GetCatalog()->GetTable(
        plan_->TableOid())),
    indexs_(AbstractExecutor::exec_ctx_->GetCatalog()->GetTableIndexes(
        table_info_->name_)),
    delete_ok_(false)  {}

void DeleteExecutor::Init() { 
    child_executor_->Init();
    delete_ok_ = false;
 }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    Tuple   d_tp;
    RID     d_rid;
    int d_cnt = 0;

    if (delete_ok_) return false;

    while (child_executor_->Next(&d_tp, &d_rid)) {
        TupleMeta meta = {
            .insert_txn_id_ = INVALID_TXN_ID,
            .delete_txn_id_ = INVALID_TXN_ID,
            .is_deleted_ = true
        };

        table_info_->table_->UpdateTupleMeta(meta, d_rid);


        for (auto it : indexs_) {

            auto key = d_tp.KeyFromTuple(table_info_->schema_, 
                *it->index_->GetKeySchema(),
                it->index_->GetKeyAttrs()
            );
            it->index_->DeleteEntry(key,  
                d_rid,
                nullptr
            );
        }
        d_cnt++;
    }

    *tuple = Tuple({
        std::vector<Value>({
                Value(GetOutputSchema().GetColumn(0).GetType(), d_cnt)
            }), 
        &GetOutputSchema()
    });
    delete_ok_ = true;
    return true;
}
}  // namespace bustub
