//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
     child_executor_(std::move(child_executor)),
     table_info_(AbstractExecutor::exec_ctx_->GetCatalog()->GetTable(
        plan_->TableOid())),
     indexs_(AbstractExecutor::exec_ctx_->GetCatalog()->GetTableIndexes(
        table_info_->name_)),
     insert_ok_(false) {}

void InsertExecutor::Init() {
    child_executor_->Init();
    insert_ok_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    Tuple   ins_tp;
    RID     ins_rid;
    int ins_cnt = 0;

    if (insert_ok_) return false;
    while (child_executor_->Next(&ins_tp, &ins_rid)) {
        TupleMeta meta = {
            .insert_txn_id_ = INVALID_TXN_ID,
            .delete_txn_id_ = INVALID_TXN_ID,
            .is_deleted_ = false
        };

        *rid = table_info_->table_->InsertTuple(meta, ins_tp).value();

        for (auto it : indexs_) {

            auto key = ins_tp.KeyFromTuple(table_info_->schema_, 
                *it->index_->GetKeySchema(),
                it->index_->GetKeyAttrs()
            );

            it->index_->DeleteEntry(key,  
                ins_rid,
                nullptr
            );
            it->index_->InsertEntry(
                key,  
                *rid,
                nullptr
            );
        }
        ins_cnt++;
    }


    *tuple = Tuple({
        std::vector<Value>({
                Value(GetOutputSchema().GetColumn(0).GetType(), ins_cnt)
            }), 
        &GetOutputSchema()
    });
    insert_ok_ = true;
    return true;

}

}  // namespace bustub
