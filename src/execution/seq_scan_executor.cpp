//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan), 
    table_iter_(AbstractExecutor::exec_ctx_->GetCatalog()->GetTable(
        plan_->GetTableOid()
        )->table_->MakeIterator()) {}

void SeqScanExecutor::Init() {
    // throw NotImplementedException("SeqScanExecutor is not implemented");
    // table_iter_ = AbstractExecutor::exec_ctx_->GetCatalog()->GetTable(
    //     plan_->GetTableOid()
    //     )->table_->MakeIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    while(!table_iter_.IsEnd()) {
        auto [meta, tp] = table_iter_.GetTuple();
        *rid = table_iter_.GetRID();
        ++(table_iter_);
        if (meta.is_deleted_) {
            continue;
        }

        *tuple = tp;

        return true;
    }
    return false;
}

}  // namespace bustub
