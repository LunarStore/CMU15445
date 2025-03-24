//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan),
    index_info_(AbstractExecutor::exec_ctx_->GetCatalog()->GetIndex (
        plan_->GetIndexOid()
    )),
    table_info_(AbstractExecutor::exec_ctx_->GetCatalog()->GetTable(
        index_info_->table_name_
    )),
    tree_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())),
    index_iter_(tree_->GetBeginIterator()), table_schema_(table_info_->schema_) {

}

void IndexScanExecutor::Init() {
    index_iter_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    while (!index_iter_.IsEnd()) {
        auto [cur_key, cur_rid] = *index_iter_;
        ++index_iter_;

        auto [me, tp] = table_info_->table_->GetTuple(cur_rid);
        if (me.is_deleted_) {
            continue;
        }

        *tuple = tp;
        *rid = cur_rid;
        return true;
    }
    return false;
}

}  // namespace bustub
