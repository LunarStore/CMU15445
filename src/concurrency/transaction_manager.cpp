//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto tws_qs = txn->GetWriteSet();
  while (!tws_qs->empty()) {
    auto twr = tws_qs->back();
    tws_qs->pop_back();
    auto tm = twr.table_heap_->GetTupleMeta(twr.rid_);
    BUSTUB_ASSERT(tm.delete_txn_id_ == txn->GetTransactionId() ||
      tm.insert_txn_id_ == txn->GetTransactionId(), "txn_id not match!");
    tm.is_deleted_ = !tm.is_deleted_;

    twr.table_heap_->UpdateTupleMeta(tm, twr.rid_);
  }
  auto iws_qs = txn->GetIndexWriteSet();
  while (iws_qs->empty()) {
    auto iwr = iws_qs->back();
    iws_qs->pop_back();
    auto table_info = iwr.catalog_->GetTable(iwr.table_oid_);
    auto index_info = iwr.catalog_->GetIndex(iwr.index_oid_);

    auto key = iwr.tuple_.KeyFromTuple(table_info->schema_,
      *index_info->index_->GetKeySchema(),
      index_info->index_->GetKeyAttrs());

    if (iwr.wtype_ == WType::DELETE) {
      index_info->index_->InsertEntry(key, iwr.rid_, txn);
    } else if (iwr.wtype_ == WType::INSERT) {
      index_info->index_->DeleteEntry(key, iwr.rid_, txn);
    }
  }
  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
