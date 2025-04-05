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
        )->table_->MakeEagerIterator()) {}

void SeqScanExecutor::Init() {
    // throw NotImplementedException("SeqScanExecutor is not implemented");
    auto txn = AbstractExecutor::exec_ctx_->GetTransaction();
    auto lmgr = AbstractExecutor::exec_ctx_->GetLockManager();
    auto oid = plan_->GetTableOid();
    switch(txn->GetIsolationLevel()) {
        case IsolationLevel::REPEATABLE_READ:
            if (!AbstractExecutor::exec_ctx_->IsDelete()) {
                if (txn->IsTableSharedLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
                    txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid)) {
                    break;
                }

                if (!lmgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED , plan_->GetTableOid())) {
                    throw ExecutionException("SeqScanExecutor LockTable Fail at IsolationLevel::REPEATABLE_READ!\n");
                    return ;
                }
            } else {
                if (txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid)) {
                    break;
                }
                // lmgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->GetTableOid());
                // BUSTUB_ASSERT(!txn->GetExclusiveTableLockSet()->empty() ||
                //     !txn->GetIntentionExclusiveTableLockSet()->empty() ||
                //     !txn->GetSharedIntentionExclusiveTableLockSet()->empty(), "is delete operator but no lock table!\n");
                if (!lmgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE , plan_->GetTableOid())) {
                    throw ExecutionException("SeqScanExecutor LockTable Fail at IsolationLevel::REPEATABLE_READ!\n");
                    return ;
                }
            }
            break;
        case IsolationLevel::READ_COMMITTED:
            if (!AbstractExecutor::exec_ctx_->IsDelete()) {
                if (txn->IsTableSharedLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
                    txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid)) {
                    break;
                }
                if (!lmgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid())) {
                    throw ExecutionException("SeqScanExecutor LockTable Fail at IsolationLevel::READ_COMMITTED!\n");
                    return ;
                }
            } else {
                if (txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid)) {
                    break;
                }
                // lmgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->GetTableOid());
                // BUSTUB_ASSERT(!txn->GetExclusiveTableLockSet()->empty() ||
                //     !txn->GetIntentionExclusiveTableLockSet()->empty() ||
                //     !txn->GetSharedIntentionExclusiveTableLockSet()->empty(), "is delete operator but no lock table!\n");
                if (!lmgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE , plan_->GetTableOid())) {
                    throw ExecutionException("SeqScanExecutor LockTable Fail at IsolationLevel::REPEATABLE_READ!\n");
                    return ;
                }
            }
            break;
        case IsolationLevel::READ_UNCOMMITTED:
            if (AbstractExecutor::exec_ctx_->IsDelete()) {
                if (txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid)) {
                    break;
                }
                if (!lmgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE , plan_->GetTableOid())) {
                    throw ExecutionException("SeqScanExecutor LockTable Fail at IsolationLevel::READ_UNCOMMITTED!\n");
                    return ;
                }
            }
            break;
        default:
            BUSTUB_ASSERT(false, "unknow IsolationLevel");

    }

    table_iter_ = AbstractExecutor::exec_ctx_->GetCatalog()->GetTable(
        plan_->GetTableOid()
        )->table_->MakeEagerIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    while(!table_iter_.IsEnd()) {
        *rid = table_iter_.GetRID();
        GetRowLock(*rid);

        auto [meta, tp] = table_iter_.GetTuple();
        ++(table_iter_);

        if (meta.is_deleted_) {
            PutRowLock(*rid, true);
            continue;
        }
        PutRowLock(*rid, false);
        
        *tuple = tp;
        return true;
    }
    return false;
}

auto SeqScanExecutor::GetRowLock(const RID &rid) -> bool {
    auto txn = AbstractExecutor::exec_ctx_->GetTransaction();
    auto lmgr = AbstractExecutor::exec_ctx_->GetLockManager();
    bool ret = true;
    LockManager::LockMode lock_mode = AbstractExecutor::exec_ctx_->IsDelete() ? 
        LockManager::LockMode::EXCLUSIVE : 
        LockManager::LockMode::SHARED;
    switch(txn->GetIsolationLevel()) {
        case IsolationLevel::REPEATABLE_READ:
            ret = lmgr->LockRow(txn, lock_mode, plan_->GetTableOid(), rid);

            if (!ret) {
                throw ExecutionException("SeqScanExecutor GetRowLock Fail at IsolationLevel::REPEATABLE_READ!\n");
                return false;
            }
            break;
        case IsolationLevel::READ_COMMITTED:
            ret = lmgr->LockRow(txn, lock_mode, plan_->GetTableOid(), rid);

            if (!ret) {
                throw ExecutionException("SeqScanExecutor GetRowLock Fail at IsolationLevel::READ_COMMITTED!\n");
                return false;
            }
            break;
        case IsolationLevel::READ_UNCOMMITTED:
            if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
                ret = lmgr->LockRow(txn, lock_mode, plan_->GetTableOid(), rid);

                if (!ret) {
                    throw ExecutionException("SeqScanExecutor GetRowLock Fail at IsolationLevel::READ_COMMITTED!\n");
                    return false;
                }
            }
            break;
        default:
            BUSTUB_ASSERT(false, "unknow IsolationLevel");

    }

    return ret;
}
auto SeqScanExecutor::PutRowLock(const RID &rid, bool force) -> bool {
    auto txn = AbstractExecutor::exec_ctx_->GetTransaction();
    auto lmgr = AbstractExecutor::exec_ctx_->GetLockManager();
    bool ret = true;
    if (AbstractExecutor::exec_ctx_->IsDelete() && !force) {
        return false;
    }
    switch(txn->GetIsolationLevel()) {
        case IsolationLevel::REPEATABLE_READ:
            if (force) {
                ret = lmgr->UnlockRow(txn, plan_->GetTableOid(), rid, force);

                if (!ret) {
                    throw ExecutionException("SeqScanExecutor PutRowLock Fail at IsolationLevel::REPEATABLE_READ!\n");
                    return false;
                }
            } // else 
            /**
             * There is also a row lock that has not been added to prevent it from 
             * entering the shrink state in advance.
             */

            break;
        case IsolationLevel::READ_COMMITTED:
            ret = lmgr->UnlockRow(txn, plan_->GetTableOid(), rid, force);

            if (!ret) {
                throw ExecutionException("SeqScanExecutor PutRowLock Fail at IsolationLevel::READ_COMMITTED!\n");
                return false;
            }
            break;
        case IsolationLevel::READ_UNCOMMITTED:
            if (AbstractExecutor::exec_ctx_->IsDelete()) {
                ret = lmgr->UnlockRow(txn, plan_->GetTableOid(), rid, force);

                if (!ret) {
                    throw ExecutionException("SeqScanExecutor PutRowLock Fail at IsolationLevel::READ_UNCOMMITTED!\n");
                    return false;
                }
            }
            break;
        default:
            BUSTUB_ASSERT(false, "unknow IsolationLevel");

    }

    return ret;
}

}  // namespace bustub
