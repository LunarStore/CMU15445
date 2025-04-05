//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (!txn) return false;

  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  std::unique_lock<std::mutex> tlm_ulg(table_lock_map_latch_);
  // lock upgrade?
  // 判断是否存在事务在oid表是否已经有锁
  auto it_tlm = table_lock_map_.find(oid);
  if (it_tlm != table_lock_map_.end()) {
    auto lock_req_queue = it_tlm->second;
    std::unique_lock<std::mutex> lrq_ulg(lock_req_queue->latch_);
    tlm_ulg.unlock();

    std::shared_ptr<LockRequest> grantted_lock = nullptr;
    auto it_grantted_pair = lock_req_queue->grantted_.find(txn->GetTransactionId());

    if (it_grantted_pair != lock_req_queue->grantted_.end()) {
      std::shared_ptr<LockRequest> it = it_grantted_pair->second;
      // has lock.
      BUSTUB_ASSERT(it->granted_ == true, "error, reuqest lock is not granted, but is not block!");

      if (it->lock_mode_ == lock_mode) {
        // it already has the lock.
        return true;
      }
      if (!CanLockUpgrade(it->lock_mode_, lock_mode)) {
        // faild upgrade lock.
        // INCOMPATIBLE_UPGRADE
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return false;
      }

      if (lock_req_queue->upgrading_ != INVALID_TXN_ID) {
        // 已经有事务在升级该资源的锁
        // UPGRADE_CONFLICT
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      } // else can upgrade
      lock_req_queue->upgrading_ = txn->GetTransactionId();

      grantted_lock = it;
    }

    std::shared_ptr<LockRequest> lreq = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    if (grantted_lock) {
      BUSTUB_ASSERT(grantted_lock->oid_ == oid && 
        grantted_lock->txn_id_ == txn->GetTransactionId(), "");
      // 升级
      lock_req_queue->grantted_.erase(grantted_lock->txn_id_);
      BUSTUB_ASSERT(DeleteLockTableSets(txn, grantted_lock->lock_mode_, grantted_lock->oid_), "");
      grantted_lock.reset();

      lock_req_queue->request_queue_.push_front(lreq);

    } else {
      // 非升级
      lock_req_queue->request_queue_.push_back(lreq);
    }

    while (!lreq->granted_) {

      if (txn->GetState() == TransactionState::ABORTED) {
        if (lock_req_queue->upgrading_ == txn->GetTransactionId()) {
          // 升级成功，将upgrading_置为无效。
          lock_req_queue->upgrading_ = INVALID_TXN_ID;
        }

        if (lock_req_queue->request_queue_.front() == lreq) {
          lock_req_queue->cv_.notify_all();
        }
        lock_req_queue->request_queue_.remove(lreq);
        lreq.reset();
        return false;
      }
      if (lock_req_queue->request_queue_.front() == lreq) {
        bool get_lock = true;
        // 并发加锁 / 升级
        for (auto &it : lock_req_queue->grantted_) {
          BUSTUB_ASSERT(it.second->granted_, "lock request node is in grantted_queue_, but granted_ equal false!");

          if (!AreLocksCompatible(it.second->lock_mode_, lreq->lock_mode_)) {
            get_lock = false;
            break;
          }
        }

        if (get_lock) {
          // 都适配
          break;
        }
      }
      // lock_req_queue->cv_.wait(lrq_ulg);
      lock_req_queue->cv_.wait_for(lrq_ulg, std::chrono::milliseconds(50));
    }
    
    if (lock_req_queue->upgrading_ == txn->GetTransactionId()) {
      // 升级成功，将upgrading_置为无效。
      lock_req_queue->upgrading_ = INVALID_TXN_ID;
    }

    lreq->granted_ = true;
    lock_req_queue->request_queue_.remove(lreq);
    lock_req_queue->grantted_.insert(std::make_pair(lreq->txn_id_, lreq));
    BUSTUB_ASSERT(UpdateLockTableSets(txn, lreq->lock_mode_, lreq->oid_), "");
    lock_req_queue->cv_.notify_all();

  } else { // else no txn to lock the table.
    std::shared_ptr<LockRequest> lreq = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    auto lock_req_queue = std::make_shared<LockRequestQueue>();
    table_lock_map_.insert(std::make_pair(oid, lock_req_queue));

    lreq->granted_ = true;
    lock_req_queue->grantted_.insert(std::make_pair(lreq->txn_id_, lreq));
    BUSTUB_ASSERT(UpdateLockTableSets(txn, lreq->lock_mode_, lreq->oid_), "");
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> tlm_ulg(table_lock_map_latch_);
  // 确保事务在对应的表上有锁
  auto it_tlm = table_lock_map_.find(oid);

  if (it_tlm == table_lock_map_.end()) {
    // 没有任何事务在该表上加过锁
    // ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  auto lock_req_queue = it_tlm->second;
  std::unique_lock<std::mutex> lrq_ulg(lock_req_queue->latch_);
  tlm_ulg.unlock();

  std::shared_ptr<LockRequest> grantted_lock = nullptr;
  auto it_grantted_pair = lock_req_queue->grantted_.find(txn->GetTransactionId());
  
  if (it_grantted_pair != lock_req_queue->grantted_.end()) {
    grantted_lock = it_grantted_pair->second;
  }

  if (!grantted_lock) {
    // 事务在该表上未加过锁
    // ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  auto txn_x_row_l_set = txn->GetExclusiveRowLockSet();
  auto txn_s_row_l_set = txn->GetSharedRowLockSet();

  if ((txn_x_row_l_set->find(oid) != txn_x_row_l_set->end() &&
    !txn_x_row_l_set->find(oid)->second.empty()) ||
    (txn_s_row_l_set->find(oid) != txn_s_row_l_set->end() &&
    !txn_s_row_l_set->find(oid)->second.empty())) {
    // TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS

    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }

  // 准备解锁，更新txn的状态
  TransactionStateUpdate(txn, grantted_lock);

  lock_req_queue->grantted_.erase(it_grantted_pair);

  // below it_grantted_pair is nullptr !!!

  BUSTUB_ASSERT(DeleteLockTableSets(txn, grantted_lock->lock_mode_, grantted_lock->oid_), "");
  grantted_lock.reset();

  lock_req_queue->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (!txn) return false;
  if ((lock_mode != LockMode::EXCLUSIVE) && (lock_mode != LockMode::SHARED)) {
    // ATTEMPTED_INTENTION_LOCK_ON_ROW
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }

  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }


  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {

    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }


  std::unique_lock<std::mutex> rlm_ulg(row_lock_map_latch_);
  // lock upgrade?
  // S -> X
  // 判断是否存在事务在oid表是否已经有锁
  auto it_rlm = row_lock_map_.find(rid);
  if (it_rlm != row_lock_map_.end()) {
    auto lock_req_queue = it_rlm->second;
    std::unique_lock<std::mutex> lrq_ulg(lock_req_queue->latch_);
    rlm_ulg.unlock();

    std::shared_ptr<LockRequest> grantted_lock = nullptr;
    auto it_grantted_pair = lock_req_queue->grantted_.find(txn->GetTransactionId());

    if (it_grantted_pair != lock_req_queue->grantted_.end()) {
      std::shared_ptr<LockRequest> it = it_grantted_pair->second;
      // has lock.
      BUSTUB_ASSERT(it->granted_ == true, "error, reuqest lock is not granted, but is not block!");

      if (it->lock_mode_ == lock_mode) {
        // it already has the lock.
        return true;
      }
      if (!CanLockUpgrade(it->lock_mode_, lock_mode)) {
        // faild upgrade lock.
        // INCOMPATIBLE_UPGRADE
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return false;
      }

      if (lock_req_queue->upgrading_ != INVALID_TXN_ID) {
        // 已经有事务在升级该资源的锁
        // UPGRADE_CONFLICT
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      } // else can upgrade
      lock_req_queue->upgrading_ = txn->GetTransactionId();

      grantted_lock = it;
    }

    std::shared_ptr<LockRequest> lreq =std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    if (grantted_lock) {
      BUSTUB_ASSERT(grantted_lock->oid_ == oid && 
        grantted_lock->txn_id_ == txn->GetTransactionId() &&
        grantted_lock->rid_ == rid, "");
      // 升级
      lock_req_queue->grantted_.erase(grantted_lock->txn_id_);
      BUSTUB_ASSERT(DeleteLockRowSets(txn, grantted_lock->lock_mode_, grantted_lock->oid_, grantted_lock->rid_), "");
      grantted_lock.reset();

      lock_req_queue->request_queue_.push_front(lreq);

    } else {
      // 非升级
      lock_req_queue->request_queue_.push_back(lreq);
    }

    while (!lreq->granted_) {

      if (txn->GetState() == TransactionState::ABORTED) {
        if (lock_req_queue->upgrading_ == txn->GetTransactionId()) {
          // 升级成功，将upgrading_置为无效。
          lock_req_queue->upgrading_ = INVALID_TXN_ID;
        }

        if (lock_req_queue->request_queue_.front() == lreq) {
          lock_req_queue->cv_.notify_all();
        }
        lock_req_queue->request_queue_.remove(lreq);
        lreq.reset();
        return false;
      }
      if (lock_req_queue->request_queue_.front() == lreq) {
        bool get_lock = true;
        // 并发加锁 / 升级
        for (auto &it : lock_req_queue->grantted_) {
          BUSTUB_ASSERT(it.second->granted_, "lock request node is in grantted_queue_, but granted_ equal false!");

          if (!AreLocksCompatible(it.second->lock_mode_, lreq->lock_mode_)) {
            get_lock = false;
            break;
          }
        }

        if (get_lock) {
          // 都适配
          break;
        }
      }
      // lock_req_queue->cv_.wait(lrq_ulg);
      lock_req_queue->cv_.wait_for(lrq_ulg, std::chrono::milliseconds(50));
    }
    
    if (lock_req_queue->upgrading_ == txn->GetTransactionId()) {
      // 升级成功，将upgrading_置为无效。
      lock_req_queue->upgrading_ = INVALID_TXN_ID;
    }

    lreq->granted_ = true;
    lock_req_queue->request_queue_.remove(lreq);
    lock_req_queue->grantted_.insert(std::make_pair(lreq->txn_id_, lreq));
    BUSTUB_ASSERT(UpdateLockRowSets(txn, lreq->lock_mode_, lreq->oid_, lreq->rid_), "");
    lock_req_queue->cv_.notify_all();

  } else { // else no txn to lock the table.
    std::shared_ptr<LockRequest> lreq = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    auto lock_req_queue = std::make_shared<LockRequestQueue>();
    row_lock_map_.insert(std::make_pair(rid, lock_req_queue));

    lreq->granted_ = true;
    lock_req_queue->grantted_.insert(std::make_pair(lreq->txn_id_, lreq));
    BUSTUB_ASSERT(UpdateLockRowSets(txn, lreq->lock_mode_, lreq->oid_, lreq->rid_), "");
  }
  return true;
}

/**
 * to do:
 * when lock_req_queue.grantted_ and lock_req_queue.request_queue_ is empty, to delete lock_req_queue.
*/
auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  std::unique_lock<std::mutex> rlm_ulg(row_lock_map_latch_);
  // 确保事务在对应的表上有锁
  auto it_rlm = row_lock_map_.find(rid);

  if (it_rlm == row_lock_map_.end()) {
    // 没有任何事务在该行上加过锁
    // ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  auto lock_req_queue = it_rlm->second;
  std::unique_lock<std::mutex> lrq_ulg(lock_req_queue->latch_);
  rlm_ulg.unlock();

  std::shared_ptr<LockRequest> grantted_lock = nullptr;
  auto it_grantted_pair = lock_req_queue->grantted_.find(txn->GetTransactionId());
  
  if (it_grantted_pair != lock_req_queue->grantted_.end()) {
    grantted_lock = it_grantted_pair->second;
  }

  if (!grantted_lock) {
    // 事务在该行上未加过锁
    // ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  // 准备解锁，更新txn的状态
  if (!force) {
    TransactionStateUpdate(txn, grantted_lock);
  }

  lock_req_queue->grantted_.erase(it_grantted_pair);

  // below it_grantted_pair is nullptr !!!

  BUSTUB_ASSERT(DeleteLockRowSets(txn, grantted_lock->lock_mode_, grantted_lock->oid_, grantted_lock->rid_), "");
  grantted_lock.reset();

  lock_req_queue->cv_.notify_all();
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto it = waits_for_.find(t1);

  if (it != waits_for_.end()) {
    for (auto t : it->second) {
      if (t == t2) {
        // 已经存在
        return;
      }
    }
  } else {
    it = waits_for_.insert(std::make_pair(t1, std::vector<txn_id_t>())).first;
  }
  it->second.push_back(t2);
  // 保证升序
  for (int i = (int)it->second.size() - 2; i >= 0; i--) {
    if (it->second[i] > it->second[i + 1]) {
      std::swap(it->second[i], it->second[i + 1]);
    }
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = waits_for_.find(t1);

  if (it != waits_for_.end()) {
    for (int i = 0; i < (int)it->second.size() ; i++) {
      if (it->second[i] == t2) {
        it->second.erase(it->second.begin() + i);
        break;
      }
    }
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_set<txn_id_t> visited;
  std::vector<txn_id_t> vec_ts;
  for (auto it_wf : waits_for_) {
    vec_ts.push_back(it_wf.first);
  }
  std::sort(vec_ts.begin(), vec_ts.end());

  for (auto t : vec_ts) {
    if (FindCycle(t, visited, txn_id)) {
      return true;
    }
    visited.clear();
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::unique_lock<std::mutex> wf_ul(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  for (auto it_wf : waits_for_) {
    for (auto t : it_wf.second) {
      edges.push_back(std::make_pair(it_wf.first, t));
    }
  }
  return edges;
}

void LockManager::MakeWaitsForGraph(std::shared_ptr<LockRequestQueue> lrq) {
  std::unique_lock<std::mutex> lrq_ul(lrq->latch_);
  for (auto rq : lrq->request_queue_) {

    if (txn_manager_->GetTransaction(rq->txn_id_)->GetState() == TransactionState::ABORTED) {
      continue;
    }
    for (auto it_g: lrq->grantted_) {
      auto g = it_g.second;

      if (txn_manager_->GetTransaction(g->txn_id_)->GetState() == TransactionState::ABORTED) {
        continue;
      }
      BUSTUB_ASSERT(g->granted_, "lock request node is in grantted_queue_, but granted_ equal false!");

      if (!AreLocksCompatible(g->lock_mode_, rq->lock_mode_)) {
        // rq -> g
        AddEdge(rq->txn_id_, g->txn_id_);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      std::unique_lock<std::mutex> wf_ul(waits_for_latch_);
      std::unique_lock<std::mutex> tlm_ul(table_lock_map_latch_);
      waits_for_.clear();
      // 构造表依赖图
      for (auto it_tlm_pair : table_lock_map_) {
        auto lrq = it_tlm_pair.second;

        MakeWaitsForGraph(lrq);
      }

      std::unique_lock<std::mutex> rlm_ul(row_lock_map_latch_);

      // 构造行依赖图
      for (auto it_rlm_pair : row_lock_map_) {
        auto lrq = it_rlm_pair.second;

        MakeWaitsForGraph(lrq);
      }

      // dfs遍历依赖图，干预循环依赖，解决死锁
      txn_id_t abort_txn = -1;

      while (HasCycle(&abort_txn)) {
        auto txn = txn_manager_->GetTransaction(abort_txn);
        txn->SetState(TransactionState::ABORTED);

        for (auto it_tlm_pair : table_lock_map_) {
          auto lrq = it_tlm_pair.second;

          for (auto rq : lrq->request_queue_) {
            if (rq->txn_id_ == abort_txn) {
              lrq->cv_.notify_all();
              // 外层循环是否也能一并break？
              break;
            }
          }

        }

        for (auto it_rlm_pair : row_lock_map_) {
          auto lrq = it_rlm_pair.second;

          for (auto rq : lrq->request_queue_) {
            if (rq->txn_id_ == abort_txn) {
              lrq->cv_.notify_all();
              // 外层循环是否也能一并break？
              break;
            }
          }
        }
      }
    }
  }
}

auto LockManager::FindCycle(txn_id_t source_txn, std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {

  if (txn_manager_->GetTransaction(source_txn)->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (visited.find(source_txn) != visited.end()) {
    // 找到一个环
    *abort_txn_id = -1;
    return true;
  }

  visited.insert(source_txn);

  auto it_wf = waits_for_.find(source_txn);

  if (it_wf == waits_for_.end()) {
    return false;
  }

  auto &vec_end = it_wf->second;
  for (auto t : vec_end) {
    txn_id_t temp_youngest = -1;
    if (FindCycle(t, visited, &temp_youngest)) {
      *abort_txn_id = std::max(temp_youngest, source_txn);
      return true;
    }
  }
  return false;
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  
  return true;
}
auto LockManager::UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}
auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  BUSTUB_ASSERT(l1 >= (LockMode)0 && l1 < LockMode::NULL_LOCK && l2 >= (LockMode)0 && l2 < LockMode::NULL_LOCK, "LockMode is valid!");
  return compatibility_matrix_[(int)l1][(int)l2];
}
auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || 
    txn->GetState() == TransactionState::COMMITTED) {
    return false;
  }

  if ((txn->GetState() == TransactionState::SHRINKING) &&
    (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {

    // LOCK_ON_SHRINKING
    txn->SetState(TransactionState::ABORTED);

    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      // AbortReason::LOCK_ON_SHRINKING
      txn->SetState(TransactionState::ABORTED);
      // No locks are allowed in the SHRINKING state
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if ((txn->GetState() == TransactionState::SHRINKING) && 
      !(lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED)) {
      // AbortReason::LOCK_ON_SHRINKING
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (((txn->GetState() == TransactionState::GROWING) &&
      !(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) ||
      (txn->GetState() == TransactionState::SHRINKING)) {

      // LOCK_SHARED_ON_READ_UNCOMMITTED
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  } else {
    // invalid isolation level.
    BUSTUB_ASSERT(false, "invalid isolation level.");
    return false;
  }

  return true;
}
void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {

}
auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  BUSTUB_ASSERT(curr_lock_mode >= (LockMode)0 && curr_lock_mode < LockMode::NULL_LOCK && 
    requested_lock_mode >= (LockMode)0 && requested_lock_mode < LockMode::NULL_LOCK, "LockMode is valid!");

  if (curr_lock_mode == LockMode::INTENTION_SHARED && 
    (requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
    requested_lock_mode == LockMode::INTENTION_EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      return true;
  } else if (curr_lock_mode == LockMode::SHARED && 
    (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      return true;
  } else if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE && 
    (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      return true;
  } else if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE &&
    (requested_lock_mode == LockMode::EXCLUSIVE)) {
      return true;
  }

  return false;
}
auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool {

  txn->LockTxn();
  switch(row_lock_mode) {
    case LockMode::EXCLUSIVE:
      if (txn->IsTableExclusiveLocked(oid) ||
        txn->IsTableIntentionExclusiveLocked(oid) ||
        txn->IsTableSharedIntentionExclusiveLocked(oid)) {

        txn->UnlockTxn();
        return true;
      }
      break;
    case LockMode::SHARED:
      if (txn->IsTableSharedLocked(oid) ||
        txn->IsTableIntentionSharedLocked(oid) ||
        txn->IsTableSharedIntentionExclusiveLocked(oid) ||
        txn->IsTableExclusiveLocked(oid) ||
        txn->IsTableIntentionExclusiveLocked(oid)) {

        txn->UnlockTxn();
        return true;
      }
      break;
    default:
      BUSTUB_ASSERT(false, "invalid row lock!");
  }
  txn->UnlockTxn();
  // TABLE_LOCK_NOT_PRESENT
  return false;
}

auto LockManager::DeleteLockTableSets(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  std::unordered_set<table_oid_t>::iterator it;

  txn->LockTxn();
  switch(lock_mode) {
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveTableLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      lock_set = txn->GetIntentionExclusiveTableLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::INTENTION_SHARED:
      lock_set = txn->GetIntentionSharedTableLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::SHARED:
      lock_set = txn->GetSharedTableLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
      it = lock_set->find(oid);
      break;
    default:
      BUSTUB_ASSERT(false, "unknow LockMode");
  }

  if (it == lock_set->end()) {
    txn->UnlockTxn();
    return false;
  }
  lock_set->erase(it);

  txn->UnlockTxn();
  return true;
}
auto LockManager::UpdateLockTableSets(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  std::unordered_set<table_oid_t>::iterator it;

  txn->LockTxn();
  switch(lock_mode) {
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveTableLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      lock_set = txn->GetIntentionExclusiveTableLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::INTENTION_SHARED:
      lock_set = txn->GetIntentionSharedTableLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::SHARED:
      lock_set = txn->GetSharedTableLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
      it = lock_set->find(oid);
      break;
    default:
      BUSTUB_ASSERT(false, "unknow LockMode");
  }

  if (it != lock_set->end()) {
    txn->UnlockTxn();
    return false;
  }
  lock_set->insert(oid);
  txn->UnlockTxn();
  return true;
}

auto LockManager::DeleteLockRowSets(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_set;
  std::unordered_map<table_oid_t, std::unordered_set<RID>>::iterator it;
  std::unordered_set<RID>::iterator it2;

  txn->LockTxn();
  switch(lock_mode) {
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveRowLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::SHARED:
      lock_set = txn->GetSharedRowLockSet();
      it = lock_set->find(oid);
      break;
    default:
      BUSTUB_ASSERT(false, "unknow LockMode");
  }

  if (it == lock_set->end()) {
    txn->UnlockTxn();
    return false;
  }

  it2 = it->second.find(rid);
  if (it2 == it->second.end()) {
    txn->UnlockTxn();
    return false;
  }

  it->second.erase(it2);

  txn->UnlockTxn();
  return true;
}
auto LockManager::UpdateLockRowSets(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_set;
  std::unordered_map<table_oid_t, std::unordered_set<RID>>::iterator it;
  std::unordered_set<RID>::iterator it2;

  txn->LockTxn();
  switch(lock_mode) {
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveRowLockSet();
      it = lock_set->find(oid);
      break;
    case LockMode::SHARED:
      lock_set = txn->GetSharedRowLockSet();
      it = lock_set->find(oid);
      break;
    default:
      BUSTUB_ASSERT(false, "unknow LockMode");
  }

  if (it == lock_set->end()) {
    lock_set->insert(std::make_pair(oid,  std::unordered_set<RID>({rid})));

    txn->UnlockTxn();
    return true;
  }

  it2 = it->second.find(rid);
  if (it2 != it->second.end()) {
    txn->UnlockTxn();
    return false;
  }

  it->second.insert(rid);

  txn->UnlockTxn();
  return true;
}

auto LockManager::TransactionStateUpdate(Transaction *txn, std::shared_ptr<LockRequest> grantted_lock) -> bool {
  if (grantted_lock->lock_mode_ != LockMode::EXCLUSIVE && 
    grantted_lock->lock_mode_ != LockMode::SHARED) {

    return false;
  }

  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) {
    txn->SetState(TransactionState::SHRINKING);
    return true;
  } else if ((txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
    (grantted_lock->lock_mode_ == LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::SHRINKING);
    return true;
  } else if ((txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) &&
    (grantted_lock->lock_mode_ == LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::SHRINKING);
    return true;
  }

  return false;
}

}  // namespace bustub
