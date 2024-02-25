//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //   throw NotImplementedException(
  //       "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //       "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id = -1;
  *page_id = INVALID_PAGE_ID;
  std::lock_guard<std::mutex> lock(latch_);

  if (NewFrameUnlocked(frame_id) == nullptr) return nullptr;

  replacer_->RecordAccess(frame_id);  // 确保frame存在于replacer中，并添加一条history
  replacer_->SetEvictable(frame_id, false);

  Page *page = pages_ + frame_id;

  page->page_id_ = AllocatePage();
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  *page_id = page->page_id_;

  page_table_.insert(std::make_pair(*page_id, frame_id));
  return page;
}

auto BufferPoolManager::NewFrameUnlocked(frame_id_t &frame_id) -> Page * {
  frame_id = -1;  // 初始化为无效
  if (!free_list_.empty()) {
    // 优先找free_list
    frame_id = free_list_.front();
    free_list_.pop_front();
    // BUSTUB_ASSERT(frame_id != -1, "");
  } else {
    // 最坏情况，去驱除内存页
    bool ret = replacer_->Evict(&frame_id);
    if (ret == true) {  // 驱除成功
      // BUSTUB_ASSERT(frame_id != -1, "");
      BUSTUB_ASSERT(pages_[frame_id].GetPinCount() == 0, "");

      if (pages_[frame_id].IsDirty()) {
        // 脏页写回磁盘
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      }
      pages_[frame_id].ResetMemory();
      page_table_.erase(pages_[frame_id].GetPageId());  // 在page_table_上清除pageid -> frameid

      pages_[frame_id].page_id_ = INVALID_PAGE_ID;
      pages_[frame_id].pin_count_ = 0;
      pages_[frame_id].is_dirty_ = false;

    } else {
      // 没有多余或者可驱除的frame。
      return nullptr;
    }
  }

  BUSTUB_ASSERT(frame_id != -1, "");

  BUSTUB_ASSERT(pages_[frame_id].page_id_ == INVALID_PAGE_ID, "");  // 没被占用
  return pages_ + frame_id;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  auto target = page_table_.find(page_id);
  frame_id_t frame_id = -1;
  if (target != page_table_.end()) {
    // page在内存中
    frame_id = target->second;

    BUSTUB_ASSERT(page_id == pages_[frame_id].GetPageId(), "");
    replacer_->RecordAccess(frame_id);  // 确保frame存在于replacer中，并添加一条history
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;  // 引用计数加一

    // return pages_ + frame_id;
  } else {
    // page不在内存中
    if (NewFrameUnlocked(frame_id) == nullptr) return nullptr;  // 缓存满

    replacer_->RecordAccess(frame_id);  // 确保frame存在于replacer中，并添加一条history
    replacer_->SetEvictable(frame_id, false);

    // 从磁盘读数据到frame上
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

    // 初始化frame相关的参数
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;

    // 建立page_id -> frame_id的映射
    page_table_.insert(std::make_pair(page_id, frame_id));
  }
  return pages_ + frame_id;
}
// to do
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto target = page_table_.find(page_id);

  // 合法性判断
  if (target == page_table_.end() || pages_[target->second].GetPinCount() == 0) {
    return false;
  }
  frame_id_t frame_id = target->second;
  BUSTUB_ASSERT(page_id == pages_[frame_id].GetPageId(), "");

  pages_[frame_id].pin_count_--;
  BUSTUB_ASSERT(pages_[frame_id].GetPinCount() >= 0, "");

  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  if (is_dirty == true) {
    pages_[frame_id].is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto target = page_table_.find(page_id);

  if (target == page_table_.end()) {
    // page 不在内存中
    return false;
  }
  frame_id_t frame_id = target->second;

  BUSTUB_ASSERT(page_id == pages_[frame_id].GetPageId(), "");
  // flush
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  for (const auto &it : page_table_) {
    page_id_t page_id = it.first;
    frame_id_t frame_id = it.second;

    BUSTUB_ASSERT(page_id == pages_[frame_id].GetPageId(), "");
    // flush
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  auto target = page_table_.find(page_id);

  if (target == page_table_.end()) {
    // page 不在内存中
    return true;
  }

  if (pages_[target->second].GetPinCount() > 0) {
    return false;
  }
  frame_id_t frame_id = target->second;
  // 确定该page可以删除
  BUSTUB_ASSERT(page_id == pages_[frame_id].GetPageId(), "");
  BUSTUB_ASSERT(pages_[frame_id].GetPinCount() == 0, "");

  if (pages_[frame_id].is_dirty_ == true) {
    // 脏页写回磁盘
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  pages_[frame_id].ResetMemory();
  // 删除访问历史，停止追踪
  replacer_->Remove(frame_id);
  // 从page_table中删除 page_id -> frame_id的映射
  page_table_.erase(page_id);

  DeallocatePage(page_id);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  free_list_.push_back(frame_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
