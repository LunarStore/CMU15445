//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::lock_guard<std::mutex> lock(latch_);
    auto target = node_store_.end();
    size_t targetBKDist = 0;
    if(node_store_.empty()){    // 没有可Evict的帧
        return false;
    }

    for(auto it = node_store_.begin(); it != node_store_.end(); it++){
        const LRUKNode& node = it->second;
        size_t bKDist = 0;
        if(node.is_evictable_ == false)
            continue;

        if(node.history_.size() == node.k_){    // 记录了k次历史
            bKDist = current_timestamp_ - node.history_.back(); // 和前k次时间戳做差
        }else{  // 访问不足k次。
            bKDist = UINT64_MAX;
        }

        if(targetBKDist < bKDist){    // 找到间隔更久的node
            target = it;
            targetBKDist = bKDist;
        }else if(targetBKDist == bKDist){ // 相等就比较第kth个时间戳。
            if(target->second.history_.back() > node.history_.back()){
                target = it;
                targetBKDist = bKDist;
            }
        }
    }

    if(target == node_store_.end()){    // 找不到要剔除的frame
        return false;
    }
    BUSTUB_ASSERT(target->second.is_evictable_ == true, "is_evictable_ == false!");
    *frame_id = target->second.fid_;
    node_store_.erase(target);  // remove the frame's access history.
    BUSTUB_ASSERT(curr_size_ - 1 < curr_size_, "");
    curr_size_--;   // size decrement
    // delete
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex> lock(latch_);
    BUSTUB_ASSERT((frame_id != -1) && (frame_id < (frame_id_t)replacer_size_), "");

    auto target = node_store_.find(frame_id);
    if(target == node_store_.end()){    // 没有
        target = node_store_.insert(std::make_pair(frame_id, LRUKNode())).first;
        target->second.fid_ = frame_id;
        target->second.is_evictable_ = false;
        target->second.k_ = k_;

        //curr_size_++;
        BUSTUB_ASSERT(curr_size_ <= replacer_size_, "");
    }
    // need mark to is_evictable_ == false?
    if(target->second.history_.size() < target->second.k_){
        target->second.history_.push_front(++current_timestamp_);
    }else{  // ==
        BUSTUB_ASSERT(target->second.history_.size() == target->second.k_, "");
        target->second.history_.push_front(++current_timestamp_);
        target->second.history_.pop_back();
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> lock(latch_);
    BUSTUB_ASSERT((frame_id != -1) && (frame_id < (frame_id_t)replacer_size_), "");

    auto target = node_store_.find(frame_id);

    if(target != node_store_.end()){
        if(target->second.is_evictable_ == true && set_evictable == false){
            target->second.is_evictable_ = false;
            BUSTUB_ASSERT(curr_size_ - 1 < curr_size_, "");
            curr_size_--;
        }else if(target->second.is_evictable_ == false && set_evictable == true){
            target->second.is_evictable_ = true;
            curr_size_++;
            BUSTUB_ASSERT(curr_size_ <= replacer_size_, "");
        }
    }   // 什么也不做
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    BUSTUB_ASSERT(frame_id <= (frame_id_t)replacer_size_, "");

    auto target = node_store_.find(frame_id);
    if(target != node_store_.end()){
        BUSTUB_ASSERT(target->second.is_evictable_  == true, "");
        node_store_.erase(target);
        BUSTUB_ASSERT(curr_size_ - 1 < curr_size_, "");
        curr_size_--;
    }
}

auto LRUKReplacer::Size() -> size_t {
    std::lock_guard<std::mutex> lock(latch_);
    return curr_size_; 
}

}  // namespace bustub
