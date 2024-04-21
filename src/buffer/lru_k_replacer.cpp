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
  std::scoped_lock<std::mutex> lock(latch_);
  if (evict_.empty()) {
    return false;
  }
  size_t max_timestamp = 0;
  size_t front_timestamp = SIZE_MAX;
  frame_id_t ret_frameid = 0;
  for (const auto &fid : evict_) {
    auto node = node_store_.at(fid);
    auto [front, timestamp] = node.Calculate(current_timestamp_);
    if (timestamp > max_timestamp) {
      max_timestamp = timestamp;
      ret_frameid = fid;
      front_timestamp = front;
    } else if (timestamp == max_timestamp && front_timestamp > front) {
      ret_frameid = fid;
      front_timestamp = front;
    }
  }
  evict_.erase(ret_frameid);
  node_store_.erase(ret_frameid);
  *frame_id = ret_frameid;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "Error: frame_id greater than replacer_size_");
  // 1. not in map, then create mapping
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_[frame_id] = LRUKNode(k_, frame_id);
  }
  // 2. Record Access
  node_store_[frame_id].Record(current_timestamp_);
  unevict_.insert(frame_id);
  // 3. update current timestamp
  ++current_timestamp_;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) != node_store_.end()) {
    auto &node = node_store_[frame_id];
    auto now_evict = node.IsEvictable();
    if (now_evict != set_evictable) {
      node.SetEvict(set_evictable);
      if (now_evict) {
        evict_.erase(frame_id);
        unevict_.insert(frame_id);
      } else {
        evict_.insert(frame_id);
        unevict_.erase(frame_id);
      }
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_.erase(frame_id);
    evict_.erase(frame_id);
    unevict_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return evict_.size();
}

}  // namespace bustub
