//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages): num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (nodes_.empty()) {
    return false;
  }
  *frame_id = nodes_.back();
  nodes_.pop_back();
  index_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (index_.count(frame_id) == 0) {
    return;
  }
  auto it = index_[frame_id];
  nodes_.erase(it);
  index_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (index_.count(frame_id) != 0) {
    return;
  }
  nodes_.push_front(frame_id);
  index_[frame_id] = nodes_.begin();
}

size_t LRUReplacer::Size() {
  std::unique_lock<std::mutex> lock(mutex_);
  return nodes_.size();
}


}  // namespace bustub
