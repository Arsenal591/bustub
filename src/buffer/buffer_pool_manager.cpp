//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  //LOG_ERROR("New instance %d.", global_cnt);
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::unique_lock<std::mutex> lock(latch_);

  Page *result_page = nullptr;
  const auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    const frame_id_t frame_id = it->second;
    replacer_->Pin(frame_id);
    result_page = &(pages_[frame_id]);
    result_page->WLatch();
    result_page->pin_count_++;
    result_page->WUnlatch();
    return result_page;
  }
  frame_id_t replace_frame_id;
  bool found = FindAvailablePage(&replace_frame_id);
  if (!found) {
    return nullptr;
  }
  result_page = &(pages_[replace_frame_id]);

  result_page->WLatch();
  page_table_.erase(result_page->GetPageId());
  page_table_[page_id] = replace_frame_id;
  FlushPageIfPossible(result_page);
  disk_manager_->ReadPage(page_id, result_page->GetData());
  result_page->page_id_ = page_id;
  result_page->pin_count_ = 1;
  result_page->WUnlatch();
  return result_page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::unique_lock<std::mutex> lock(latch_);

  const auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = it->second;
  Page *page = &(pages_[frame_id]);
  page->WLatch();

  bool ret = false;
  if (page->pin_count_ > 0) {
    ret = true;
    page->is_dirty_ |= is_dirty; // move to if branch?
    page->pin_count_--;
    if (page->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
  }
  page->WUnlatch();
  return ret;
}

bool BufferPoolManager::flushPageWithoutLock(bustub::page_id_t page_id) {
  const auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  const frame_id_t frame_id = it->second;
  Page *page = &(pages_[frame_id]);
  page->WLatch();
  FlushPageIfPossible(page);
  page->WUnlatch();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::unique_lock<std::mutex> lock(latch_);

  return flushPageWithoutLock(page_id);
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  std::unique_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  bool found = FindAvailablePage(&frame_id);
  if (!found) {
    return nullptr;
  }
  Page *page = &(pages_[frame_id]);

  page->WLatch();
  *page_id = disk_manager_->AllocatePage();
  page_table_.erase(page->GetPageId());
  page_table_[*page_id] = frame_id;
  replacer_->Pin(frame_id);
  FlushPageIfPossible(page);
  page->ResetMemory();
  //disk_manager_->WritePage(*page_id, page->GetData());
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->WUnlatch();
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::unique_lock<std::mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    //disk_manager_->DeallocatePage(page_id);
    return true;
  }
  frame_id_t frame_id = it->second;
  Page *page = &(pages_[frame_id]);

  page->WLatch();
  if (page->GetPinCount() > 0) {
    page->WUnlatch();
    return false;
  }
  disk_manager_->DeallocatePage(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->ResetMemory();
  page->WUnlatch();
  replacer_->Pin(frame_id);
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::unique_lock<std::mutex> lock(latch_);

  for (const auto &kv : page_table_) {
    flushPageWithoutLock(kv.first);
  }
}

bool BufferPoolManager::FindAvailablePage(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  return replacer_->Victim(frame_id);
}

void BufferPoolManager::FlushPageIfPossible(Page *page) {
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
}

}  // namespace bustub
