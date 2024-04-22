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
#include <mutex>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"
#include "type/type_id.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
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
  std::scoped_lock<std::mutex> lock(latch_);
  // pick the replacement frame from free list or the replacer
  // pages_数组的下标
  frame_id_t new_frame_id;
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&new_frame_id)) {
    return nullptr;
  }
  Page *page = &pages_[new_frame_id];
  // delete old mapping
  if (page_table_.find(page->GetPageId()) != page_table_.end()) {
    page_table_.erase(page->GetPageId());
  }
  // Dirty page, write to disk
  if (page->IsDirty()) {
    // Schedule write request
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
  // Allocate new page, get page_id, update mapping
  *page_id = AllocatePage();
  // add new mapping
  page_table_[*page_id] = new_frame_id;
  // BPM is friend class of Page
  // reset data
  page->ResetMemory();
  page->is_dirty_ = false;
  page->page_id_ = *page_id;
  page->pin_count_ = 0;
  // RecordAccess must be called before SetEvictable
  // record access
  replacer_->RecordAccess(new_frame_id);
  // pin the frame
  replacer_->SetEvictable(new_frame_id, false);
  ++page->pin_count_;
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // first search from buffer pool
  frame_id_t new_frame_id;
  if (page_table_.find(page_id) == page_table_.end()) {
    // pick replacement frame
    if (!free_list_.empty()) {
      new_frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (!replacer_->Evict(&new_frame_id)) {
      return nullptr;
    }
  } else {
    new_frame_id = page_table_[page_id];
    replacer_->RecordAccess(new_frame_id);
    replacer_->SetEvictable(new_frame_id, false);
    Page *page = &pages_[new_frame_id];
    ++page->pin_count_;
    return page;
  }
  Page *page = &pages_[new_frame_id];

  // delete old mapping
  if (page_table_.find(page->GetPageId()) != page_table_.end()) {
    page_table_.erase(page->GetPageId());
  }
  // add new mapping
  page_table_[page_id] = new_frame_id;
  // Dirty page, write to disk
  if (page->IsDirty()) {
    // Schedule write request
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
  // BPM is friend class of Page
  // reset data
  page->ResetMemory();
  page->is_dirty_ = false;
  page->page_id_ = page_id;
  page->pin_count_ = 0;
  // read data
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();

  // RecordAccess must be called before SetEvictable
  // record access
  replacer_->RecordAccess(new_frame_id);
  // pin the frame
  replacer_->SetEvictable(new_frame_id, false);
  ++page->pin_count_;
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // page_id not in buffer pool
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = &pages_[frame_id];
  // pin count is already zero
  if (page->GetPinCount() == 0) {
    return false;
  }
  // decrease the pin count
  --page->pin_count_;
  // if pin count is zero, mean this frame can be evicted by replacer
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  // set dirty flag
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto page = &pages_[page_table_[page_id]];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    auto page = &pages_[frame_id];
    if (page->GetPageId() != INVALID_PAGE_ID) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
      future.get();
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  auto page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  free_list_.push_back(frame_id);

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
