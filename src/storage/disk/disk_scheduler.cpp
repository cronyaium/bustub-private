//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <optional>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  std::optional<DiskRequest> it = std::move(r);
  request_queue_.Put(std::move(it));
}

void DiskScheduler::StartWorkerThread() {
  while (disk_manager_ != nullptr) {
    auto opt = request_queue_.Get();
    if (opt.has_value()) {
      if (opt->is_write_) {
        disk_manager_->WritePage(opt->page_id_, opt->data_);
      } else {
        disk_manager_->ReadPage(opt->page_id_, opt->data_);
      }
      opt->callback_.set_value(true);
    } else {
      // receive std::nullopt
      break;
    }
  }
}

}  // namespace bustub
