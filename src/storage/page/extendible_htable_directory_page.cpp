//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  std::fill(std::begin(local_depths_), std::end(local_depths_), 0);
  std::fill(std::begin(bucket_page_ids_), std::end(bucket_page_ids_), INVALID_PAGE_ID);
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx out of range");
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx out of range");
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx out of range");
  return bucket_idx ^ (1 << (global_depth_ - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx out of range");
  return local_depths_[bucket_idx];
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1 << GetLocalDepth(bucket_idx)) - 1;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ < max_depth_) {
    for (uint32_t i = 0; i < Size(); i++) {
      uint32_t j = i | (1 << global_depth_);
      local_depths_[j] = local_depths_[i];
      bucket_page_ids_[j] = bucket_page_ids_[i];
    }
    ++global_depth_;
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ > 0) {
    --global_depth_;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;
  }
  // Only shrink the directory if the local depth of every bucket is strictly less than the global depth of the
  // directory.
  bool ret = true;
  for (auto i = 0; i < (1 << (global_depth_)); i++) {
    if (local_depths_[i] >= global_depth_) {
      ret = false;
      break;
    }
  }
  return ret;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return HTABLE_DIRECTORY_ARRAY_SIZE; }

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx out of range");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx out of range");
  ++local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx out of range");
  --local_depths_[bucket_idx];
}

}  // namespace bustub
