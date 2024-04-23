//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  index_name_ = name;
  auto guard = bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto header_page = guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

  auto hash_value = Hash(key);
  auto directory_index = header_page->HashToDirectoryIndex(hash_value);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_index);
  if (static_cast<page_id_t>(directory_page_id) == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  if (!directory_guard.IsPageValid()) {
    return false;
  }
  header_guard.Drop();
  auto directory_page = directory_guard.template As<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory_page->HashToBucketIndex(hash_value);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  if (!bucket_guard.IsPageValid()) {
    return false;
  }
  directory_guard.Drop();
  auto bucket_page = bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  auto ret = bucket_page->Lookup(key, value, cmp_);
  if (ret) {
    result->emplace_back(std::move(value));
  }

  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  // key already exist
  std::vector<V> tmp;
  if (GetValue(key, &tmp, transaction)) {
    return false;
  }

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  auto hash_value = Hash(key);
  auto directory_index = header_page->HashToDirectoryIndex(hash_value);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_index);
  if (static_cast<page_id_t>(directory_page_id) == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, directory_index, hash_value, key, value);
  }
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  if (!directory_guard.IsPageValid()) {
    return InsertToNewDirectory(header_page, directory_index, hash_value, key, value);
  }
  // header unused
  header_guard.Drop();
  auto directory_page = directory_guard.template AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory_page->HashToBucketIndex(hash_value);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  if (!bucket_guard.IsPageValid()) {
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }
  auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  while (!bucket_page->Insert(key, value, cmp_)) {
    // bucket is full, try to grow
    // First, try to split bucket
    if (directory_page->GetLocalDepth(bucket_idx) < directory_page->GetGlobalDepth()) {
      auto new_bucket_idx = bucket_idx ^ (1 << directory_page->GetLocalDepth(bucket_idx));
      directory_page->IncrLocalDepth(bucket_idx);
      directory_page->IncrLocalDepth(new_bucket_idx);
      // Create a new bucket page
      page_id_t new_bucket_page_id = INVALID_PAGE_ID;
      auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
      if (new_bucket_page_id == INVALID_PAGE_ID) {
        return false;
      }
      auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      new_bucket_page->Init(bucket_max_size_);
      directory_page->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

      std::vector<uint32_t> others;
      for (uint32_t i = 0; i < bucket_page->Size(); i++) {
        auto new_hash_value = Hash(bucket_page->KeyAt(i));
        auto belong = directory_page->HashToBucketIndex(new_hash_value);
        if (belong == new_bucket_idx) {
          others.push_back(i);
          new_bucket_page->Insert(bucket_page->KeyAt(i), bucket_page->ValueAt(i), cmp_);
        }
      }
      for (auto it = others.rbegin(); it != others.rend(); it++) {
        bucket_page->RemoveAt(*it);
      }
      auto belong = directory_page->HashToBucketIndex(hash_value);
      if (belong == new_bucket_idx) {
        bucket_guard = std::move(new_bucket_guard);
        bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        bucket_idx = new_bucket_idx;
        bucket_page_id = new_bucket_page_id;
      }
    }
    // Second, bucket cannot be split, consider directory growing
    else if (directory_page->GetGlobalDepth() < directory_page->GetMaxDepth()) {
      directory_page->IncrGlobalDepth();
    } else {
      return false;
    }
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto guard = bpm_->NewPageGuarded(&new_page_id).UpgradeWrite();
  if (new_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_page = guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, new_page_id);
  auto bucket_idx = directory_page->HashToBucketIndex(Hash(key));
  return InsertToNewBucket(directory_page, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto guard = bpm_->NewPageGuarded(&new_page_id).UpgradeWrite();
  if (new_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_page = guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, new_page_id);
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // key not exist
  std::vector<V> tmp;
  if (!GetValue(key, &tmp, transaction)) {
    return false;
  }

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  auto hash_value = Hash(key);
  auto directory_index = header_page->HashToDirectoryIndex(hash_value);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_index);
  if (static_cast<page_id_t>(directory_page_id) == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  if (!directory_guard.IsPageValid()) {
    return false;
  }
  // header unused
  header_guard.Drop();
  auto directory_page = directory_guard.template AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory_page->HashToBucketIndex(hash_value);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  if (!bucket_guard.IsPageValid()) {
    return false;
  }
  auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }

  // apply bucket merging
  while (directory_page->GetLocalDepth(bucket_idx) > 0) {
    auto mask_bucket_idx = bucket_idx & directory_page->GetLocalDepthMask(bucket_idx);
    auto image_bucket_idx = mask_bucket_idx ^ (1 << (directory_page->GetLocalDepth(bucket_idx) - 1));
    auto image_page_id = directory_page->GetBucketPageId(image_bucket_idx);
    BUSTUB_ASSERT(image_page_id != INVALID_PAGE_ID, "Error: image_page_id is INVALID_PAGE_ID");
    auto image_guard = bpm_->FetchPageWrite(image_page_id);
    BUSTUB_ASSERT(image_guard.IsPageValid() == true, "Error: image_page is nullptr");
    auto image_page = image_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    // Buckets can only be merged with their split image if their split image has the same local depth.
    if (directory_page->GetLocalDepth(image_bucket_idx) != directory_page->GetLocalDepth(bucket_idx)) {
      break;
    }
    if (!bucket_page->IsEmpty() && !image_page->IsEmpty()) {
      break;
    }
    if (bucket_page->IsEmpty()) {
      bpm_->DeletePage(bucket_page_id);
      bucket_guard = std::move(image_guard);
      bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      bucket_idx = image_bucket_idx;
      bucket_page_id = image_page_id;

      auto inc = 1 << (directory_page->GetLocalDepth(bucket_idx) - 1);
      for (auto i = std::min(image_bucket_idx, mask_bucket_idx); i < directory_page->Size(); i += inc) {
        directory_page->SetBucketPageId(i, image_page_id);
        directory_page->DecrLocalDepth(i);
      }

    } else {
      bpm_->DeletePage(image_page_id);
      auto inc = 1 << (directory_page->GetLocalDepth(bucket_idx) - 1);
      for (auto i = std::min(image_bucket_idx, mask_bucket_idx); i < directory_page->Size(); i += inc) {
        directory_page->SetBucketPageId(i, bucket_page_id);
        directory_page->DecrLocalDepth(i);
      }
    }
  }
  // apply directory shrinking
  while (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
