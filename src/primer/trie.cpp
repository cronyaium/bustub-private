#include "primer/trie.h"
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (root_ == nullptr) {
    return nullptr;
  }
  auto p = root_;
  for (const auto &c : key) {
    if (p->children_.find(c) != p->children_.end()) {
      p = p->children_.at(c);
    } else {
      return nullptr;
    }
  }
  // auto ret = std::static_pointer_cast<const TrieNodeWithValue<T>>(p);
  // auto _ = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(p);
  const auto *ret = dynamic_cast<const TrieNodeWithValue<T> *>(p.get());  // TAG:DEBUG
  if (ret == nullptr) {
    return nullptr;
  }
  return (ret->value_).get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

// return: Base*
template <class T>
void Trie::Construct(std::shared_ptr<TrieNode> &rt, const std::shared_ptr<TrieNode> &now, unsigned idx,
                     const std::string_view &key, T &value) const {
  if (idx == 0) {
    // 1. clone old root as new root
    std::shared_ptr<TrieNode> root = nullptr;
    if (root_ != nullptr) {
      auto clone = root_->Clone();
      root = std::shared_ptr<TrieNode>(std::move(clone));
    } else {
      root = std::make_shared<TrieNode>();
    }

    // 2. create/update TrieNode/TrieNodeWithValue corresponding key[0]
    if (idx + 1 == key.size()) {
      // 2.1 last key, then create TrieNodeWithValue
      std::shared_ptr<TrieNodeWithValue<T>> nxt = nullptr;
      if (root->children_.find(key[0]) != root->children_.end()) {
        // 2.1.1 original key exist, clone and then modify
        auto clone = root->children_.at(key[0])->Clone();
        auto tnode = std::shared_ptr<TrieNode>(std::move(clone));
        nxt =
            std::make_shared<TrieNodeWithValue<T>>(std::move(tnode->children_), std::make_shared<T>(std::move(value)));
      } else {
        // 2.1.2 original key not exist, create a new node
        nxt = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      }
      // 2.1.3 update
      root->children_[key[0]] = nxt;
      auto nxt_tnode = std::dynamic_pointer_cast<TrieNode>(nxt);
      Construct(rt, nxt_tnode, 1, key, value);
    } else {
      // 2.2 not last key, then create TrieNode
      std::shared_ptr<TrieNode> nxt = nullptr;
      if (root->children_.find(key[0]) != root->children_.end()) {
        auto clone = root->children_.at(key[0])->Clone();
        nxt = std::shared_ptr<TrieNode>(std::move(clone));
      } else {
        nxt = std::make_shared<TrieNode>();
      }
      root->children_[key[0]] = nxt;
      Construct(rt, nxt, 1, key, value);
    }
    // 2.3 update via reference
    rt = root;
    return;
  }

  // idx != 0
  if (idx + 1 == key.size()) {
    std::shared_ptr<TrieNodeWithValue<T>> nxt = nullptr;
    if (now->children_.find(key[idx]) != now->children_.end()) {
      auto clone = now->children_.at(key[idx])->Clone();
      auto tnode = std::shared_ptr<TrieNode>(std::move(clone));
      nxt = std::make_shared<TrieNodeWithValue<T>>(std::move(tnode->children_), std::make_shared<T>(std::move(value)));
    } else {
      nxt = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    }
    now->children_[key[idx]] = nxt;
    return;
  }

  std::shared_ptr<TrieNode> nxt = nullptr;
  if (now->children_.find(key[idx]) != now->children_.end()) {
    auto clone = now->children_.at(key[idx])->Clone();
    nxt = std::shared_ptr<TrieNode>(std::move(clone));
  } else {
    nxt = std::make_shared<TrieNode>();
  }
  now->children_[key[idx]] = nxt;
  Construct(rt, nxt, idx + 1, key, value);
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // Special Case
  if (key.empty()) {
    // we should set new root as a TrieNodeWithValue
    if (root_ == nullptr) {
      auto root = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      return Trie(root);
    }
    auto clone = root_->Clone();
    auto tnode = std::shared_ptr<TrieNode>(std::move(clone));
    auto root =
        std::make_shared<TrieNodeWithValue<T>>(std::move(tnode->children_), std::make_shared<T>(std::move(value)));
    return Trie(root);
  }

  auto root = std::make_shared<TrieNode>();
  Construct(root, root, 0, key, value);
  // Printer<T>(root, 0);
  return Trie(root);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Rem(const std::shared_ptr<const TrieNode> &now, unsigned idx, const std::string_view &key) const
    -> std::shared_ptr<const TrieNode> {
  if (idx == key.size()) {
    auto clone = now->Clone();
    auto now2 = std::shared_ptr<TrieNode>(std::move(clone));
    // leaf value node, then delete it.
    if (now2->children_.empty()) {
      return nullptr;
    }
    // non-leaf value node, convert to TrieNode, then return
    auto ret = std::make_shared<TrieNode>(std::move(now2->children_));
    return ret;
  }

  auto nxt = now->children_.at(key[idx]);
  auto ret = Rem(nxt, idx + 1, key);
  if (ret == nullptr) {
    auto clone = now->Clone();
    auto now2 = std::shared_ptr<TrieNode>(std::move(clone));
    now2->children_.erase(key[idx]);
    // !now2->is_value_node_
    if (now2->children_.empty() && !now2->is_value_node_) {
      return nullptr;
    }
    return now2;
  }
  auto clone = now->Clone();
  auto now2 = std::shared_ptr<TrieNode>(std::move(clone));
  now2->children_[key[idx]] = ret;
  return now2;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // Special Case
  if (root_ == nullptr) {
    return *this;
  }
  // Judge whether key exist or not
  auto p = root_;
  bool flag = true;
  for (const auto &k : key) {
    if (p->children_.find(k) == p->children_.end()) {
      flag = false;
      break;
    }
    p = p->children_.at(k);
  }
  if (!flag || !p->is_value_node_) {
    return *this;
  }
  // key exist, start remove
  auto nroot = Rem(root_, 0, key);
  // Printer<uint32_t>(nroot, 0);
  return Trie(nroot);

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

template <class T>
void Trie::Printer(const std::shared_ptr<const TrieNode> &p, int spaces) const {
  std::cerr << "---------------------Printer-------------------" << std::endl;
  if (p == nullptr) {
    return;
  }
  std::cerr << std::hex << p << " is value node: " << p->is_value_node_ << std::endl;
  if (p->is_value_node_) {
    auto q = std::static_pointer_cast<const TrieNodeWithValue<T>>(p);
    if (q == nullptr) {
      std::cerr << "Q IS NULLPTR" << std::endl;
    } else {
      std::cerr << "Q VALUE is " << std::hex << (q->value_) << std::endl;
    }
  }
  for (const auto &[k, child] : p->children_) {
    std::string sp(spaces, ' ');
    std::cerr << sp << "key: " << k << std::hex << "-- child: " << child << " is value node: " << child->is_value_node_
              << std::endl;
    Printer<T>(child, spaces + 2);
  }
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
