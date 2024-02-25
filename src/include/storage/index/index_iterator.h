//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(page_id_t page_id, size_t offset, BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return (leaf_page_id_ == itr.leaf_page_id_) && (offset_ == itr.offset_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !((*this) == itr); }

 private:
  // add your own private member variables here
  page_id_t leaf_page_id_;  // 读到的叶子的叶id
  size_t offset_;           // 叶子id的偏移
  ReadPageGuard leaf_page_guard_;
  const B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_;
  BufferPoolManager *bpm_;
};

}  // namespace bustub
