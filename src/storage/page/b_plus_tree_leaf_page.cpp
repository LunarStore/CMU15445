//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  BPlusTreePage::SetPageType(IndexPageType::LEAF_PAGE);
  BPlusTreePage::SetSize(0);
  BPlusTreePage::SetMaxSize(max_size);

  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType{
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::EntryAt(int index) const -> const MappingType&{
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "");
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int{
  int begin = 0, end = GetSize();

  while(begin < end){
    int mid = (begin + end) / 2;

    if(comparator(array_[mid].first, key) < 0){
      begin = mid + 1;
    }else{
      end = mid;
    }
  }

  return end;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int{
  if(GetSize() >= GetMaxSize()){
    // 已经满了
    // 不可能走到这里
    BUSTUB_ASSERT(false, "");
    return GetSize();
  }

  int pos = KeyIndex(key, comparator);
    // 插入一个已经存在的key？
  BUSTUB_ASSERT(!(pos < GetSize() && comparator(array_[pos].first, key) == 0), "");

  // 直接从后往前遍历
  for(int index = GetSize() - 1; index >= pos; index--){
    
    array_[index + 1] = array_[index];
  }
  //BUSTUB_ASSERT(index + 1 == pos, "");
  array_[pos] = std::make_pair(key, value);
  // 大小自增1.
  IncreaseSize(1);

  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveTo(int begin, int end, B_PLUS_TREE_LEAF_PAGE_TYPE& dest){
  dest.Append(array_ + begin, array_ + end);

  IncreaseSize(-(end - begin));
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Append(const MappingType* begin, const MappingType* end){
  int offset = 0;

  BUSTUB_ASSERT(GetSize() + (end - begin) < GetMaxSize(), "");
  while(begin < end){
    array_[GetSize() + offset] = *begin;
    offset++;
    begin++;
  }

  IncreaseSize(offset);
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> int{
  int pos = KeyIndex(key, comparator);

  if(pos >= GetSize() || comparator(array_[pos].first, key) != 0){ // 不存在就返回 -1;
    // 在并发环境下，有可能走到这里
    return -1;
  }

  for(pos = pos + 1; pos < GetSize(); pos++){
    array_[pos - 1] = array_[pos];
  }

  IncreaseSize(-1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Prepend(const MappingType& entry){
  int pos = GetSize() - 1;
  for(; pos >= 0; pos--){
    array_[pos + 1] = array_[pos];
  }
  array_[0] = entry;

  IncreaseSize(1);

  BUSTUB_ASSERT(GetSize() < GetMaxSize(), "");
  return;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Append(const MappingType& entry){
  array_[GetSize()] = entry;

  IncreaseSize(1);
  BUSTUB_ASSERT(GetSize() < GetMaxSize(), "");
  return;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopBack() -> MappingType{
  BUSTUB_ASSERT(GetSize() > 0, "");
  MappingType res = array_[GetSize() - 1];

  IncreaseSize(-1);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopFront()-> MappingType{
  BUSTUB_ASSERT(GetSize() > 0, "");
  MappingType res = array_[0];

  int pos = 1;
  for(; pos < GetSize(); pos++){
    array_[pos - 1] = array_[pos];
  }
  IncreaseSize(-1);

  return res;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
