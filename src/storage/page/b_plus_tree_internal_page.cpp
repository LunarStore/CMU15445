//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
    BPlusTreePage::SetPageType(IndexPageType::INTERNAL_PAGE);
    BPlusTreePage::SetSize(1);
    BPlusTreePage::SetMaxSize(max_size);

    array_[0].second = INVALID_PAGE_ID;
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "");
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
    BUSTUB_ASSERT(index >= 0 && index < GetSize(), "");
    return array_[index].second;
}

  // 二分查找第一个大于等于key的位置
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int{
  int begin = 1, end = GetSize();

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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const ValueType& left_child , const KeyType &key, const ValueType &right_child, const KeyComparator &comparator) -> int{

  if(GetSize() == 1){
    // 初次插入
    array_[0].second = left_child;
    array_[1] = std::make_pair(key, right_child);
  }else{
    int pos = KeyIndex(key, comparator);

    if(pos < GetSize() && comparator(array_[pos].first, key) == 0){
      // 当相等时，left_child 必须为空，否则，有问题
      BUSTUB_ASSERT(left_child == INVALID_PAGE_ID, "");
    }else if(right_child == INVALID_PAGE_ID){ // pos < GetSize() && 
      // 右孩子为空。
      //array_[pos].first = key;  // 更新索引范围？？
      BUSTUB_ASSERT(false, "");
      //return GetSize();
    }
    // 直接从后往前遍历
    for(int index = GetSize() - 1; index >= pos; index--){
      
      array_[index + 1] = array_[index];
    }

    //BUSTUB_ASSERT(index + 1 == pos, "");
    array_[pos] = std::make_pair(key, right_child);

    BUSTUB_ASSERT(array_[pos - 1].second == left_child, "");
  }

  // 大小自增1.
  IncreaseSize(1);

  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveTo(int begin, int end, B_PLUS_TREE_INTERNAL_PAGE_TYPE& dest){
  if(begin >= end){
    return;
  }
  dest.Append(array_ + begin, array_ + end);

  IncreaseSize(-(end - begin));
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Append(const MappingType* begin, const MappingType* end){
  int offset = 0;

  BUSTUB_ASSERT(GetSize() + (end - begin) <= GetMaxSize(), "");
  while(begin < end){
    array_[GetSize() + offset] = *begin;
    offset++;
    begin++;
  }

  IncreaseSize(offset);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value){
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "");
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index, const KeyComparator &comparator) -> int{
  //一定要存在
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "");

  for(int pos = index + 1; pos < GetSize(); pos++){
    array_[pos - 1] = array_[pos];
  }

  IncreaseSize(-1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Prepend(const MappingType& entry){

  for(int pos = GetSize() - 1; pos >= 0; pos--){
    array_[pos + 1] = array_[pos];
  }
  array_[0].second = entry.second;
  array_[1].first = entry.first;

  IncreaseSize(1);

  BUSTUB_ASSERT(GetSize() <= GetMaxSize(), "");
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Append(const MappingType& entry){
  array_[GetSize()] = entry;

  IncreaseSize(1);
  BUSTUB_ASSERT(GetSize() <= GetMaxSize(), "");
  return;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopBack() -> MappingType{
  BUSTUB_ASSERT(GetSize() > 0, "");
  MappingType res = array_[GetSize() - 1];

  IncreaseSize(-1);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopFront()-> MappingType{
  BUSTUB_ASSERT(GetSize() > 0, "");
  MappingType res;
  res.second = array_[0].second;
  res.first = array_[1].first;

  int pos = 1;
  for(; pos < GetSize(); pos++){
    array_[pos - 1] = array_[pos];
  }
  IncreaseSize(-1);

  return res;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
