/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, size_t offset, BufferPoolManager *bpm):
    leaf_page_id_(page_id),
    offset_(offset),
    leaf_page_guard_(),
    leaf_page_(nullptr),
    bpm_(bpm){
    
    // BUSTUB_ASSERT(bpm_ != nullptr, "");
    if(leaf_page_id_ != INVALID_PAGE_ID){
        leaf_page_guard_ = bpm_->FetchPageRead(leaf_page_id_);
        leaf_page_ = leaf_page_guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    }

}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
    if(leaf_page_id_ != INVALID_PAGE_ID){
        leaf_page_guard_.Drop();
        leaf_page_ = nullptr;
        bpm_ = nullptr;
        offset_ = 0;
    }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    return leaf_page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    BUSTUB_ASSERT(leaf_page_id_ != INVALID_PAGE_ID, "iterator invalid!");

    if((int)offset_ >= leaf_page_->GetSize()){
        offset_ = 0;
        leaf_page_id_ = leaf_page_->GetNextPageId();
        if(leaf_page_id_ != INVALID_PAGE_ID){
            leaf_page_guard_ = bpm_->FetchPageRead(leaf_page_id_);
            leaf_page_ = leaf_page_guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        }else{
            leaf_page_guard_.Drop();
            leaf_page_ = nullptr;
            bpm_ = nullptr;
        }
    }
    return leaf_page_->EntryAt(offset_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    BUSTUB_ASSERT(leaf_page_id_ != INVALID_PAGE_ID, "iterator invalid!");
    ++offset_;
    if((int)offset_ >= leaf_page_->GetSize()){
        offset_ = 0;
        leaf_page_id_ = leaf_page_->GetNextPageId();
        if(leaf_page_id_ != INVALID_PAGE_ID){
            leaf_page_guard_ = bpm_->FetchPageRead(leaf_page_id_);
            leaf_page_ = leaf_page_guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
        }else{
            leaf_page_guard_.Drop();
            leaf_page_ = nullptr;
            bpm_ = nullptr;
        }
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
