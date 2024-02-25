#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return GetRootPageId() == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
    //   Context ctx;
    //   (void)ctx;
    Context context;

    FindPath(key, context, false);  // 读的方式寻找路径。

    if(context.root_page_id_ == INVALID_PAGE_ID){
      // 树为空;
      return false;
    }

    const BPlusTreePage *page = context.read_set_.back().As<BPlusTreePage>();

    BUSTUB_ASSERT(page->IsLeafPage(), "");

    const LeafPage* leaf_page = reinterpret_cast<const LeafPage *>(page);
    int target = leaf_page->KeyIndex(key, comparator_);
    if(target < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(target), key) == 0){
        result->push_back(leaf_page->ValueAt(target));
        return true;
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindPath(const KeyType &key, Context& ctx, bool write, Transaction *txn){
    //   ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
    // auto head_page = guard.As<BPlusTreeHeaderPage>();
    // return head_page->root_page_id_;


    // page_id_t root_page_id = GetRootPageId();

    ReadPageGuard header_page_guard;    // 默认空
    const BPlusTreeHeaderPage* header_page = nullptr;

    page_id_t root_page_id = INVALID_PAGE_ID;

    if(write){
        // 写的方式，加全局写锁，而不加读锁
        // header_page_guard
        ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
        // header_page
        header_page = ctx.header_page_.value().As<BPlusTreeHeaderPage>();


    }else{
        // 读的方式,加函数内部读锁
        header_page_guard = bpm_->FetchPageRead(header_page_id_);

        header_page = header_page_guard.As<BPlusTreeHeaderPage>();
    }

    root_page_id = header_page->root_page_id_;

    ctx.write_set_.clear();
    ctx.read_set_.clear();
    ctx.root_page_id_ = root_page_id;

    if(root_page_id == INVALID_PAGE_ID){
        // 空B+树
        return ;
    }
    const BPlusTreePage* page = nullptr;

    if(write){
      ctx.write_set_.push_back(bpm_->FetchPageWrite(root_page_id));
      page = ctx.write_set_.back().As<BPlusTreePage>();
    }else{
      ctx.read_set_.push_back(bpm_->FetchPageRead(root_page_id));
      page = ctx.read_set_.back().As<BPlusTreePage>();
    }

    while(!page->IsLeafPage()){
        // 重解释成internal_page
        const InternalPage* internal_page = reinterpret_cast<const InternalPage *>(page);
        int index = internal_page->KeyIndex(key, comparator_);
        page_id_t child_page_id = INVALID_PAGE_ID;

        BUSTUB_ASSERT(index > 0 && index <= internal_page->GetSize(), "");
        if(index < internal_page->GetSize() && comparator_(internal_page->KeyAt(index), key) == 0){
            child_page_id = internal_page->ValueAt(index);
        }else{
            child_page_id = internal_page->ValueAt(index - 1);
        }
        BUSTUB_ASSERT(child_page_id != INVALID_PAGE_ID, "");

        if(write){
          ctx.write_set_.push_back(bpm_->FetchPageWrite(child_page_id));
          page = ctx.write_set_.back().As<BPlusTreePage>();
        }else{
          ctx.read_set_.push_back(bpm_->FetchPageRead(child_page_id));
          page = ctx.read_set_.back().As<BPlusTreePage>();
        }
    }

    if(write){
      BUSTUB_ASSERT(ctx.write_set_.empty() == false && ctx.read_set_.empty() == true && ctx.header_page_.has_value() == true, "");
    }else{
      BUSTUB_ASSERT(ctx.read_set_.empty() == false && ctx.write_set_.empty() == true && ctx.header_page_.has_value() == false, "");
    }

    // const LeafPage* leaf_page = reinterpret_cast<const LeafPage *>(page);
    // int target = leaf_page->KeyIndex(key, comparator_);
    // if(target < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(target), key) == 0){
    //     result->push_back(leaf_page->ValueAt(target));
    //     return true;
    // }
    return;
}



/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
    // Declaration of context instance.
    // Context ctx;
    // (void)ctx;

    Context context;

    FindPath(key, context, true);  // 写的方式寻找路径。

    if(context.root_page_id_ == INVALID_PAGE_ID){
        // 空B+树
        page_id_t new_page_id = INVALID_PAGE_ID;
        BasicPageGuard new_page_guard = bpm_->NewPageGuarded(&new_page_id);

        if(new_page_id == INVALID_PAGE_ID){
            // out of memory
            throw "out of memory";
        }
        LeafPage* new_page = new_page_guard.AsMut<LeafPage>();

        new_page->Init(leaf_max_size_);
        new_page->Insert(key, value, comparator_);

        // SetRootPageId(new_page_id);
        context.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_page_id;
        return true;
    }
    // 非空

    LeafPage* leaf_page = context.write_set_.back().AsMut<LeafPage>();
    // 判断是否存在
    int target = leaf_page->KeyIndex(key, comparator_);
    if(target < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(target), key) == 0){
        // 多线程情况下，可能会走到这里。
        // 冗余的操作。
      // 插入一个已经存在的key
      return false;
    } // else 不存在

    // leaf_page插入后判断大小
    if(leaf_page->Insert(key, value, comparator_) >= leaf_page->GetMaxSize()){
      // 满了，将进行split
      page_id_t new_page_id = INVALID_PAGE_ID;
      BasicPageGuard new_page_guard = bpm_->NewPageGuarded(&new_page_id);

      if(new_page_id == INVALID_PAGE_ID){
          // out of memory
          throw "out of memory";
      }
      LeafPage* new_page = new_page_guard.AsMut<LeafPage>();
      new_page->Init(leaf_max_size_);

      leaf_page->MoveTo((leaf_page->GetSize() / 2), leaf_page->GetSize(), *new_page);

      new_page->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->SetNextPageId(new_page_id);

      InsertInParent(context.write_set_.back().PageId(), new_page->KeyAt(0), new_page_guard.PageId(), context);
    } // else 空间足够，不需要分裂。

    return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(page_id_t page_id){
    WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
    auto head_page = guard.AsMut<BPlusTreeHeaderPage>();
    head_page->root_page_id_ = page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(page_id_t left_child, KeyType key, page_id_t right_child, Context& ctx){
  ctx.write_set_.pop_back();  // 移除leaf_page_guard----解除叶子写锁

  if(ctx.write_set_.empty()){
    // 叶子就是根
    BUSTUB_ASSERT(ctx.IsRootPage(left_child) == true, "");
    ChangeRoot(left_child, key, right_child, ctx);
    return ;  // 插入完毕
  }

  while(!ctx.write_set_.empty()){
    WritePageGuard page_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    InternalPage* internal_page = page_guard.AsMut<InternalPage>();
    if(internal_page->Insert(left_child, key, right_child, comparator_) <= internal_page->GetMaxSize()){ //是否需要分裂？
      // 无需分裂

      return;
    }else{
      // 需要分裂
      // 满了，将进行split
      int mid = internal_page->GetSize() / 2;

      if(mid + 1 >= internal_page->GetSize()){
        // right_child为空
        key = internal_page->KeyAt(mid);
        left_child = page_guard.PageId();
        right_child = INVALID_PAGE_ID;
        BUSTUB_ASSERT(false, "");
        continue;
      }
      page_id_t new_page_id = INVALID_PAGE_ID;
      BasicPageGuard new_page_guard = bpm_->NewPageGuarded(&new_page_id);
      key = internal_page->KeyAt(mid);
      left_child = page_guard.PageId();
      right_child = new_page_id;
      if(new_page_id == INVALID_PAGE_ID){
          // out of memory
          throw "out of memory";
      }
      InternalPage* new_page = new_page_guard.AsMut<InternalPage>();

      new_page->Init(internal_max_size_);

      internal_page->MoveTo(mid + 1, internal_page->GetSize(), *new_page);

      //将上移节点的指针赋值给new_page的0号指针。
      new_page->SetValueAt(0, internal_page->ValueAt(mid));
      internal_page->IncreaseSize(-1);
    }
  }

  // 最后一次换根。
  ChangeRoot(left_child, key, right_child, ctx);
  return;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ChangeRoot(page_id_t left_child, KeyType key, page_id_t right_child, Context &ctx){
    page_id_t new_page_id = INVALID_PAGE_ID;
    BasicPageGuard new_page_guard = bpm_->NewPageGuarded(&new_page_id);

    if(new_page_id == INVALID_PAGE_ID){
        // out of memory
        throw "out of memory";
    }
    InternalPage* new_page = new_page_guard.AsMut<InternalPage>();

    new_page->Init(internal_max_size_);
    new_page->Insert(left_child, key, right_child, comparator_);

    //SetRootPageId(new_page_id);
    ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_page_id;
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  FindPath(key, ctx, true);

  if(ctx.root_page_id_ == INVALID_PAGE_ID){
    // 空B+树
    return;
  }

  // 对叶子进行删除

  //取叶
  WritePageGuard page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  LeafPage* leaf_page = page_guard.AsMut<LeafPage>();

  // 先删除
  if(leaf_page->Remove(key, comparator_) < 0){  // rt == -1
    // 冗余操作。
    return;
  }


  // 如果根就是叶，简单处理即可
  if(ctx.IsRootPage(page_guard.PageId())){
    if(leaf_page->GetSize() == 0){
      // 空B+树

      page_guard.Drop();
      // 释放该页
      bpm_->DeletePage(ctx.root_page_id_);

      // root_page_id置为无效
      // SetRootPageId(INVALID_PAGE_ID);
      ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }
  if(leaf_page->GetSize() < leaf_page->GetMinSize()){
    // 需要拆借或合并
    BUSTUB_ASSERT(ctx.write_set_.empty() == false, "");
    InternalPage* parent_page = ctx.write_set_.back().AsMut<InternalPage>();
    int index = parent_page->KeyIndex(key, comparator_);

    if(index < parent_page->GetSize() && comparator_(parent_page->KeyAt(index), key) == 0){
      index++;
    }
    // index - 1 . second -> page_guard.PageId()
    BUSTUB_ASSERT(index - 1 >= 0 && parent_page->ValueAt(index - 1) == page_guard.PageId(), "");

    // index - 2  . second -> left sibling
    // 尝试左
    if(index - 2 >= 0){
      WritePageGuard left_sibling_page_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 2));
      LeafPage* left_sibling_page = left_sibling_page_guard.AsMut<LeafPage>();

      // 如果可以借
      if(left_sibling_page->GetSize() > left_sibling_page->GetMinSize()){
        // 保证兄弟合理的情况下，至少可以借一个
        MappingType entry = left_sibling_page->PopBack();

        // 更新parent的索引key
        parent_page->SetKeyAt(index - 1, entry.first);
        leaf_page->Prepend(entry);
      }else{
        // 应该可以合并
        BUSTUB_ASSERT(left_sibling_page->GetSize() + leaf_page->GetSize() < leaf_page->GetMaxSize(), "");

        // 大的往小的挪
        leaf_page->MoveTo(0, leaf_page->GetSize(), *left_sibling_page);
        left_sibling_page->SetNextPageId(leaf_page->GetNextPageId());

        // 移除父类的 index - 1 entry
        RemoveInParent(index - 1, ctx);

        {
          page_id_t page_id = page_guard.PageId();
          page_guard.Drop();

          // 将page释放掉
          bpm_->DeletePage(page_id);
        }
      }
    }else if(index < parent_page->GetSize()){
      // index . second -> right sibling
      // 尝试右

      WritePageGuard right_sibling_page_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index));
      LeafPage* right_sibling_page = right_sibling_page_guard.AsMut<LeafPage>();

      // 如果可以借
      if(right_sibling_page->GetSize() > right_sibling_page->GetMinSize()){
        // 保证兄弟合理的情况下，至少可以借一个
        MappingType entry = right_sibling_page->PopFront();

        // 更新parent的索引key
        parent_page->SetKeyAt(index, right_sibling_page->KeyAt(0));
        leaf_page->Append(entry);
      }else{
        // 应该可以合并
        BUSTUB_ASSERT(right_sibling_page->GetSize() + leaf_page->GetSize() < leaf_page->GetMaxSize(), "");

        // 大的往小的挪
        right_sibling_page->MoveTo(0, right_sibling_page->GetSize(), *leaf_page);
        leaf_page->SetNextPageId(right_sibling_page->GetNextPageId());
        
        // 移除父类的 index entry
        RemoveInParent(index, ctx);

        {
          page_id_t page_id = right_sibling_page_guard.PageId();
          right_sibling_page_guard.Drop();

          // 将page释放掉
          bpm_->DeletePage(page_id);
        }

      }
    }
  }// else remove完成
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInParent(int idx, Context& ctx){
  WritePageGuard page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  InternalPage* internal_page = page_guard.AsMut<InternalPage>();

  while(!ctx.write_set_.empty()){
    InternalPage* parent_page = ctx.write_set_.back().AsMut<InternalPage>();
    KeyType idx_key = internal_page->KeyAt(idx);

    internal_page->Remove(idx, comparator_);
    if(internal_page->GetSize() < internal_page->GetMinSize()){
      // 需要拆借或合并
      // 搜索cur page在父节点的位置
      int index = parent_page->KeyIndex(idx_key, comparator_);

      if(index < parent_page->GetSize() && comparator_(parent_page->KeyAt(index), idx_key) == 0){
        index++;
      }

      BUSTUB_ASSERT(index - 1 >= 0 && parent_page->ValueAt(index - 1) == page_guard.PageId(), "");

      // index - 2  . second -> left sibling
      // 尝试左
      if(index - 2 >= 0){
        WritePageGuard left_sibling_page_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 2));
        InternalPage* left_sibling_page = left_sibling_page_guard.AsMut<InternalPage>();

        // 如果可以借
        if(left_sibling_page->GetSize() > left_sibling_page->GetMinSize()){
          // 保证兄弟合理的情况下，至少可以借一个
          auto entry = left_sibling_page->PopBack();
          KeyType temp = entry.first;

          entry.first = parent_page->KeyAt(index - 1);
          // 更新parent的索引key
          parent_page->SetKeyAt(index - 1, temp);

          internal_page->Prepend(entry);
          // 调整完毕
          return;
        }else{
          // 应该可以合并
          BUSTUB_ASSERT(left_sibling_page->GetSize() + internal_page->GetSize() <= internal_page->GetMaxSize(), "");

          // 大的往小的挪 // parent的index -1处的key下沉
          int size = left_sibling_page->GetSize();
          internal_page->MoveTo(0, internal_page->GetSize(), *left_sibling_page);
          left_sibling_page->SetKeyAt(size, parent_page->KeyAt(index - 1));

          // 移除父类的 index - 1 entry
          //RemoveInParent(index - 1, ctx);
          idx = index - 1;

          {
            page_id_t page_id = page_guard.PageId();
            page_guard.Drop();

            // 将page释放掉
            bpm_->DeletePage(page_id);
          }
        }
      }else if(index < parent_page->GetSize()){
        // index . second -> right sibling
        // 尝试右

        WritePageGuard right_sibling_page_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index));
        InternalPage* right_sibling_page = right_sibling_page_guard.AsMut<InternalPage>();

        // 如果可以借
        if(right_sibling_page->GetSize() > right_sibling_page->GetMinSize()){
          // 保证兄弟合理的情况下，至少可以借一个
          auto entry = right_sibling_page->PopFront();
          KeyType temp = entry.first;

          entry.first = parent_page->KeyAt(index);
          // 更新parent的索引key
          parent_page->SetKeyAt(index, temp);
          internal_page->Append(entry);

          // 调整完毕
          return;
        }else{  // to do
          // 应该可以合并
          BUSTUB_ASSERT(right_sibling_page->GetSize() + internal_page->GetSize() <= internal_page->GetMaxSize(), "");

          int size = internal_page->GetSize();
          // 大的往小的挪
          right_sibling_page->MoveTo(0, right_sibling_page->GetSize(), *internal_page);
          internal_page->SetKeyAt(size, parent_page->KeyAt(index));

          // 移除父类的 index entry
          idx = index;

          {
            page_id_t page_id = right_sibling_page_guard.PageId();
            right_sibling_page_guard.Drop();

            // 将page释放掉
            bpm_->DeletePage(page_id);
          }

        }
      }else{
        BUSTUB_ASSERT(false, "never reach");
      }

      page_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      internal_page = page_guard.AsMut<InternalPage>();

      BUSTUB_ASSERT(internal_page == parent_page, "");

    }else{
      // 当前内部节点是符合要求的
      // 调整完毕

      return;
    }
  }

  // 调整根节点
  internal_page->Remove(idx, comparator_);

  if(internal_page->GetSize() == 1){
    page_id_t new_root_page_id = internal_page->ValueAt(0);  // 仅剩的一个孩子
    page_guard.Drop();
    // 释放该页
    bpm_->DeletePage(ctx.root_page_id_);

    // 更新root_page_id
    // SetRootPageId(new_root_page_id);
    ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;
  }

}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;
  page_id_t root_page_id = GetRootPageId();

  ctx.write_set_.clear();
  ctx.read_set_.clear();
  ctx.root_page_id_ = root_page_id;

  if(root_page_id == INVALID_PAGE_ID){
      // 空B+树
      return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, nullptr);
  }
  const BPlusTreePage* page = nullptr;

  ctx.read_set_.push_back(bpm_->FetchPageRead(root_page_id));
  page = ctx.read_set_.back().As<BPlusTreePage>();


  while(!page->IsLeafPage()){
      // 重解释成internal_page
      const InternalPage* internal_page = reinterpret_cast<const InternalPage *>(page);
      page_id_t child_page_id = INVALID_PAGE_ID;

      BUSTUB_ASSERT(internal_page->GetSize() >= 2, "");

      child_page_id = internal_page->ValueAt(0);

      BUSTUB_ASSERT(child_page_id != INVALID_PAGE_ID, "");


      ctx.read_set_.push_back(bpm_->FetchPageRead(child_page_id));
      page = ctx.read_set_.back().As<BPlusTreePage>();
  }

  BUSTUB_ASSERT(ctx.read_set_.empty() == false && ctx.write_set_.empty() == true, "");
  page_id_t page_id = ctx.read_set_.back().PageId();
  ctx.read_set_.clear();
  return INDEXITERATOR_TYPE(page_id, 0, bpm_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;

  FindPath(key, ctx, false);  // 读的方式寻找路径。

  if(ctx.root_page_id_ == INVALID_PAGE_ID){
    // 树为空;
      return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, nullptr);
  }

  const BPlusTreePage *page = ctx.read_set_.back().As<BPlusTreePage>();

  BUSTUB_ASSERT(page->IsLeafPage(), "");

  const LeafPage* leaf_page = reinterpret_cast<const LeafPage *>(page);
  int target = leaf_page->KeyIndex(key, comparator_);
  page_id_t page_id = ctx.read_set_.back().PageId();
  ctx.read_set_.clear();
  return INDEXITERATOR_TYPE(page_id, target, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, nullptr); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
    ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
    auto head_page = guard.As<BPlusTreeHeaderPage>();
    return head_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
    int64_t key;
    std::ifstream input(file_name);
    while (input) {
        input >> key;

        KeyType index_key;
        index_key.SetFromInteger(key);
        RID rid(key);
        Insert(index_key, rid, txn);
    }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
    int64_t key;
    std::ifstream input(file_name);
    while (input) {
        input >> key;
        KeyType index_key;
        index_key.SetFromInteger(key);
        Remove(index_key, txn);
    }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
