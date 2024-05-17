#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int l=1,r=GetSize()-1,mid,ans=GetSize();//l=1是因为第一个key是无效的
  if(GetSize()==0) return INVALID_PAGE_ID;
  if(KM.CompareKeys(key,KeyAt(1))<0) return ValueAt(0);
  //如果key比第一个key还小，那么返回第一个value指针
  /*按照顺序存储 m个键和m+1个指针（这些指针记录的是⼦结点的 page_id ）。由于键和指针的数量不相等，因此我们需
要将第⼀个键设置为INVALID，也就是说，顺序查找时需要从第2个键开始查找
         | 0  | 1 | 2  |
    key  | /  | a | b  |
    value| x  | y | z  |
  */
  //用二分，找到最后一个<=key的位置
  while(l<=r){
    mid=(l+r)/2;
    if(KM.CompareKeys(KeyAt(mid),key)<=0){
      ans=mid;
      l=mid+1;
    }else r=mid-1;
  }
  return ValueAt(ans);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetSize(2);//size=value个数=key个数+1
  SetKeyAt(0,nullptr);
  SetKeyAt(1,new_key);
  SetValueAt(0, old_value);
  SetValueAt(1,new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int index=ValueIndex(old_value);
  //先右移，后插入到index+1位置
  PairCopy(PairPtrAt(index+2),PairPtrAt(index+1),GetSize()-index-1);
  SetKeyAt(index+1, new_key);
  SetValueAt(index+1, new_value);
  IncreaseSize(1);
  return 0;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size=GetSize();//把后size/2个元素移动到recipient
  recipient->CopyNFrom(PairPtrAt(size-size/2),size/2,buffer_pool_manager);
  IncreaseSize(-size/2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  int pos=GetSize();//从当前size的位置开始插入
  PairCopy(PairPtrAt(pos),src,size);
  IncreaseSize(size);
  //对于复制过来的子节点，要把他门的父节点更新成当前Page
  for(int i=0;i<size;++i){
    page_id_t page_id=ValueAt(pos+i);//id
    Page *page=buffer_pool_manager->FetchPage(page_id);//fetch->修改->unPin
    if(page==nullptr){
      LOG(INFO)<<"CopyNFrom fetch page failed";
      return;
    }
    InternalPage *internal_page=reinterpret_cast<InternalPage *>(page->GetData());
    internal_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page_id,true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  PairCopy(PairPtrAt(index),PairPtrAt(index+1),GetSize()-index-1);
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  if(GetSize()!=1){
    LOG(INFO)<<"RemoveAndReturnOnlyChild not only child";
    return INVALID_PAGE_ID;
  }
  page_id_t value=ValueAt(0);
  Remove(0);
  return value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key,ValueAt(0),buffer_pool_manager);
  recipient->CopyNFrom(PairPtrAt(1),GetSize()-1,buffer_pool_manager);
  buffer_pool_manager->DeletePage(GetPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
    recipient->CopyLastFrom(middle_key,ValueAt(0),buffer_pool_manager);//插到末尾
    Remove(0);//自己删除value(0)
                      
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int pos=GetSize();
  SetKeyAt(pos,key);
  SetValueAt(pos,value);
  IncreaseSize(1);
  Page *page=buffer_pool_manager->FetchPage(value);
  if(page==nullptr){
    LOG(INFO)<<"CopyLastFrom fetch page failed";
    return;
  }
  InternalPage *internal_page=reinterpret_cast<InternalPage *>(page->GetData());
  internal_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value,true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    recipient->CopyFirstFrom(ValueAt(GetSize()-1),buffer_pool_manager);//插到开头作为key,不需要value
    Remove(GetSize()-1);//删除最后一个value
    recipient->SetKeyAt(1,middle_key);//更新middle_key
    
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  InsertNodeAfter(INVALID_PAGE_ID, KeyAt(0), value);//????? 我不理解
  Page *page=buffer_pool_manager->FetchPage(value);
  if(page==nullptr){
    LOG(INFO)<<"CopyLastFrom fetch page failed";
    return;
  }
  InternalPage *internal_page=reinterpret_cast<InternalPage *>(page->GetData());
  internal_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value,true);
}