#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**d
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  Page* pages = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage* index_roots_page = reinterpret_cast<IndexRootsPage*>(pages);
  index_roots_page->GetRootId(index_id, &root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  if (leaf_max_size == UNDEFINED_SIZE)
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));
  if (internal_max_size == UNDEFINED_SIZE)
    internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t));
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  buffer_pool_manager_->DeletePage(current_page_id);
  return ;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if (IsEmpty()) return false; // Empty tree
  // result.clear(); result不用清空,每查一次就把结果放到最后面
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key)->GetData());
  RowId rowid;
  bool is_found = leaf_page->Lookup(key, rowid, processor_);
  // Find the corresponding answer
  if (is_found) {
    result.push_back(rowid);
    // LOG(ERROR)<<rowid.GetPageId()<<" "<<rowid.GetSlotNum();
  }else{
    // LOG(ERROR)<<"not found";
  }
  
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return is_found;
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
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  // Check if the tree is empty
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  } else {
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) throw ("out of memory"); // Out of memory exception.
  root_page_id_ = new_page_id;
  LeafPage *root_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  root_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  root_page->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  // Find the right leaf page
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key)->GetData());
  // Check if the key already exists
  RowId rowid;
  if (leaf_page->Lookup(key, rowid, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false; // Key already exists
  }
  // Insert the key & value pair
  leaf_page->Insert(key, value, processor_);
  // Check if split is necessary
  if (leaf_page->GetSize() > leaf_max_size_) {
    // Split the leaf page
    // LOG(ERROR)<<"Split";
    LeafPage *new_leaf_page = Split(leaf_page, transaction);
    leaf_page->SetNextPageId(new_leaf_page->GetPageId());//把当前叶子和新叶子连起来
    /*把分裂后新叶子最左侧的插入到父亲   调用keyAt(0)
    如[1,2,3,4]->  [3]
               [1,2] [3,4]                                              
    */
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
    //记得unpin new leaf page
    buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(),true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  // New page.
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) throw ("out of memory"); // Out of memory exception.
  InternalPage *new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
  // Init.
  new_internal_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
  // Split data.
  node->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  // Unpin.
  // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  //不需要unpin, 因为这个node在InsertIntoParent的时候还要用到
  return new_internal_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  // New page.
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) throw ("out of memory"); // Out of memory exception.
  LeafPage *new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  // Init.
  new_leaf_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  // Split data.
  node->MoveHalfTo(new_leaf_page);
  // Unpin.
  // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  //不需要unpin, 因为这个node在InsertIntoParent的时候还要用到
  return new_leaf_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  // If old_node is root page, create a new root page
  if (old_node->IsRootPage()) {
    // Create a new root page
    page_id_t new_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_page == nullptr) throw ("out of memory"); // Out of memory exception.
    InternalPage *new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    // Init.
    new_root_page->Init(new_page_id, INVALID_PAGE_ID, old_node->GetKeySize(), internal_max_size_);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    root_page_id_ = new_page_id;
    buffer_pool_manager_->UnpinPage(new_page_id, true);
  } else {
    // Find the parent page
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
    int size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    // Keep iteration of split
    if (size >= internal_max_size_) {
      auto *new_internal_page = Split(parent_page, transaction);
      InsertIntoParent(parent_page, key, new_internal_page, transaction);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
  return ;
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
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) return ; // Empty tree
  // Find the right leaf page   (isLeftMost=false, 因为要根据key去查)
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false)->GetData());
  // Delete the key & value pair
  int del_index=leaf_page->KeyIndex(key, processor_);
  int new_size = leaf_page->RemoveAndDeleteRecord(key, processor_);
  // Check if merge is necessary
  if (new_size < leaf_page->GetMinSize()) {
    // Merge the leaf page
    if (CoalesceOrRedistribute<LeafPage>(leaf_page, transaction)){
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);//先unpin
      buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    }
  } else if (!del_index) { // The deleted key is the first key, need pop up to delete.
    GenericKey *new_key = leaf_page->KeyAt(0); // New key to replace the old one.
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(
      buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId())->GetData()//GetData()
      );
    page_id_t pageid = leaf_page->GetPageId();
    // Pop up to delete
    while (!parent_page->IsRootPage() && parent_page->ValueIndex(pageid) == 0) {
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      parent_page = reinterpret_cast<InternalPage *>(
        buffer_pool_manager_->FetchPage(parent_page->GetParentPageId())->GetData()
      );
      pageid = parent_page->GetPageId();
    }
    int tmp_index = parent_page->ValueIndex(pageid);
    if (tmp_index != 0 && processor_.CompareKeys(parent_page->KeyAt(tmp_index), new_key) != 0) {
      parent_page->SetKeyAt(tmp_index, new_key);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if (node->IsRootPage()) return AdjustRoot(node);//根节点  
  Page* parent_page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent=reinterpret_cast<InternalPage*>(parent_page->GetData());
  //获取兄弟节点
  int index=parent->ValueIndex(node->GetPageId());
  int sib_index=index==0?index+1:index-1;
  //如果node是第一个节点，就取右边的兄弟。否则取左边的
  Page* sib_page=buffer_pool_manager_->FetchPage(parent->ValueAt(sib_index));
  N* sib=reinterpret_cast<N*>(sib_page->GetData());//node可能是叶子，也可能不是叶子，所以用N*
  //index 是node在parent数组的位置  而id代表不同节点的编号
  bool del_node=false;
  if(node->GetSize()+sib->GetSize()>node->GetMaxSize()){
    //If sibling's size + input page's size > page's max size, then redistribute.
    Redistribute(sib,node,index);
    del_node=false;//不删除
  }else{
    if(index==0){//把sibling移到node,node不删除
      Coalesce(node,sib,parent,1,transaction);
      del_node=false;
    }else{//把node移到sibling,并删除node
      Coalesce(sib,node,parent,index,transaction);
      del_node=true;
    }
  }
  //sibling和parent都被修改了,所以is_dirty=true
  buffer_pool_manager_->UnpinPage(sib->GetPageId(),true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
  return del_node;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  node->MoveAllTo(neighbor_node);
  parent->Remove(index);
  if(parent->GetSize()>=parent->GetMinSize())return false;
  return CoalesceOrRedistribute<InternalPage>(parent,transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  node->MoveAllTo(neighbor_node,parent->KeyAt(index),buffer_pool_manager_);
  parent->Remove(index);
  if(parent->GetSize()>=parent->GetMinSize())return false;
  return CoalesceOrRedistribute<InternalPage>(parent,transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  page_id_t parent_id=node->GetParentPageId();
  //根据id取出对应的page
  Page* ptr=buffer_pool_manager_->FetchPage(parent_id);
  InternalPage* parent=reinterpret_cast<InternalPage*>(ptr->GetData());
   //更新后，修改parent的key,注意两种情况是不一样的
  if(index!=0){
    neighbor_node->MoveLastToFrontOf(node);
    page_id_t mid_index=parent->ValueIndex(node->GetPageId());
    parent->SetKeyAt(mid_index,node->KeyAt(0));//自己在右边，把自己的第一个节点给父母
  }else{
    neighbor_node->MoveFirstToEndOf(node);
    page_id_t mid_index=parent->ValueIndex(neighbor_node->GetPageId());
    parent->SetKeyAt(mid_index,neighbor_node->KeyAt(0));//把右边兄弟的第一个节点给父母
    
  }
 
  
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  page_id_t parent_id=node->GetParentPageId();
  //根据id取出对应的page
  Page* ptr=buffer_pool_manager_->FetchPage(parent_id);
  InternalPage* parent=reinterpret_cast<InternalPage*>(ptr->GetData());
  int id=0;
  GenericKey * newKey=0;
  if(index==0){
    id=parent->ValueIndex(neighbor_node->GetPageId());
    newKey=neighbor_node->KeyAt(1);
    neighbor_node->MoveFirstToEndOf(node,parent->KeyAt(id),buffer_pool_manager_);
  }else{
    id=parent->ValueIndex(node->GetPageId());
    newKey=neighbor_node->KeyAt(neighbor_node->GetSize()-1);
    neighbor_node->MoveLastToFrontOf(node,parent->KeyAt(id),buffer_pool_manager_);
  }
  parent->SetKeyAt(id,newKey);
  buffer_pool_manager_->UnpinPage(parent_id,true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  page_id_t old_root_id=old_root_node->GetPageId();
  if(!old_root_node->IsLeafPage() && old_root_node->GetSize()==1){
    //case 1
    InternalPage* old_root=reinterpret_cast<InternalPage*>(old_root_node);
    root_page_id_=old_root->RemoveAndReturnOnlyChild();//返回唯一的child作为新根
    UpdateRootPageId(false);//更新Index_roots_page
    //获取新根
    Page* new_root_page=buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage*new_root=reinterpret_cast<BPlusTreePage*>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_,true);//因为新根改了parent
    buffer_pool_manager_->UnpinPage(old_root_id,false);
    buffer_pool_manager_->DeletePage(old_root_id);
    return true;
  }else if(old_root_node->GetSize()==0){
    //case 2
    root_page_id_=INVALID_PAGE_ID;
    UpdateRootPageId(false);
    buffer_pool_manager_->UnpinPage(old_root_id,false);
    buffer_pool_manager_->DeletePage(old_root_id);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  Page *page = FindLeafPage(nullptr, INVALID_PAGE_ID, true);
  //flag=true,找最左边的节点,不需要key和pageid
  if (page == nullptr) return IndexIterator();
  int page_id=page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_,0);//最左边,index=0
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
   Page *page = FindLeafPage(key,INVALID_PAGE_ID, false);
  //flag=false, 根据key去找对应的叶子
  LeafPage* leaf=reinterpret_cast<LeafPage*>(page);
  if (page == nullptr) return IndexIterator();
  int page_id=page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  //根据key, 在叶子里找到对应的index
  return IndexIterator(page_id, buffer_pool_manager_,leaf->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  //这里的page_id似乎没有用?
  if(IsEmpty()) return nullptr;
  page_id=root_page_id_;
  while(1){
    Page* page=buffer_pool_manager_->FetchPage(page_id);
    BPlusTreePage* node=reinterpret_cast<BPlusTreePage*>(page->GetData());
    if(node->IsLeafPage()) return page;//到叶子了
    InternalPage* internal=reinterpret_cast<InternalPage*>(node);
    //如果是一直往左，就选0位置的page_id. 否则就根据key去查找
    if(leftMost) page_id=internal->ValueAt(0);
    else page_id=internal->Lookup(key,processor_);
    buffer_pool_manager_->UnpinPage(internal->GetPageId(),false);
  }
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
//insert_record看起来是int,实际是bool
//index_roots_page 记录了数据库里所有的索引(B+树)的root_page_id。 
//它本身也是一个page,id为INDEX_ROOTS_PAGE_ID
//如果当前B+树的根变了，就要修改index_roots_page
  Page* page=buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage* index_roots_page=reinterpret_cast<IndexRootsPage*>(page->GetData());
  if(insert_record){//找到当前B+树的id,插入
    index_roots_page->Insert(index_id_,root_page_id_);
  }else{
    index_roots_page->Update(index_id_,root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}