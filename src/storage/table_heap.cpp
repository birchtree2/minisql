#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  if(row.GetSerializedSize(this->schema_) >= PAGE_SIZE) return false;
  page_id_t current_page_id = first_page_id_;
  TablePage *current_page;
  // LOG(ERROR)<<current_page<<" "<<current_page_id;
  while(true){
    current_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if(!current_page) return false;
    if(current_page->GetTupleCount()==0&&current_page->GetNextPageId()==0){
       //说明这是一个从buffer pool直接出来，没经过初始化的page
      current_page->Init(current_page_id, INVALID_PAGE_ID, log_manager_, txn);//FIX
    }
    if(current_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)){
      // LOG(ERROR)<<"ok3";
      buffer_pool_manager_->UnpinPage(current_page_id, true);
      return true;
    }
    // LOG(ERROR)<<"next page";
    page_id_t next_page_id = current_page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID){
      // 新建一页
      page_id_t new_page_id;
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
      if(new_page_id==current_page_id){
        LOG(ERROR)<<"new page allocate error!";
      }
      if (!new_page) return false;
      new_page->Init(new_page_id, current_page_id, log_manager_, txn);
      new_page->SetNextPageId(INVALID_PAGE_ID); 
      current_page->SetNextPageId(new_page_id);
      buffer_pool_manager_->UnpinPage(current_page_id, true); 
      // LOG(ERROR)<<new_page->GetFreeSpaceRemaining();
      current_page_id = new_page_id; 
      continue;
    }
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    current_page_id = next_page_id;
  }
  return false; 
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Txn *txn) { 
  if(row.GetSerializedSize(this->schema_) >= PAGE_SIZE) {
    LOG(ERROR)<<"UpdateTuple: tuple size is too large";
    return false;
  }
  page_id_t current_page_id = rid.GetPageId(); // 找到元组所在页的Id
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
  if(page == nullptr) {
    LOG(ERROR)<<"UpdateTuple: page is nullptr";
    return false;
  }
  page->WLatch(); // 写锁
  Row *old_row = new Row(rid);
  if(page->UpdateTuple(row, old_row, schema_, txn, lock_manager_, log_manager_)){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    delete old_row;
    page->WUnlatch();
    return true;
  }
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  delete old_row;
  page->WUnlatch(); // 释放
  return false; 
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  page_id_t current_page_id = rid.GetPageId();
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
  if(page == nullptr) {
    LOG(ERROR)<<"ApplyDelete: page is nullptr";
    return;
  }
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
  RowId rid = row->GetRowId();
  page_id_t current_page_id = rid.GetPageId();
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
  if(page == nullptr) {
    LOG(ERROR)<<"GetTuple: page is nullptr";
    return false;
  }
  page->RLatch(); // 读锁
  if(page->GetTuple(row, schema_, txn, lock_manager_)){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false); //
    page->RUnlatch();
    return true;
  }
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false); // 只读没改动
  page->RUnlatch(); // 释放
  return false; 
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { 
  page_id_t begin_page_id = first_page_id_;
  RowId begin_page_rid_;
  while(begin_page_id != INVALID_PAGE_ID){ // 遍历所有页
    TablePage *begin_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(begin_page_id));
    if(begin_page == nullptr) {
      // begin_page_id = begin_page->GetNextPageId();
      begin_page_id = begin_page->GetNextPageId();
      continue;
    }
    if(begin_page->GetFirstTupleRid(&begin_page_rid_)){
      buffer_pool_manager_->UnpinPage(begin_page_id, false);
      return TableIterator(this, begin_page_rid_, txn);
    } // 否则当前页面没有元组
    buffer_pool_manager_->UnpinPage(begin_page_id, false);
    begin_page_id = begin_page->GetNextPageId();
  }
  LOG(ERROR)<<"TableHeap::Begin: no tuple in table";
  return TableIterator(this, RowId(), txn); 
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { 
  if(first_page_id_ == INVALID_PAGE_ID) { // 空表，返回空迭代器
    return TableIterator(nullptr, RowId(INVALID_PAGE_ID, 0), nullptr);
  }
  page_id_t end_page_id = first_page_id_;
  TablePage *last_page = nullptr;
  while (end_page_id != INVALID_PAGE_ID) {
    last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(end_page_id));
    if (last_page == nullptr) {
      // 如果获取页面失败，这通常是个严重错误一般不会出现，但返回空迭代器
      return TableIterator(nullptr, RowId(INVALID_PAGE_ID, 0), nullptr);
    }
    buffer_pool_manager_->UnpinPage(end_page_id, false);
    end_page_id = last_page->GetNextPageId();
  }
  return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr);
  // return TableIterator(nullptr, RowId(), nullptr); 
}
