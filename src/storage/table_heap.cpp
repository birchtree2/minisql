#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  if(row.GetSerializedSize(this->schema_) >= PAGE_SIZE) return false;
  page_id_t current_page_id = first_page_id_;
  TablePage *current_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
  while(true){
    if(!current_page) return false;
    if(current_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)){
      buffer_pool_manager_->UnpinPage(current_page_id, true);
      return true;
    }
    page_id_t next_page_id = current_page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID){
      page_id_t new_page_id;
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
      if (!new_page) return false;
      new_page->Init(new_page_id, current_page_id, log_manager_, txn);
      new_page->SetNextPageId(INVALID_PAGE_ID); 
      current_page->SetNextPageId(new_page_id);
      buffer_pool_manager_->UnpinPage(current_page_id, true); 
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
  page_id_t current_page_id = rid.GetPageId();
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
bool TableHeap::GetTuple(Row *row, Txn *txn) { return false; }

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
TableIterator TableHeap::Begin(Txn *txn) { return TableIterator(nullptr, RowId(), nullptr); }

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(nullptr, RowId(), nullptr); }
