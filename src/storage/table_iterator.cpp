#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
  : table_heap_(table_heap), row_(nullptr), txn_(txn) {
  this->row_ = new Row(rid);
  if(!table_heap->GetTuple(row_, txn)){
    delete row_;
    row_ = nullptr;
  }
}

TableIterator::TableIterator(const TableIterator &other) {
  this->row_ = new Row(*other.row_);
  table_heap_ = other.table_heap_;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (this->row_->GetRowId() == itr.row_->GetRowId() && this->table_heap_ == itr.table_heap_);
  // return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if(this->row_->GetRowId() == itr.row_->GetRowId() && this->table_heap_ == itr.table_heap_){
    return false;
  }
  return true;
}

const Row &TableIterator::operator*() {
  ASSERT(false, "Not implemented yet.");
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
  // return nullptr;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  // ASSERT(false, "Not implemented yet.");
  if(this != &itr){
    delete row_;
    row_ = new Row(*itr.row_);
    table_heap_ = itr.table_heap_;
    txn_ = itr.txn_;
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (table_heap_ == nullptr || row_ == nullptr) {
    return *this;
  }
  RowId next_rid;
  bool hasNext = false;
  do {
    TablePage *page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_->GetRowId().GetPageId()));
    if (page == nullptr) {
      hasNext = false;
      break;
    }
    RowId currentRid = row_->GetRowId();
    if (page->GetNextTupleRid(currentRid, &next_rid)) {
      hasNext = true;
      break;
    }
    // 当前页没有更多元组
    page_id_t nextPageId = page->GetNextPageId();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    if (nextPageId == INVALID_PAGE_ID) {
      hasNext = false;
      break;
    }
    row_->SetRowId(RowId(nextPageId, 0)); // 下一页
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(nextPageId));
    if (page->GetFirstTupleRid(&next_rid)) {
      hasNext = true;
    }
  } while (!hasNext);
  if (!hasNext) {
    row_->SetRowId(INVALID_ROWID);
    return *this; // 到达表尾部
  }
  row_->SetRowId(next_rid);
  if (*this != table_heap_->End()) {
    table_heap_->GetTuple(row_, txn_);
  }
  table_heap_->buffer_pool_manager_->UnpinPage(row_->GetRowId().GetPageId(), false);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator tmp(*this);
  ++(*this);
  return TableIterator(tmp);
  // return TableIterator(nullptr, RowId(), nullptr); 
}
