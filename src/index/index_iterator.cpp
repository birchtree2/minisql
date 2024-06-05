#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return std::make_pair(page->KeyAt(item_index), page->ValueAt(item_index));
}

IndexIterator &IndexIterator::operator++() {
  if (item_index + 1 < page->GetSize()) {
    item_index++;
  } else { // 溢出到下一页
    int next_page_id = page->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) { // 是否已经到达最后一页
      buffer_pool_manager->UnpinPage(page->GetPageId(), false);
      page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(next_page_id)->GetData());
    } else {
      page = nullptr; // 置为 nullptr
    }
    item_index = 0;
    current_page_id = next_page_id;
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}