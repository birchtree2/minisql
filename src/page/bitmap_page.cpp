#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // LOG(INFO)<<page_allocated_<<" "<<GetMaxSupportedSize()<<"alloc";
  if(page_allocated_>=GetMaxSupportedSize()) return false; //已经分配的块超过索引上限
  // LOG(INFO)<<"next_free_page_="<<next_free_page_;
  page_offset=next_free_page_;
  uint32_t i=page_offset/8,j=page_offset%8;
  bytes[i]|=(1<<j);
  page_allocated_++;
  for(uint32_t flag=0,x=0;x<MAX_CHARS;x++){
    for(uint32_t y=0;y<8;y++){
      if(IsPageFreeLow(x,y)){
        next_free_page_=x*8+y;
        flag=1;
        break;
      }
    }
    if(flag) break;//直接跳出2层循环
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(page_offset>=GetMaxSupportedSize()) return false; //要free的块超过索引上限
  uint32_t i=page_offset/8,j=page_offset%8;
  // LOG(INFO)<<"dealloc: "<<page_offset<<" "<<IsPageFreeLow(i,j);
  if(IsPageFreeLow(i,j)) return false;//已经free掉了
  page_allocated_--;
  next_free_page_=page_offset;
  bytes[i]&=(~(1<<j));
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_allocated_>=GetMaxSupportedSize()) return false;
  return IsPageFreeLow(page_offset/8,page_offset%8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index]&(1<<bit_index))==0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;