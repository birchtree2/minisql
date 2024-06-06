#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if(page_id==INVALID_PAGE_ID) return nullptr;
  auto it=page_table_.find(page_id);
  frame_id_t frame_id;
  if(it!=page_table_.end()){//1.1
    frame_id=it->second;
    replacer_->Pin(frame_id);//pin it
    pages_[frame_id].pin_count_++;
    // if(pages_[frame_id].page_id_==0) throw "Error";
    pages_[frame_id].page_id_=page_id;
    return &pages_[frame_id];
  }else{//1.2
    if(free_list_.size()){//find from free list first
      frame_id=free_list_.front();
      free_list_.pop_front();
    }else if(replacer_->Size()){
      replacer_->Victim(&frame_id);
    }else return nullptr;
  }
  //2
  Page* r=pages_+frame_id;
  if(r->is_dirty_){//write r back
    disk_manager_->WritePage(r->page_id_,r->data_);
    r->is_dirty_=false;
  }
  page_table_.erase(r->page_id_);//delete R from the page table
  page_table_.emplace(page_id,frame_id);//insert P(page_id,frame_id) 
  //现在R就变成了P   frame_id不变,但page_id需要修改,信息仍然存储在pages_[frame_id]
  r->ResetMemory();
  r->page_id_=page_id;
  disk_manager_->ReadPage(r->page_id_,r->data_);
  replacer_->Pin(frame_id);//同1,Pin
  r->pin_count_++;
  // if(frame_id==1&&r->page_id_==0){throw "ERROR!";}
  return r;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // if(pages_[1].page_id_==0){throw "ERROR!";}
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  if(free_list_.empty()&&replacer_->Size()==0) return nullptr;//1.
  frame_id_t frame_id;
  if(free_list_.size()){//2.
    frame_id=free_list_.front();
    free_list_.pop_front();
  }else if(replacer_->Size()){
    replacer_->Victim(&frame_id);
  }
  Page* p=pages_+frame_id;//victim page P
  if(p->is_dirty_){
    disk_manager_->WritePage(p->page_id_,p->data_);
    p->is_dirty_=false;
  }
  page_id=disk_manager_->AllocatePage();//在磁盘上新建page,更新page_id
  page_table_.erase(p->page_id_);//后面跟Fetch Page一样，只不过page_id是新生成的
  page_table_.emplace(page_id,frame_id);
  p->ResetMemory();
  p->page_id_=page_id;
  disk_manager_->ReadPage(p->page_id_,p->data_);
  replacer_->Pin(frame_id);
  p->pin_count_++;
  return p;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(page_id==INVALID_PAGE_ID) return true;//已经删除过了
  auto it=page_table_.find(page_id);//1.
  if(it==page_table_.end()) return true;//1. if P does not exist
  frame_id_t frame_id=it->second;
  Page* p=pages_+frame_id;
  if(p->pin_count_!=0) return false;//2.
  page_table_.erase(p->page_id_);//3.
  p->ResetMemory();
  p->page_id_=INVALID_PAGE_ID;//如果 Page 对象不包含物理页，那么 page_id_ =INVALID_PAGE_ID 
  free_list_.push_back(frame_id);
  return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  auto it=page_table_.find(page_id);
  if(it==page_table_.end()) return false;
  frame_id_t frame_id=it->second;
  Page* p=pages_+frame_id;
  if(p->pin_count_ == 0) return false;
  p->pin_count_--;
  if(p->pin_count_==0) replacer_->Unpin(frame_id);
  p->is_dirty_=is_dirty;
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto it=page_table_.find(page_id);
  if(it==page_table_.end()) return false;
  frame_id_t frame_id=it->second;
  Page* p=pages_+frame_id;
  disk_manager_->WritePage(page_id,p->data_);
  p->is_dirty_=false;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}