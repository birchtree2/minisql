#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  auto pMetaPage=reinterpret_cast<DiskFileMetaPage*>(GetMetaData());
  //返回的是meta_data_的起始地址(char*),要类型转换
  if(pMetaPage->GetAllocatedPages()>=MAX_VALID_PAGE_ID){ 
    return INVALID_PAGE_ID;
  }
  //一个位图页+一段数据页(最多BITMAP_SIZE个)=分区(Extent)
  //找到没有满的分区,插入数据页
  uint32_t ext_id=0;
  for(ext_id=0;ext_id<pMetaPage->GetExtentNums();ext_id++){
    if(pMetaPage->GetExtentUsedPage(ext_id)<BITMAP_SIZE) break;
  }
  //每个分区有(BITMAP_SIZE+1)个物理页，再加上开头的metadata页
  page_id_t bmap_phy_id=ext_id*(BITMAP_SIZE+1)+1;//第ext_id哥分区的位图页物理id
  char buf[PAGE_SIZE]={'\0'};
  ReadPhysicalPage(bmap_phy_id,buf);//读取位图页信息
  BitmapPage<PAGE_SIZE>* bmap=reinterpret_cast< BitmapPage<PAGE_SIZE>* >(buf);
  uint32_t page_offset=0;
  if(bmap->AllocatePage(page_offset)){
    WritePhysicalPage(bmap_phy_id,buf);
    pMetaPage->num_allocated_pages_++;//更新总的数据页数量
    pMetaPage->extent_used_page_[ext_id]++;//更新每个分区的数据页数量
    if(ext_id==pMetaPage->GetExtentNums()){
      pMetaPage->num_extents_++;//更新分区数量
    }
    //返回新分配的数据页的逻辑页id  每个分区BITMAP_SIZE个数据页
    page_id_t logical_id=ext_id*(BITMAP_SIZE)+page_offset;
    return logical_id;
  }else{
    LOG(ERROR)<<"bitmap allocate failed";
    return INVALID_PAGE_ID;
  }


  return INVALID_PAGE_ID;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  auto pMetaPage=reinterpret_cast<DiskFileMetaPage*>(GetMetaData());
  uint32_t ext_id=logical_page_id/BITMAP_SIZE;//得到分区
  page_id_t bmap_phy_id=ext_id*(BITMAP_SIZE+1)+1;//得到当前分区位图页id
  char buf[PAGE_SIZE]={0};
  ReadPhysicalPage(bmap_phy_id,buf);//读取位图页信息
  auto bmap=reinterpret_cast< BitmapPage<PAGE_SIZE>* >(buf);
  uint32_t page_offset=logical_page_id%BITMAP_SIZE;
  if(bmap->DeAllocatePage(page_offset)){  
    WritePhysicalPage(bmap_phy_id,buf);
    pMetaPage->num_allocated_pages_--;
    pMetaPage->extent_used_page_[ext_id]--;
  }else{
    LOG(ERROR)<<"bitmap allocate failed";
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  // auto pMetaPage=reinterpret_cast<DiskFileMetaPage*>(GetMetaData());
  uint32_t ext_id=logical_page_id/BITMAP_SIZE;//得到分区
  page_id_t bmap_phy_id=ext_id*(BITMAP_SIZE+1)+1;
  char buf[PAGE_SIZE]={0};
  ReadPhysicalPage(bmap_phy_id,buf);//读取位图页信息
  auto bmap=reinterpret_cast< BitmapPage<PAGE_SIZE>* >(bmap_phy_id);
  uint32_t page_offset=logical_page_id%BITMAP_SIZE;
  return bmap->IsPageFree(page_offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  int ext_id=logical_page_id/BITMAP_SIZE;
  //物理页=1meta + 前面ext_id个分区(每个BITMAP_SIZE+1) + 1 bitmap + 当前数据页编号
  return 1+ext_id*(BITMAP_SIZE+1)+1+logical_page_id%BITMAP_SIZE;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}