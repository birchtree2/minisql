#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):max_pages(num_pages){
  page_map.reserve(num_pages);
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mut); // 锁定互斥锁
  if(pages.size()){//页面不为空
    *frame_id=pages.back();//删除链表尾部的页面(least recently used)
    pages.pop_back();
    page_map.erase(*frame_id);
    return true;
  }else return false;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mut);
  if(!page_map.count(frame_id)) return;
  auto iter=page_map[frame_id];//将frame_id对应的链表节点删除
  pages.erase(iter);
  page_map.erase(frame_id);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mut);
  if(page_map.count(frame_id)) return;
  //如果已经unpin过了,下一次unpin就忽略(并不是放到最前面，见lru_replacer_test scene1)
  if (pages.size() < max_pages) {
      pages.push_front(frame_id); // 将页面添加到链表的头部
      page_map[frame_id] = pages.begin(); // 更新映射
  } else {
      auto it = page_map.find(pages.back());
      if (it != page_map.end()) {
          pages.pop_back(); // 移除最久未使用的页面
          page_map.erase(it); // 从映射中移除页面
      }
      pages.push_front(frame_id); // 将新页面添加到链表的头部
      page_map[frame_id] = pages.begin(); // 更新映射
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return pages.size();
}