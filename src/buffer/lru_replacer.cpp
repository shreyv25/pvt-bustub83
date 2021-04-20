//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <unordered_map>
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lck(latch);
  if (replacer.empty()) return false;  
  *(frame_id) = lst.back();           
  lst.pop_back();
  replacer.erase(*(frame_id));  
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch);
  auto temp = replacer.find(frame_id);  
  if (temp == replacer.end()) {
    return; 
  }
  auto lst_iter = temp->second;
  replacer.erase(temp);  
  lst.erase(lst_iter);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch);
  auto temp = replacer.find(frame_id);  
  if (temp == replacer.end()) {
    lst.emplace_front(frame_id);
    replacer[frame_id] = lst.begin();  
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lck(latch);
  return replacer.size();  
}

}  // namespace bustub