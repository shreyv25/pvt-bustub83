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
//victim function is for getting value of lru block or frame
bool LRUReplacer::Victim(frame_id_t *frame_id)
{ std::lock_guard<std::mutex> lck(latch);
  if(replacer.empty()) return false;//if we do not find frame with given frame id we will return false
  *(frame_id)=lst.back();// otherwise we will return last element in list
  lst.pop_back();
  replacer.erase(*(frame_id));//Then we will simply erase that perticular frame from list and map.
  return true;
}
//this function is used for pinning memory block to not allow it to read or write back to disk
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch);
        auto temp = replacer.find(frame_id);//searching for block with that id.
        if (temp == replacer.end()) {
            return;//if not found return.
        }
        auto lst_iter = temp->second;
        replacer.erase(temp);//otherwise simply erase that from list and map.
        lst.erase(lst_iter);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch);
      auto temp = replacer.find(frame_id);//searching for block with that id.
      if (temp == replacer.end()) {
          lst.emplace_front(frame_id);
          replacer[frame_id]=lst.begin();//if not found simply push that at start in list and at frame_idth index in map.
      } else {
          lst.splice(lst.begin(), lst, temp->second);
          replacer[frame_id] = lst.begin();//if found then move the block from that mid position to starting index and for map same work is done.
      }
}

size_t LRUReplacer::Size()
{
  std::lock_guard <std::mutex> lck(latch);
  return replacer.size();//simply returning size of map.
 }

}  // namespace bustub
