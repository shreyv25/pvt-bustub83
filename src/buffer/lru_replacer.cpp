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
#include "storage/page/page.h"

namespace bustub {

//constructor
LRUReplacer::LRUReplacer(size_t num_pages) {}

//destructor
LRUReplacer::~LRUReplacer() = default;


bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::lock_guard<std::mutex> lck(latch);
        if (replacer.empty()) {
            return false;
        }
        *(frame_id)= lst.back();
        lst.pop_back();
        replacer.erase(*(frame_id));
        return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lck(latch);
    //if the frame corresponding to frame_id is found
    //then erase it from both list and map(pin that page), otherwise return 
    auto id = replacer.find(frame_id);
    if (id == replacer.end()) {
        return;
    }
    auto lst_it = id->second;
    replacer.erase(id);
    lst.erase(lst_it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lck(latch);
    auto id = replacer.find(frame_id);
    if (id == replacer.end()) {
        //if frame id is not found then insert in both list as well as map
        lst.insert(lst.begin(), frame_id);
        replacer.emplace(frame_id , lst.begin());
        } 
    
}

size_t LRUReplacer::Size() { 
    std::lock_guard<std::mutex> lck(latch);
    return replacer.size();
}

}  // namespace bustub
