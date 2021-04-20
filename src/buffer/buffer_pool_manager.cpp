//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);


  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  latch_.lock();                  
  frame_id_t free_frame_id = -1;  

  if (page_table_.find(page_id) != page_table_.end()) {
    free_frame_id = page_table_[page_id];
    replacer_->Pin(free_frame_id);
    pages_[free_frame_id].pin_count_ += 1;
    latch_.unlock();
    return &pages_[free_frame_id];
  }
  
  if (!free_list_.empty()) {
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&free_frame_id)) {
      latch_.unlock();
      return nullptr;
    }
   
    Page *replacedPage = &pages_[free_frame_id];
    if (replacedPage->is_dirty_) {
      FlushPageImpl(replacedPage->page_id_);
      replacedPage->is_dirty_ = false;
    }
    page_table_.erase(replacedPage->page_id_);
  }
 
  pages_[free_frame_id].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[free_frame_id].GetData());
  
  pages_[free_frame_id].page_id_ = page_id;
  page_table_.insert({page_id, free_frame_id});
  pages_[free_frame_id].pin_count_ = 1;
  replacer_->Pin(free_frame_id);

  latch_.unlock();
  return &pages_[free_frame_id];
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  
  latch_.lock();  
 
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

 
  Page *pageToUnpin = &pages_[page_table_[page_id]];
  
  if (pageToUnpin->pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }

  pageToUnpin->pin_count_ -= 1;
  pageToUnpin->is_dirty_ = is_dirty;
  if (pageToUnpin->pin_count_ == 0) {
    replacer_->Unpin(page_table_[page_id]);
  }
  latch_.unlock();
  return true;
}


bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
 
  if (page_table_.find(page_id) == page_table_.end()) {

    return false;
  }

  Page *pageToFlush = &pages_[page_table_[page_id]];

  if (pageToFlush->is_dirty_) {
    disk_manager_->WritePage(pageToFlush->page_id_, pageToFlush->GetData());
    pageToFlush->is_dirty_ = false;
  }
  return true;
}


Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  
  latch_.lock();
  frame_id_t free_frame_id = -1;
  if (!free_list_.empty()) {
   
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    
    if (!replacer_->Victim(&free_frame_id)) {
      latch_.unlock();
      return nullptr;
    }

   
    Page *replacedPage = &pages_[free_frame_id];
    if (replacedPage->is_dirty_) {
      FlushPageImpl(replacedPage->page_id_);
      replacedPage->is_dirty_ = false;
    }
    page_table_.erase(replacedPage->page_id_);
  }

 
  *page_id = disk_manager_->AllocatePage();
  pages_[free_frame_id].page_id_ = *page_id;
  page_table_.insert({*page_id, free_frame_id});
  pages_[free_frame_id].ResetMemory();
  replacer_->Pin(free_frame_id);
  pages_[free_frame_id].pin_count_ = 1;
  pages_[free_frame_id].is_dirty_ = false;
  latch_.unlock();
  return &(pages_[free_frame_id]);
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
 
  if (page_table_.find(page_id) == page_table_.end()) {
    disk_manager_->DeallocatePage(page_id);
    return true;  
  }
 
  frame_id_t frame_id = page_table_[page_id];
  Page *pageToDelete = &pages_[frame_id];
 
  if (pageToDelete->GetPinCount() > 0) {
    return false;
  }
 
  free_list_.emplace_back(page_table_[page_id]);
  page_table_.erase(pageToDelete->page_id_);
  pageToDelete->ResetMemory();
 
  pageToDelete->page_id_ = INVALID_PAGE_ID;
  pageToDelete->is_dirty_ = false;
  pageToDelete->pin_count_ = 0;
  return false;
}
void BufferPoolManager::FlushAllPagesImpl() {
  
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(pages_[i].page_id_);
  }
}
}  // namespace bustub