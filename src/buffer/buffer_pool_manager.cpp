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
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  
  //use latch to prevent multiple threads from accessing this process
  latch_.lock(); 
  frame_id_t free_frame_id = -1;
  if (page_table_.find(page_id) != page_table_.end()) {
    free_frame_id = page_table_[page_id];
    replacer_->Pin(free_frame_id);
    pages_[free_frame_id].pin_count_ += 1;
    latch_.unlock();
    return &pages_[free_frame_id];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.

  if (!free_list_.empty()) {
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } 
  else {
    if (!replacer_->Victim(&free_frame_id)) {
      latch_.unlock();
      return nullptr;
    }
  // 2.     If R is dirty, write it back to the disk.

  Page *replacedPage = &pages_[free_frame_id];
    if (replacedPage->is_dirty_) {
      FlushPageImpl(replacedPage->page_id_);
      replacedPage->is_dirty_ = false;
    }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(replacedPage->page_id_);

  // read in the page content from disk
  pages_[free_frame_id].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[free_frame_id].GetData());

  }
  // 4.    Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[free_frame_id].page_id_ = page_id;
  page_table_.insert({page_id, free_frame_id});
  pages_[free_frame_id].pin_count_ = 1;
  replacer_->Pin(free_frame_id);

  latch_.unlock();
  return &pages_[free_frame_id];

}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  //decrement pin count of given page with id page_id
  latch_.lock(); //use latch to lock multiple threads from accessing this process
  //if we could not find page with given id then return false
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  //pageToUnpin is the pointer that stores the reference to the page to be unpinned
  Page *pageToUnpin = &pages_[page_table_[page_id]];
  //if pin_count_ <=0 then return
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

//write content of page from buffer pool to disk
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_table_.find(page_id) == page_table_.end()) {
    //if page does not exist then return false
    return false;
  }

  Page *pageToFlush = &pages_[page_table_[page_id]];

  if (pageToFlush->is_dirty_) {
    disk_manager_->WritePage(pageToFlush->page_id_, pageToFlush->GetData());
    pageToFlush->is_dirty_ = false;
  }

  return true;

}

//create a new page and assign it a position in the buffer pool
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  latch_.lock();
  frame_id_t free_frame_id = -1;
  if (!free_list_.empty()) {
    //give more priority to the free list --> allot free_frame_id = first page from free list
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } 
  else {
    //if neither free list or replacer has an unpinned page, return nullptr
    if (!replacer_->Victim(&free_frame_id)) {
      latch_.unlock();
      return nullptr;
    }

    //page to be replaced in replacer
    Page *replacedPage = &pages_[free_frame_id];
    if (replacedPage->is_dirty_) {
      FlushPageImpl(replacedPage->page_id_);
      replacedPage->is_dirty_ = false;
    }

    page_table_.erase(replacedPage->page_id_);
  }

  //update P's metadata after allocating position for the new page 
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

//this function deallocates the page from the buffer pool
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  //if P does not exist in page table return true
  if (page_table_.find(page_id) == page_table_.end()) {
    disk_manager_->DeallocatePage(page_id);
    return true;
  }

  //get frame_id of page with given page_id
  frame_id_t frame_id = page_table_[page_id];
  Page *pageToDelete = &pages_[frame_id];

  // 2.   If P exists, but has a non-zero pin-count, return false, as page is currently under use
  if (pageToDelete->GetPinCount() > 0) {
    return false;
  }

  // 3.   Otherwise, P can be deleted.
  // Remove P from the page table, reset its metadata and return it to the free list.
  free_list_.emplace_back(page_table_[page_id]);
  page_table_.erase(pageToDelete->page_id_);
  pageToDelete->ResetMemory();
  //reset page parameters
  pageToDelete->page_id_ = INVALID_PAGE_ID;
  pageToDelete->is_dirty_ = false;
  pageToDelete->pin_count_ = 0;
  return false;

}  

void BufferPoolManager::FlushAllPagesImpl() {
  //flush all pages from buffer pool to disk 
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(pages_[i].page_id_);
  }
}

}  // namespace bustub
