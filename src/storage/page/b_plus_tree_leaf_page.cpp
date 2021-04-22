//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <string.h>
#include <bits/stdc++.h>

#include "common/exception.h"
#include "common/rid.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {

  this->SetPageType(IndexPageType:: LEAF_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
  this->SetNextPageId(INVALID_PAGE_ID);

}


INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return this->next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  this->next_page_id_ = next_page_id;
}


INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(key, array[i].first) <= 0) {
      return i;
    }
  }
  return GetSize();
}


INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
 
  assert(0 <= index && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  assert(0 <= index && index < GetSize());
  return array[index];
}


INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
   if (GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0) {
    array[GetSize()] = {key, value};
  } else if (comparator(key, array[0].first) < 0) {

    memmove(static_cast<void*>(array + 1), static_cast<void*>(array), static_cast<size_t>(GetSize()*sizeof(MappingType)));
    array[0] = {key, value};
  } else {
    int low = 0, high = GetSize() - 1, mid;
    while (low < high && low + 1 != high) {
      mid = low + (high - low)/2;
      if (comparator(key, array[mid].first) < 0) {
        high = mid;
      } else if (comparator(key, array[mid].first) > 0) {
        low = mid;
      } else {
      
        assert(0);
      }
    }
    memmove(static_cast<void*>(array + high + 1), static_cast<void*>(array + high), static_cast<size_t>((GetSize() - high)*sizeof(MappingType)));
    array[high] = {key, value};
  }

  IncreaseSize(1);
  assert(GetSize() <= GetMaxSize());
  return GetSize();
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {

  int size = GetSize()/2;
  MappingType *src = array + GetSize() - size;
  recipient->CopyNFrom(src, size);
  IncreaseSize(-1*size);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
   assert(IsLeafPage() && GetSize() == 0);
  for (int i = 0; i < size; ++i) {
    array[i] = *items++;
  }
  IncreaseSize(size);
}


INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key,ValueType &value, const KeyComparator &comparator) const {
 
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0) {
    return false;
  }
  // binary search
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + (high - low)/2;
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    } else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    } else {
      value = array[mid].second;
      return true;
    }
  }
  return false;
}


INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
   if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0) {
    return GetSize();
  }

 
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + (high - low)/2;
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    } else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    } else {
     
      memmove(static_cast<void*>(array + mid), static_cast<void*>(array + mid + 1), static_cast<size_t>((GetSize() - mid - 1)*sizeof(MappingType)));
      IncreaseSize(-1);
      break;
    }
  }
  return GetSize();
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
   recipient->CopyAllFrom(array, GetSize());
  recipient->SetNextPageId(GetNextPageId());
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
CopyAllFrom(MappingType *items, int size) {
  assert(GetSize() + size <= GetMaxSize());
  auto start = GetSize();
  for (int i = 0; i < size; ++i) {
    array[start + i] = *items++;
  }
  IncreaseSize(size);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager){
  MappingType pair = GetItem(0);
  IncreaseSize(-1);
  memmove(static_cast<void*>(array), static_cast<void*>(array + 1), static_cast<size_t>(GetSize()*sizeof(MappingType)));

  recipient->CopyLastFrom(pair);

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
   }
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),KeyComparator> *>(page->GetData());

  parent->SetKeyAt(parent->ValueIndex(GetPageId()), pair.first);

   buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item){
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, int parentIndex, BufferPoolManager *buffer_pool_manager) {
   MappingType pair = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item, int parentIndex, BufferPoolManager *buffer_pool_manager){
   assert(GetSize() + 1 < GetMaxSize());
  memmove(static_cast<void*>(array + 1), static_cast<void*>(array), GetSize()*sizeof(MappingType));
  IncreaseSize(1);
  array[0] = item;

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
  }
 auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(page->GetData());

   parent->SetKeyAt(parentIndex, item.first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
