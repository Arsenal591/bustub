//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

namespace {
void UpdateParent(BufferPoolManager *buffer_pool_manager, page_id_t child_page_id, page_id_t parent_page_id) {
  Page *child_raw_page = buffer_pool_manager->FetchPage(child_page_id);
  assert(child_raw_page != nullptr);
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(child_raw_page->GetData());
  child_page->SetParentPageId(parent_page_id);
  buffer_pool_manager->UnpinPage(child_page_id, true);
}
}  // namespace

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetLSN(INVALID_LSN);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array_[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int lo = 1;
  int hi = GetSize() - 1;
  while (lo < hi) {
    int mid = (lo + hi) / 2;
    if (comparator(array_[mid].first, key) > 0) {
      hi = mid;
    } else {
      lo = mid + 1;
    }
  }
  if (comparator(array_[lo].first, key) > 0) {
    return array_[lo - 1].second;
  }
  return array_[lo].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  assert(CanInsert());
  int index = ValueIndex(old_value);
  assert(index != -1);
  const int size = GetSize();
  for (int i = size; i > index + 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index].first = new_key;
  array_[index].second = new_value;

  IncreaseSize(1);
  return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);
  assert(NeedToSplit());
  assert(recipient->IsEmpty());

  const int size = GetSize();
  const int half = size / 2;
  const int need_move = size - half;
  for (int i = 0; i < need_move; i++) {
    recipient->array_[i] = array_[half + i];
    const int child_page_id = array_[half + i].second;
    UpdateParent(buffer_pool_manager, child_page_id, recipient->GetPageId());
  }
  SetSize(half);
  recipient->SetSize(need_move);
}
/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  assert(items != nullptr);
  assert(buffer_pool_manager != nullptr);
  assert(size >= 0);
  assert(CanInsert(size));

  const int original_size = GetSize();
  const int page_id = GetPageId();
  for (int i = 0; i < size; i++) {
    array_[i + original_size] = items[i];
    const int child_page_id = items[i].second;
    UpdateParent(buffer_pool_manager, child_page_id, page_id);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  const int size = GetSize();
  assert(index >= 0 && index < size);
  for (int i = index; i < size - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  assert(GetSize() == 1);
  ValueType ret = array_[0].second;
  IncreaseSize(-1);
  return ret;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);
  const int size = GetSize();
  assert(recipient->CanInsertWithoutSplit(size));

  const int recipient_size = recipient->GetSize();
  const int recipient_page_id = recipient->GetPageId();
  array_[0].first = middle_key;
  for (int i = 0; i < size; i++) {
    recipient->array_[recipient_size + i] = array_[i];
    const int child_page_id = array_[i].second;
    UpdateParent(buffer_pool_manager, child_page_id, recipient_page_id);
  }
  recipient->IncreaseSize(size);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);
  assert(!IsEmpty());
  assert(recipient->CanInsertWithoutSplit());

  const int size = GetSize();
  MappingType item = array_[0];
  item.first = middle_key;
  for (int i = 0; i < size - 1; i++) {
    array_[i] = array_[i + 1];
  }
  recipient->CopyLastFrom(item, buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(buffer_pool_manager != nullptr);
  assert(CanInsertWithoutSplit());
  const int size = GetSize();
  array_[size] = pair;

  const int child_page_id = pair.second;
  UpdateParent(buffer_pool_manager, child_page_id, GetPageId());
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);
  assert(!IsEmpty());
  assert(recipient->CanInsertWithoutSplit());

  const int size = GetSize();
  MappingType item = array_[size - 1];
  recipient->CopyFirstFrom(item, buffer_pool_manager);
  recipient->array_[1].first = middle_key;
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(buffer_pool_manager != nullptr);
  assert(CanInsertWithoutSplit());
  const int size = GetSize();
  for (int i = size; i >= 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0] = pair;

  const int child_page_id = pair.second;
  UpdateParent(buffer_pool_manager, child_page_id, GetPageId());
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
