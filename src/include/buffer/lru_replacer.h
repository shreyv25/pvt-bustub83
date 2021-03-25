//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

class LRUReplacer : public Replacer {
 public:
  explicit LRUReplacer(size_t num_pages);
  ~LRUReplacer() override;
  bool Victim(frame_id_t *frame_id) override;
  void Pin(frame_id_t frame_id) override;
  void Unpin(frame_id_t frame_id) override;
  size_t Size() override;

 private:
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> replacer;
  std::list<frame_id_t> lst;
  mutable std::mutex latch;
};

}  // namespace bustub
