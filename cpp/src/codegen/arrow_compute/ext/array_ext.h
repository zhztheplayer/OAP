#pragma once

#include <arrow/array.h>

namespace arrow {
namespace extra {

class DictionaryExtArray : public Array {
 public:
  using TypeClass = DictionaryType;
  explicit DictionaryExtArray(const std::shared_ptr<Array>& indices,
                              const std::shared_ptr<Array>& value_counts) {
    indices_ = indices;
    value_counts_ = value_counts;
  }
  /// \brief Return the dictionary for this array, which is stored as
  //// a member of the ArrayData internal structure
  std::shared_ptr<Array> dict_indices() const { return indices_; }
  std::shared_ptr<Array> value_counts() const { return value_counts_; }

 private:
  std::shared_ptr<Array> indices_;
  std::shared_ptr<Array> value_counts_;
};

}  // namespace extra
}  // namespace arrow
