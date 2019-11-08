#pragma once

#include <arrow/array.h>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

class DictionaryExtArray {
 public:
  explicit DictionaryExtArray(const std::shared_ptr<arrow::Array>& indices,
                              const std::shared_ptr<arrow::Array>& value_counts) {
    indices_ = indices;
    value_counts_ = value_counts;
  }
  ~DictionaryExtArray() {}
  /// \brief Return the dictionary for this array, which is stored as
  //// a member of the ArrayData internal structure
  std::shared_ptr<arrow::Array> dict_indices() const { return indices_; }
  std::shared_ptr<arrow::Array> value_counts() const { return value_counts_; }

 private:
  std::shared_ptr<arrow::Array> indices_;
  std::shared_ptr<arrow::Array> value_counts_;
};

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
