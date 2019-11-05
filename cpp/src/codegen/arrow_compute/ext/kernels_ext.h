#pragma once

#include <arrow/array.h>
#include <arrow/compute/context.h>
#include <arrow/status.h>

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

namespace arrow {
namespace compute {
namespace extra {
namespace splitter {
class ArrayVisitorImpl {
 public:
  static Status Eval(FunctionContext* ctx, const ArrayList& in,
                     const std::shared_ptr<arrow::Array>& dict,
                     const std::shared_ptr<arrow::Array>& counts,
                     std::vector<ArrayList>* out, std::vector<int>* out_sizes);
  ~ArrayVisitorImpl() {}

 private:
  ArrayVisitorImpl() = default;
  class Impl;
};
}  // namespace splitter
Status SplitArrayList(FunctionContext* ctx, const ArrayList& in,
                      const std::shared_ptr<arrow::Array>& dict,
                      const std::shared_ptr<arrow::Array>& counts,
                      std::vector<ArrayList>* out, std::vector<int>* out_sizes);

}  // namespace extra
}  // namespace compute
}  // namespace arrow
