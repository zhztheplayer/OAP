#pragma once

#include <arrow/array.h>
#include <arrow/compute/context.h>
#include <arrow/status.h>
#include "codegen/arrow_compute/ext/array_ext.h"

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
using DictionaryExtArray =
    sparkcolumnarplugin::codegen::arrowcompute::extra::DictionaryExtArray;

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
arrow::Status SplitArrayList(arrow::compute::FunctionContext* ctx, const ArrayList& in,
                             const std::shared_ptr<arrow::Array>& dict,
                             const std::shared_ptr<arrow::Array>& counts,
                             std::vector<ArrayList>* out, std::vector<int>* out_sizes);

arrow::Status SumArray(arrow::compute::FunctionContext* ctx,
                       const std::shared_ptr<arrow::Array>& in,
                       std::shared_ptr<arrow::Array>* out);

arrow::Status CountArray(arrow::compute::FunctionContext* ctx,
                         const std::shared_ptr<arrow::Array>& in,
                         std::shared_ptr<arrow::Array>* out);

arrow::Status EncodeArray(arrow::compute::FunctionContext* ctx,
                          const std::shared_ptr<arrow::Array>& in,
                          std::shared_ptr<DictionaryExtArray>* out);
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
