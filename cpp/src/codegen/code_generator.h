#pragma once

#include <arrow/array.h>
#include <arrow/type.h>
#include <gandiva/expression.h>
#include <gandiva/node.h>
#include "codegen/common/result_iterator.h"
namespace sparkcolumnarplugin {
namespace codegen {

class CodeGenerator {
 public:
  explicit CodeGenerator() = default;
  virtual arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) = 0;
  virtual arrow::Status getResSchema(std::shared_ptr<arrow::Schema>* out) = 0;
  virtual arrow::Status evaluate(
      const std::shared_ptr<arrow::RecordBatch>& in,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* out) = 0;
  virtual arrow::Status finish(std::vector<std::shared_ptr<arrow::RecordBatch>>* out) = 0;
  virtual arrow::Status finish(std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "Finish return with ResultIterator is not NotImplemented");
  }
};
}  // namespace codegen
}  // namespace sparkcolumnarplugin
