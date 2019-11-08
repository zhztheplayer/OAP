#pragma once

#include <arrow/type.h>
#include <gandiva/expression.h>
#include <gandiva/node.h>
namespace sparkcolumnarplugin {
namespace codegen {

class CodeGenerator {
 public:
  explicit CodeGenerator() = default;
  virtual arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) = 0;
  virtual arrow::Status evaluate(
      const std::shared_ptr<arrow::RecordBatch>& in,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* out) = 0;
};
}  // namespace codegen
}  // namespace sparkcolumnarplugin
