#pragma once

#include <arrow/type.h>
#include "codegen/code_generator.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace computeext {

class ComputeExtCodeGenerator : public CodeGenerator {
 public:
  ComputeExtCodeGenerator(std::shared_ptr<arrow::Schema> schema_ptr,
                          std::vector<std::shared_ptr<gandiva::Expression>> exprs_vector,
                          std::vector<std::shared_ptr<arrow::Field>> ret_types) {}
  ~ComputeExtCodeGenerator() {}
  arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) {
    return arrow::Status::OK();
  }

  arrow::Status evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    arrow::Status status = arrow::Status::OK();
    return status;
  }
};

}  // namespace computeext
}  // namespace codegen
}  // namespace sparkcolumnarplugin
