#ifndef COMPUTE_EXT_CODE_GENERATOR
#define COMPUTE_EXT_CODE_GENERATOR

#include <arrow/type.h>

#include "codegen/code_generator.h"

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

  arrow::Status evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                         std::vector<arrow::MapArray>* hash_map,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    return arrow::Status::OK();
  }
};

#endif
