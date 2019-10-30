#ifndef GANDIVA_CODE_GENERATOR
#define GANDIVA_COMPUTE_CODE_GENERATOR

#include <arrow/type.h>
#include "code_generator.h"

class GandivaCodeGenerator : public CodeGenerator {
 public:
  GandivaCodeGenerator(std::shared_ptr<arrow::Schema> schema_ptr,
                       std::vector<std::shared_ptr<gandiva::Expression>> exprs_vector,
                       std::vector<std::shared_ptr<arrow::Field>> ret_types) {}
  ~GandivaCodeGenerator() {}
  arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) {
    return arrow::Status::OK();
  }
  arrow::Status evaluate(std::shared_ptr<arrow::RecordBatch>& in,
                         std::shared_ptr<arrow::RecordBatch>* out) {
    return arrow::Status::OK();
  }
};

#endif
