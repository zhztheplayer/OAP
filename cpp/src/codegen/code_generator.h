#ifndef CODE_GENERATOR_H
#define CODE_GENERATOR_H

#include <arrow/type.h>
#include <gandiva/expression.h>
#include <gandiva/node.h>

class CodeGenerator {
 public:
  explicit CodeGenerator() = default;
  virtual arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) = 0;
  virtual arrow::Status evaluate(
      const std::shared_ptr<arrow::RecordBatch>& in,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* out) = 0;
  virtual arrow::Status evaluate(
      const std::shared_ptr<arrow::RecordBatch>& in,
      std::vector<arrow::MapArray>* hash_map,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* out) = 0;
};

#endif
