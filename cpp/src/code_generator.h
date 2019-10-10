#ifndef CODE_GENERATOR_H
#define CODE_GENERATOR_H

#include <arrow/type.h>
#include <gandiva/node.h>
#include <gandiva/expression.h>

class CodeGenerator {
public:
  explicit CodeGenerator() = default;
  virtual arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) = 0;
  virtual arrow::Status evaluate(std::shared_ptr<arrow::RecordBatch> &in,
                        std::shared_ptr<arrow::RecordBatch> *out) = 0;

};

#endif
