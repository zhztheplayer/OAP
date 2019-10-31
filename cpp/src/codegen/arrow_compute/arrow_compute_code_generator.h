#ifndef ARROW_COMPUTE_CODE_GENERATOR
#define ARROW_COMPUTE_CODE_GENERATOR

#include <arrow/type.h>

#include "codegen/arrow_compute/arrow_compute_expr_visitor.h"
#include "codegen/code_generator.h"

class ArrowComputeCodeGenerator : public CodeGenerator {
 public:
  ArrowComputeCodeGenerator(
      std::shared_ptr<arrow::Schema> schema_ptr,
      std::vector<std::shared_ptr<gandiva::Expression>> exprs_vector,
      std::vector<std::shared_ptr<arrow::Field>> ret_types)
      : schema(schema_ptr), exprs_vector(exprs_vector), ret_types(ret_types) {
    for (auto expr : exprs_vector) {
      auto visitor = std::make_shared<ArrowComputeExprVisitor>(schema_ptr, expr);
      visitor_list.push_back(visitor);
    }
  }

  ~ArrowComputeCodeGenerator() {}

  arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) {
    *out = schema;
    return arrow::Status::OK();
  }

  arrow::Status evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    arrow::Status status = arrow::Status::OK();
    std::vector<std::shared_ptr<arrow::Array>> result_vector;
    int64_t res_len = 0;
    for (auto visitor : visitor_list) {
      std::shared_ptr<arrow::Array> result_column;
      status = visitor->eval(in, &result_column);
      if (!status.ok()) {
        return status;
      }
      res_len = (res_len < result_column->length()) ? result_column->length() : res_len;
      result_vector.push_back(result_column);
    }
    auto res_schema = arrow::schema(ret_types);
    out->push_back(arrow::RecordBatch::Make(res_schema, res_len, result_vector));
    return status;
  }

  arrow::Status evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                         std::vector<arrow::MapArray>* hash_map,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    return arrow::Status::OK();
  }

 private:
  std::vector<std::shared_ptr<ArrowComputeExprVisitor>> visitor_list;
  std::shared_ptr<arrow::Schema> schema;
  std::vector<std::shared_ptr<gandiva::Expression>> exprs_vector;
  std::vector<std::shared_ptr<arrow::Field>> ret_types;
};

#endif
