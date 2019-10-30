#ifndef ARROW_COMPUTE_EXPR_VISITOR_H
#define ARROW_COMPUTE_EXPR_VISITOR_H

#include <arrow/compute/context.h>
#include <gandiva/node.h>
#include <gandiva/node_visitor.h>
#include <iostream>
#include "code_generator.h"

class ArrowComputeExprVisitor : public gandiva::NodeVisitor {
 public:
  ArrowComputeExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                          std::shared_ptr<gandiva::Expression> expr)
      : schema(schema_ptr), expr(expr) {}

  arrow::Status eval(std::shared_ptr<arrow::RecordBatch>& in,
                     std::shared_ptr<arrow::Array>* out) {
    in_record_batch = in;
    result = out;
    arrow::Status status = arrow::Status::OK();
    status = expr->root()->Accept(*this);
    if (!status.ok()) {
      return status;
    }
    return status;
  }

 private:
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<gandiva::Expression> expr;
  std::shared_ptr<arrow::RecordBatch> in_record_batch;
  std::shared_ptr<arrow::Array>* result;
  arrow::compute::FunctionContext ctx;

  arrow::Status Visit(const gandiva::FieldNode& node) override;
  arrow::Status Visit(const gandiva::FunctionNode& node) override;
  arrow::Status Visit(const gandiva::IfNode& node) override;
  arrow::Status Visit(const gandiva::LiteralNode& node) override;
  arrow::Status Visit(const gandiva::BooleanNode& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<int32_t>& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<int64_t>& node) override;
  arrow::Status Visit(const gandiva::InExpressionNode<std::string>& node) override;
};

#endif
