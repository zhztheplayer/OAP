#ifndef EXPR_VISITOR_H
#define EXPR_VISITOR_H

#include <gandiva/node.h>
#include <gandiva/node_visitor.h>
#include <iostream>
#include "code_generator.h"

#define ARROW_COMPUTE 0x0001
#define GANDIVA 0x0002
#define COMPUTE_EXT 0x0003

class ExprVisitor : public gandiva::NodeVisitor {
 public:
  ExprVisitor() {}

  arrow::Status create(std::vector<std::shared_ptr<gandiva::Expression>> exprs_vector,
                       int* out) {
    arrow::Status status = arrow::Status::OK();
    for (auto expr : exprs_vector) {
      status = expr->root()->Accept(*this);
      if (!status.ok()) {
        return status;
      }
    }
    *out = codegen_type;
    return status;
  }

 private:
  std::vector<std::string> ac{"sum", "max", "min", "count"};
  std::vector<std::string> gdv{"add", "substract", "multiply", "divide"};
  int codegen_type;
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
