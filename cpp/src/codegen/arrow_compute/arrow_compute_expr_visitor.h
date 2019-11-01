#pragma once

#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <gandiva/node.h>
#include <gandiva/node_visitor.h>
#include <iostream>

#include "codegen/code_generator.h"

#include "codegen/arrow_compute/arrow_compute_expr_visitor_base.h"
#include "codegen/arrow_compute/ext/array_ext.h"

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
enum class ArrowComputeResultType { Array, ExtraArray, ArrayList, BatchList };

class ArrowComputeExprVisitor;
class PrefixVisitor : public ArrowComputeExprVisitorBase {
 public:
  PrefixVisitor() = default;
  ~PrefixVisitor() = default;
  bool IfPreFunc() { return if_pre_node_; }
  bool IfPostFunc() { return if_post_node_; }
  std::shared_ptr<gandiva::FunctionNode> GetActualFunc();

 private:
  bool if_pre_node_ = false;
  bool if_post_node_ = false;
  std::shared_ptr<gandiva::FunctionNode> actual_func_node_;
  arrow::Status Visit(const gandiva::FunctionNode& node) override;
};

class ArrowComputeExprVisitor : public ArrowComputeExprVisitorBase {
 public:
  ArrowComputeExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                          std::shared_ptr<gandiva::Expression> expr)
      : schema_(schema_ptr), expr_(expr) {}

  ArrowComputeExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                          std::shared_ptr<gandiva::Expression> expr,
                          std::shared_ptr<arrow::Array> input_array)
      : schema_(schema_ptr), expr_(expr) {}

  arrow::Status eval(const std::shared_ptr<arrow::RecordBatch>& in) {
    in_record_batch_ = in;
    RETURN_NOT_OK(expr_->root()->Accept(*this));
    return arrow::Status::OK();
  }

  arrow::Status eval(const std::shared_ptr<arrow::RecordBatch>& in,
                     const std::shared_ptr<ArrowComputeExprVisitor>& prepare_visitor) {
    prepare_visitor_ = prepare_visitor;
    return eval(in);
  }

  bool IfPreFunc() {
    auto status = expr_->root()->Accept(prefix_visitor_);
    return prefix_visitor_.IfPreFunc();
  }

  bool IfPostFunc() {
    auto status = expr_->root()->Accept(prefix_visitor_);
    return prefix_visitor_.IfPostFunc();
  }

  std::shared_ptr<ArrowComputeExprVisitor> GetFuncVisitor();

  ArrowComputeResultType GetResultType() { return return_type_; }

  arrow::Status GetResult(std::shared_ptr<arrow::Array>* out) {
    if (!result_array_) {
      return arrow::Status::Invalid(
          "ArrowComputeExprVisitor::GetResult result_array does not generated.");
    }
    *out = result_array_;
    return arrow::Status::OK();
  }

  arrow::Status GetResult(std::shared_ptr<arrow::extra::DictionaryExtArray>* out) {
    if (!dict_ext_array_) {
      return arrow::Status::Invalid(
          "ArrowComputeExprVisitor::GetResult result_array does not generated.");
    }
    *out = dict_ext_array_;
    return arrow::Status::OK();
  }

  arrow::Status GetResult(ArrayList* out) {
    if (result_array_list_.empty()) {
      return arrow::Status::Invalid(
          "ArrowComputeExprVisitor::GetResult result_array does not generated.");
    }
    *out = result_array_list_;
    return arrow::Status::OK();
  }

  arrow::Status GetResult(std::vector<ArrayList>* out, std::vector<int>* out_sizes) {
    if (result_batch_list_.empty()) {
      return arrow::Status::Invalid(
          "ArrowComputeExprVisitor::GetResult result_array does not generated.");
    }
    *out = result_batch_list_;
    *out_sizes = result_batch_size_list_;
    return arrow::Status::OK();
  }

 private:
  // Input data holder.
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<gandiva::Expression> expr_;
  std::shared_ptr<arrow::RecordBatch> in_record_batch_;
  std::shared_ptr<ArrowComputeExprVisitor> prepare_visitor_;

  // Output data types.
  ArrowComputeResultType return_type_;
  // This is used when we want to output an Array after evaluate.
  std::shared_ptr<arrow::Array> result_array_;
  std::shared_ptr<arrow::extra::DictionaryExtArray> dict_ext_array_;
  // This is used when we want to output an ArrayList after evaluation.
  ArrayList result_array_list_;
  // This is used when we want to output an BatchList after evaluation.
  std::vector<ArrayList> result_batch_list_;
  std::vector<int> result_batch_size_list_;

  arrow::compute::FunctionContext ctx_;
  PrefixVisitor prefix_visitor_;

  arrow::Status Visit(const gandiva::FunctionNode& node) override;
};
