#include "arrow_compute_expr_visitor.h"
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/sum.h>
#include <arrow/compute/kernels/count.h>

arrow::Status ArrowComputeExprVisitor::Visit(const gandiva::FieldNode& node) {
  arrow::Status status = arrow::Status::OK();
  return status;
}

arrow::Status ArrowComputeExprVisitor::Visit(const gandiva::FunctionNode& node) {
  auto desc = node.descriptor();
  if (node.children().size() > 1) {
    std::cerr << "node is " << node.ToString() << std::endl;
    return arrow::Status::UnknownError("[ArrowComputeExprVisitor] sum(): this node has more than one parameter.");
  }
  auto col_name = (std::dynamic_pointer_cast<gandiva::FieldNode>(node.children()[0]))->field()->name();
  auto col = in_record_batch->GetColumnByName(col_name);
  if (col == nullptr) {
    return arrow::Status::UnknownError("[ArrowComputeExprVisitor] sum(): get ", col_name, " from input failed.");
  }

  arrow::compute::Datum output;
  auto dataType = desc->return_type();
  auto status = arrow::Status::OK();

  if (desc->name().compare("sum") == 0) {
    status = arrow::compute::Sum(&ctx, *col.get(), &output);
    if (!status.ok()) return status;

  } else if (desc->name().compare("count") == 0) {
    arrow::compute::CountOptions opt =
      arrow::compute::CountOptions(arrow::compute::CountOptions::COUNT_ALL);
    status = arrow::compute::Count(&ctx, opt, *col.get(), &output);
    if (!status.ok()) return status;
  }

  //std::cerr << "Datum type is " << output.type()->ToString() << std::endl;
  status = arrow::MakeArrayFromScalar(*(output.scalar()).get(), output.length(), result);
  if (!status.ok()) return status;
  return status;
}

arrow::Status ArrowComputeExprVisitor::Visit(const gandiva::IfNode& node) {
  arrow::Status status = arrow::Status::OK();
  return status;
}

arrow::Status ArrowComputeExprVisitor::Visit(const gandiva::LiteralNode& node) {
  arrow::Status status = arrow::Status::OK();
  return status;
}

arrow::Status ArrowComputeExprVisitor::Visit(const gandiva::BooleanNode& node) {
  arrow::Status status = arrow::Status::OK();
  return status;
}

arrow::Status ArrowComputeExprVisitor::Visit(const gandiva::InExpressionNode<int32_t>& node) {
  arrow::Status status = arrow::Status::OK();
  return status;
}

arrow::Status ArrowComputeExprVisitor::Visit(const gandiva::InExpressionNode<int64_t>& node) {
  arrow::Status status = arrow::Status::OK();
  return status;
}

arrow::Status ArrowComputeExprVisitor::Visit(const gandiva::InExpressionNode<std::string>& node) {
  arrow::Status status = arrow::Status::OK();
  return status;
}
