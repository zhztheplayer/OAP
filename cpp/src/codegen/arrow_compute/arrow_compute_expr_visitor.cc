#include "arrow_compute_expr_visitor.h"

#include <arrow/array.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/count.h>
#include <arrow/compute/kernels/hash.h>
#include <arrow/compute/kernels/sum.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gandiva/tree_expr_builder.h>
#include "codegen/arrow_compute/ext/kernels_ext.h"

arrow::Status PrefixVisitor::Visit(const gandiva::FunctionNode& node) {
  auto desc = node.descriptor();
  if (desc->name().compare("getPrepareFunc") == 0) {
    if_pre_node_ = true;
    actual_func_node_ =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(node.children()[0]);
    return arrow::Status::OK();
  }

  if (desc->name().compare("getPostFunc") == 0) {
    if_post_node_ = true;
    actual_func_node_ =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(node.children()[0]);
    return arrow::Status::OK();
  }
  return arrow::Status::OK();
}

std::shared_ptr<gandiva::FunctionNode> PrefixVisitor::GetActualFunc() {
  return actual_func_node_;
}

std::shared_ptr<ArrowComputeExprVisitor> ArrowComputeExprVisitor::GetFuncVisitor() {
  auto actual_func_node = prefix_visitor_.GetActualFunc();
  if (actual_func_node) {
    auto expr =
        gandiva::TreeExprBuilder::MakeExpression(actual_func_node, expr_->result());
    return std::make_shared<ArrowComputeExprVisitor>(schema_, expr);
  }
  return std::shared_ptr<ArrowComputeExprVisitor>(this);
}

arrow::Status ArrowComputeExprVisitor::Visit(const gandiva::FunctionNode& node) {
  auto desc = node.descriptor();

  auto status = arrow::Status::OK();

  if (node.children().size() == 1) {
    auto col_name = (std::dynamic_pointer_cast<gandiva::FieldNode>(node.children()[0]))
                        ->field()
                        ->name();
    auto col = in_record_batch_->GetColumnByName(col_name);
    if (col == nullptr) {
      return arrow::Status::UnknownError("[ArrowComputeExprVisitor] get ", col_name,
                                         " from input failed.");
    }
    arrow::compute::Datum output;

    if (desc->name().compare("sum") == 0) {
      RETURN_NOT_OK(arrow::compute::Sum(&ctx_, *col.get(), &output));
      RETURN_NOT_OK(arrow::MakeArrayFromScalar(*(output.scalar()).get(), output.length(),
                                               &result_array_));
      goto generateArray;

    } else if (desc->name().compare("count") == 0) {
      arrow::compute::CountOptions opt =
          arrow::compute::CountOptions(arrow::compute::CountOptions::COUNT_ALL);
      RETURN_NOT_OK(arrow::compute::Count(&ctx_, opt, *col.get(), &output));
      RETURN_NOT_OK(arrow::MakeArrayFromScalar(*(output.scalar()).get(), output.length(),
                                               &result_array_));
      goto generateArray;

    } else if (desc->name().compare("encodeArray") == 0) {
      arrow::compute::Datum out_dict;
      arrow::compute::Datum input_datum(col);
      RETURN_NOT_OK(arrow::compute::DictionaryEncode(&ctx_, input_datum, &out_dict));
      auto dict =
          std::dynamic_pointer_cast<arrow::DictionaryArray>(out_dict.make_array());

      std::shared_ptr<arrow::Array> out_counts;
      RETURN_NOT_OK(arrow::compute::ValueCounts(&ctx_, input_datum, &out_counts));
      auto value_counts = std::dynamic_pointer_cast<arrow::StructArray>(out_counts);
      dict_ext_array_ = std::make_shared<arrow::extra::DictionaryExtArray>(
          dict->indices(), value_counts->field(1));
      goto generateExtArray;
    }
    goto unrecognizedFail;
  } else if (node.children().size() > 1) {
    ArrayList col_list;
    for (auto arg_node : node.children()) {
      auto col_name =
          std::dynamic_pointer_cast<gandiva::FieldNode>(arg_node)->field()->name();
      auto col = in_record_batch_->GetColumnByName(col_name);
      if (col == nullptr) {
        return arrow::Status::UnknownError("[ArrowComputeExprVisitor] get ", col_name,
                                           " from input failed.");
      }
      col_list.push_back(col);
    }
    if (desc->name().compare("splitArrayList") == 0) {
      if (!prepare_visitor_) {
        return arrow::Status::Invalid("This function requires input, but there is none.");
      }
      std::shared_ptr<arrow::extra::DictionaryExtArray> dict_ext_array;
      RETURN_NOT_OK(prepare_visitor_->GetResult(&dict_ext_array));
      RETURN_NOT_OK(arrow::compute::extra::SplitArrayList(
          &ctx_, col_list, dict_ext_array->dict_indices(), dict_ext_array->value_counts(),
          &result_batch_list_, &result_batch_size_list_));
      goto generateBatchList;
    }
    goto unrecognizedFail;
  }

generateArray:
  return_type_ = ArrowComputeResultType::Array;
  goto finish;

generateExtArray:
  return_type_ = ArrowComputeResultType::ExtraArray;
  goto finish;

generateArrayList:
  return_type_ = ArrowComputeResultType::ArrayList;
  goto finish;

generateBatchList:
  return_type_ = ArrowComputeResultType::BatchList;
  goto finish;

finish:
  return arrow::Status::OK();

unrecognizedFail:
  return arrow::Status::NotImplemented("Function name is not impelmented yet.");
}
