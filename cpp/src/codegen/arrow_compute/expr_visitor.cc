#include "codegen/arrow_compute/expr_visitor.h"

#include <arrow/array.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gandiva/tree_expr_builder.h>
#include <memory>
#include "codegen/arrow_compute/ext/kernels_ext.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {

arrow::Status MakeExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                              std::shared_ptr<gandiva::Expression> expr,
                              ExprVisitorMap* expr_visitor_cache,
                              std::shared_ptr<ExprVisitor>* out) {
  auto visitor =
      std::make_shared<BuilderVisitor>(schema_ptr, expr->root(), expr_visitor_cache);
  RETURN_NOT_OK(visitor->Eval());
  RETURN_NOT_OK(visitor->GetResult(out));
  return arrow::Status::OK();
}

arrow::Status BuilderVisitor::Visit(const gandiva::FieldNode& node) {
  field_name_ = node.field()->name();
  node_type_ = BuilderVisitorNodeType::FieldNode;
  return arrow::Status::OK();
}

arrow::Status BuilderVisitor::Visit(const gandiva::FunctionNode& node) {
  auto desc = node.descriptor();
  std::string node_id = desc->name();
  node_type_ = BuilderVisitorNodeType::FunctionNode;
  std::shared_ptr<ExprVisitor> dependency;
  std::vector<std::string> param_names;
  for (auto child_node : node.children()) {
    auto child_visitor =
        std::make_shared<BuilderVisitor>(schema_, child_node, expr_visitor_cache_);
    RETURN_NOT_OK(child_visitor->Eval());
    switch (child_visitor->GetNodeType()) {
      case BuilderVisitorNodeType::FunctionNode: {
        if (dependency) {
          return arrow::Status::Invalid(
              "BuilderVisitor build ExprVisitor failed, got two depency while only "
              "support one.");
        }
        RETURN_NOT_OK(child_visitor->GetResult(&dependency));
      } break;
      case BuilderVisitorNodeType::FieldNode: {
        std::string col_name = child_visitor->GetResult();
        node_id.append(col_name);
        param_names.push_back(col_name);
      } break;
      default:
        return arrow::Status::Invalid("BuilderVisitorNodeType is invalid");
    }
  }

  auto search = expr_visitor_cache_->find(node_id);
  if (search == expr_visitor_cache_->end()) {
    auto node_ptr = std::make_shared<gandiva::FunctionNode>(desc->name(), node.children(),
                                                            desc->return_type());
    if (dependency) {
      expr_visitor_ = std::make_shared<ExprVisitor>(
          schema_, std::dynamic_pointer_cast<gandiva::Node>(node_ptr), param_names,
          dependency);
    } else {
      expr_visitor_ = std::make_shared<ExprVisitor>(
          schema_, std::dynamic_pointer_cast<gandiva::Node>(node_ptr), param_names);
    }
    expr_visitor_cache_->emplace(node_id, expr_visitor_);
    return arrow::Status::OK();
  }
  expr_visitor_ = search->second;

  return arrow::Status::OK();
}

std::string BuilderVisitor::GetResult() { return field_name_; }

arrow::Status BuilderVisitor::GetResult(std::shared_ptr<ExprVisitor>* out) {
  if (!expr_visitor_) {
    return arrow::Status::Invalid(
        "BuilderVisitor GetResult Failed, expr_visitor does not be made.");
  }
  *out = expr_visitor_;
  return arrow::Status::OK();
}

//////////////////////// ExprVisitor ////////////////////////
arrow::Status ExprVisitor::Eval(const std::shared_ptr<arrow::RecordBatch>& in) {
  // leaf dependency should be process firstly, or we should visit expr cache for
  // previous result.
  in_record_batch_ = in;
  RETURN_NOT_OK(Reset());
  if (dependency_) {
    RETURN_NOT_OK(dependency_->Eval(in));
    dependency_result_type_ = dependency_->GetResultType();
    switch (dependency_result_type_) {
      case ArrowComputeResultType::BatchList: {
        RETURN_NOT_OK(
            dependency_->GetResult(&in_batch_array_, &in_batch_size_array_, &in_fields_));
      } break;
      case ArrowComputeResultType::Batch: {
        RETURN_NOT_OK(dependency_->GetResult(&in_batch_, &in_fields_));
      } break;
      case ArrowComputeResultType::ArrayList: {
        RETURN_NOT_OK(dependency_->GetResult(&in_array_list_, &in_fields_));
      } break;
      case ArrowComputeResultType::Array: {
        RETURN_NOT_OK(dependency_->GetResult(&in_array_, &in_fields_));
      } break;
      case ArrowComputeResultType::ExtraArray: {
        RETURN_NOT_OK(dependency_->GetResult(&in_ext_array_, &in_fields_));
      } break;
      default:
        return arrow::Status::Invalid("ArrowComputeResultType is invalid.");
    }
  }
  if (return_type_ != ArrowComputeResultType::None) {
    return arrow::Status::OK();
  }
  RETURN_NOT_OK(func_->Accept(*this));
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Reset() {
  if (dependency_) {
    RETURN_NOT_OK(dependency_->Reset());
  }
  switch (dependency_result_type_) {
    case ArrowComputeResultType::Array: {
      // in_array_.reset();
    } break;
    case ArrowComputeResultType::ExtraArray: {
      // in_ext_array_.reset();
    } break;
    case ArrowComputeResultType::ArrayList: {
      in_array_list_.clear();
    } break;
    case ArrowComputeResultType::Batch: {
      in_batch_.clear();
    } break;
    case ArrowComputeResultType::BatchList: {
      in_batch_array_.clear();
      in_batch_size_array_.clear();
    } break;
    default:
      break;
  }
  in_fields_.clear();
  switch (return_type_) {
    case ArrowComputeResultType::Array: {
      // result_array_.reset();
    } break;
    case ArrowComputeResultType::ExtraArray: {
      // dict_ext_array_.reset();
    } break;
    case ArrowComputeResultType::ArrayList: {
      result_array_list_.clear();
    } break;
    case ArrowComputeResultType::Batch: {
      result_batch_.clear();
    } break;
    case ArrowComputeResultType::BatchList: {
      result_batch_list_.clear();
      result_batch_size_list_.clear();
    } break;
    default:
      break;
  }
  result_fields_.clear();

  return_type_ = ArrowComputeResultType::None;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetColumnAndFieldByName(
    std::shared_ptr<arrow::RecordBatch> in, std::shared_ptr<arrow::Schema> schema,
    std::string col_name, std::shared_ptr<arrow::Array>* out,
    std::shared_ptr<arrow::Field>* out_field) {
  auto col = in->GetColumnByName(col_name);
  auto field = schema->GetFieldByName(col_name);
  if (!col) {
    return arrow::Status::Invalid("ExprVisitor: ", col_name, " doesn't exist in batch.");
  }
  if (!field) {
    return arrow::Status::Invalid("ExprVisitor: ", col_name, " doesn't exist in schema.");
  }
  *out = col;
  *out_field = field;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetColumnAndFieldByName(
    ArrayList in, std::vector<std::shared_ptr<arrow::Field>> field_list,
    std::string col_name, std::shared_ptr<arrow::Array>* out,
    std::shared_ptr<arrow::Field>* out_field) {
  std::shared_ptr<arrow::Array> col;
  std::shared_ptr<arrow::Field> field;
  for (int i = 0; i < field_list.size(); i++) {
    if (field_list[i]->name().compare(col_name) == 0) {
      col = in[i];
      field = field_list[i];
      break;
    }
  }
  if (!col) {
    return arrow::Status::Invalid(
        "ExprVisitor: dependency returned batch doesn't contain expected column.");
  }
  *out = col;
  *out_field = field;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Visit(const gandiva::FunctionNode& node) {
  auto desc = node.descriptor();
  auto status = arrow::Status::OK();

  if (desc->name().compare("splitArrayList") == 0) {
    switch (dependency_result_type_) {
      case ArrowComputeResultType::ExtraArray: {
        ArrayList col_list;
        for (auto col_name : param_field_names_) {
          std::shared_ptr<arrow::Array> col;
          std::shared_ptr<arrow::Field> field;
          RETURN_NOT_OK(
              GetColumnAndFieldByName(in_record_batch_, schema_, col_name, &col, &field));
          col_list.push_back(col);
          result_fields_.push_back(field);
        }
        if (!in_ext_array_) {
          return arrow::Status::Invalid("ExprVisitor splitArrayList: lacks of a dict.");
        }
        RETURN_NOT_OK(extra::SplitArrayList(
            &ctx_, col_list, in_ext_array_->dict_indices(), in_ext_array_->value_counts(),
            &result_batch_list_, &result_batch_size_list_));
        goto generateBatchList;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor aggregate: Does not support this type of input.");
    }
  }
  if (desc->name().compare("sum") == 0 || desc->name().compare("count") == 0) {
    if (param_field_names_.size() != 1) {
      return arrow::Status::Invalid(
          "ExprVisitor aggregate: expects parameter size as 1.");
    }
    auto col_name = param_field_names_[0];
    std::shared_ptr<arrow::Array> col;
    std::shared_ptr<arrow::Field> field;
    switch (dependency_result_type_) {
      case ArrowComputeResultType::None: {
        RETURN_NOT_OK(
            GetColumnAndFieldByName(in_record_batch_, schema_, col_name, &col, &field));
        result_fields_.push_back(field);

        if (desc->name().compare("sum") == 0) {
          RETURN_NOT_OK(extra::SumArray(&ctx_, col, &result_array_));
        } else if (desc->name().compare("count") == 0) {
          RETURN_NOT_OK(extra::CountArray(&ctx_, col, &result_array_));
        }
        goto generateArray;
      } break;
      case ArrowComputeResultType::BatchList: {
        bool first_batch = true;
        for (auto batch : in_batch_array_) {
          if (in_fields_.size() != batch.size()) {
            return arrow::Status::Invalid(
                "ExprVisitor aggregate: dependency returned batch doesn't match "
                "returned "
                "fields.");
          }
          RETURN_NOT_OK(
              GetColumnAndFieldByName(batch, in_fields_, col_name, &col, &field));
          if (first_batch) {
            result_fields_.push_back(field);
          }
          std::shared_ptr<arrow::Array> result_array;
          if (desc->name().compare("sum") == 0) {
            RETURN_NOT_OK(extra::SumArray(&ctx_, col, &result_array));
          } else if (desc->name().compare("count") == 0) {
            RETURN_NOT_OK(extra::CountArray(&ctx_, col, &result_array));
          }
          result_array_list_.push_back(result_array);
          first_batch = false;
        }
        goto generateArrayList;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor aggregate: Does not support this type of input.");
    }
  }
  if (desc->name().compare("encodeArray") == 0) {
    if (param_field_names_.size() != 1) {
      return arrow::Status::Invalid(
          "ExprVisitor encodeArray: expects parameter size as 1.");
    }
    auto col_name = param_field_names_[0];
    std::shared_ptr<arrow::Array> col;
    switch (dependency_result_type_) {
      case ArrowComputeResultType::None: {
        col = in_record_batch_->GetColumnByName(col_name);
        result_fields_.push_back(schema_->GetFieldByName(col_name));
        RETURN_NOT_OK(extra::EncodeArray(&ctx_, col, &dict_ext_array_));
        goto generateExtArray;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor encodeArray: Does not support this type of input.");
    }
  }
  goto unrecognizedFail;
generateArray:
  return_type_ = ArrowComputeResultType::Array;
  goto finish;

generateExtArray:
  return_type_ = ArrowComputeResultType::ExtraArray;
  goto finish;

generateArrayList:
  return_type_ = ArrowComputeResultType::ArrayList;
  goto finish;

generateBatch:
  return_type_ = ArrowComputeResultType::Batch;
  goto finish;

generateBatchList:
  return_type_ = ArrowComputeResultType::BatchList;
  goto finish;

finish:
  return arrow::Status::OK();

unrecognizedFail:
  return arrow::Status::NotImplemented("Function name ", desc->name(),
                                       " is not implemented yet.");
}

arrow::Status ExprVisitor::GetResult(
    std::shared_ptr<arrow::Array>* out,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields) {
  if (!result_array_) {
    return arrow::Status::Invalid(
        "ArrowComputeExprVisitor::GetResult result_array does not generated.");
  }
  *out = result_array_;
  *out_fields = result_fields_;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    std::vector<ArrayList>* out, std::vector<int>* out_sizes,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields) {
  if (result_batch_list_.empty()) {
    return arrow::Status::Invalid(
        "ArrowComputeExprVisitor::GetResult result_batch_list does not generated.");
  }
  *out = result_batch_list_;
  *out_sizes = result_batch_size_list_;
  *out_fields = result_fields_;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    ArrayList* out, std::vector<std::shared_ptr<arrow::Field>>* out_fields) {
  if (return_type_ == ArrowComputeResultType::ArrayList) {
    if (result_array_list_.empty()) {
      return arrow::Status::Invalid(
          "ArrowComputeExprVisitor::GetResult result_array_list does not generated.");
    }
    *out = result_array_list_;
  }
  if (return_type_ == ArrowComputeResultType::Batch) {
    if (result_batch_.empty()) {
      return arrow::Status::Invalid(
          "ArrowComputeExprVisitor::GetResult result_batch does not generated.");
    }
    *out = result_batch_;
  }
  *out_fields = result_fields_;

  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    std::shared_ptr<DictionaryExtArray>* out,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields) {
  if (!dict_ext_array_) {
    return arrow::Status::Invalid(
        "ArrowComputeExprVisitor::GetResult result_ext_array does not generated.");
  }
  *out = dict_ext_array_;
  *out_fields = result_fields_;
  return arrow::Status::OK();
}

}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
