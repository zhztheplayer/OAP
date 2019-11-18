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
    if (dependency) {
      expr_visitor_ =
          std::make_shared<ExprVisitor>(schema_, &node, param_names, dependency);
    } else {
      expr_visitor_ = std::make_shared<ExprVisitor>(schema_, &node, param_names);
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

//////////////////////// ExprVisitor::Impl //////////////////////
class ExprVisitor::Impl {
 public:
  Impl(ExprVisitor* p) : p_(p) {
    func_name_ = p_->func_->descriptor()->name();
    if (func_name_.compare("appendToCachedBatch") == 0) {
      auto s = extra::AppendToCacheArrayListKernel::Make(&p_->ctx_, &kernel_);
    }
  }
  ~Impl() {}
  arrow::Status SplitArrayList() {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::ExtraArray: {
        ArrayList col_list;
        for (auto col_name : p_->param_field_names_) {
          std::shared_ptr<arrow::Array> col;
          std::shared_ptr<arrow::Field> field;
          RETURN_NOT_OK(GetColumnAndFieldByName(p_->in_record_batch_, p_->schema_,
                                                col_name, &col, &field));
          col_list.push_back(col);
          p_->result_fields_.push_back(field);
        }
        if (!p_->in_ext_array_) {
          return arrow::Status::Invalid("ExprVisitor splitArrayList: lacks of a dict.");
        }
        RETURN_NOT_OK(
            extra::SplitArrayList(&p_->ctx_, col_list, p_->in_ext_array_->dict_indices(),
                                  &p_->result_batch_list_, &p_->result_batch_size_list_));
        p_->return_type_ = ArrowComputeResultType::BatchList;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor aggregate: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Aggregate(std::string func_name) {
    if (p_->param_field_names_.size() != 1) {
      return arrow::Status::Invalid(
          "ExprVisitor aggregate: expects parameter size as 1.");
    }
    auto col_name = p_->param_field_names_[0];
    std::shared_ptr<arrow::Array> col;
    std::shared_ptr<arrow::Field> field;
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        RETURN_NOT_OK(GetColumnAndFieldByName(p_->in_record_batch_, p_->schema_, col_name,
                                              &col, &field));
        p_->result_fields_.push_back(field);

        if (func_name.compare("sum") == 0) {
          RETURN_NOT_OK(extra::SumArray(&p_->ctx_, col, &(p_->result_array_)));
        } else if (func_name.compare("count") == 0) {
          RETURN_NOT_OK(extra::CountArray(&p_->ctx_, col, &(p_->result_array_)));
        }
        p_->return_type_ = ArrowComputeResultType::Array;
      } break;
      case ArrowComputeResultType::BatchList: {
        bool first_batch = true;
        for (auto batch : p_->in_batch_array_) {
          if (p_->in_fields_.size() != batch.size()) {
            return arrow::Status::Invalid(
                "ExprVisitor aggregate: dependency returned batch doesn't match "
                "returned "
                "fields.");
          }
          RETURN_NOT_OK(
              GetColumnAndFieldByName(batch, p_->in_fields_, col_name, &col, &field));
          if (first_batch) {
            p_->result_fields_.push_back(field);
          }
          std::shared_ptr<arrow::Array> result_array;
          if (func_name.compare("sum") == 0) {
            RETURN_NOT_OK(extra::SumArray(&p_->ctx_, col, &result_array));
          } else if (func_name.compare("count") == 0) {
            RETURN_NOT_OK(extra::CountArray(&p_->ctx_, col, &result_array));
          }
          p_->result_array_list_.push_back(result_array);
          first_batch = false;
        }
        p_->return_type_ = ArrowComputeResultType::ArrayList;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor aggregate: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status EncodeArray() {
    if (p_->param_field_names_.size() != 1) {
      return arrow::Status::Invalid(
          "ExprVisitor encodeArray: expects parameter size as 1.");
    }
    auto col_name = p_->param_field_names_[0];
    std::shared_ptr<arrow::Array> col;
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        col = p_->in_record_batch_->GetColumnByName(col_name);
        p_->result_fields_.push_back(p_->schema_->GetFieldByName(col_name));
        RETURN_NOT_OK(extra::EncodeArray(&p_->ctx_, col, &p_->dict_ext_array_));
        p_->return_type_ = ArrowComputeResultType::ExtraArray;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor encodeArray: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status AppendToCachedArrayList() {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        ArrayList col_list;
        for (auto col_name : p_->param_field_names_) {
          std::shared_ptr<arrow::Array> col;
          std::shared_ptr<arrow::Field> field;
          RETURN_NOT_OK(GetColumnAndFieldByName(p_->in_record_batch_, p_->schema_,
                                                col_name, &col, &field));
          col_list.push_back(col);
          p_->result_fields_.push_back(field);
          if (col->length() == 0) {
            return arrow::Status::OK();
          }
        }
        if (!kernel_) {
          return arrow::Status::NotImplemented(
              "AppendToCachedArrayList kernel is not implemented.");
        }
        RETURN_NOT_OK(kernel_->Evaluate(col_list));
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor aggregate: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() {
    if (kernel_) {
      if (p_->return_type_ == ArrowComputeResultType::None) {
        RETURN_NOT_OK(kernel_->Finish(&p_->result_batch_));
        p_->return_type_ = ArrowComputeResultType::Batch;
      }
    }
    return arrow::Status::OK();
  }

 private:
  ExprVisitor* p_;
  std::string func_name_;
  std::shared_ptr<extra::KernalBase> kernel_;
  arrow::Status GetColumnAndFieldByName(std::shared_ptr<arrow::RecordBatch> in,
                                        std::shared_ptr<arrow::Schema> schema,
                                        std::string col_name,
                                        std::shared_ptr<arrow::Array>* out,
                                        std::shared_ptr<arrow::Field>* out_field) {
    auto col = in->GetColumnByName(col_name);
    auto field = schema->GetFieldByName(col_name);
    if (!col) {
      return arrow::Status::Invalid("ExprVisitor: ", col_name,
                                    " doesn't exist in batch.");
    }
    if (!field) {
      return arrow::Status::Invalid("ExprVisitor: ", col_name,
                                    " doesn't exist in schema.");
    }
    *out = col;
    *out_field = field;
    return arrow::Status::OK();
  }

  arrow::Status GetColumnAndFieldByName(
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
};

//////////////////////// ExprVisitor ////////////////////////
ExprVisitor::ExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                         const gandiva::FunctionNode* func,
                         std::vector<std::string> param_field_names)
    : schema_(schema_ptr), func_(func), param_field_names_(param_field_names) {
  impl_.reset(new Impl(this));
}

ExprVisitor::ExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                         const gandiva::FunctionNode* func,
                         std::vector<std::string> param_field_names,
                         std::shared_ptr<ExprVisitor> dependency)
    : schema_(schema_ptr),
      func_(func),
      param_field_names_(param_field_names),
      dependency_(dependency) {
  impl_.reset(new Impl(this));
}

arrow::Status ExprVisitor::Execute() {
  auto desc = func_->descriptor();
  auto status = arrow::Status::OK();

  if (desc->name().compare("splitArrayList") == 0) {
    RETURN_NOT_OK(impl_->SplitArrayList());
    goto finish;
  }
  if (desc->name().compare("sum") == 0 || desc->name().compare("count") == 0) {
    RETURN_NOT_OK(impl_->Aggregate(desc->name()));
    goto finish;
  }
  if (desc->name().compare("encodeArray") == 0) {
    RETURN_NOT_OK(impl_->EncodeArray());
    goto finish;
  }
  if (desc->name().compare("appendToCachedBatch") == 0) {
    RETURN_NOT_OK(impl_->AppendToCachedArrayList());
    goto finish;
  }
  goto unrecognizedFail;

finish:
  return arrow::Status::OK();

unrecognizedFail:
  return arrow::Status::NotImplemented("Function name ", desc->name(),
                                       " is not implemented yet.");
}

arrow::Status ExprVisitor::Eval(const std::shared_ptr<arrow::RecordBatch>& in) {
  // leaf dependency should be process firstly, or we should visit expr cache for
  // previous result.
  in_record_batch_ = in;
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
  RETURN_NOT_OK(Execute());
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

ArrowComputeResultType ExprVisitor::GetResultType() {
  auto status = impl_->Finish();
  if (!status.ok()) {
    return ArrowComputeResultType::None;
  }
  return return_type_;
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
