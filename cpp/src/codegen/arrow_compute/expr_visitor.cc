#include "codegen/arrow_compute/expr_visitor.h"

#include <arrow/array.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gandiva/node.h>
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

arrow::Status MakeExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                              std::shared_ptr<gandiva::Expression> expr,
                              std::shared_ptr<gandiva::Expression> finish_expr,
                              ExprVisitorMap* expr_visitor_cache,
                              std::shared_ptr<ExprVisitor>* out) {
  auto visitor = std::make_shared<BuilderVisitor>(
      schema_ptr, expr->root(), finish_expr->root(), expr_visitor_cache);
  RETURN_NOT_OK(visitor->Eval());
  RETURN_NOT_OK(visitor->GetResult(out));
  return arrow::Status::OK();
}
arrow::Status BuilderVisitor::Visit(const gandiva::FieldNode& node) {
  node_id_ = node.field()->name();
  node_type_ = BuilderVisitorNodeType::FieldNode;
  return arrow::Status::OK();
}

arrow::Status BuilderVisitor::Visit(const gandiva::FunctionNode& node) {
  auto desc = node.descriptor();
  node_id_ = desc->name();
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
        node_id_.append(child_visitor->GetResult());
      } break;
      case BuilderVisitorNodeType::FieldNode: {
        std::string col_name = child_visitor->GetResult();
        node_id_.append(col_name);
        param_names.push_back(col_name);
      } break;
      default:
        return arrow::Status::Invalid("BuilderVisitorNodeType is invalid");
    }
  }

  auto search = expr_visitor_cache_->find(node_id_);
  if (search == expr_visitor_cache_->end()) {
    if (dependency) {
      expr_visitor_ = std::make_shared<ExprVisitor>(
          schema_, node.descriptor()->name(), param_names, dependency, finish_func_);
    } else {
      expr_visitor_ = std::make_shared<ExprVisitor>(schema_, node.descriptor()->name(),
                                                    param_names, finish_func_);
    }
    expr_visitor_cache_->emplace(node_id_, expr_visitor_);
    return arrow::Status::OK();
  }
  expr_visitor_ = search->second;

  return arrow::Status::OK();
}

std::string BuilderVisitor::GetResult() { return node_id_; }

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
  Impl(ExprVisitor* p) : p_(p) { func_name_ = p_->func_name_; }
  ~Impl() {}
  arrow::Status SplitArrayList() {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::Array: {
        ArrayList col_list;
        for (auto col_name : p_->param_field_names_) {
          std::shared_ptr<arrow::Array> col;
          std::shared_ptr<arrow::Field> field;
          RETURN_NOT_OK(GetColumnAndFieldByName(p_->in_record_batch_, p_->schema_,
                                                col_name, &col, &field));
          col_list.push_back(col);
          p_->result_fields_.push_back(field);
        }
        RETURN_NOT_OK(extra::SplitArrayList(
            &p_->ctx_, col_list, p_->in_array_, &p_->result_batch_list_,
            &p_->result_batch_size_list_, &p_->group_indices_));
        p_->return_type_ = ArrowComputeResultType::BatchList;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor aggregate: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Aggregate(std::string func_name) {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        if (p_->param_field_names_.size() != 1) {
          return arrow::Status::Invalid(
              "ExprVisitor aggregate: expects parameter size as 1.");
        }
        auto col_name = p_->param_field_names_[0];
        std::shared_ptr<arrow::Array> col;
        std::shared_ptr<arrow::Field> field;
        RETURN_NOT_OK(GetColumnAndFieldByName(p_->in_record_batch_, p_->schema_, col_name,
                                              &col, &field));
        p_->result_fields_.push_back(field);

        if (func_name.compare("sum") == 0) {
          RETURN_NOT_OK(extra::SumArray(&p_->ctx_, col, &(p_->result_array_)));
        } else if (func_name.compare("count") == 0) {
          RETURN_NOT_OK(extra::CountArray(&p_->ctx_, col, &(p_->result_array_)));
        } else if (func_name.compare("unique") == 0) {
          RETURN_NOT_OK(extra::UniqueArray(&p_->ctx_, col, &(p_->result_array_)));
        }
        p_->return_type_ = ArrowComputeResultType::Array;
      } break;
      case ArrowComputeResultType::ArrayList: {
        p_->result_fields_ = p_->in_fields_;
        for (auto col : p_->in_array_list_) {
          std::shared_ptr<arrow::Array> result_array;
          if (func_name.compare("sum") == 0) {
            RETURN_NOT_OK(extra::SumArray(&p_->ctx_, col, &result_array));
          } else if (func_name.compare("count") == 0) {
            RETURN_NOT_OK(extra::CountArray(&p_->ctx_, col, &result_array));
          } else if (func_name.compare("unique") == 0) {
            RETURN_NOT_OK(extra::UniqueArray(&p_->ctx_, col, &result_array));
          }
          p_->result_array_list_.push_back(result_array);
        }
        p_->return_type_ = ArrowComputeResultType::ArrayList;
      } break;
      case ArrowComputeResultType::BatchList: {
        if (p_->param_field_names_.size() != 1) {
          return arrow::Status::Invalid(
              "ExprVisitor aggregate: expects parameter size as 1.");
        }
        auto col_name = p_->param_field_names_[0];
        std::shared_ptr<arrow::Array> col;
        std::shared_ptr<arrow::Field> field;
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
          } else if (func_name.compare("unique") == 0) {
            RETURN_NOT_OK(extra::UniqueArray(&p_->ctx_, col, &result_array));
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
        if (!kernel_) {
          RETURN_NOT_OK(extra::EncodeArrayKernel::Make(&p_->ctx_, &kernel_));
        }
        RETURN_NOT_OK(kernel_->Evaluate(col, &p_->result_array_));
        p_->return_type_ = ArrowComputeResultType::Array;
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
          if (p_->result_fields_.size() < p_->param_field_names_.size()) {
            p_->result_fields_.push_back(field);
          }
          if (col->length() == 0) {
            return arrow::Status::OK();
          }
        }
        if (!kernel_) {
          RETURN_NOT_OK(extra::AppendToCacheArrayListKernel::Make(&p_->ctx_, &kernel_));
        }
        RETURN_NOT_OK(kernel_->Evaluate(col_list));
        finish_return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor AppendToCachedArrayList: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status AppendToCachedArray() {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::Array: {
        auto col = p_->in_array_;
        p_->result_fields_ = p_->in_fields_;

        if (col->length() == 0) {
          return arrow::Status::OK();
        }
        if (!kernel_) {
          RETURN_NOT_OK(extra::AppendToCacheArrayKernel::Make(&p_->ctx_, &kernel_));
        }
        RETURN_NOT_OK(kernel_->Evaluate(col));
        finish_return_type_ = ArrowComputeResultType::Array;
      } break;
      case ArrowComputeResultType::ArrayList: {
        p_->result_fields_ = p_->in_fields_;
        if (p_->group_indices_.size() != p_->in_array_list_.size()) {
          return arrow::Status::Invalid(
              "ExprVisitor::Impl AppendToCachedArray group_indices size does not match "
              "with ArrayList size.");
        }
        for (int i = 0; i < p_->in_array_list_.size(); i++) {
          auto batch_index = p_->group_indices_[i];
          auto col = p_->in_array_list_[i];
          if (col->length() == 0) {
            continue;
          }
          if (!kernel_) {
            RETURN_NOT_OK(extra::AppendToCacheArrayKernel::Make(&p_->ctx_, &kernel_));
          }
          RETURN_NOT_OK(kernel_->Evaluate(col, batch_index));
        }
        finish_return_type_ = ArrowComputeResultType::ArrayList;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ExprVisitor AppendToCachedArray: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() {
    if (kernel_ && p_->return_type_ == ArrowComputeResultType::None) {
      switch (finish_return_type_) {
        case ArrowComputeResultType::Batch: {
          RETURN_NOT_OK(kernel_->Finish(&p_->result_batch_));
          p_->return_type_ = ArrowComputeResultType::Batch;
        } break;
        case ArrowComputeResultType::Array: {
          RETURN_NOT_OK(kernel_->Finish(&p_->result_array_));
          p_->return_type_ = ArrowComputeResultType::Array;
        } break;
        case ArrowComputeResultType::ArrayList: {
          RETURN_NOT_OK(kernel_->Finish(&p_->result_array_list_));
          p_->return_type_ = ArrowComputeResultType::ArrayList;
        } break;
        case ArrowComputeResultType::BatchList: {
          return arrow::Status::NotImplemented(
              "ExprVisitor Finish not support BatchList");
        } break;
        default:
          break;
      }
    }
    return arrow::Status::OK();
  }

 private:
  ExprVisitor* p_;
  std::string func_name_;
  std::shared_ptr<extra::KernalBase> kernel_;
  ArrowComputeResultType finish_return_type_;
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
ExprVisitor::ExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr, std::string func_name,
                         std::vector<std::string> param_field_names,
                         std::shared_ptr<gandiva::Node> finish_func)
    : schema_(schema_ptr),
      func_name_(func_name),
      param_field_names_(param_field_names),
      finish_func_(finish_func) {
  impl_ = std::make_shared<Impl>(this);
}

ExprVisitor::ExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr, std::string func_name,
                         std::vector<std::string> param_field_names,
                         std::shared_ptr<ExprVisitor> dependency,
                         std::shared_ptr<gandiva::Node> finish_func)
    : schema_(schema_ptr),
      func_name_(func_name),
      param_field_names_(param_field_names),
      finish_func_(finish_func) {
  dependency_ = dependency;
  impl_ = std::make_shared<Impl>(this);
}

ExprVisitor::ExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr, std::string func_name,
                         std::vector<std::string> param_field_names,
                         std::shared_ptr<ExprVisitor> dependency)
    : schema_(schema_ptr), func_name_(func_name), param_field_names_(param_field_names) {
  dependency_ = dependency;
  impl_ = std::make_shared<Impl>(this);
}

arrow::Status ExprVisitor::Execute() {
  auto status = arrow::Status::OK();

  if (func_name_.compare("splitArrayList") == 0) {
    RETURN_NOT_OK(impl_->SplitArrayList());
    goto finish;
  }
  if (func_name_.compare("sum") == 0 || func_name_.compare("count") == 0 ||
      func_name_.compare("unique") == 0) {
    RETURN_NOT_OK(impl_->Aggregate(func_name_));
    goto finish;
  }
  if (func_name_.compare("encodeArray") == 0) {
    RETURN_NOT_OK(impl_->EncodeArray());
    goto finish;
  }
  if (func_name_.compare("appendToCachedBatch") == 0) {
    RETURN_NOT_OK(impl_->AppendToCachedArrayList());
    goto finish;
  }
  if (func_name_.compare("appendToCachedArray") == 0) {
    RETURN_NOT_OK(impl_->AppendToCachedArray());
    goto finish;
  }
  goto unrecognizedFail;

finish:
  return arrow::Status::OK();

unrecognizedFail:
  return arrow::Status::NotImplemented("Function name ", func_name_,
                                       " is not implemented yet.");
}

arrow::Status ExprVisitor::Eval(const std::shared_ptr<arrow::RecordBatch>& in) {
  in_record_batch_ = in;
  RETURN_NOT_OK(Eval());
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Eval() {
  if (return_type_ != ArrowComputeResultType::None) {
#ifdef DEBUG
    std::cout << "ExprVisitor::Eval " << func_name_ << ", already evaluated, skip"
              << std::endl;
#endif
    return arrow::Status::OK();
  }
#ifdef DEBUG
  std::cout << "ExprVisitor::Eval " << func_name_ << ", start to check dependency"
            << std::endl;
#endif
  if (dependency_) {
    RETURN_NOT_OK(dependency_->Eval(in_record_batch_));
    dependency_result_type_ = dependency_->GetResultType();
    switch (dependency_result_type_) {
      case ArrowComputeResultType::BatchList: {
        RETURN_NOT_OK(dependency_->GetResult(&in_batch_array_, &in_batch_size_array_,
                                             &in_fields_, &group_indices_));
      } break;
      case ArrowComputeResultType::Batch: {
        RETURN_NOT_OK(dependency_->GetResult(&in_batch_, &in_fields_, &group_indices_));
      } break;
      case ArrowComputeResultType::ArrayList: {
        RETURN_NOT_OK(
            dependency_->GetResult(&in_array_list_, &in_fields_, &group_indices_));
      } break;
      case ArrowComputeResultType::Array: {
        RETURN_NOT_OK(dependency_->GetResult(&in_array_, &in_fields_, &group_indices_));
      } break;
      default:
        return arrow::Status::Invalid("ArrowComputeResultType is invalid.");
    }
  }
#ifdef DEBUG
  std::cout << "ExprVisitor::Eval " << func_name_ << ", start to execute" << std::endl;
#endif
  RETURN_NOT_OK(Execute());
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::ResetDependency() {
  if (dependency_) {
    RETURN_NOT_OK(dependency_->Reset());
  }
  switch (dependency_result_type_) {
    case ArrowComputeResultType::Array: {
      // in_array_.reset();
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
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Reset() {
  RETURN_NOT_OK(ResetDependency());
  switch (return_type_) {
    case ArrowComputeResultType::Array: {
      // result_array_.reset();
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

arrow::Status ExprVisitor::Finish(std::shared_ptr<ExprVisitor>* finish_visitor) {
  RETURN_NOT_OK(impl_->Finish());
  // call finish_func_ here.
  if (finish_func_) {
    std::string finish_func_name =
        std::dynamic_pointer_cast<gandiva::FunctionNode>(finish_func_)
            ->descriptor()
            ->name();
#ifdef DEBUG
    std::cout << "ExprVisitor::Finish call finish func " << finish_func_name << std::endl;
#endif
    *finish_visitor = std::make_shared<ExprVisitor>(
        schema_, finish_func_name, param_field_names_, shared_from_this());
    RETURN_NOT_OK((*finish_visitor)->Eval());
  }

  return arrow::Status::OK();
}

ArrowComputeResultType ExprVisitor::GetResultType() { return return_type_; }
arrow::Status ExprVisitor::GetResult(
    std::shared_ptr<arrow::Array>* out,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields) {
  if (!result_array_) {
    return arrow::Status::Invalid(
        "ArrowComputeExprVisitor::GetResult result_array was not generated ", func_name_);
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
        "ArrowComputeExprVisitor::GetResult result_batch_list was not generated ",
        func_name_);
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
          "ArrowComputeExprVisitor::GetResult result_array_list was not generated ",
          func_name_);
    }
    *out = result_array_list_;
  }
  if (return_type_ == ArrowComputeResultType::Batch) {
    if (result_batch_.empty()) {
      return arrow::Status::Invalid(
          "ArrowComputeExprVisitor::GetResult result_batch was not generated ",
          func_name_);
    }
    *out = result_batch_;
  }
  *out_fields = result_fields_;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    std::shared_ptr<arrow::Array>* out,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields,
    std::vector<int>* group_indices) {
  RETURN_NOT_OK(GetResult(out, out_fields));
  *group_indices = group_indices_;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    std::vector<ArrayList>* out, std::vector<int>* out_sizes,
    std::vector<std::shared_ptr<arrow::Field>>* out_fields,
    std::vector<int>* group_indices) {
  RETURN_NOT_OK(GetResult(out, out_sizes, out_fields));
  *group_indices = group_indices_;
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::GetResult(
    ArrayList* out, std::vector<std::shared_ptr<arrow::Field>>* out_fields,
    std::vector<int>* group_indices) {
  RETURN_NOT_OK(GetResult(out, out_fields));
  *group_indices = group_indices_;
  return arrow::Status::OK();
}

}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
