#include "codegen/arrow_compute/expr_visitor.h"

#include <arrow/array.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>
#include <memory>
#include "codegen/arrow_compute/expr_visitor_impl.h"
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

  // Add a new type of Function "Action", which will not create a new expr_visitor,
  // instead, it will register itself to its dependency
  auto func_name = desc->name();
  if (func_name.compare(0, 7, "action_") == 0) {
    if (dependency) {
      if (param_names.size() != 1) {
        return arrow::Status::Invalid("BuilderVisitor Action Parameter should be one.");
      }
      RETURN_NOT_OK(dependency->AppendAction(func_name, param_names[0]));
      expr_visitor_ = dependency;
#ifdef DEBUG
      std::cout << "Build ExprVisitor for " << node_id_ << ", return ExprVisitor is "
                << expr_visitor_ << std::endl;
#endif
      return arrow::Status::OK();
    } else {
      return arrow::Status::Invalid(
          "BuilderVisitor is processing an action without dependency, this is invalid.");
    }
  }
  // Get or insert exprVisitor
  auto search = expr_visitor_cache_->find(node_id_);
  if (search == expr_visitor_cache_->end()) {
    if (dependency) {
      RETURN_NOT_OK(ExprVisitor::Make(schema_, node.descriptor()->name(), param_names,
                                      dependency, finish_func_, &expr_visitor_));
    } else {
      RETURN_NOT_OK(ExprVisitor::Make(schema_, node.descriptor()->name(), param_names,
                                      nullptr, finish_func_, &expr_visitor_));
    }
    expr_visitor_cache_->insert(
        std::pair<std::string, std::shared_ptr<ExprVisitor>>(node_id_, expr_visitor_));
#ifdef DEBUG
    std::cout << "Build ExprVisitor for " << node_id_ << ", return ExprVisitor is "
              << expr_visitor_ << std::endl;
#endif
    return arrow::Status::OK();
  }
  expr_visitor_ = search->second;
#ifdef DEBUG
  std::cout << "Build ExprVisitor for " << node_id_ << ", return ExprVisitor is "
            << expr_visitor_ << std::endl;
#endif

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

//////////////////////// ExprVisitor ////////////////////////
arrow::Status ExprVisitor::Make(std::shared_ptr<arrow::Schema> schema_ptr,
                                std::string func_name,
                                std::vector<std::string> param_field_names,
                                std::shared_ptr<ExprVisitor> dependency,
                                std::shared_ptr<gandiva::Node> finish_func,
                                std::shared_ptr<ExprVisitor>* out) {
  auto expr = std::make_shared<ExprVisitor>(schema_ptr, func_name, param_field_names,
                                            dependency, finish_func);
  RETURN_NOT_OK(expr->MakeExprVisitorImpl(func_name, expr.get()));
  *out = expr;
  return arrow::Status::OK();
}

ExprVisitor::ExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr, std::string func_name,
                         std::vector<std::string> param_field_names,
                         std::shared_ptr<ExprVisitor> dependency,
                         std::shared_ptr<gandiva::Node> finish_func)
    : schema_(schema_ptr), func_name_(func_name), param_field_names_(param_field_names) {
  if (dependency) {
    dependency_ = dependency;
  }
  if (finish_func) {
    finish_func_ = finish_func;
  }
}

arrow::Status ExprVisitor::MakeExprVisitorImpl(const std::string& func_name,
                                               ExprVisitor* p) {
  if (func_name.compare("splitArrayListWithAction") == 0) {
    RETURN_NOT_OK(SplitArrayListWithActionVisitorImpl::Make(p, &impl_));
    goto finish;
  }
  if (func_name.compare("sum") == 0 || func_name.compare("count") == 0 ||
      func_name.compare("unique") == 0 || func_name.compare("append") == 0) {
    RETURN_NOT_OK(AggregateVisitorImpl::Make(p, func_name, &impl_));
    goto finish;
  }
  if (func_name.compare("encodeArray") == 0) {
    RETURN_NOT_OK(EncodeVisitorImpl::Make(p, &impl_));
    goto finish;
  }
  goto unrecognizedFail;
finish:
  return arrow::Status::OK();

unrecognizedFail:
  return arrow::Status::NotImplemented("Function name ", func_name,
                                       " is not implemented yet.");
}

arrow::Status ExprVisitor::AppendAction(const std::string& func_name,
                                        const std::string& param_name) {
  action_name_list_.push_back(func_name);
  action_param_list_.push_back(param_name);
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Eval(const std::shared_ptr<arrow::RecordBatch>& in) {
  in_record_batch_ = in;
  RETURN_NOT_OK(Eval());
  return arrow::Status::OK();
}

arrow::Status ExprVisitor::Eval() {
  if (return_type_ != ArrowComputeResultType::None) {
#ifdef DEBUG_LEVEL_2
    std::cout << "ExprVisitor::Eval " << func_name_ << ", ptr " << this
              << ", already evaluated, skip" << std::endl;
#endif
    return arrow::Status::OK();
  }
#ifdef DEBUG_LEVEL_2
  std::cout << "ExprVisitor::Eval " << func_name_ << ", ptr " << this
            << ", start to check dependency" << std::endl;
#endif
  if (dependency_) {
    // if this visitor has dependency, we need to get dependency result firstly.
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
#ifdef DEBUG_LEVEL_2
  std::cout << "ExprVisitor::Eval " << func_name_ << ", ptr " << this
            << ", start to execute" << std::endl;
#endif
  // now we has dependeny result as this visitor's input.
  RETURN_NOT_OK(impl_->Eval());
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

arrow::Status ExprVisitor::Init() {
  if (dependency_) {
    RETURN_NOT_OK(dependency_->Init());
  }
  RETURN_NOT_OK(impl_->Init());
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
    RETURN_NOT_OK(ExprVisitor::Make(schema_, finish_func_name, param_field_names_,
                                    shared_from_this(), nullptr, finish_visitor));
    RETURN_NOT_OK((*finish_visitor)->Eval());
    std::shared_ptr<ExprVisitor> dummy;
    RETURN_NOT_OK((*finish_visitor)->Finish(&dummy));
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
