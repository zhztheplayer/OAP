#include <arrow/status.h>
#include <codegen/arrow_compute/expr_visitor.h>
#include <unistd.h>
#include <chrono>
#include <memory>
#include "codegen/arrow_compute/ext/kernels_ext.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {

class ExprVisitorImpl {
 public:
  ExprVisitorImpl(ExprVisitor* p) : p_(p) {}
  virtual arrow::Status Eval() {
    return arrow::Status::NotImplemented("ExprVisitorImpl Eval is abstract.");
  }

  virtual arrow::Status Init() {
    return arrow::Status::NotImplemented("ExprVisitorImpl Init is abstract.");
  }

  virtual arrow::Status Finish() {
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

 protected:
  ExprVisitor* p_;
  bool initialized_ = false;
  ArrowComputeResultType finish_return_type_;
  std::shared_ptr<extra::KernalBase> kernel_;
  arrow::Status GetColumnIdAndFieldByName(std::shared_ptr<arrow::Schema> schema,
                                          std::string col_name, int* id,
                                          std::shared_ptr<arrow::Field>* out_field) {
    *id = schema->GetFieldIndex(col_name);
    *out_field = schema->GetFieldByName(col_name);
    return arrow::Status::OK();
  }
};

//////////////////////// SplitArrayListWithActionVisitorImpl //////////////////////
class SplitArrayListWithActionVisitorImpl : public ExprVisitorImpl {
 public:
  SplitArrayListWithActionVisitorImpl(ExprVisitor* p) : ExprVisitorImpl(p) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<SplitArrayListWithActionVisitorImpl>(p);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    if (p_->action_name_list_.empty()) {
      return arrow::Status::Invalid(
          "ExprVisitor::SplitArrayListWithAction have empty action_name_list, "
          "this "
          "is invalid.");
    }
    RETURN_NOT_OK(extra::SplitArrayListWithActionKernel::Make(
        &p_->ctx_, p_->action_name_list_, &kernel_));

    for (auto col_name : p_->action_param_list_) {
      std::shared_ptr<arrow::Field> field;
      int col_id;
      RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, col_name, &col_id, &field));
      p_->result_fields_.push_back(field);
      col_id_list_.push_back(col_id);
    }
    initialized_ = true;
    return arrow::Status::OK();
  }
  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::Array: {
        ArrayList col_list;
        for (auto col_id : col_id_list_) {
          auto col = p_->in_record_batch_->column(col_id);
          col_list.push_back(col);
        }
        auto start = std::chrono::steady_clock::now();
        RETURN_NOT_OK(kernel_->Evaluate(col_list, p_->in_array_));
        auto end = std::chrono::steady_clock::now();
        p_->elapse_time_ +=
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        finish_return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "SplitArrayListWithActionVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

 private:
  std::vector<int> col_id_list_;
};

////////////////////////// AggregateVisitorImpl ///////////////////////
class AggregateVisitorImpl : public ExprVisitorImpl {
 public:
  AggregateVisitorImpl(ExprVisitor* p, std::string func_name)
      : ExprVisitorImpl(p), func_name_(func_name) {}
  static arrow::Status Make(ExprVisitor* p, std::string func_name,
                            std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<AggregateVisitorImpl>(p, func_name);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    if (func_name_.compare("sum") == 0) {
      RETURN_NOT_OK(extra::SumArrayKernel::Make(&p_->ctx_, &kernel_));
    } else if (func_name_.compare("count") == 0) {
      RETURN_NOT_OK(extra::CountArrayKernel::Make(&p_->ctx_, &kernel_));
    } else if (func_name_.compare("unique") == 0) {
      RETURN_NOT_OK(extra::UniqueArrayKernel::Make(&p_->ctx_, &kernel_));
    }
    if (p_->param_field_names_.size() != 1) {
      return arrow::Status::Invalid(
          "AggregateVisitorImpl expects param_field_name_list only contains one "
          "element.");
    }
    auto col_name = p_->param_field_names_[0];
    std::shared_ptr<arrow::Field> field;
    RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, col_name, &col_id_, &field));
    p_->result_fields_.push_back(field);
    initialized_ = true;
    return arrow::Status::OK();
  }
  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        auto col = p_->in_record_batch_->column(col_id_);
        RETURN_NOT_OK(kernel_->Evaluate(col));
        finish_return_type_ = ArrowComputeResultType::Array;
      } break;
      case ArrowComputeResultType::Array: {
        // this case is for finish_func
        if (p_->in_array_->length() < 2) {
          p_->result_array_ = p_->in_array_;
          p_->return_type_ = ArrowComputeResultType::Array;
          return arrow::Status::OK();
        }
        RETURN_NOT_OK(kernel_->Evaluate(p_->in_array_));
        finish_return_type_ = ArrowComputeResultType::Array;
      } break;
      case ArrowComputeResultType::ArrayList: {
        for (auto col : p_->in_array_list_) {
          RETURN_NOT_OK(kernel_->Evaluate(col));
        }
        finish_return_type_ = ArrowComputeResultType::Array;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "AggregateVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

 private:
  int col_id_;
  std::string func_name_;
};

////////////////////////// EncodeVisitorImpl ///////////////////////
class EncodeVisitorImpl : public ExprVisitorImpl {
 public:
  EncodeVisitorImpl(ExprVisitor* p) : ExprVisitorImpl(p) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<EncodeVisitorImpl>(p);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::EncodeArrayKernel::Make(&p_->ctx_, &kernel_));
    if (p_->param_field_names_.size() != 1) {
      return arrow::Status::Invalid(
          "EncodeVisitorImpl expects param_field_name_list only contains one "
          "element.");
    }
    auto col_name = p_->param_field_names_[0];
    std::shared_ptr<arrow::Field> field;
    RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, col_name, &col_id_, &field));
    p_->result_fields_.push_back(field);
    initialized_ = true;
    return arrow::Status::OK();
  }
  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        auto col = p_->in_record_batch_->column(col_id_);
        auto start = std::chrono::steady_clock::now();
        RETURN_NOT_OK(kernel_->Evaluate(col, &p_->result_array_));
        auto end = std::chrono::steady_clock::now();
        p_->elapse_time_ +=
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        p_->return_type_ = ArrowComputeResultType::Array;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "EncodeVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

 private:
  int col_id_;
};

}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
