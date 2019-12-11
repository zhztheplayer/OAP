#include <arrow/status.h>
#include <codegen/arrow_compute/expr_visitor.h>
#include <unistd.h>
#include <chrono>
#include <memory>
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/common/result_iterator.h"
#include "utils/macros.h"

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

  virtual arrow::Status SetMember() {
    return arrow::Status::NotImplemented("ExprVisitorImpl Init is abstract.");
  }

  virtual arrow::Status Finish() {
#ifdef DEBUG
    std::cout << "ExprVisitorImpl::Finish visitor is " << p_->func_name_ << ", ptr is "
              << p_ << std::endl;
#endif
    return arrow::Status::OK();
  }

  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented("ExprVisitorImpl ", p_->func_name_,
                                         " MakeResultIterator is abstract.");
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
    if (*id < 0) {
      return arrow::Status::Invalid("GetColumnIdAndFieldByName doesn't found col_name ",
                                    col_name);
    }
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

    std::vector<std::shared_ptr<arrow::DataType>> type_list;
    for (auto col_name : p_->action_param_list_) {
      std::shared_ptr<arrow::Field> field;
      int col_id;
      RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, col_name, &col_id, &field));
      p_->result_fields_.push_back(field);
      col_id_list_.push_back(col_id);
      type_list.push_back(field->type());
    }
    RETURN_NOT_OK(extra::SplitArrayListWithActionKernel::Make(
        &p_->ctx_, p_->action_name_list_, type_list, &kernel_));
    initialized_ = true;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::Array: {
        ArrayList col_list;
        for (auto col_id : col_id_list_) {
          if (col_id >= p_->in_record_batch_->num_columns()) {
            return arrow::Status::Invalid(
                "SplitArrayListWithActionVisitorImpl Eval col_id is bigger than input "
                "batch numColumns.");
          }
          auto col = p_->in_record_batch_->column(col_id);
          col_list.push_back(col);
        }
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(col_list, p_->in_array_));
        finish_return_type_ = ArrowComputeResultType::Batch;
        p_->dependency_result_type_ = ArrowComputeResultType::None;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "SplitArrayListWithActionVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    RETURN_NOT_OK(ExprVisitorImpl::Finish());
    switch (finish_return_type_) {
      case ArrowComputeResultType::Batch: {
        RETURN_NOT_OK(kernel_->Finish(&p_->result_batch_));
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default: {
        return arrow::Status::NotImplemented(
            "SplitArrayListWithActionVisitorImpl only support finish_return_type as "
            "Batch.");
        break;
      }
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
    } else if (func_name_.compare("append") == 0) {
      RETURN_NOT_OK(extra::AppendArrayKernel::Make(&p_->ctx_, &kernel_));
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
        if (col_id_ >= p_->in_record_batch_->num_columns()) {
          return arrow::Status::Invalid(
              "AggregateVisitorImpl Eval col_id is bigger than input "
              "batch numColumns.");
        }
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
        p_->dependency_result_type_ = ArrowComputeResultType::None;
      } break;
      case ArrowComputeResultType::ArrayList: {
        for (auto col : p_->in_array_list_) {
          RETURN_NOT_OK(kernel_->Evaluate(col));
        }
        finish_return_type_ = ArrowComputeResultType::Array;
        p_->dependency_result_type_ = ArrowComputeResultType::None;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "AggregateVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    RETURN_NOT_OK(ExprVisitorImpl::Finish());
    switch (finish_return_type_) {
      case ArrowComputeResultType::Array: {
        RETURN_NOT_OK(kernel_->Finish(&p_->result_array_));
        p_->return_type_ = ArrowComputeResultType::Array;
      } break;
      default: {
        return arrow::Status::NotImplemented(
            "AggregateVisitorImpl only support finish_return_type as "
            "Array.");
        break;
      }
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
        if (col_id_ >= p_->in_record_batch_->num_columns()) {
          return arrow::Status::Invalid(
              "EncodeVisitorImpl Eval col_id is bigger than input "
              "batch numColumns.");
        }
        auto col = p_->in_record_batch_->column(col_id_);
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Evaluate(col, &p_->result_array_));
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

////////////////////////// ProbeVisitorImpl ///////////////////////
class ProbeVisitorImpl : public ExprVisitorImpl {
 public:
  ProbeVisitorImpl(ExprVisitor* p) : ExprVisitorImpl(p) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<ProbeVisitorImpl>(p);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::ProbeArrayKernel::Make(&p_->ctx_, &kernel_));
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
        RETURN_NOT_OK(kernel_->Evaluate(col));
        auto end = std::chrono::steady_clock::now();
        p_->elapse_time_ +=
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        finish_return_type_ = ArrowComputeResultType::Array;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "EncodeVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }
  arrow::Status SetMember() override {
    if (!initialized_) {
      return arrow::Status::Invalid("Kernel is not initialized");
    }
    RETURN_NOT_OK(kernel_->SetMember(p_->member_record_batch_));
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    RETURN_NOT_OK(ExprVisitorImpl::Finish());
    switch (finish_return_type_) {
      case ArrowComputeResultType::Array: {
        RETURN_NOT_OK(kernel_->Finish(&p_->result_array_));
        p_->return_type_ = ArrowComputeResultType::Array;
      } break;
      default: {
        return arrow::Status::NotImplemented(
            "AggregateVisitorImpl only support finish_return_type as "
            "Array.");
        break;
      }
    }
    return arrow::Status::OK();
  }

 private:
  int col_id_;
};

////////////////////////// TakeVisitorImpl ///////////////////////
class TakeVisitorImpl : public ExprVisitorImpl {
 public:
  TakeVisitorImpl(ExprVisitor* p) : ExprVisitorImpl(p) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<TakeVisitorImpl>(p);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::TakeArrayKernel::Make(&p_->ctx_, &kernel_));
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
        RETURN_NOT_OK(kernel_->Evaluate(col));
        auto end = std::chrono::steady_clock::now();
        p_->elapse_time_ +=
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        finish_return_type_ = ArrowComputeResultType::Array;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "EncodeVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }
  arrow::Status SetMember() override {
    if (!initialized_) {
      return arrow::Status::Invalid("Kernel is not initialized");
    }
    RETURN_NOT_OK(kernel_->SetMember(p_->member_record_batch_));
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    RETURN_NOT_OK(ExprVisitorImpl::Finish());
    switch (finish_return_type_) {
      case ArrowComputeResultType::Array: {
        RETURN_NOT_OK(kernel_->Finish(&p_->result_array_));
        p_->return_type_ = ArrowComputeResultType::Array;
      } break;
      default: {
        return arrow::Status::NotImplemented(
            "AggregateVisitorImpl only support finish_return_type as "
            "Array.");
        break;
      }
    }
    return arrow::Status::OK();
  }

 private:
  int col_id_;
};
////////////////////////// NTakeVisitorImpl ///////////////////////
class NTakeVisitorImpl : public ExprVisitorImpl {
 public:
  NTakeVisitorImpl(ExprVisitor* p) : ExprVisitorImpl(p) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<NTakeVisitorImpl>(p);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::NTakeArrayKernel::Make(&p_->ctx_, &kernel_));
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
        RETURN_NOT_OK(kernel_->Evaluate(col));
        auto end = std::chrono::steady_clock::now();
        p_->elapse_time_ +=
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        finish_return_type_ = ArrowComputeResultType::Array;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "EncodeVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }
  arrow::Status SetMember() override {
    if (!initialized_) {
      return arrow::Status::Invalid("Kernel is not initialized");
    }
    RETURN_NOT_OK(kernel_->SetMember(p_->member_record_batch_));
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    RETURN_NOT_OK(ExprVisitorImpl::Finish());
    switch (finish_return_type_) {
      case ArrowComputeResultType::Array: {
        RETURN_NOT_OK(kernel_->Finish(&p_->result_array_));
        p_->return_type_ = ArrowComputeResultType::Array;
      } break;
      default: {
        return arrow::Status::NotImplemented(
            "AggregateVisitorImpl only support finish_return_type as "
            "Array.");
        break;
      }
    }
    return arrow::Status::OK();
  }

 private:
  int col_id_;
};
////////////////////////// SortArraysToIndicesVisitorImpl ///////////////////////
class SortArraysToIndicesVisitorImpl : public ExprVisitorImpl {
 public:
  SortArraysToIndicesVisitorImpl(ExprVisitor* p) : ExprVisitorImpl(p) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<SortArraysToIndicesVisitorImpl>(p);
    *out = impl;
    return arrow::Status::OK();
  }
  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }
    RETURN_NOT_OK(extra::SortArraysToIndicesKernel::Make(&p_->ctx_, &kernel_));
    if (p_->param_field_names_.size() != 1) {
      return arrow::Status::Invalid(
          "SortArraysToIndicesVisitorImpl expects param_field_name_list only "
          "contains "
          "one element.");
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
        if (col_id_ >= p_->in_record_batch_->num_columns()) {
          return arrow::Status::Invalid(
              "SortArraysToIndicesVisitorImpl Eval col_id is bigger than input "
              "batch numColumns.");
        }
        auto col = p_->in_record_batch_->column(col_id_);
        RETURN_NOT_OK(kernel_->Evaluate(col));
        finish_return_type_ = ArrowComputeResultType::Array;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "SortArraysToIndicesVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    RETURN_NOT_OK(ExprVisitorImpl::Finish());
    switch (finish_return_type_) {
      case ArrowComputeResultType::Array: {
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Finish(&p_->result_array_));
        p_->return_type_ = ArrowComputeResultType::Array;
      } break;
      default: {
        return arrow::Status::NotImplemented(
            "SortArraysToIndicesVisitorImpl only support finish_return_type as "
            "Array.");
        break;
      }
    }
    return arrow::Status::OK();
  }

 private:
  int col_id_;
};

//////////////////////// ShuffleArrayListVisitorImpl //////////////////////
class ShuffleArrayListVisitorImpl : public ExprVisitorImpl {
 public:
  ShuffleArrayListVisitorImpl(ExprVisitor* p) : ExprVisitorImpl(p) {}
  static arrow::Status Make(ExprVisitor* p, std::shared_ptr<ExprVisitorImpl>* out) {
    auto impl = std::make_shared<ShuffleArrayListVisitorImpl>(p);
    *out = impl;
    return arrow::Status::OK();
  }

  arrow::Status Init() override {
    if (initialized_) {
      return arrow::Status::OK();
    }

    std::vector<std::shared_ptr<arrow::DataType>> type_list;
    for (auto col_name : p_->param_field_names_) {
      std::shared_ptr<arrow::Field> field;
      int col_id;
      RETURN_NOT_OK(GetColumnIdAndFieldByName(p_->schema_, col_name, &col_id, &field));
      p_->result_fields_.push_back(field);
      col_id_list_.push_back(col_id);
      type_list.push_back(field->type());
    }

    RETURN_NOT_OK(extra::ShuffleArrayListKernel::Make(&p_->ctx_, type_list, &kernel_));

    initialized_ = true;
    return arrow::Status::OK();
  }

  arrow::Status Eval() override {
    switch (p_->dependency_result_type_) {
      case ArrowComputeResultType::None: {
        // This indicates shuffle indices was not cooked yet.
        ArrayList col_list;
        for (auto col_id : col_id_list_) {
          if (col_id >= p_->in_record_batch_->num_columns()) {
            return arrow::Status::Invalid(
                "ShuffleArrayListVisitorImpl Eval col_id is bigger than input "
                "batch numColumns.");
          }
          auto col = p_->in_record_batch_->column(col_id);
          col_list.push_back(col);
        }
        RETURN_NOT_OK(kernel_->Evaluate(col_list));
        finish_return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::NotImplemented(
            "ShuffleArrayListVisitorImpl: Does not support this type of input.");
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish() override {
    RETURN_NOT_OK(ExprVisitorImpl::Finish());
    // override ExprVisitorImpl Finish
    if (!p_->in_array_) {
      return arrow::Status::Invalid(
          "ShuffleArrayListVisitorImpl depends on an indices array to indicate "
          "shuffle, "
          "while input_array is invalid.");
    }
    RETURN_NOT_OK(kernel_->SetDependencyInput(p_->in_array_));
    switch (finish_return_type_) {
      case ArrowComputeResultType::Batch: {
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->Finish(&p_->result_batch_));
        p_->return_type_ = ArrowComputeResultType::Batch;
      } break;
      default:
        return arrow::Status::Invalid(
            "ShuffleArrayListVisitorImpl Finish does not support dependency type "
            "other "
            "than Batch and BatchList.");
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    if (!p_->in_array_) {
      return arrow::Status::Invalid(
          "ShuffleArrayListVisitorImpl depends on an indices array to indicate "
          "shuffle, "
          "while input_array is invalid.");
    }
    RETURN_NOT_OK(kernel_->SetDependencyInput(p_->in_array_));
    switch (finish_return_type_) {
      case ArrowComputeResultType::Batch: {
        TIME_MICRO_OR_RAISE(p_->elapse_time_, kernel_->MakeResultIterator(schema, out));
        p_->return_type_ = ArrowComputeResultType::BatchIterator;
      } break;
      default:
        return arrow::Status::Invalid(
            "ShuffleArrayListVisitorImpl Finish does not support dependency type "
            "other "
            "than Batch and BatchList.");
    }
    return arrow::Status::OK();
  }

 private:
  std::vector<int> col_id_list_;
};

}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
