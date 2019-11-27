#pragma once

#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/checked_cast.h>
#include <memory>
#include "codegen/arrow_compute/ext/array_builder_impl.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_signed_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_unsigned_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_floating_point<I>> {
  using Type = arrow::DoubleType;
};

class ActionBase {
 public:
  virtual arrow::Status SetInputArray(const std::shared_ptr<arrow::Array>& in) {
    return arrow::Status::NotImplemented("ActionBase Eval is abstract.");
  }
  virtual arrow::Status ConfigureGroupSize(int max_group_id,
                                           const std::shared_ptr<arrow::Array>& in) {
    return arrow::Status::NotImplemented("ActionBase Eval is abstract.");
  }
  virtual arrow::Status Eval(int src_row_id, int dest_group_id) {
    return arrow::Status::NotImplemented("ActionBase Eval is abstract.");
  }
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("ActionBase Eval is abstract.");
  }
};

template <typename ArrayType, typename DataType,
          typename ResArrayType = typename arrow::TypeTraits<DataType>::ArrayType>
class UniqueAction : public ActionBase {
 public:
  UniqueAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~UniqueAction() {
#ifdef DEBUG
    std::cout << "Destruct UniqueAction" << std::endl;
#endif
  }

  arrow::Status SetInputArray(const std::shared_ptr<arrow::Array>& in) override {
    return arrow::Status::OK();
  }

  arrow::Status ConfigureGroupSize(int max_group_id,
                                   const std::shared_ptr<arrow::Array>& in) override {
    result_ = in;
    return arrow::Status::OK();
  }

  arrow::Status Eval(int src_row_id, int dest_group_id) override {
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    *out = result_;
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<arrow::Array> result_;
  arrow::compute::FunctionContext* ctx_;
};

template <typename ArrayType, typename DataType,
          typename ResArrayType = typename arrow::TypeTraits<DataType>::ArrayType>
class CountAction : public ActionBase {
 public:
  CountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~CountAction() {
#ifdef DEBUG
    std::cout << "Destruct CountAction" << std::endl;
#endif
  }

  arrow::Status SetInputArray(const std::shared_ptr<arrow::Array>& in) override {
    in_ = std::dynamic_pointer_cast<ArrayType>(in);
    if (!builder_) {
      MakeArrayBuilder<ResArrayType, DataType>(
          arrow::TypeTraits<DataType>::type_singleton(), ctx_->memory_pool(), &builder_);
    }
    return arrow::Status::OK();
  }

  arrow::Status ConfigureGroupSize(int max_group_id,
                                   const std::shared_ptr<arrow::Array>& in) override {
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }
    return arrow::Status::OK();
  }

  arrow::Status Eval(int src_row_id, int dest_group_id) override {
    cache_validity_[dest_group_id] = true;
    if (!in_->IsNull(src_row_id)) {
      cache_[dest_group_id] += 1;
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    int group_id = 0;
    for (int i = 0; i < cache_validity_.size(); i++) {
      if (cache_validity_[i]) {
        auto value = cache_[i];
        RETURN_NOT_OK(builder_->template AppendValue<DataType>(group_id, value));
      }
    }
    RETURN_NOT_OK(builder_->Finish(out));

    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<ArrayType> in_;
  arrow::compute::FunctionContext* ctx_;
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::shared_ptr<ArrayBuilderImpl<ResArrayType, DataType>> builder_;
};

template <typename ArrayType, typename DataType,
          typename ResArrayType = typename arrow::TypeTraits<DataType>::ArrayType>
class SumAction : public ActionBase {
 public:
  SumAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~SumAction() {
#ifdef DEBUG
    std::cout << "Destruct SumAction" << std::endl;
#endif
  }

  arrow::Status SetInputArray(const std::shared_ptr<arrow::Array>& in) override {
    in_ = std::dynamic_pointer_cast<ArrayType>(in);
    if (!builder_) {
      MakeArrayBuilder<ResArrayType, DataType>(
          arrow::TypeTraits<DataType>::type_singleton(), ctx_->memory_pool(), &builder_);
    }
    return arrow::Status::OK();
  }

  arrow::Status ConfigureGroupSize(int max_group_id,
                                   const std::shared_ptr<arrow::Array>& in) override {
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }
    return arrow::Status::OK();
  }

  arrow::Status Eval(int src_row_id, int dest_group_id) override {
    cache_validity_[dest_group_id] = true;
    if (!in_->IsNull(src_row_id)) {
      auto value = in_->GetView(src_row_id);
      cache_[dest_group_id] += value;
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    int group_id = 0;
    for (int i = 0; i < cache_validity_.size(); i++) {
      if (cache_validity_[i]) {
        auto value = cache_[i];
        RETURN_NOT_OK(builder_->template AppendValue<DataType>(group_id, value));
      }
    }
    RETURN_NOT_OK(builder_->Finish(out));

    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<ArrayType> in_;
  arrow::compute::FunctionContext* ctx_;
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::shared_ptr<ArrayBuilderImpl<ResArrayType, DataType>> builder_;
};

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)

/*  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::Time32Type)             \
  PROCESS(arrow::Time64Type)             \
  PROCESS(arrow::TimestampType)          \
  PROCESS(arrow::BinaryType)             \
  PROCESS(arrow::StringType)             \
  PROCESS(arrow::FixedSizeBinaryType)    \
  PROCESS(arrow::Decimal128Type)*/
arrow::Status MakeUniqueAction(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> type,
                               std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                       \
  case InType::type_id: {                                                     \
    using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;          \
    auto action_ptr = std::make_shared<UniqueAction<ArrayType, InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                 \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeCountAction(arrow::compute::FunctionContext* ctx,
                              std::shared_ptr<arrow::DataType> type,
                              std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                           \
  case InType::type_id: {                                                         \
    using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;              \
    using ResDataType = typename FindAccumulatorType<InType>::Type;               \
    auto action_ptr = std::make_shared<CountAction<ArrayType, ResDataType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                     \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeSumAction(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                         \
  case InType::type_id: {                                                       \
    using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;            \
    using ResDataType = typename FindAccumulatorType<InType>::Type;             \
    auto action_ptr = std::make_shared<SumAction<ArrayType, ResDataType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);                   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
