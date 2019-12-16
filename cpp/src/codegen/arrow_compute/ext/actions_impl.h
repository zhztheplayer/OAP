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
  virtual arrow::Status Submit(const std::shared_ptr<arrow::Array>& in, int max_group_id,
                               std::function<arrow::Status(int)>* out) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Submit(std::vector<std::shared_ptr<arrow::Array>> in,
                               uint64_t reserved_length,
                               std::function<arrow::Status(uint64_t, uint64_t)>* out) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("ActionBase Finish is abstract.");
  }
};

//////////////// UniqueAction ///////////////
template <typename DataType>
class UniqueAction : public ActionBase {
 public:
  UniqueAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
    MakeArrayBuilder<ResArrayType, DataType>(
        arrow::TypeTraits<DataType>::type_singleton(), ctx_->memory_pool(), &builder_);
  }
  ~UniqueAction() {
#ifdef DEBUG
    std::cout << "Destruct UniqueAction" << std::endl;
#endif
  }

  arrow::Status Submit(const std::shared_ptr<arrow::Array>& in, int max_group_id,
                       std::function<arrow::Status(int)>* out) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    // prepare evaluate lambda
    auto data = in->data()->GetValues<CType>(1);
    valid_reader = std::make_shared<arrow::internal::BitmapReader>(
        in->data()->buffers[0]->data(), in->data()->offset, in->data()->length);
    row_id = 0;
    if (in->null_count()) {
      *out = [this, data](int dest_group_id) {
        const bool is_null = valid_reader->IsNotSet();
        valid_reader->Next();
        if (!cache_validity_[dest_group_id]) {
          cache_validity_[dest_group_id] = true;
          if (!is_null) {
            cache_[dest_group_id] = data[row_id];
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *out = [this, data](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_validity_[dest_group_id] = true;
          cache_[dest_group_id] = data[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    int group_id = 0;
    RETURN_NOT_OK(
        builder_->template AppendValues<DataType>(group_id, cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(out));

    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::internal::BitmapReader> valid_reader;
  int row_id;
  // output
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::shared_ptr<ArrayBuilderImpl<ResArrayType, DataType>> builder_;
};

//////////////// CountAction ///////////////
template <typename DataType>
class CountAction : public ActionBase {
 public:
  CountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
    MakeArrayBuilder<ResArrayType, DataType>(
        arrow::TypeTraits<DataType>::type_singleton(), ctx_->memory_pool(), &builder_);
  }
  ~CountAction() {
#ifdef DEBUG
    std::cout << "Destruct CountAction" << std::endl;
#endif
  }

  arrow::Status Submit(const std::shared_ptr<arrow::Array>& in, int max_group_id,
                       std::function<arrow::Status(int)>* out) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    // prepare evaluate lambda
    valid_reader = std::make_shared<arrow::internal::BitmapReader>(
        in->data()->buffers[0]->data(), in->data()->offset, in->data()->length);
    if (in->null_count()) {
      *out = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        const bool is_null = valid_reader->IsNotSet();
        valid_reader->Next();
        if (!is_null) {
          cache_[dest_group_id] += 1;
        }
        return arrow::Status::OK();
      };
    } else {
      *out = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += 1;
        return arrow::Status::OK();
      };
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    int group_id = 0;
    RETURN_NOT_OK(
        builder_->template AppendValues<DataType>(group_id, cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(out));

    return arrow::Status::OK();
  }

 private:
  using ResArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::internal::BitmapReader> valid_reader;
  // result
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
  std::shared_ptr<ArrayBuilderImpl<ResArrayType, DataType>> builder_;
};

//////////////// SumAction ///////////////
template <typename DataType>
class SumAction : public ActionBase {
 public:
  SumAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
    MakeArrayBuilder<ResArrayType, ResDataType>(
        arrow::TypeTraits<ResDataType>::type_singleton(), ctx_->memory_pool(), &builder_);
  }
  ~SumAction() {
#ifdef DEBUG
    std::cout << "Destruct SumAction" << std::endl;
#endif
  }

  arrow::Status Submit(const std::shared_ptr<arrow::Array>& in, int max_group_id,
                       std::function<arrow::Status(int)>* out) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    // prepare evaluate lambda
    auto data = in->data()->GetValues<CType>(1);
    valid_reader = std::make_shared<arrow::internal::BitmapReader>(
        in->data()->buffers[0]->data(), in->data()->offset, in->data()->length);
    row_id = 0;
    if (in->null_count()) {
      *out = [this, data](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        const bool is_null = valid_reader->IsNotSet();
        valid_reader->Next();
        if (!is_null) {
          cache_[dest_group_id] += data[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *out = [this, data](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += data[row_id];
        row_id++;
        return arrow::Status::OK();
      };
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    int group_id = 0;
    RETURN_NOT_OK(
        builder_->template AppendValues<ResDataType>(group_id, cache_, cache_validity_));
    RETURN_NOT_OK(builder_->Finish(out));

    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::internal::BitmapReader> valid_reader;
  int row_id;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
  std::shared_ptr<ArrayBuilderImpl<ResArrayType, ResDataType>> builder_;
};

//////////////// ShuffleAction ///////////////
template <typename DataType>
class ShuffleAction : public ActionBase {
 public:
  ShuffleAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~ShuffleAction() {
#ifdef DEBUG
    std::cout << "Destruct ShuffleAction" << std::endl;
#endif
  }

  arrow::Status Submit(std::vector<std::shared_ptr<arrow::Array>> in,
                       uint64_t reserved_length,
                       std::function<arrow::Status(uint64_t, uint64_t)>* out) override {
    for (auto array : in) {
      typed_arrays_.push_back(std::dynamic_pointer_cast<ResArrayType>(array));
    }
    std::unique_ptr<arrow::ArrayBuilder> builder;
    RETURN_NOT_OK(arrow::MakeBuilder(
        ctx_->memory_pool(), arrow::TypeTraits<ResDataType>::type_singleton(), &builder));
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(builder.release()));
    builder_->Reserve(reserved_length);
    // prepare evaluate lambda
    *out = [this](uint64_t array_id, uint64_t id) {
      if (typed_arrays_[array_id]->IsNull(id)) {
        builder_->UnsafeAppendNull();
      } else {
        builder_->UnsafeAppend(typed_arrays_[array_id]->Value(id));
      }
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    RETURN_NOT_OK(builder_->Finish(out));
    return arrow::Status::OK();
  }

 private:
  using ResDataType = DataType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::shared_ptr<ResArrayType>> typed_arrays_;
  // result
  std::shared_ptr<BuilderType> builder_;
};

///////////////////// Public Functions //////////////////
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
#define PROCESS(InType)                                            \
  case InType::type_id: {                                          \
    auto action_ptr = std::make_shared<UniqueAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);      \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeCountAction(arrow::compute::FunctionContext* ctx,
                              std::shared_ptr<ActionBase>* out) {
  auto action_ptr = std::make_shared<CountAction<arrow::UInt64Type>>(ctx);
  *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
  return arrow::Status::OK();
}

arrow::Status MakeSumAction(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<SumAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeShuffleAction(arrow::compute::FunctionContext* ctx,
                                std::shared_ptr<arrow::DataType> type,
                                std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                             \
  case InType::type_id: {                                           \
    auto action_ptr = std::make_shared<ShuffleAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);       \
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
