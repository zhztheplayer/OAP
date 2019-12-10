#pragma once

#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/memory_pool.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <iostream>
namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
class ArrayBuilderImplBase {
 public:
  virtual arrow::Status AppendScalar(const std::shared_ptr<arrow::Scalar>& in,
                                     int group_id = 0) = 0;
  virtual arrow::Status AppendArray(arrow::Array* in, int group_id = 0) = 0;
  virtual arrow::Status AppendArrayItem(arrow::Array* in, int group_id, int row_id) = 0;
  virtual arrow::Status Finish(ArrayList* out) = 0;
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) = 0;
};

template <typename ArrayType, typename T,
          typename BuilderType = typename arrow::TypeTraits<T>::BuilderType>
class ArrayBuilderImpl : public ArrayBuilderImplBase {
 public:
  static arrow::Status Make(const std::shared_ptr<arrow::DataType> type,
                            arrow::MemoryPool* pool,
                            std::shared_ptr<ArrayBuilderImpl>* builder) {
    auto builder_ptr = std::make_shared<ArrayBuilderImpl<ArrayType, T>>(type, pool);
    *builder = builder_ptr;
    return arrow::Status::OK();
  }

  ArrayBuilderImpl(const std::shared_ptr<arrow::DataType> type, arrow::MemoryPool* pool)
      : pool_(pool) {
    type_ = type;
  }
  ~ArrayBuilderImpl() {
#ifdef DEBUG
    std::cout << "Destruct ArrayBuilderImpl" << std::endl;
#endif
  }

  arrow::Status InitBuilder() {
    // prepare builder, should be size of key number
    std::unique_ptr<arrow::ArrayBuilder> builder;
    RETURN_NOT_OK(arrow::MakeBuilder(pool_, type_, &builder));

    std::shared_ptr<BuilderType> builder_ptr;
    builder_ptr.reset(arrow::internal::checked_cast<BuilderType*>(builder.release()));
    builder_list_.push_back(builder_ptr);
    return arrow::Status::OK();
  }

  arrow::Status GetOrCreateBuilder(int group_id, std::shared_ptr<BuilderType>* builder) {
    while (builder_list_.size() <= group_id) {
      RETURN_NOT_OK(InitBuilder());
    }
    *builder = builder_list_[group_id];
    return arrow::Status::OK();
  }

  arrow::Status AppendArrayItem(arrow::Array* in, int group_id, int row_id) {
    std::shared_ptr<BuilderType> builder;
    RETURN_NOT_OK(GetOrCreateBuilder(group_id, &builder));
    RETURN_NOT_OK(builder->Reserve(1));
    auto in_ = arrow::internal::checked_cast<ArrayType*>(in);
    if (in_->IsNull(row_id)) {
      builder->UnsafeAppendNull();
    } else {
      auto value = in_->GetView(row_id);
#ifdef DEBUG_DATA
      std::cout << "AppendArrayItem group_id is " << group_id << ", data is " << value
                << std::endl;
#endif
      UnsafeAppend<T>(builder, value);
    }
    return arrow::Status::OK();
  }

  arrow::Status AppendArray(arrow::Array* in, int group_id = 0) {
    std::shared_ptr<BuilderType> builder;
    RETURN_NOT_OK(GetOrCreateBuilder(group_id, &builder));
    RETURN_NOT_OK(builder->Reserve(in->length()));
    auto in_ = arrow::internal::checked_cast<ArrayType*>(in);
    for (int row_id = 0; row_id < in_->length(); row_id++) {
      if (in_->IsNull(row_id)) {
        builder->UnsafeAppendNull();
      } else {
        auto value = in_->GetView(row_id);
#ifdef DEBUG_DATA
        std::cout << "AppendArray group_id is " << group_id << ", data is " << value
                  << std::endl;
#endif
        UnsafeAppend<T>(builder, value);
      }
    }
    return arrow::Status::OK();
  }

  template <typename DataType, typename CType>
  arrow::enable_if_has_c_type<DataType, arrow::Status> AppendValue(int group_id,
                                                                   CType&& value) {
    std::shared_ptr<BuilderType> builder;
    RETURN_NOT_OK(GetOrCreateBuilder(group_id, &builder));
    RETURN_NOT_OK(builder->Reserve(1));
#ifdef DEBUG_DATA
    std::cout << "AppendValue group_id is " << group_id << ", data is " << value
              << std::endl;
#endif
    builder->UnsafeAppend(value);
    return arrow::Status::OK();
  }

  template <typename DataType, typename CType>
  arrow::enable_if_has_c_type<DataType, arrow::Status> AppendValues(
      int group_id, const std::vector<CType>& values, const std::vector<bool>& is_valid) {
    std::shared_ptr<BuilderType> builder;
    RETURN_NOT_OK(GetOrCreateBuilder(group_id, &builder));
    builder->AppendValues(values, is_valid);
    return arrow::Status::OK();
  }
  arrow::Status AppendScalar(const std::shared_ptr<arrow::Scalar>& in, int group_id = 0) {
    return arrow::Status::NotImplemented("AppendScalar is not implemented yet.");
  }

  arrow::Status Finish(ArrayList* out) {
    for (int i = 0; i < builder_list_.size(); i++) {
      std::shared_ptr<arrow::Array> out_arr;
      builder_list_[i]->Finish(&out_arr);
      out->push_back(out_arr);
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    std::shared_ptr<arrow::Array> out_arr;
    builder_list_[0]->Finish(&out_arr);
    *out = out_arr;
    return arrow::Status::OK();
  }

 private:
  std::vector<std::shared_ptr<BuilderType>> builder_list_;
  arrow::MemoryPool* pool_;
  std::shared_ptr<arrow::DataType> type_;

  // For non-binary builders, use regular value append
  template <typename Scalar, typename ValueType>
  static typename std::enable_if<!std::is_base_of<arrow::BaseBinaryType, Scalar>::value,
                                 arrow::Status>::type
  UnsafeAppend(std::shared_ptr<BuilderType> builder, ValueType&& value) {
    builder->UnsafeAppend(value);
    return arrow::Status::OK();
  }

  // For binary builders, need to reserve byte storage first
  template <typename Scalar>
  static arrow::enable_if_base_binary<Scalar, arrow::Status> UnsafeAppend(
      std::shared_ptr<BuilderType> builder, arrow::util::string_view value) {
    RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
    builder->UnsafeAppend(value);
    return arrow::Status::OK();
  }
};
#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)             \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::Time32Type)             \
  PROCESS(arrow::Time64Type)             \
  PROCESS(arrow::TimestampType)          \
  PROCESS(arrow::BinaryType)             \
  PROCESS(arrow::StringType)             \
  PROCESS(arrow::FixedSizeBinaryType)    \
  PROCESS(arrow::Decimal128Type)
static arrow::Status MakeArrayBuilder(const std::shared_ptr<arrow::DataType> type,
                                      arrow::MemoryPool* pool,
                                      std::shared_ptr<ArrayBuilderImplBase>* builder) {
  switch (type->id()) {
#define PROCESS(InType)                                                    \
  case InType::type_id: {                                                  \
    using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;       \
    auto builder_ptr =                                                     \
        std::make_shared<ArrayBuilderImpl<ArrayType, InType>>(type, pool); \
    *builder = builder_ptr;                                                \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES

template <typename ArrayType, typename InType>
static arrow::Status MakeArrayBuilder(
    const std::shared_ptr<arrow::DataType> type, arrow::MemoryPool* pool,
    std::shared_ptr<ArrayBuilderImpl<ArrayType, InType>>* builder) {
  *builder = std::make_shared<ArrayBuilderImpl<ArrayType, InType>>(type, pool);
  return arrow::Status::OK();
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
