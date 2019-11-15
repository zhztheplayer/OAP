#pragma once

#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type.h>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
namespace splitter {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
class ArrayBuilderImplBase {
 public:
  virtual arrow::Status Process(int row_id, int group_id) = 0;
  virtual arrow::Status Finish(ArrayList* out) = 0;
};

template <typename ArrayType, typename T>
class ArrayBuilderImpl : public ArrayBuilderImplBase {
 public:
  static arrow::Status Make(const ArrayType* in,
                            const std::shared_ptr<arrow::DataType> type,
                            const std::shared_ptr<arrow::Array>& counts,
                            arrow::MemoryPool* pool,
                            std::shared_ptr<ArrayBuilderImplBase>* builder) {
    auto builder_ptr =
        std::make_shared<ArrayBuilderImpl<ArrayType, T>>(in, type, counts, pool);
    RETURN_NOT_OK(builder_ptr->Init());
    *builder = std::dynamic_pointer_cast<ArrayBuilderImplBase>(builder_ptr);
    return arrow::Status::OK();
  }

  ArrayBuilderImpl(const ArrayType* in, const std::shared_ptr<arrow::DataType> type,
                   const std::shared_ptr<arrow::Array>& counts, arrow::MemoryPool* pool)
      : pool_(pool) {
    in_ = const_cast<ArrayType*>(in);
    type_ = type;
    counts_ = counts;
  }

  arrow::Status Init() {
    // prepare builder, should be size of key number
    for (int i = 0; i < counts_->length(); i++) {
      std::unique_ptr<arrow::ArrayBuilder> builder;
      RETURN_NOT_OK(arrow::MakeBuilder(pool_, type_, &builder));
      auto count = arrow::internal::checked_cast<const arrow::Int64Array&>(*counts_.get())
                       .GetView(i);
      RETURN_NOT_OK(builder->Reserve(count));
      std::shared_ptr<BuilderType> builder_ptr;
      builder_ptr.reset(arrow::internal::checked_cast<BuilderType*>(builder.release()));
      builder_list_.push_back(builder_ptr);
    }
    return arrow::Status::OK();
  }

  arrow::Status Process(int row_id, int group_id) {
    if (in_->IsNull(row_id)) {
      builder_list_[group_id]->UnsafeAppendNull();
      return arrow::Status::OK();
    }
    auto value = in_->GetView(row_id);
    UnsafeAppend(builder_list_[group_id], value);
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    for (int i = 0; i < builder_list_.size(); i++) {
      std::shared_ptr<arrow::Array> out_arr;
      builder_list_[i]->Finish(&out_arr);
      out->push_back(out_arr);
    }
    return arrow::Status::OK();
  }

 private:
  using BuilderType = typename arrow::TypeTraits<T>::BuilderType;
  std::vector<std::shared_ptr<BuilderType>> builder_list_;
  ArrayType* in_;
  std::shared_ptr<arrow::Array> counts_;
  arrow::MemoryPool* pool_;
  std::shared_ptr<arrow::DataType> type_;

  // For non-binary builders, use regular value append
  template <typename Builder, typename Scalar>
  static typename std::enable_if<
      !std::is_base_of<arrow::BaseBinaryType, typename Builder::TypeClass>::value,
      arrow::Status>::type
  UnsafeAppend(std::shared_ptr<Builder> builder, Scalar&& value) {
    builder->UnsafeAppend(std::forward<Scalar>(value));
    return arrow::Status::OK();
  }

  // For binary builders, need to reserve byte storage first
  template <typename Builder>
  static arrow::enable_if_base_binary<typename Builder::TypeClass, arrow::Status>
  UnsafeAppend(std::shared_ptr<Builder> builder, arrow::util::string_view value) {
    RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
    builder->UnsafeAppend(value);
    return arrow::Status::OK();
  }
};

class ArrayVisitorImpl : public arrow::ArrayVisitor {
 public:
  arrow::Status GetResult(ArrayList* out) {
    *out = out_;
    return arrow::Status::OK();
  }
  ArrayVisitorImpl(arrow::compute::FunctionContext* ctx,
                   const std::shared_ptr<arrow::Array>& counts)
      : ctx_(ctx) {
    counts_ = counts;
  }
  ~ArrayVisitorImpl(){};

  arrow::Status Eval(int row_id, int group_id) {
    return builder_->Process(row_id, group_id);
  }

  arrow::Status Finish() { return builder_->Finish(&out_); }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> counts_;
  ArrayList out_;
  std::shared_ptr<ArrayBuilderImplBase> builder_;

  template <typename T, typename ArrayType>
  arrow::Status VisitImpl(const ArrayType& in) {
    return ArrayBuilderImpl<ArrayType, T>::Make(&in, in.type(), counts_,
                                                ctx_->memory_pool(), &builder_);
  }
#define PROCESS_SUPPORTED_TYPES(PROCESS)                   \
  PROCESS(arrow::BooleanType, arrow::BooleanArray)         \
  PROCESS(arrow::UInt8Type, arrow::UInt8Array)             \
  PROCESS(arrow::Int8Type, arrow::Int8Array)               \
  PROCESS(arrow::UInt16Type, arrow::UInt16Array)           \
  PROCESS(arrow::Int16Type, arrow::Int16Array)             \
  PROCESS(arrow::UInt32Type, arrow::UInt32Array)           \
  PROCESS(arrow::Int32Type, arrow::Int32Array)             \
  PROCESS(arrow::UInt64Type, arrow::UInt64Array)           \
  PROCESS(arrow::Int64Type, arrow::Int64Array)             \
  PROCESS(arrow::HalfFloatType, arrow::HalfFloatArray)     \
  PROCESS(arrow::FloatType, arrow::FloatArray)             \
  PROCESS(arrow::DoubleType, arrow::DoubleArray)           \
  PROCESS(arrow::BinaryType, arrow::BinaryArray)           \
  PROCESS(arrow::StringType, arrow::StringArray)           \
  PROCESS(arrow::LargeBinaryType, arrow::LargeBinaryArray) \
  PROCESS(arrow::LargeStringType, arrow::LargeStringArray)

#define PROCESS(DataType, ArrayType) \
  arrow::Status Visit(const ArrayType& array) { return VisitImpl<DataType>(array); }
  PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
#undef PROCESS_SUPPORTED_TYPES

  arrow::Status Visit(const arrow::NullArray& array) {
    return arrow::Status::NotImplemented("SplitArray: NullArray is not supported.");
  }
};
}  // namespace splitter
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
