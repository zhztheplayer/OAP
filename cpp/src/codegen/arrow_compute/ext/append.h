#pragma once

#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <memory>
#include "codegen/arrow_compute/ext/array_builder_impl.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
namespace appender {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
class ArrayViewerImplBase {
 public:
  virtual arrow::Status MakeBuilder(arrow::MemoryPool* pool,
                                    std::shared_ptr<ArrayBuilderImplBase>* builder) = 0;
  virtual arrow::Status Process(const std::shared_ptr<ArrayBuilderImplBase>& builder) = 0;
};

template <typename ArrayType, typename T>
class ArrayViewerImpl : public ArrayViewerImplBase {
 public:
  static arrow::Status Make(const ArrayType* in,
                            std::shared_ptr<ArrayViewerImplBase>* out) {
    auto builder_ptr = std::make_shared<ArrayViewerImpl<ArrayType, T>>(in);
    *out = std::dynamic_pointer_cast<ArrayViewerImplBase>(builder_ptr);
    return arrow::Status::OK();
  }

  ArrayViewerImpl(const ArrayType* in) { in_ = const_cast<ArrayType*>(in); }

  arrow::Status MakeBuilder(arrow::MemoryPool* pool,
                            std::shared_ptr<ArrayBuilderImplBase>* builder) {
    std::shared_ptr<ArrayBuilderImpl<ArrayType, T>> builder_ptr;
    ArrayBuilderImpl<ArrayType, T>::Make(in_->type(), pool, &builder_ptr);
    *builder = std::dynamic_pointer_cast<ArrayBuilderImplBase>(builder_ptr);

    return arrow::Status::OK();
  }

  arrow::Status Process(const std::shared_ptr<ArrayBuilderImplBase>& builder) {
    return builder->AppendArray(in_);
  }

 private:
  ArrayType* in_;
};

class ArrayVisitorImpl : public arrow::ArrayVisitor {
 public:
  arrow::Status GetResult(ArrayList* out) {
    *out = out_;
    return arrow::Status::OK();
  }
  ArrayVisitorImpl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~ArrayVisitorImpl(){};

  arrow::Status GetBuilder(std::shared_ptr<ArrayBuilderImplBase>* out) {
    RETURN_NOT_OK(viewer_->MakeBuilder(ctx_->memory_pool(), out));
    return arrow::Status::OK();
  }

  arrow::Status Eval(const std::shared_ptr<ArrayBuilderImplBase>& builder) {
    return viewer_->Process(builder);
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  ArrayList out_;
  std::shared_ptr<ArrayViewerImplBase> viewer_;

  template <typename T, typename ArrayType>
  arrow::Status VisitImpl(const ArrayType& in) {
    return ArrayViewerImpl<ArrayType, T>::Make(&in, &viewer_);
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
}  // namespace appender
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
