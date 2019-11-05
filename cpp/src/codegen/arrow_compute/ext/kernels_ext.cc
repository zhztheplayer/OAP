#include "codegen/arrow_compute/ext/kernels_ext.h"

#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <iostream>

namespace arrow {
namespace compute {
namespace extra {
namespace splitter {

class ArrayVisitorBase;

class ArrayVisitorImpl::Impl : public ArrayVisitor {
 public:
  ~Impl() {}
  Status GetResult(ArrayList* out) {
    *out = out_;
    return Status::OK();
  }

  Impl(FunctionContext* ctx, const std::shared_ptr<arrow::Array>& dict,
       const std::shared_ptr<arrow::Array>& counts)
      : ctx_(ctx) {
    dict_ = dict;
    counts_ = counts;
  }

 private:
  FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> dict_;
  std::shared_ptr<arrow::Array> counts_;
  ArrayList out_;

  template <typename ArrayType, typename T>
  Status VisitImpl(const ArrayType& in, const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    auto type = in.type();

    std::vector<std::shared_ptr<BuilderType>> builder_list;

    // prepare builder, should be size of key number
    for (int i = 0; i < counts_->length(); i++) {
      std::unique_ptr<ArrayBuilder> builder;
      RETURN_NOT_OK(arrow::MakeBuilder(ctx_->memory_pool(), type, &builder));
      auto count =
          arrow::internal::checked_cast<const Int64Array&>(*counts_.get()).GetView(i);
      RETURN_NOT_OK(builder->Reserve(count));
      std::shared_ptr<BuilderType> builder_ptr;
      builder_ptr.reset(arrow::internal::checked_cast<BuilderType*>(builder.release()));
      builder_list.push_back(builder_ptr);
    }

    if (in.length() != dict_->length()) {
      return Status::Invalid(
          "ArrayVisitorImpl: input array size does not match dict_indices size.");
    }
    for (int i = 0; i < dict_->length(); i++) {
      auto group_id =
          arrow::internal::checked_cast<const Int32Array&>(*dict_.get()).GetView(i);
      if (in.IsNull(i)) {
        builder_list[group_id]->UnsafeAppendNull();
        continue;
      }
      auto value = arrow::internal::checked_cast<const ArrayType&>(in).GetView(i);
      UnsafeAppend(builder_list[group_id], value);
    }

    for (int i = 0; i < builder_list.size(); i++) {
      std::shared_ptr<Array> out_arr;
      builder_list[i]->Finish(&out_arr);
      out_.push_back(out_arr);
    }
    return Status::OK();
  }

  // For non-binary builders, use regular value append
  template <typename Builder, typename Scalar>
  static typename std::enable_if<
      !std::is_base_of<BaseBinaryType, typename Builder::TypeClass>::value, Status>::type
  UnsafeAppend(std::shared_ptr<Builder> builder, Scalar&& value) {
    builder->UnsafeAppend(std::forward<Scalar>(value));
    return Status::OK();
  }

  // For binary builders, need to reserve byte storage first
  template <typename Builder>
  static enable_if_base_binary<typename Builder::TypeClass, Status> UnsafeAppend(
      std::shared_ptr<Builder> builder, util::string_view value) {
    RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
    builder->UnsafeAppend(value);
    return Status::OK();
  }

  Status Visit(const NullArray& array) {
    return Status::NotImplemented("SplitArray: NullArray is not supported.");
  }
  Status Visit(const BooleanArray& array) {
    BooleanType t;
    return VisitImpl(array, t);
  }
  Status Visit(const Int8Array& array) {
    Int8Type t;
    return VisitImpl(array, t);
  }
  Status Visit(const Int16Array& array) {
    Int16Type t;
    return VisitImpl(array, t);
  }
  Status Visit(const Int32Array& array) {
    Int32Type t;
    return VisitImpl(array, t);
  }
  Status Visit(const Int64Array& array) {
    Int64Type t;
    return VisitImpl(array, t);
  }
  Status Visit(const UInt8Array& array) {
    UInt8Type t;
    return VisitImpl(array, t);
  }
  Status Visit(const UInt16Array& array) {
    UInt16Type t;
    return VisitImpl(array, t);
  }
  Status Visit(const UInt32Array& array) {
    UInt32Type t;
    return VisitImpl(array, t);
  }
  Status Visit(const UInt64Array& array) {
    UInt64Type t;
    return VisitImpl(array, t);
  }
  Status Visit(const HalfFloatArray& array) {
    HalfFloatType t;
    return VisitImpl(array, t);
  }
  Status Visit(const FloatArray& array) {
    FloatType t;
    return VisitImpl(array, t);
  }
  Status Visit(const DoubleArray& array) {
    DoubleType t;
    return VisitImpl(array, t);
  }
  Status Visit(const StringArray& array) {
    StringType t;
    return VisitImpl(array, t);
  }
  Status Visit(const BinaryArray& array) {
    BinaryType t;
    return VisitImpl(array, t);
  }
  Status Visit(const LargeStringArray& array) {
    LargeStringType t;
    return VisitImpl(array, t);
  }
  Status Visit(const LargeBinaryArray& array) {
    LargeBinaryType t;
    return VisitImpl(array, t);
  }

};  // namespace splitter

Status ArrayVisitorImpl::Eval(FunctionContext* ctx, const ArrayList& in,
                              const std::shared_ptr<arrow::Array>& dict,
                              const std::shared_ptr<arrow::Array>& counts,
                              std::vector<ArrayList>* out, std::vector<int>* out_sizes) {
  out->resize(counts->length());
  out_sizes->resize(counts->length());
  for (auto col : in) {
    Impl visitor(ctx, dict, counts);
    RETURN_NOT_OK(col->Accept(&visitor));
    ArrayList res;
    visitor.GetResult(&res);
    if (res.size() != out->size()) {
      return Status::UnknownError(
          "ArrayVicitorImpl failed: result array number does not match expectation.");
    }
    for (int i = 0; i < out->size(); i++) {
      out->at(i).push_back(res[i]);
      out_sizes->at(i) = res[i]->length();
    }
  }
  return Status::OK();
}

}  // namespace splitter

Status SplitArrayList(FunctionContext* ctx, const ArrayList& in,
                      const std::shared_ptr<arrow::Array>& dict,
                      const std::shared_ptr<arrow::Array>& counts,
                      std::vector<ArrayList>* out, std::vector<int>* out_sizes) {
  if (!dict || !counts) {
    return Status::Invalid("input data is invalid");
  }
  RETURN_NOT_OK(splitter::ArrayVisitorImpl::Eval(ctx, in, dict, counts, out, out_sizes));
  return Status::OK();
}

}  // namespace extra
}  // namespace compute
}  // namespace arrow
