#include "codegen/arrow_compute/ext/kernels_ext.h"
#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/count.h>
#include <arrow/compute/kernels/hash.h>
#include <arrow/compute/kernels/sum.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <iostream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
namespace splitter {

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

  arrow::Status Finish() { return builder_->Finish(); }

 private:
  class ArrayBuilderImplBase;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> counts_;
  ArrayList out_;
  std::shared_ptr<ArrayBuilderImplBase> builder_;

  class ArrayBuilderImplBase {
   public:
    virtual arrow::Status Process(int row_id, int group_id) = 0;
    virtual arrow::Status Finish() = 0;
  };

  template <typename ArrayType, typename T>
  class ArrayBuilderImpl : public ArrayBuilderImplBase {
   public:
    static arrow::Status Make(const ArrayType* in, const T& t, const ArrayVisitorImpl* e,
                              std::shared_ptr<ArrayBuilderImplBase>* builder) {
      auto builder_ptr = std::make_shared<ArrayBuilderImpl<ArrayType, T>>(in, e);
      RETURN_NOT_OK(builder_ptr->Init());
      *builder = std::dynamic_pointer_cast<ArrayBuilderImplBase>(builder_ptr);
      return arrow::Status::OK();
    }

    ArrayBuilderImpl(const ArrayType* in, const ArrayVisitorImpl* e) {
      in_ = const_cast<ArrayType*>(in);
      e_ = const_cast<ArrayVisitorImpl*>(e);
    }

    arrow::Status Init() {
      // prepare builder, should be size of key number
      auto counts = e_->counts_;
      for (int i = 0; i < counts->length(); i++) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        RETURN_NOT_OK(arrow::MakeBuilder(e_->ctx_->memory_pool(), in_->type(), &builder));
        auto count =
            arrow::internal::checked_cast<const arrow::Int64Array&>(*counts.get())
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

    arrow::Status Finish() {
      for (int i = 0; i < builder_list_.size(); i++) {
        std::shared_ptr<arrow::Array> out_arr;
        builder_list_[i]->Finish(&out_arr);
        e_->out_.push_back(out_arr);
      }
      return arrow::Status::OK();
    }

   private:
    using BuilderType = typename arrow::TypeTraits<T>::BuilderType;
    std::vector<std::shared_ptr<BuilderType>> builder_list_;
    ArrayType* in_;
    ArrayVisitorImpl* e_;

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

  template <typename ArrayType, typename T>
  arrow::Status VisitImpl(const ArrayType& in, const T& t) {
    ArrayBuilderImpl<ArrayType, T>::Make(&in, t, this, &builder_);
    return arrow::Status::OK();
  }
  arrow::Status Visit(const arrow::NullArray& array) {
    return arrow::Status::NotImplemented("SplitArray: NullArray is not supported.");
  }
  arrow::Status Visit(const arrow::BooleanArray& array) {
    arrow::BooleanType t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::Int8Array& array) {
    arrow::Int8Type t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::Int16Array& array) {
    arrow::Int16Type t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::Int32Array& array) {
    arrow::Int32Type t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::Int64Array& array) {
    arrow::Int64Type t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::UInt8Array& array) {
    arrow::UInt8Type t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::UInt16Array& array) {
    arrow::UInt16Type t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::UInt32Array& array) {
    arrow::UInt32Type t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::UInt64Array& array) {
    arrow::UInt64Type t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::HalfFloatArray& array) {
    arrow::HalfFloatType t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::FloatArray& array) {
    arrow::FloatType t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::DoubleArray& array) {
    arrow::DoubleType t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::StringArray& array) {
    arrow::StringType t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::BinaryArray& array) {
    arrow::BinaryType t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::LargeStringArray& array) {
    arrow::LargeStringType t;
    return VisitImpl(array, t);
  }
  arrow::Status Visit(const arrow::LargeBinaryArray& array) {
    arrow::LargeBinaryType t;
    return VisitImpl(array, t);
  }
};
}  // namespace splitter

arrow::Status SplitArrayList(arrow::compute::FunctionContext* ctx, const ArrayList& in,
                             const std::shared_ptr<arrow::Array>& dict,
                             const std::shared_ptr<arrow::Array>& counts,
                             std::vector<ArrayList>* out, std::vector<int>* out_sizes) {
  if (!dict || !counts) {
    return arrow::Status::Invalid("input data is invalid");
  }
  out->resize(counts->length());
  out_sizes->resize(counts->length());
  std::vector<std::shared_ptr<splitter::ArrayVisitorImpl>> array_visitor_list;
  for (auto col : in) {
    auto visitor = std::make_shared<splitter::ArrayVisitorImpl>(ctx, counts);
    RETURN_NOT_OK(col->Accept(&(*visitor.get())));
    array_visitor_list.push_back(visitor);
  }

  for (int i = 0; i < dict->length(); i++) {
    auto group_id =
        arrow::internal::checked_cast<const arrow::Int32Array&>(*dict.get()).GetView(i);
    for (auto array_visitor : array_visitor_list) {
      array_visitor->Eval(i, group_id);
    }
  }

  for (auto array_visitor : array_visitor_list) {
    ArrayList res;
    array_visitor->Finish();
    array_visitor->GetResult(&res);
    if (res.size() != out->size()) {
      return arrow::Status::Invalid(
          "ArrayVicitorImpl failed: result array number does not match expectation.");
    }
    for (int i = 0; i < out->size(); i++) {
      // move array from single array batch to output multiple array batch.
      out->at(i).push_back(res[i]);
      out_sizes->at(i) = res[i]->length();
    }
  }
  return arrow::Status::OK();
}

arrow::Status SumArray(arrow::compute::FunctionContext* ctx,
                       const std::shared_ptr<arrow::Array>& in,
                       std::shared_ptr<arrow::Array>* out) {
  arrow::compute::Datum output;
  RETURN_NOT_OK(arrow::compute::Sum(ctx, *in.get(), &output));
  RETURN_NOT_OK(
      arrow::MakeArrayFromScalar(*(output.scalar()).get(), output.length(), out));
  return arrow::Status::OK();
}

arrow::Status CountArray(arrow::compute::FunctionContext* ctx,
                         const std::shared_ptr<arrow::Array>& in,
                         std::shared_ptr<arrow::Array>* out) {
  arrow::compute::Datum output;
  arrow::compute::CountOptions opt =
      arrow::compute::CountOptions(arrow::compute::CountOptions::COUNT_ALL);
  RETURN_NOT_OK(arrow::compute::Count(ctx, opt, *in.get(), &output));
  RETURN_NOT_OK(
      arrow::MakeArrayFromScalar(*(output.scalar()).get(), output.length(), out));
  return arrow::Status::OK();
}

arrow::Status EncodeArray(arrow::compute::FunctionContext* ctx,
                          const std::shared_ptr<arrow::Array>& in,
                          std::shared_ptr<DictionaryExtArray>* out) {
  arrow::compute::Datum input_datum(in);
  arrow::compute::Datum out_dict;

  RETURN_NOT_OK(arrow::compute::DictionaryEncode(ctx, input_datum, &out_dict));
  auto dict = std::dynamic_pointer_cast<arrow::DictionaryArray>(out_dict.make_array());

  std::shared_ptr<arrow::Array> out_counts;
  RETURN_NOT_OK(arrow::compute::ValueCounts(ctx, input_datum, &out_counts));
  auto value_counts = std::dynamic_pointer_cast<arrow::StructArray>(out_counts);

  *out = std::make_shared<DictionaryExtArray>(dict->indices(), value_counts->field(1));
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
