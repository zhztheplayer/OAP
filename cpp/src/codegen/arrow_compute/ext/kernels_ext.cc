#include "codegen/arrow_compute/ext/kernels_ext.h"
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
#include "codegen/arrow_compute/ext/append.h"
#include "codegen/arrow_compute/ext/split.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

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
  if (in->length() == 0) {
    *out = in;
    return arrow::Status::OK();
  }
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
  if (in->length() == 0) {
    *out = in;
    return arrow::Status::OK();
  }
  RETURN_NOT_OK(arrow::compute::Count(ctx, opt, *in.get(), &output));
  RETURN_NOT_OK(
      arrow::MakeArrayFromScalar(*(output.scalar()).get(), output.length(), out));
  return arrow::Status::OK();
}

template <typename InType, typename MemoTableType>
arrow::Status EncodeArrayImpl(arrow::compute::FunctionContext* ctx,
                              const std::shared_ptr<arrow::Array>& in,
                              std::shared_ptr<DictionaryExtArray>* out) {
  arrow::compute::Datum input_datum(in);
  static auto hash_table = std::make_shared<MemoTableType>(ctx->memory_pool());

  std::shared_ptr<arrow::Array> out_counts;
  RETURN_NOT_OK(
      arrow::compute::ValueCounts<InType>(ctx, input_datum, hash_table, &out_counts));
  auto value_counts = std::dynamic_pointer_cast<arrow::StructArray>(out_counts);

  arrow::compute::Datum out_dict;
  RETURN_NOT_OK(
      arrow::compute::DictionaryEncode<InType>(ctx, input_datum, hash_table, &out_dict));
  auto dict = std::dynamic_pointer_cast<arrow::DictionaryArray>(out_dict.make_array());

  *out = std::make_shared<DictionaryExtArray>(dict->indices(), value_counts->field(1));
  return arrow::Status::OK();
}

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
arrow::Status EncodeArray(arrow::compute::FunctionContext* ctx,
                          const std::shared_ptr<arrow::Array>& in,
                          std::shared_ptr<DictionaryExtArray>* out) {
  switch (in->type_id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using MemoTableType = typename arrow::internal::HashTraits<InType>::MemoTableType; \
    return EncodeArrayImpl<InType, MemoTableType>(ctx, in, out);                       \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES

class AppendToCacheArrayListKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    std::vector<std::shared_ptr<appender::ArrayVisitorImpl>> array_visitor_list;
    for (auto col : in) {
      auto visitor = std::make_shared<appender::ArrayVisitorImpl>(ctx_);
      RETURN_NOT_OK(col->Accept(&(*visitor.get())));
      array_visitor_list.push_back(visitor);
    }

    int need_to_append = array_visitor_list.size() - builder_list_.size() + 1;
    if (need_to_append < 0) {
      return arrow::Status::Invalid(
          "AppendToCacheArrayListKernel::Impl array size is smaller than total array "
          "builder size, unable to map the relation.appender");
    }

    for (int i = 0; i < array_visitor_list.size(); i++) {
      std::shared_ptr<ArrayBuilderImplBase> builder;
      if (builder_list_.size() <= i) {
        RETURN_NOT_OK(array_visitor_list[i]->GetBuilder(&builder));
        builder_list_.push_back(builder);

      } else {
        builder = builder_list_[i];
      }
      RETURN_NOT_OK(array_visitor_list[i]->Eval(builder));
    }

    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    for (auto builder : builder_list_) {
      std::shared_ptr<arrow::Array> arr_out;
      RETURN_NOT_OK(builder->Finish(&arr_out));
      out->push_back(arr_out);
    }
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::shared_ptr<ArrayBuilderImplBase>> builder_list_;
};
arrow::Status AppendToCacheArrayListKernel::Make(arrow::compute::FunctionContext* ctx,
                                                 std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<AppendToCacheArrayListKernel>(ctx);
  return arrow::Status::OK();
}

AppendToCacheArrayListKernel::AppendToCacheArrayListKernel(
    arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
}

arrow::Status AppendToCacheArrayListKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status AppendToCacheArrayListKernel::Finish(ArrayList* out) {
  return impl_->Finish(out);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
