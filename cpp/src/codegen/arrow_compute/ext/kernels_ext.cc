#include "codegen/arrow_compute/ext/kernels_ext.h"
#include <arrow/compute/context.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/count.h>
#include <arrow/compute/kernels/hash.h>
#include <arrow/compute/kernels/sum.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <iostream>
#include <unordered_map>
#include "codegen/arrow_compute/ext/append.h"
#include "codegen/arrow_compute/ext/split.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

///////////////  SplitArrayList  ////////////////
arrow::Status SplitArrayList(arrow::compute::FunctionContext* ctx, const ArrayList& in,
                             const std::shared_ptr<arrow::Array>& dict,
                             std::vector<ArrayList>* out, std::vector<int>* out_sizes,
                             std::vector<int>* group_indices) {
  if (!dict) {
    return arrow::Status::Invalid("input data is invalid");
  }
  std::vector<std::shared_ptr<splitter::ArrayVisitorImpl>> array_visitor_list;
  std::vector<std::shared_ptr<ArrayBuilderImplBase>> builder_list_;
  std::unordered_map<int, int> group_id_to_index;
  for (auto col : in) {
    auto visitor = std::make_shared<splitter::ArrayVisitorImpl>(ctx);
    RETURN_NOT_OK(col->Accept(&(*visitor.get())));
    array_visitor_list.push_back(visitor);

    std::shared_ptr<ArrayBuilderImplBase> builder;
    visitor->GetBuilder(&builder);
    builder_list_.push_back(builder);
  }

  for (int row_id = 0; row_id < dict->length(); row_id++) {
    auto group_id = arrow::internal::checked_cast<const arrow::Int32Array&>(*dict.get())
                        .GetView(row_id);
    auto find = group_id_to_index.find(group_id);
    int index;
    if (find == group_id_to_index.end()) {
      index = group_id_to_index.size();
      group_id_to_index.emplace(group_id, index);
      group_indices->push_back(group_id);
    } else {
      index = find->second;
    }
    for (int i = 0; i < array_visitor_list.size(); i++) {
      array_visitor_list[i]->Eval(builder_list_[i], index, row_id);
    }
  }

  for (auto builder : builder_list_) {
    std::vector<std::shared_ptr<arrow::Array>> arr_list_out;
    RETURN_NOT_OK(builder->Finish(&arr_list_out));
    for (int i = 0; i < arr_list_out.size(); i++) {
      if (out->size() <= i) {
        ArrayList arr_list;
        out->push_back(arr_list);
        out_sizes->push_back(arr_list_out[i]->length());
      }
      out->at(i).push_back(arr_list_out[i]);
    }
  }

  return arrow::Status::OK();
}

///////////////  UniqueArray  ////////////////
class UniqueArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    std::shared_ptr<arrow::Array> out;
    if (in->length() == 0) {
      return arrow::Status::OK();
    }
    arrow::compute::Datum input_datum(in);
    RETURN_NOT_OK(arrow::compute::Unique(ctx_, input_datum, &out));
    if (!builder) {
      RETURN_NOT_OK(MakeArrayBuilder(out->type(), ctx_->memory_pool(), &builder));
    }

    RETURN_NOT_OK(builder->AppendArray(&(*out.get()), 0, 0));

    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(builder->Finish(out));
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayBuilderImplBase> builder;
};

arrow::Status UniqueArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                      std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<UniqueArrayKernel>(ctx);
  return arrow::Status::OK();
}

UniqueArrayKernel::UniqueArrayKernel(arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
}

arrow::Status UniqueArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status UniqueArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}

///////////////  SumArray  ////////////////
class SumArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    arrow::compute::Datum output;
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in.get(), &output));
    std::shared_ptr<arrow::Array> out;
    RETURN_NOT_OK(
        arrow::MakeArrayFromScalar(*(output.scalar()).get(), output.length(), &out));
    if (!builder) {
      RETURN_NOT_OK(MakeArrayBuilder(out->type(), ctx_->memory_pool(), &builder));
    }
    RETURN_NOT_OK(builder->AppendArray(&(*out.get()), 0, 0));
    // TODO: We should only append Scalar instead of array
    // RETURN_NOT_OK(builder->AppendScalar(output.scalar()));

    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(builder->Finish(out));
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayBuilderImplBase> builder;
};

arrow::Status SumArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<SumArrayKernel>(ctx);
  return arrow::Status::OK();
}

SumArrayKernel::SumArrayKernel(arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
}

arrow::Status SumArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status SumArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}

///////////////  CountArray  ////////////////
class CountArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    arrow::compute::Datum output;
    arrow::compute::CountOptions opt =
        arrow::compute::CountOptions(arrow::compute::CountOptions::COUNT_ALL);
    RETURN_NOT_OK(arrow::compute::Count(ctx_, opt, *in.get(), &output));
    std::shared_ptr<arrow::Array> out;
    RETURN_NOT_OK(
        arrow::MakeArrayFromScalar(*(output.scalar()).get(), output.length(), &out));
    if (!builder) {
      RETURN_NOT_OK(MakeArrayBuilder(out->type(), ctx_->memory_pool(), &builder));
    }

    RETURN_NOT_OK(builder->AppendArray(&(*out.get()), 0, 0));
    // TODO: We should only append Scalar instead of array

    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(builder->Finish(out));
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayBuilderImplBase> builder;
};

arrow::Status CountArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                     std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<CountArrayKernel>(ctx);
  return arrow::Status::OK();
}

CountArrayKernel::CountArrayKernel(arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
}

arrow::Status CountArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status CountArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}

///////////////  EncodeArray  ////////////////
class EncodeArrayKernel::Impl {
 public:
  Impl() {}
  ~Impl() {}
  virtual arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                                 std::shared_ptr<arrow::Array>* out) = 0;
};

template <typename InType, typename MemoTableType>
class EncodeArrayTypedImpl : public EncodeArrayKernel::Impl {
 public:
  EncodeArrayTypedImpl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
    hash_table_ = std::make_shared<MemoTableType>(ctx_->memory_pool());
  }
  ~EncodeArrayTypedImpl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                         std::shared_ptr<arrow::Array>* out) {
    arrow::compute::Datum input_datum(in);

    arrow::compute::Datum out_dict;
    RETURN_NOT_OK(arrow::compute::DictionaryEncode<InType>(ctx_, input_datum, hash_table_,
                                                           &out_dict));
    auto dict = std::dynamic_pointer_cast<arrow::DictionaryArray>(out_dict.make_array());

    *out = dict->indices();
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<MemoTableType> hash_table_;
};

arrow::Status EncodeArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                      std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<EncodeArrayKernel>(ctx);
  return arrow::Status::OK();
}

EncodeArrayKernel::EncodeArrayKernel(arrow::compute::FunctionContext* ctx) { ctx_ = ctx; }

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
arrow::Status EncodeArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in,
                                          std::shared_ptr<arrow::Array>* out) {
  if (!impl_) {
    switch (in->type_id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using MemoTableType = typename arrow::internal::HashTraits<InType>::MemoTableType; \
    impl_.reset(new EncodeArrayTypedImpl<InType, MemoTableType>(ctx_));                \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      default:
        break;
    }
  }
  return impl_->Evaluate(in, out);
}
#undef PROCESS_SUPPORTED_TYPES

///////////////  AppendToCacheArrayList  ////////////////
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

///////////////  AppendToCacheArray  ////////////////
class AppendToCacheArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in, int group_id) {
    auto visitor = std::make_shared<appender::ArrayVisitorImpl>(ctx_);
    RETURN_NOT_OK(in->Accept(&(*visitor.get())));

    if (!builder) {
      RETURN_NOT_OK(visitor->GetBuilder(&builder));
    }
    RETURN_NOT_OK(visitor->Eval(builder, group_id));

    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    RETURN_NOT_OK(builder->Finish(out));
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(builder->Finish(out));
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayBuilderImplBase> builder;
};
arrow::Status AppendToCacheArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                             std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<AppendToCacheArrayKernel>(ctx);
  return arrow::Status::OK();
}

AppendToCacheArrayKernel::AppendToCacheArrayKernel(arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
}

arrow::Status AppendToCacheArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in,
                                                 int group_id) {
  return impl_->Evaluate(in, group_id);
}

arrow::Status AppendToCacheArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}

arrow::Status AppendToCacheArrayKernel::Finish(ArrayList* out) {
  return impl_->Finish(out);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
