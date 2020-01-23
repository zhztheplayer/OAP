#include "codegen/arrow_compute/ext/kernels_ext.h"
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/count.h>
#include <arrow/compute/kernels/groupby_aggregate.h>
#include <arrow/compute/kernels/hash.h>
#include <arrow/compute/kernels/minmax.h>
#include <arrow/compute/kernels/ntake.h>
#include <arrow/compute/kernels/probe.h>
#include <arrow/compute/kernels/sort_arrays_to_indices.h>
#include <arrow/compute/kernels/sum.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <gandiva/configuration.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>
#include <cstring>
#include <iostream>
#include <unordered_map>
#include "codegen/arrow_compute/ext/actions_impl.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

#define MAXBATCHNUMROWS 4096

///////////////  SplitArrayListWithAction  ////////////////
class SplitArrayListWithActionKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::vector<std::string> action_name_list,
       std::vector<std::shared_ptr<arrow::DataType>> type_list)
      : ctx_(ctx), action_name_list_(action_name_list) {
    InitActionList(type_list);
  }
  ~Impl() {}

  arrow::Status InitActionList(std::vector<std::shared_ptr<arrow::DataType>> type_list) {
    if (action_name_list_.size() != type_list.size()) {
      return arrow::Status::Invalid(
          "SplitArrayListWithActionKernel Init expects same size action list and array "
          "type list.");
    }
    for (int col_id = 0; col_id < type_list.size(); col_id++) {
      std::shared_ptr<ActionBase> action;
      if (action_name_list_[col_id].compare("action_unique") == 0) {
        RETURN_NOT_OK(MakeUniqueAction(ctx_, type_list[col_id], &action));
      } else if (action_name_list_[col_id].compare("action_count") == 0) {
        RETURN_NOT_OK(MakeCountAction(ctx_, &action));
      } else if (action_name_list_[col_id].compare("action_sum") == 0) {
        RETURN_NOT_OK(MakeSumAction(ctx_, type_list[col_id], &action));
      } else {
        return arrow::Status::NotImplemented(action_name_list_[col_id],
                                             " is not implementetd.");
      }
      action_list_.push_back(action);
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const ArrayList& in,
                         const std::shared_ptr<arrow::Array>& in_dict) {
    if (!in_dict) {
      return arrow::Status::Invalid("input data is invalid");
    }

    if (in.size() != action_list_.size()) {
      return arrow::Status::Invalid(
          "SplitArrayListWithAction input arrayList size does not match numActions");
    }

    // using minmax
    arrow::compute::MinMaxOptions options;
    arrow::compute::Datum minMaxOut;
    RETURN_NOT_OK(arrow::compute::MinMax(ctx_, options, *in_dict.get(), &minMaxOut));
    if (!minMaxOut.is_collection()) {
      return arrow::Status::Invalid(
          "SplitArrayListWithActionKernel MinMax return an invalid result.");
    }
    auto col = minMaxOut.collection();
    if (col.size() < 2) {
      return arrow::Status::Invalid(
          "SplitArrayListWithActionKernel MinMax return an invalid result.");
    }
    auto max = col[1].scalar();
    // std::cout << "max is " << max->ToString() << ", type is "
    //          << in_dict->type()->ToString() << std::endl;
    auto max_group_id =
        arrow::internal::checked_pointer_cast<arrow::Int32Scalar>(max)->value;

    std::vector<std::function<arrow::Status(int)>> eval_func_list;
    for (int i = 0; i < in.size(); i++) {
      auto col = in[i];
      auto action = action_list_[i];
      std::function<arrow::Status(int)> func;
      action->Submit(col, max_group_id, &func);
      eval_func_list.push_back(func);
    }

    const int32_t* data = in_dict->data()->GetValues<int32_t>(1);
    for (int row_id = 0; row_id < in_dict->length(); row_id++) {
      auto group_id = data[row_id];
      for (auto eval_func : eval_func_list) {
        eval_func(group_id);
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    for (auto action : action_list_) {
      std::shared_ptr<arrow::Array> arr_out;
      RETURN_NOT_OK(action->Finish(&arr_out));
      out->push_back(arr_out);
    }
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::string> action_name_list_;
  std::vector<std::shared_ptr<extra::ActionBase>> action_list_;
};

arrow::Status SplitArrayListWithActionKernel::Make(
    arrow::compute::FunctionContext* ctx, std::vector<std::string> action_name_list,
    std::vector<std::shared_ptr<arrow::DataType>> type_list,
    std::shared_ptr<KernalBase>* out) {
  *out =
      std::make_shared<SplitArrayListWithActionKernel>(ctx, action_name_list, type_list);
  return arrow::Status::OK();
}

SplitArrayListWithActionKernel::SplitArrayListWithActionKernel(
    arrow::compute::FunctionContext* ctx, std::vector<std::string> action_name_list,
    std::vector<std::shared_ptr<arrow::DataType>> type_list) {
  impl_.reset(new Impl(ctx, action_name_list, type_list));
  kernel_name_ = "SplitArrayListWithActionKernel";
}

arrow::Status SplitArrayListWithActionKernel::Evaluate(
    const ArrayList& in, const std::shared_ptr<arrow::Array>& dict) {
  return impl_->Evaluate(in, dict);
}

arrow::Status SplitArrayListWithActionKernel::Finish(ArrayList* out) {
  return impl_->Finish(out);
}

///////////////  ShuffleArrayList  ////////////////
class ShuffleArrayListKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::DataType>> type_list)
      : ctx_(ctx) {
    InitActionList(type_list);
  }
  ~Impl() {}

  arrow::Status InitActionList(std::vector<std::shared_ptr<arrow::DataType>> type_list) {
    for (auto type : type_list) {
      std::shared_ptr<ActionBase> action;
      RETURN_NOT_OK(MakeShuffleAction(ctx_, type, &action));
      action_list_.push_back(action);
    }
    input_cache_.resize(type_list.size());
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const ArrayList& in) {
    if (in.size() != input_cache_.size()) {
      return arrow::Status::Invalid(
          "ShuffleArrayListKernel input arrayList size does not match numCols in cache");
    }
    // we need to convert std::vector<Batch> to std::vector<ArrayList>
    for (int col_id = 0; col_id < input_cache_.size(); col_id++) {
      input_cache_[col_id].push_back(in[col_id]);
    }
    return arrow::Status::OK();
  }

  arrow::Status SetDependencyInput(const std::shared_ptr<arrow::Array>& in) {
    in_indices_ = in;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    if (input_cache_.size() == 0 || !in_indices_) {
      return arrow::Status::Invalid("input data is invalid");
    }

    std::vector<std::function<arrow::Status(uint64_t, uint64_t)>> eval_func_list;
    for (int i = 0; i < input_cache_.size(); i++) {
      auto col_list = input_cache_[i];
      auto action = action_list_[i];
      std::function<arrow::Status(uint64_t, uint64_t)> func;
      action->Submit(col_list, in_indices_->length(), &func);
      eval_func_list.push_back(func);
    }

    ArrayItemIndex* data =
        (ArrayItemIndex*)std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(
            in_indices_)
            ->raw_values();
    for (int row_id = 0; row_id < in_indices_->length(); row_id++) {
      ArrayItemIndex* item = data + row_id;
      for (auto eval_func : eval_func_list) {
        eval_func(item->array_id, item->id);
      }
    }
    for (auto action : action_list_) {
      std::shared_ptr<arrow::Array> arr_out;
      RETURN_NOT_OK(action->Finish(&arr_out));
      out->push_back(arr_out);
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    if (input_cache_.size() == 0 || !in_indices_) {
      return arrow::Status::Invalid("input data is invalid");
    }

    std::vector<std::function<arrow::Status(uint64_t, uint64_t)>> eval_func_list;
    for (int i = 0; i < input_cache_.size(); i++) {
      auto col_list = input_cache_[i];
      auto action = action_list_[i];
      std::function<arrow::Status(uint64_t, uint64_t)> func;
      action->Submit(col_list, MAXBATCHNUMROWS, &func);
      eval_func_list.push_back(func);
    }

    *out = std::make_shared<ShuffleArrayListResultIterator>(in_indices_, schema,
                                                            action_list_, eval_func_list);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::shared_ptr<extra::ActionBase>> action_list_;
  std::shared_ptr<arrow::Array> in_indices_;
  std::vector<ArrayList> input_cache_;
  struct ArrayItemIndex {
    uint64_t id = 0;
    uint64_t array_id = 0;
  };

  class ShuffleArrayListResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    ShuffleArrayListResultIterator(
        std::shared_ptr<arrow::Array> in_indices, std::shared_ptr<arrow::Schema> schema,
        std::vector<std::shared_ptr<extra::ActionBase>> action_list,
        std::vector<std::function<arrow::Status(uint64_t, uint64_t)>> eval_func_list)
        : action_list_(action_list), eval_func_list_(eval_func_list) {
      data_ = (ArrayItemIndex*)std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(
                  in_indices)
                  ->raw_values();
      total_length_ = in_indices->length();
      schema_ = schema;
    }

    bool HasNext() {
      if (row_id_ < total_length_) {
        return true;
      } else {
        return false;
      }
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      int output_num_rows = 0;
      for (; output_num_rows < MAXBATCHNUMROWS; output_num_rows++) {
        ArrayItemIndex* item = data_ + row_id_;
        for (auto eval_func : eval_func_list_) {
          eval_func(item->array_id, item->id);
        }
        if (row_id_++ >= total_length_) {
          break;
        }
      }

      std::vector<std::shared_ptr<arrow::Array>> out_array_list;
      for (auto action : action_list_) {
        std::shared_ptr<arrow::Array> arr_out;
        RETURN_NOT_OK(action->FinishAndReset(&arr_out));
        out_array_list.push_back(arr_out);
      }

      *out = arrow::RecordBatch::Make(schema_, output_num_rows, out_array_list);

      return arrow::Status::OK();
    }

   private:
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::function<arrow::Status(uint64_t, uint64_t)>> eval_func_list_;
    std::vector<std::shared_ptr<extra::ActionBase>> action_list_;
    ArrayItemIndex* data_;
    uint64_t total_length_;
    uint64_t row_id_ = 0;
  };
};

arrow::Status ShuffleArrayListKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ShuffleArrayListKernel>(ctx, type_list);
  return arrow::Status::OK();
}

ShuffleArrayListKernel::ShuffleArrayListKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list) {
  impl_.reset(new Impl(ctx, type_list));
  kernel_name_ = "ShuffleArrayListKernel";
}

arrow::Status ShuffleArrayListKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status ShuffleArrayListKernel::Finish(ArrayList* out) {
  return impl_->Finish(out);
}

arrow::Status ShuffleArrayListKernel::SetDependencyInput(
    const std::shared_ptr<arrow::Array>& in) {
  return impl_->SetDependencyInput(in);
}

arrow::Status ShuffleArrayListKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

///////////////  SortArraysToIndices  ////////////////
class SortArraysToIndicesKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, bool nulls_first, bool asc)
      : ctx_(ctx), nulls_first_(nulls_first), asc_(asc) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    if (in->length() == 0) {
      return arrow::Status::OK();
    }
    array_cache_.push_back(in);
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(
        arrow::compute::SortArraysToIndices(ctx_, array_cache_, out, nulls_first_, asc_));
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  bool nulls_first_;
  bool asc_;
  std::vector<std::shared_ptr<arrow::Array>> array_cache_;
};

arrow::Status SortArraysToIndicesKernel::Make(arrow::compute::FunctionContext* ctx,
                                              std::shared_ptr<KernalBase>* out,
                                              bool nulls_first, bool asc) {
  *out = std::make_shared<SortArraysToIndicesKernel>(ctx, nulls_first, asc);
  return arrow::Status::OK();
}

SortArraysToIndicesKernel::SortArraysToIndicesKernel(arrow::compute::FunctionContext* ctx,
                                                     bool nulls_first, bool asc) {
  impl_.reset(new Impl(ctx, nulls_first, asc));
  kernel_name_ = "SortArraysToIndicesKernelKernel";
}

arrow::Status SortArraysToIndicesKernel::Evaluate(
    const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status SortArraysToIndicesKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
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

    RETURN_NOT_OK(builder->AppendArrayItem(&(*out.get()), 0, 0));

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
  kernel_name_ = "UniqueArrayKernel";
}

arrow::Status UniqueArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status UniqueArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}

///////////////  AppendArray  ////////////////
class AppendArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    if (!builder) {
      RETURN_NOT_OK(MakeArrayBuilder(in->type(), ctx_->memory_pool(), &builder));
    }
    RETURN_NOT_OK(builder->AppendArray(&(*in.get()), 0));

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

arrow::Status AppendArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                      std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<AppendArrayKernel>(ctx);
  return arrow::Status::OK();
}

AppendArrayKernel::AppendArrayKernel(arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
  kernel_name_ = "AppendArrayKernel";
}

arrow::Status AppendArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status AppendArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}

///////////////  ProbeArray  ////////////////
class ProbeArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    arrow::compute::Datum output;
    RETURN_NOT_OK(arrow::compute::Probe(ctx_, in, member_set_, &output));
    if (!builder) {
      RETURN_NOT_OK(MakeArrayBuilder(in->type(), ctx_->memory_pool(), &builder));
    }
    RETURN_NOT_OK(builder->AppendArray(output.make_array().get(), 0));

    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(builder->Finish(out));
    return arrow::Status::OK();
  }

  // TODO: move to private
 public:
  std::shared_ptr<arrow::Array> member_set_;

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayBuilderImplBase> builder;
};

arrow::Status ProbeArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                     std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ProbeArrayKernel>(ctx);
  return arrow::Status::OK();
}

ProbeArrayKernel::ProbeArrayKernel(arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
}

arrow::Status ProbeArrayKernel::SetMember(const std::shared_ptr<arrow::RecordBatch>& ms) {
  // TODO: check if multiple columns found
  impl_->member_set_ = ms->column(0);
  return arrow::Status::OK();
}

arrow::Status ProbeArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status ProbeArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}

///////////////  TakeArray  ////////////////
class TakeArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    arrow::compute::NTakeOptions options;
    arrow::compute::Datum output;
    RETURN_NOT_OK(arrow::compute::NTake(ctx_, in, member_set_, options, &output));
    if (!builder) {
      RETURN_NOT_OK(MakeArrayBuilder(in->type(), ctx_->memory_pool(), &builder));
    }
    RETURN_NOT_OK(builder->AppendArray(output.make_array().get(), 0));

    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(builder->Finish(out));
    return arrow::Status::OK();
  }

  // TODO: move to private
 public:
  std::shared_ptr<arrow::Array> member_set_;

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayBuilderImplBase> builder;
};

arrow::Status TakeArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<TakeArrayKernel>(ctx);
  return arrow::Status::OK();
}

TakeArrayKernel::TakeArrayKernel(arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
}

arrow::Status TakeArrayKernel::SetMember(const std::shared_ptr<arrow::RecordBatch>& ms) {
  // TODO: check if multiple columns found
  impl_->member_set_ = ms->column(0);
  return arrow::Status::OK();
}

arrow::Status TakeArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status TakeArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}

///////////////  NTakeArray  ////////////////
class NTakeArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    arrow::compute::NTakeOptions options;
    arrow::compute::Datum output;
    // construct a new list based on row id
    arrow::NumericBuilder<arrow::UInt32Type> new_builder(ctx_->memory_pool());
    new_builder.Resize(member_set_->length());

    for (int id = 0; id < member_set_->length(); id++) {
      if (member_set_->IsNull(id)) {
        new_builder.AppendNull();
      } else {
        new_builder.Append(id);
      }
    }
    new_builder.Finish(&new_mb_builder);

    RETURN_NOT_OK(arrow::compute::NTake(ctx_, in, new_mb_builder, options, &output));

    if (!builder) {
      RETURN_NOT_OK(MakeArrayBuilder(in->type(), ctx_->memory_pool(), &builder));
    }
    RETURN_NOT_OK(builder->AppendArray(output.make_array().get(), 0));

    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    RETURN_NOT_OK(builder->Finish(out));
    return arrow::Status::OK();
  }

  // TODO: move to private
 public:
  std::shared_ptr<arrow::Array> member_set_;

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayBuilderImplBase> builder;
  std::shared_ptr<arrow::Array> new_mb_builder;
};

arrow::Status NTakeArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                     std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<NTakeArrayKernel>(ctx);
  return arrow::Status::OK();
}

NTakeArrayKernel::NTakeArrayKernel(arrow::compute::FunctionContext* ctx) {
  impl_.reset(new Impl(ctx));
}

arrow::Status NTakeArrayKernel::SetMember(const std::shared_ptr<arrow::RecordBatch>& ms) {
  // TODO: check if multiple columns found
  impl_->member_set_ = ms->column(0);
  return arrow::Status::OK();
}

arrow::Status NTakeArrayKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status NTakeArrayKernel::Finish(std::shared_ptr<arrow::Array>* out) {
  return impl_->Finish(out);
}

///////////////  SumArray  ////////////////
class SumArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {}
  ~Impl() {}
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    arrow::compute::Datum output;
    // std::cout << "SumArray Evaluate Input is " << std::endl;
    // arrow::PrettyPrint(*in.get(), 2, &std::cout);
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in.get(), &output));
    std::shared_ptr<arrow::Array> out;
    RETURN_NOT_OK(
        arrow::MakeArrayFromScalar(*(output.scalar()).get(), output.length(), &out));
    if (!builder) {
      RETURN_NOT_OK(MakeArrayBuilder(out->type(), ctx_->memory_pool(), &builder));
    }
    // std::cout << "SumArray Evaluate Output is " << std::endl;
    // arrow::PrettyPrint(*out.get(), 2, &std::cout);
    RETURN_NOT_OK(builder->AppendArrayItem(&(*out.get()), 0, 0));
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
  kernel_name_ = "SumArrayKernel";
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

    RETURN_NOT_OK(builder->AppendArrayItem(&(*out.get()), 0, 0));
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
  kernel_name_ = "CountArrayKernel";
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
  virtual ~Impl() {}
  virtual arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                                 std::shared_ptr<arrow::Array>* out) = 0;
};

template <typename InType, typename MemoTableType>
class EncodeArrayTypedImpl : public EncodeArrayKernel::Impl {
 public:
  EncodeArrayTypedImpl(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
    hash_table_ = std::make_shared<MemoTableType>(ctx_->memory_pool(), 64UL);
  }
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                         std::shared_ptr<arrow::Array>* out) {
    arrow::compute::Datum input_datum(in);

    // std::cout << "Encode input " << in->ToString() << std::endl;
    RETURN_NOT_OK(arrow::compute::Group<InType>(ctx_, input_datum, hash_table_, out));
    // std::cout << "Encode output " << (*out)->ToString() << std::endl;
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

EncodeArrayKernel::EncodeArrayKernel(arrow::compute::FunctionContext* ctx) {
  ctx_ = ctx;
  kernel_name_ = "EncodeArrayKernel";
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

///////////////  ConcatArray  ////////////////
class ConcatArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::DataType>> type_list)
      : ctx_(ctx) {
    // create a new result array type here
    for (auto type : type_list) {
      std::shared_ptr<ActionBase> action;
      MakeConcatAction(ctx_, type, &action);
      action_list_.push_back(action);
    }
    builder_.reset(new arrow::StringBuilder(ctx_->memory_pool()));
  }

  arrow::Status Evaluate(const ArrayList& in, std::shared_ptr<arrow::Array>* out) {
    auto length = in[0]->length();
    auto num_columns = in.size();

    std::stringstream ss;
    std::vector<std::function<arrow::Status(int)>> func_list;
    for (int i = 0; i < num_columns; i++) {
      auto col = in[i];
      std::function<arrow::Status(int)> func;
      action_list_[i]->Submit(col, &ss, &func);
      func_list.push_back(func);
    }

    std::vector<std::string> concat_res;
    std::vector<uint8_t> concat_valid;
    concat_res.resize(length, "");
    concat_valid.resize(length, 0);
    for (int i = 0; i < length; i++) {
      bool is_null = true;
      ss.str("");
      for (auto eval_func : func_list) {
        eval_func(i);
      }
      if (concat_res.size() >= 0) {
        concat_res[i] = ss.str();
        concat_valid[i] = 1;
      }
    }
    RETURN_NOT_OK(builder_->ReserveData(length));
    RETURN_NOT_OK(builder_->AppendValues(concat_res, concat_valid.data()));
    RETURN_NOT_OK(builder_->Finish(out));
    builder_->Reset();

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::shared_ptr<ActionBase>> action_list_;
  std::unique_ptr<arrow::StringBuilder> builder_;
};

arrow::Status ConcatArrayKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ConcatArrayKernel>(ctx, type_list);
  return arrow::Status::OK();
}

ConcatArrayKernel::ConcatArrayKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list) {
  impl_.reset(new Impl(ctx, type_list));
  kernel_name_ = "ConcatArrayKernel";
}

arrow::Status ConcatArrayKernel::Evaluate(const ArrayList& in,
                                          std::shared_ptr<arrow::Array>* out) {
  return impl_->Evaluate(in, out);
}

///////////////  HashAggrArray  ////////////////
class HashAggrArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::DataType>> type_list)
      : ctx_(ctx) {
    // create a new result array type here
    std::vector<std::shared_ptr<gandiva::Node>> func_node_list = {};
    std::vector<std::shared_ptr<arrow::Field>> field_list = {};
    char index = '0';
    for (auto type : type_list) {
      auto field = arrow::field(std::string(&index), type);
      field_list.push_back(field);
      auto field_node = gandiva::TreeExprBuilder::MakeField(field);
      auto func_node =
          gandiva::TreeExprBuilder::MakeFunction("hash64", {field_node}, arrow::int64());
      func_node_list.push_back(func_node);
      if (func_node_list.size() == 2) {
        auto tmp_func_node =
            gandiva::TreeExprBuilder::MakeFunction("add", func_node_list, arrow::int64());
        func_node_list.clear();
        func_node_list.push_back(tmp_func_node);
      }
    }
    auto expr = gandiva::TreeExprBuilder::MakeExpression(
        func_node_list[0], arrow::field("res", arrow::int64()));
    // std::cout << expr->ToString() << std::endl;
    schema_ = arrow::schema(field_list);
    auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
    auto status = gandiva::Projector::Make(schema_, {expr}, configuration, &projector);
    pool_ = ctx_->memory_pool();
  }

  arrow::Status Evaluate(const ArrayList& in, std::shared_ptr<arrow::Array>* out) {
    auto length = in[0]->length();
    auto num_columns = in.size();

    auto in_batch = arrow::RecordBatch::Make(schema_, length, in);

    arrow::ArrayVector outputs;
    RETURN_NOT_OK(projector->Evaluate(*in_batch, pool_, &outputs));
    *out = outputs[0];

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<gandiva::Projector> projector;
  std::shared_ptr<arrow::Schema> schema_;
  arrow::MemoryPool* pool_;
};

arrow::Status HashAggrArrayKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<HashAggrArrayKernel>(ctx, type_list);
  return arrow::Status::OK();
}

HashAggrArrayKernel::HashAggrArrayKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list) {
  impl_.reset(new Impl(ctx, type_list));
  kernel_name_ = "HashAggrArrayKernel";
}

arrow::Status HashAggrArrayKernel::Evaluate(const ArrayList& in,
                                            std::shared_ptr<arrow::Array>* out) {
  return impl_->Evaluate(in, out);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
