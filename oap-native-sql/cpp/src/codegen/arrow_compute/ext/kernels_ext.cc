/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/arrow_compute/ext/kernels_ext.h"

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/concatenate.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/count.h>
#include <arrow/compute/kernels/hash.h>
#include <arrow/compute/kernels/minmax.h>
#include <arrow/compute/kernels/sum.h>
#include <arrow/compute/kernels/mean.h>
#include <arrow/scalar.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <arrow/visitor_inline.h>
#include <dlfcn.h>
#include <gandiva/configuration.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <unordered_map>

#include "codegen/arrow_compute/ext/actions_impl.h"
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
//#include "codegen/arrow_compute/ext/codegen_node_visitor.h"
#include "third_party/arrow/utils/hashing.h"
#include "utils/macros.h"
#include "codegen/arrow_compute/ext/window_sort_kernel.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

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
    int type_id = 0;
#ifdef DEBUG
    std::cout << "action_name_list_ has " << action_name_list_.size()
              << " elements, and type_list has " << type_list.size() << " elements."
              << std::endl;
#endif
    for (int action_id = 0; action_id < action_name_list_.size(); action_id++) {
      std::shared_ptr<ActionBase> action;
      if (action_name_list_[action_id].compare("action_unique") == 0) {
        RETURN_NOT_OK(MakeUniqueAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_count") == 0) {
        RETURN_NOT_OK(MakeCountAction(ctx_, &action));
      } else if (action_name_list_[action_id].compare("action_sum") == 0) {
        RETURN_NOT_OK(MakeSumAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_avg") == 0) {
        RETURN_NOT_OK(MakeAvgAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_min") == 0) {
        RETURN_NOT_OK(MakeMinAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_max") == 0) {
        RETURN_NOT_OK(MakeMaxAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_sum_count") == 0) {
        RETURN_NOT_OK(MakeSumCountAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_sum_count_merge") == 0) {
        RETURN_NOT_OK(MakeSumCountMergeAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_avgByCount") == 0) {
        RETURN_NOT_OK(MakeAvgByCountAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare(0, 20, "action_countLiteral_") ==
                 0) {
        int arg = std::stoi(action_name_list_[action_id].substr(20));
        RETURN_NOT_OK(MakeCountLiteralAction(ctx_, arg, &action));
      } else if (action_name_list_[action_id].compare("action_stddev_samp_partial") == 0) {
        RETURN_NOT_OK(MakeStddevSampPartialAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_stddev_samp_final") == 0) {
        RETURN_NOT_OK(MakeStddevSampFinalAction(ctx_, type_list[type_id], &action));
      } else {
        return arrow::Status::NotImplemented(action_name_list_[action_id],
                                             " is not implementetd.");
      }
      type_id += action->RequiredColNum();
      action_list_.push_back(action);
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const ArrayList& in,
                         const std::shared_ptr<arrow::Array>& in_dict) {
    if (!in_dict) {
      return arrow::Status::Invalid("input data is invalid");
    }

    // TODO: We used to use arrow::Minmax, while I noticed when there is null or -1 data
    // inside array, max will output incorrect result, change to handmade function for now
    int32_t max_group_id = 0;
    auto typed_in_dict = std::dynamic_pointer_cast<arrow::Int32Array>(in_dict);
    for (int i = 0; i < typed_in_dict->length(); i++) {
      if (typed_in_dict->IsValid(i)) {
        if (typed_in_dict->GetView(i) > max_group_id) {
          max_group_id = typed_in_dict->GetView(i);
        }
      }
    }

    std::vector<std::function<arrow::Status(int)>> eval_func_list;
    std::vector<std::function<arrow::Status()>> eval_null_func_list;
    int col_id = 0;
    ArrayList cols;
    for (int i = 0; i < action_list_.size(); i++) {
      cols.clear();
      auto action = action_list_[i];
      for (int j = 0; j < action->RequiredColNum(); j++) {
        cols.push_back(in[col_id++]);
      }
      std::function<arrow::Status(int)> func;
      std::function<arrow::Status()> null_func;
      action->Submit(cols, max_group_id, &func, &null_func);
      eval_func_list.push_back(func);
      eval_null_func_list.push_back(null_func);
    }

    for (int row_id = 0; row_id < in_dict->length(); row_id++) {
      if (in_dict->IsValid(row_id)) {
        auto group_id = typed_in_dict->GetView(row_id);
        for (auto eval_func : eval_func_list) {
          eval_func(group_id);
        }
      } else {
        for (auto eval_func : eval_null_func_list) {
          eval_func();
        }
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    for (auto action : action_list_) {
      RETURN_NOT_OK(action->Finish(out));
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    uint64_t total_length = action_list_[0]->GetResultLength();
    auto eval_func = [this, schema](uint64_t offset, uint64_t length,
                                    std::shared_ptr<arrow::RecordBatch>* out) {
      ArrayList arr_list;
      for (auto action : action_list_) {
        RETURN_NOT_OK(action->Finish(offset, length, &arr_list));
      }
      *out = arrow::RecordBatch::Make(schema, length, arr_list);
      return arrow::Status::OK();
    };
    *out = std::make_shared<SplitArrayWithActionResultIterator>(ctx_, total_length,
                                                                eval_func);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::string> action_name_list_;
  std::vector<std::shared_ptr<extra::ActionBase>> action_list_;

  class SplitArrayWithActionResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    SplitArrayWithActionResultIterator(
        arrow::compute::FunctionContext* ctx, uint64_t total_length,
        std::function<arrow::Status(uint64_t offset, uint64_t length,
                                    std::shared_ptr<arrow::RecordBatch>* out)>
            eval_func)
        : ctx_(ctx), total_length_(total_length), eval_func_(eval_func) {}
    ~SplitArrayWithActionResultIterator() {}

    std::string ToString() override { return "SplitArrayWithActionResultIterator"; }

    bool HasNext() override {
      if (offset_ >= total_length_) {
        return false;
      }
      return true;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      if (offset_ >= total_length_) {
        *out = nullptr;
        return arrow::Status::OK();
      }
      auto length = (total_length_ - offset_) > GetBatchSize()
                        ? GetBatchSize()
                        : (total_length_ - offset_);
      TIME_MICRO_OR_RAISE(elapse_time_, eval_func_(offset_, length, out));
      offset_ += length;
      // arrow::PrettyPrint(*(*out).get(), 2, &std::cout);
      return arrow::Status::OK();
    }

   private:
    arrow::compute::FunctionContext* ctx_;
    std::function<arrow::Status(uint64_t offset, uint64_t length,
                                std::shared_ptr<arrow::RecordBatch>* out)>
        eval_func_;
    uint64_t offset_ = 0;
    const uint64_t total_length_;
    uint64_t elapse_time_ = 0;
  };
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

arrow::Status SplitArrayListWithActionKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

///////////////  UniqueArray  ////////////////
/*class UniqueArrayKernel::Impl {
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
}*/

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)

///////////////  SumArray  ////////////////
class SumArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum output;
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &output));
    res_data_type_ = output.scalar()->type;
    scalar_list_.push_back(output.scalar());
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    switch (res_data_type_->id()) {
#define PROCESS(DataType)                         \
  case DataType::type_id: {                       \
    RETURN_NOT_OK(FinishInternal<DataType>(out)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    }
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    using CType = typename arrow::TypeTraits<DataType>::CType;
    CType res = 0;
    for (auto scalar_item : scalar_list_) {
      auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_item);
      res += typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::shared_ptr<arrow::DataType> res_data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> scalar_list_;
};

arrow::Status SumArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> data_type,
                                   std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<SumArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

SumArrayKernel::SumArrayKernel(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "SumArrayKernel";
}

arrow::Status SumArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status SumArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  CountArray  ////////////////
class CountArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum output;
    arrow::compute::CountOptions option(arrow::compute::CountOptions::COUNT_ALL);
    RETURN_NOT_OK(arrow::compute::Count(ctx_, option, *in[0].get(), &output));
    scalar_list_.push_back(output.scalar());
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    RETURN_NOT_OK(FinishInternal<arrow::Int64Type>(out));
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using CType = typename arrow::TypeTraits<DataType>::CType;
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    CType res = 0;
    for (auto scalar_item : scalar_list_) {
      auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_item);
      res += typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> scalar_list_;
};

arrow::Status CountArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                     std::shared_ptr<arrow::DataType> data_type,
                                     std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<CountArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

CountArrayKernel::CountArrayKernel(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "CountArrayKernel";
}

arrow::Status CountArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status CountArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  SumCountArray  ////////////////
class SumCountArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum sum_out;
    arrow::compute::Datum cnt_out;
    arrow::compute::CountOptions option(arrow::compute::CountOptions::COUNT_ALL);
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &sum_out));
    RETURN_NOT_OK(arrow::compute::Count(ctx_, option, *in[0].get(), &cnt_out));
    res_data_type_ = sum_out.scalar()->type;
    sum_scalar_list_.push_back(sum_out.scalar());
    cnt_scalar_list_.push_back(cnt_out.scalar());
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    switch (res_data_type_->id()) {
#define PROCESS(DataType)                         \
  case DataType::type_id: {                       \
    RETURN_NOT_OK(FinishInternal<DataType>(out)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    }
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using CType = typename arrow::TypeTraits<DataType>::CType;
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    using CntScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
    double sum_res = 0;
    int64_t cnt_res = 0;
    for (size_t i = 0; i < sum_scalar_list_.size(); i++) {
      auto sum_typed_scalar = std::dynamic_pointer_cast<ScalarType>(sum_scalar_list_[i]);
      auto cnt_typed_scalar =
          std::dynamic_pointer_cast<CntScalarType>(cnt_scalar_list_[i]);
      sum_res += sum_typed_scalar->value;
      cnt_res += cnt_typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> sum_out;
    std::shared_ptr<arrow::Scalar> sum_scalar_out;
    sum_scalar_out = arrow::MakeScalar(sum_res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*sum_scalar_out.get(), 1, &sum_out));

    std::shared_ptr<arrow::Array> cnt_out;
    std::shared_ptr<arrow::Scalar> cnt_scalar_out;
    cnt_scalar_out = arrow::MakeScalar(cnt_res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*cnt_scalar_out.get(), 1, &cnt_out));

    out->push_back(sum_out);
    out->push_back(cnt_out);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> res_data_type_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> sum_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> cnt_scalar_list_;
};

arrow::Status SumCountArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                        std::shared_ptr<arrow::DataType> data_type,
                                        std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<SumCountArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

SumCountArrayKernel::SumCountArrayKernel(arrow::compute::FunctionContext* ctx,
                                         std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "SumCountArrayKernel";
}

arrow::Status SumCountArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status SumCountArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  AvgByCountArray  ////////////////
class AvgByCountArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum sum_out;
    arrow::compute::Datum cnt_out;
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &sum_out));
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[1].get(), &cnt_out));
    sum_scalar_list_.push_back(sum_out.scalar());
    cnt_scalar_list_.push_back(cnt_out.scalar());
    sum_res_data_type_ = sum_out.scalar()->type;
    cnt_res_data_type_ = cnt_out.scalar()->type;
    return arrow::Status::OK();
  }

#define PROCESS_INTERNAL(SumDataType, CntDataType) \
  case CntDataType::type_id: {                     \
    FinishInternal<SumDataType, CntDataType>(out); \
  } break;

  arrow::Status Finish(ArrayList* out) {
    switch (sum_res_data_type_->id()) {
#define PROCESS(SumDataType)                           \
  case SumDataType::type_id: {                         \
    switch (cnt_res_data_type_->id()) {                \
      PROCESS_INTERNAL(SumDataType, arrow::UInt64Type) \
      PROCESS_INTERNAL(SumDataType, arrow::Int64Type)  \
      PROCESS_INTERNAL(SumDataType, arrow::DoubleType) \
    }                                                  \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
#undef PROCESS_INTERNAL
    }
    return arrow::Status::OK();
  }

  template <typename SumDataType, typename CntDataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using SumCType = typename arrow::TypeTraits<SumDataType>::CType;
    using CntCType = typename arrow::TypeTraits<CntDataType>::CType;
    using SumScalarType = typename arrow::TypeTraits<SumDataType>::ScalarType;
    using CntScalarType = typename arrow::TypeTraits<CntDataType>::ScalarType;
    SumCType sum_res = 0;
    CntCType cnt_res = 0;
    for (size_t i = 0; i < sum_scalar_list_.size(); i++) {
      auto sum_typed_scalar =
          std::dynamic_pointer_cast<SumScalarType>(sum_scalar_list_[i]);
      auto cnt_typed_scalar =
          std::dynamic_pointer_cast<CntScalarType>(cnt_scalar_list_[i]);
      sum_res += sum_typed_scalar->value;
      cnt_res += cnt_typed_scalar->value;
    }
    double res = sum_res * 1.0 / cnt_res;
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));

    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::shared_ptr<arrow::DataType> cnt_res_data_type_;
  std::shared_ptr<arrow::DataType> sum_res_data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> sum_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> cnt_scalar_list_;
};  // namespace extra

arrow::Status AvgByCountArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                          std::shared_ptr<arrow::DataType> data_type,
                                          std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<AvgByCountArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

AvgByCountArrayKernel::AvgByCountArrayKernel(arrow::compute::FunctionContext* ctx,
                                             std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "AvgByCountArrayKernel";
}

arrow::Status AvgByCountArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status AvgByCountArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  MinArray  ////////////////
class MinArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum minMaxOut;
    arrow::compute::MinMaxOptions option;
    RETURN_NOT_OK(arrow::compute::MinMax(ctx_, option, *in[0].get(), &minMaxOut));
    if (!minMaxOut.is_collection()) {
      return arrow::Status::Invalid("MinMax return an invalid result.");
    }
    auto col = minMaxOut.collection();
    if (col.size() < 2) {
      return arrow::Status::Invalid("MinMax return an invalid result.");
    }
    auto min = col[0].scalar();
    scalar_list_.push_back(min);
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    switch (data_type_->id()) {
#define PROCESS(DataType)                         \
  case DataType::type_id: {                       \
    RETURN_NOT_OK(FinishInternal<DataType>(out)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    }
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using CType = typename arrow::TypeTraits<DataType>::CType;
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_list_[0]);
    CType res = typed_scalar->value;
    for (size_t i = 1; i < scalar_list_.size(); i++) {
      auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_list_[i]);
      if (typed_scalar->value < res) res = typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> scalar_list_;
  std::unique_ptr<arrow::ArrayBuilder> array_builder_;
};

arrow::Status MinArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> data_type,
                                   std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<MinArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

MinArrayKernel::MinArrayKernel(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "MinArrayKernel";
}

arrow::Status MinArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status MinArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  MaxArray  ////////////////
class MaxArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  ~Impl() {}
  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum minMaxOut;
    arrow::compute::MinMaxOptions option;
    RETURN_NOT_OK(arrow::compute::MinMax(ctx_, option, *in[0].get(), &minMaxOut));
    if (!minMaxOut.is_collection()) {
      return arrow::Status::Invalid("MinMax return an invalid result.");
    }
    auto col = minMaxOut.collection();
    if (col.size() < 2) {
      return arrow::Status::Invalid("MinMax return an invalid result.");
    }
    auto max = col[1].scalar();
    scalar_list_.push_back(max);
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    switch (data_type_->id()) {
#define PROCESS(DataType)                         \
  case DataType::type_id: {                       \
    RETURN_NOT_OK(FinishInternal<DataType>(out)); \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    }
    return arrow::Status::OK();
  }

  template <typename DataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using CType = typename arrow::TypeTraits<DataType>::CType;
    using ScalarType = typename arrow::TypeTraits<DataType>::ScalarType;
    auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_list_[0]);
    CType res = typed_scalar->value;
    for (size_t i = 1; i < scalar_list_.size(); i++) {
      auto typed_scalar = std::dynamic_pointer_cast<ScalarType>(scalar_list_[i]);
      if (typed_scalar->value > res) res = typed_scalar->value;
    }
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    scalar_out = arrow::MakeScalar(res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> scalar_list_;
  std::unique_ptr<arrow::ArrayBuilder> array_builder_;
};

arrow::Status MaxArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> data_type,
                                   std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<MaxArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

MaxArrayKernel::MaxArrayKernel(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "MaxArrayKernel";
}

arrow::Status MaxArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status MaxArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  StddevSampPartialArray  ////////////////
class StddevSampPartialArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  ~Impl() {}

  template <typename ValueType>
  arrow::Status getM2(arrow::compute::FunctionContext* ctx, const arrow::compute::Datum& value, 
  const arrow::compute::Datum& mean, arrow::compute::Datum* out) {
    using MeanCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;
    using MeanScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;
    using ValueCType = typename arrow::TypeTraits<ValueType>::CType;
    std::shared_ptr<arrow::Scalar> mean_scalar = mean.scalar();
    auto mean_typed_scalar =
            std::dynamic_pointer_cast<MeanScalarType>(mean_scalar);
    double mean_res = mean_typed_scalar->value * 1.0;
    double m2_res = 0;

    if(!value.is_array()) {
      return arrow::Status::Invalid("AggregateKernel expects Array");
    }
    auto array = value.make_array();
    auto typed_array = std::static_pointer_cast<arrow::NumericArray<ValueType>>(array);
    const ValueCType* input = typed_array->raw_values();
    for (int64_t i = 0; i < (*array).length(); i++) {
      auto val = input[i];
      if (val) {
        m2_res += (input[i] * 1.0 - mean_res) * (input[i] * 1.0 - mean_res);
      }
    }
    *out = arrow::MakeScalar(m2_res);
    return arrow::Status::OK();
  }

  arrow::Status M2(arrow::compute::FunctionContext* ctx, const arrow::Array& array, 
  const arrow::compute::Datum& mean, arrow::compute::Datum* out) {
    arrow::compute::Datum value = array.data();
    auto data_type = value.type();

    if (data_type == nullptr)
      return arrow::Status::Invalid("Datum must be array-like");
    else if (!is_integer(data_type->id()) && !is_floating(data_type->id()))
      return arrow::Status::Invalid("Datum must contain a NumericType");
    switch (data_type_->id()) {
      #define PROCESS(DataType)                         \
        case DataType::type_id: {                       \
          RETURN_NOT_OK(getM2<DataType>(ctx, value, mean, out)); \
        } break;
            PROCESS_SUPPORTED_TYPES(PROCESS)
      #undef PROCESS
    }
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum sum_out;
    arrow::compute::Datum cnt_out;
    arrow::compute::Datum mean_out;
    arrow::compute::Datum m2_out;
    arrow::compute::CountOptions option(arrow::compute::CountOptions::COUNT_ALL);
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &sum_out));
    RETURN_NOT_OK(arrow::compute::Count(ctx_, option, *in[0].get(), &cnt_out));
    RETURN_NOT_OK(arrow::compute::Mean(ctx_, *in[0].get(), &mean_out));
    RETURN_NOT_OK(M2(ctx_, *in[0].get(), mean_out, &m2_out));
    sum_scalar_list_.push_back(sum_out.scalar());
    cnt_scalar_list_.push_back(cnt_out.scalar());
    mean_scalar_list_.push_back(mean_out.scalar());
    m2_scalar_list_.push_back(m2_out.scalar());
    sum_res_data_type_ = sum_out.scalar()->type;
    cnt_res_data_type_ = cnt_out.scalar()->type;
    mean_res_data_type_ = mean_out.scalar()->type;
    m2_res_data_type_ = m2_out.scalar()->type;
    return arrow::Status::OK();
  }

#define PROCESS_INTERNAL(SumDataType, CntDataType) \
  case CntDataType::type_id: {                     \
    FinishInternal<SumDataType, CntDataType>(out); \
  } break;

  arrow::Status Finish(ArrayList* out) {
    switch (sum_res_data_type_->id()) {
#define PROCESS(SumDataType)                           \
  case SumDataType::type_id: {                         \
    switch (cnt_res_data_type_->id()) {                \
      PROCESS_INTERNAL(SumDataType, arrow::UInt64Type) \
      PROCESS_INTERNAL(SumDataType, arrow::Int64Type)  \
      PROCESS_INTERNAL(SumDataType, arrow::DoubleType) \
    }                                                  \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
#undef PROCESS_INTERNAL
    }
    return arrow::Status::OK();
  }

  template <typename SumDataType, typename CntDataType>
  arrow::Status FinishInternal(ArrayList* out) {
    using SumCType = typename arrow::TypeTraits<SumDataType>::CType;
    using CntCType = typename arrow::TypeTraits<CntDataType>::CType;
    using DoubleCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;
    using SumScalarType = typename arrow::TypeTraits<SumDataType>::ScalarType;
    using CntScalarType = typename arrow::TypeTraits<CntDataType>::ScalarType;
    using DoubleScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;
    SumCType sum_res = 0;
    DoubleCType cnt_res = 0;
    DoubleCType mean_res = 0;
    DoubleCType m2_res = 0;
    for (size_t i = 0; i < sum_scalar_list_.size(); i++) {
      auto sum_typed_scalar =
          std::dynamic_pointer_cast<SumScalarType>(sum_scalar_list_[i]);
      auto cnt_typed_scalar =
          std::dynamic_pointer_cast<CntScalarType>(cnt_scalar_list_[i]);
      auto mean_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(mean_scalar_list_[i]);
      auto m2_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(m2_scalar_list_[i]);
      if (cnt_typed_scalar->value > 0) {
        double pre_avg = sum_res * 1.0 / (cnt_res > 0 ? cnt_res : 1);
        double delta = mean_typed_scalar->value - pre_avg;
        double newN = (cnt_res + cnt_typed_scalar->value) * 1.0;
        double deltaN = newN > 0 ? delta / newN : 0.0;
        m2_res += m2_typed_scalar->value + 
          delta * deltaN * cnt_res * cnt_typed_scalar->value;
        sum_res += sum_typed_scalar->value;
        cnt_res += cnt_typed_scalar->value  * 1.0;
      }
    }
    double avg = 0;
    if (cnt_res > 0) {
      avg = sum_res * 1.0 / cnt_res;
    } else {
      m2_res = 0;
    } 
    std::shared_ptr<arrow::Array> cnt_out;
    std::shared_ptr<arrow::Scalar> cnt_scalar_out;
    cnt_scalar_out = arrow::MakeScalar(cnt_res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*cnt_scalar_out.get(), 1, &cnt_out));

    std::shared_ptr<arrow::Array> avg_out;
    std::shared_ptr<arrow::Scalar> avg_scalar_out;
    avg_scalar_out = arrow::MakeScalar(avg);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*avg_scalar_out.get(), 1, &avg_out));

    std::shared_ptr<arrow::Array> m2_out;
    std::shared_ptr<arrow::Scalar> m2_scalar_out;
    m2_scalar_out = arrow::MakeScalar(m2_res);
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*m2_scalar_out.get(), 1, &m2_out));

    out->push_back(cnt_out);
    out->push_back(avg_out);
    out->push_back(m2_out);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::shared_ptr<arrow::DataType> sum_res_data_type_;
  std::shared_ptr<arrow::DataType> cnt_res_data_type_;
  std::shared_ptr<arrow::DataType> mean_res_data_type_;
  std::shared_ptr<arrow::DataType> m2_res_data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> sum_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> cnt_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> mean_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> m2_scalar_list_;
};

arrow::Status StddevSampPartialArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                        std::shared_ptr<arrow::DataType> data_type,
                                        std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<StddevSampPartialArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

StddevSampPartialArrayKernel::StddevSampPartialArrayKernel(arrow::compute::FunctionContext* ctx,
                                         std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "StddevSampPartialArrayKernel";
}

arrow::Status StddevSampPartialArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status StddevSampPartialArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

///////////////  StddevSampFinalArray  ////////////////
class StddevSampFinalArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx, std::shared_ptr<arrow::DataType> data_type)
      : ctx_(ctx), data_type_(data_type) {}
  ~Impl() {}

  arrow::Status getAvgM2(arrow::compute::FunctionContext* ctx, const arrow::compute::Datum& cnt_value, 
  const arrow::compute::Datum& avg_value, const arrow::compute::Datum& m2_value, 
  arrow::compute::Datum* avg_out, arrow::compute::Datum* m2_out) {
    using MeanCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;
    using MeanScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;
    using ValueCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;

    if(!(cnt_value.is_array() && avg_value.is_array() && m2_value.is_array())) {
      return arrow::Status::Invalid("AggregateKernel expects Array datum");
    }

    auto cnt_array = cnt_value.make_array();
    auto avg_array = avg_value.make_array();
    auto m2_array = m2_value.make_array();

    auto cnt_typed_array = std::static_pointer_cast<arrow::DoubleArray>(cnt_array);
    auto avg_typed_array = std::static_pointer_cast<arrow::DoubleArray>(avg_array);
    auto m2_typed_array = std::static_pointer_cast<arrow::DoubleArray>(m2_array);
    const ValueCType* cnt_input = cnt_typed_array->raw_values();
    const MeanCType* avg_input = avg_typed_array->raw_values();
    const MeanCType* m2_input = m2_typed_array->raw_values();

    double cnt_res = 0;
    double avg_res = 0;
    double m2_res = 0;
    for (int64_t i = 0; i < (*cnt_array).length(); i++) {
      double cnt_val = cnt_input[i];
      double avg_val = avg_input[i];
      double m2_val = m2_input[i];
      if (i == 0) {
        cnt_res = cnt_val;
        avg_res = avg_val;
        m2_res = m2_val;
      } else {
        if (cnt_val > 0) {
          double delta = avg_val - avg_res;
          double deltaN = (cnt_res + cnt_val) > 0 ? delta / (cnt_res + cnt_val) : 0;
          avg_res += deltaN * cnt_val;
          m2_res += (m2_val + delta * deltaN * cnt_res * cnt_val);
          cnt_res += cnt_val;
        }
      }
    }
    *avg_out = arrow::MakeScalar(avg_res);
    *m2_out = arrow::MakeScalar(m2_res);
    return arrow::Status::OK();
  }

  arrow::Status updateValue(arrow::compute::FunctionContext* ctx, const arrow::Array& cnt_array, 
  const arrow::Array& avg_array, const arrow::Array& m2_array, arrow::compute::Datum* avg_out, 
  arrow::compute::Datum* m2_out) {
    arrow::compute::Datum cnt_value = cnt_array.data();
    arrow::compute::Datum avg_value = avg_array.data();
    arrow::compute::Datum m2_value = m2_array.data();
    auto cnt_data_type = cnt_value.type();
    if (cnt_data_type == nullptr)
      return arrow::Status::Invalid("Datum must be array-like");
    else if (!is_integer(cnt_data_type->id()) && !is_floating(cnt_data_type->id()))
      return arrow::Status::Invalid("Datum must contain a NumericType");
    RETURN_NOT_OK(getAvgM2(ctx, cnt_value, avg_value, m2_value, avg_out, m2_out));
    return arrow::Status::OK();
  }

  arrow::Status Evaluate(const ArrayList& in) {
    arrow::compute::Datum cnt_out;
    arrow::compute::Datum avg_out;
    arrow::compute::Datum m2_out;
    RETURN_NOT_OK(arrow::compute::Sum(ctx_, *in[0].get(), &cnt_out));
    RETURN_NOT_OK(updateValue(ctx_, *in[0].get(), *in[1].get(), *in[2].get(), &avg_out, &m2_out));
    cnt_scalar_list_.push_back(cnt_out.scalar());
    avg_scalar_list_.push_back(avg_out.scalar());
    m2_scalar_list_.push_back(m2_out.scalar());
    cnt_res_data_type_ = cnt_out.scalar()->type;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    using DoubleCType = typename arrow::TypeTraits<arrow::DoubleType>::CType;
    using DoubleScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;

    DoubleCType cnt_res = 0;
    DoubleCType avg_res = 0;
    DoubleCType m2_res = 0;
    for (size_t i = 0; i < cnt_scalar_list_.size(); i++) {
      auto cnt_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(cnt_scalar_list_[i]);
      auto avg_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(avg_scalar_list_[i]);
      auto m2_typed_scalar =
          std::dynamic_pointer_cast<DoubleScalarType>(m2_scalar_list_[i]);
      if (i == 0) {
        cnt_res = cnt_typed_scalar->value;
        avg_res = avg_typed_scalar->value;
        m2_res = m2_typed_scalar->value;
      } else {
        if (cnt_typed_scalar->value > 0) {
          double delta = avg_typed_scalar->value - avg_res;
          double newN = cnt_res + cnt_typed_scalar->value;
          double deltaN = newN > 0 ? delta / newN : 0;
          avg_res += deltaN * cnt_typed_scalar->value;
          m2_res += (m2_typed_scalar->value + delta * deltaN * cnt_res * cnt_typed_scalar->value);
          cnt_res += cnt_typed_scalar->value;
        }
      }
    }

    std::shared_ptr<arrow::Array> stddev_samp_out;
    std::shared_ptr<arrow::Scalar> stddev_samp_scalar_out;
    if (cnt_res - 1 < 0.00001) {
      //double stddev_samp = std::numeric_limits<double>::quiet_NaN();
      double stddev_samp = std::numeric_limits<double>::infinity();
      stddev_samp_scalar_out = arrow::MakeScalar(stddev_samp);
    } else if (cnt_res < 0.00001) {
      stddev_samp_scalar_out = MakeNullScalar(arrow::float64());
    } else {
      double stddev_samp = sqrt(m2_res / (cnt_res > 1 ? (cnt_res - 1) : 1));
      stddev_samp_scalar_out = arrow::MakeScalar(stddev_samp);
    }
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*stddev_samp_scalar_out.get(), 1, &stddev_samp_out));
    out->push_back(stddev_samp_out);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::shared_ptr<arrow::DataType> cnt_res_data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> cnt_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> avg_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> m2_scalar_list_;
};

arrow::Status StddevSampFinalArrayKernel::Make(arrow::compute::FunctionContext* ctx,
                                        std::shared_ptr<arrow::DataType> data_type,
                                        std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<StddevSampFinalArrayKernel>(ctx, data_type);
  return arrow::Status::OK();
}

StddevSampFinalArrayKernel::StddevSampFinalArrayKernel(arrow::compute::FunctionContext* ctx,
                                         std::shared_ptr<arrow::DataType> data_type) {
  impl_.reset(new Impl(ctx, data_type));
  kernel_name_ = "StddevSampFinalArrayKernel";
}

arrow::Status StddevSampFinalArrayKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status StddevSampFinalArrayKernel::Finish(ArrayList* out) { return impl_->Finish(out); }

#undef PROCESS_SUPPORTED_TYPES

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
    hash_table_ = std::make_shared<MemoTableType>(ctx_->memory_pool());
    builder_ = std::make_shared<arrow::Int32Builder>(ctx_->memory_pool());
  }
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                         std::shared_ptr<arrow::Array>* out) {
    // arrow::compute::Datum input_datum(in);
    // RETURN_NOT_OK(arrow::compute::Group<InType>(ctx_, input_datum, hash_table_, out));
    // we should put items into hashmap
    builder_->Reset();
    auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
    auto insert_on_found = [this](int32_t i) { builder_->Append(i); };
    auto insert_on_not_found = [this](int32_t i) { builder_->Append(i); };

    int cur_id = 0;
    int memo_index = 0;
    if (typed_array->null_count() == 0) {
      for (; cur_id < typed_array->length(); cur_id++) {
        hash_table_->GetOrInsert(typed_array->GetView(cur_id), insert_on_found,
                                 insert_on_not_found, &memo_index);
      }
    } else {
      for (; cur_id < typed_array->length(); cur_id++) {
        if (typed_array->IsNull(cur_id)) {
          hash_table_->GetOrInsertNull(insert_on_found, insert_on_not_found);
        } else {
          hash_table_->GetOrInsert(typed_array->GetView(cur_id), insert_on_found,
                                   insert_on_not_found, &memo_index);
        }
      }
    }
    RETURN_NOT_OK(builder_->Finish(out));
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<MemoTableType> hash_table_;
  std::shared_ptr<arrow::Int32Builder> builder_;
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
      default: {
        std::cout << "EncodeArrayKernel type not found, type is " << in->type()
                  << std::endl;
      } break;
    }
  }
  return impl_->Evaluate(in, out);
}
#undef PROCESS_SUPPORTED_TYPES

///////////////  HashAggrArray  ////////////////
class HashArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::DataType>> type_list)
      : ctx_(ctx) {
    // create a new result array type here
    std::vector<std::shared_ptr<gandiva::Node>> func_node_list = {};
    std::vector<std::shared_ptr<arrow::Field>> field_list = {};

    gandiva::ExpressionPtr expr;
    int index = 0;
    for (auto type : type_list) {
      auto field = arrow::field(std::to_string(index), type);
      field_list.push_back(field);
      auto field_node = gandiva::TreeExprBuilder::MakeField(field);
      auto func_node =
          gandiva::TreeExprBuilder::MakeFunction("hash64", {field_node}, arrow::int64());
      func_node_list.push_back(func_node);
      if (func_node_list.size() == 2) {
        auto shift_func_node = gandiva::TreeExprBuilder::MakeFunction(
            "multiply",
            {func_node_list[0], gandiva::TreeExprBuilder::MakeLiteral((int64_t)10)},
            arrow::int64());
        auto tmp_func_node = gandiva::TreeExprBuilder::MakeFunction(
            "add", {shift_func_node, func_node_list[1]}, arrow::int64());
        func_node_list.clear();
        func_node_list.push_back(tmp_func_node);
      }
      index++;
    }
    expr = gandiva::TreeExprBuilder::MakeExpression(func_node_list[0],
                                                    arrow::field("res", arrow::int64()));
#ifdef DEBUG
    std::cout << expr->ToString() << std::endl;
#endif
    schema_ = arrow::schema(field_list);
    auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
    auto status = gandiva::Projector::Make(schema_, {expr}, configuration, &projector);
    pool_ = ctx_->memory_pool();
  }

  arrow::Status Evaluate(const ArrayList& in, std::shared_ptr<arrow::Array>* out) {
    auto length = in[0]->length();
    auto num_columns = in.size();

    auto in_batch = arrow::RecordBatch::Make(schema_, length, in);
    // arrow::PrettyPrintOptions print_option(2, 500);
    // arrow::PrettyPrint(*in_batch.get(), print_option, &std::cout);

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

arrow::Status HashArrayKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<HashArrayKernel>(ctx, type_list);
  return arrow::Status::OK();
}

HashArrayKernel::HashArrayKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::DataType>> type_list) {
  impl_.reset(new Impl(ctx, type_list));
  kernel_name_ = "HashArrayKernel";
}

arrow::Status HashArrayKernel::Evaluate(const ArrayList& in,
                                        std::shared_ptr<arrow::Array>* out) {
  return impl_->Evaluate(in, out);
}

WindowPartitionKernel::WindowPartitionKernel(arrow::compute::FunctionContext *ctx):
    EncodeArrayKernel(ctx) {
  kernel_name_ = "WindowPartitionKernel";
}

arrow::Status WindowPartitionKernel::Make(arrow::compute::FunctionContext *ctx,
                                          std::shared_ptr<KernalBase> *out) {
  *out = std::make_shared<WindowPartitionKernel>(ctx);
  return arrow::Status::OK();
}

class WindowAggregateFunctionKernel::ActionFactory {
 public:
  ActionFactory(std::shared_ptr<ActionBase> action) {
    action_ = action;
  }

  static arrow::Status Make(std::string action_name,
                            arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionFactory>* out) {
    std::shared_ptr<ActionBase> action;
    if (action_name == "sum") {
      RETURN_NOT_OK(MakeSumAction(ctx, type, &action));
    } else if (action_name == "avg") {
      RETURN_NOT_OK(MakeAvgAction(ctx, type, &action));
    } else {
      return arrow::Status::Invalid("window aggregate function: unsupported action name: " + action_name);
    }
    *out = std::make_shared<ActionFactory>(action);
    return arrow::Status::OK();
  }

  std::shared_ptr<ActionBase> Get() {
    return action_;
  }


 private:
  std::shared_ptr<ActionBase> action_;
};

arrow::Status WindowAggregateFunctionKernel::Make(arrow::compute::FunctionContext *ctx,
                                                  std::string function_name,
                                                  std::vector<std::shared_ptr<arrow::DataType>> type_list,
                                                  std::shared_ptr<arrow::DataType> result_type,
                                                  std::shared_ptr<KernalBase> *out) {
  if (type_list.size() != 1) {
    return arrow::Status::Invalid("given more than 1 input argument for window function: " + function_name);
  }
  std::shared_ptr<ActionFactory> action;


  if (function_name == "sum" || function_name == "avg") {
    RETURN_NOT_OK(ActionFactory::Make(function_name, ctx, type_list[0], &action));
  } else {
    return arrow::Status::Invalid("window function not supported: " + function_name);
  }
  auto accumulated_group_ids = std::vector<std::shared_ptr<arrow::Int32Array>>();
  *out = std::make_shared<WindowAggregateFunctionKernel>(ctx, type_list, result_type, accumulated_group_ids, action);
  return arrow::Status::OK();
}

WindowAggregateFunctionKernel::WindowAggregateFunctionKernel(arrow::compute::FunctionContext *ctx,
                                                             std::vector<std::shared_ptr<arrow::DataType>> type_list,
                                                             std::shared_ptr<arrow::DataType> result_type,
                                                             std::vector<std::shared_ptr<arrow::Int32Array>> accumulated_group_ids,
                                                             std::shared_ptr<ActionFactory> action) {
  ctx_ = ctx;
  type_list_ = type_list;
  result_type_ = result_type;
  accumulated_group_ids_ = accumulated_group_ids;
  action_ = action;
  kernel_name_ = "WindowAggregateFunctionKernel";
}

/**
 * | a | group |   | group | sum |          | result |
 * | 2 |     0 | + |     0 |   8 |   --->   |      8 |
 * | 3 |     1 |   |     1 |   3 |          |      3 |
 * | 6 |     0 |                            |      8 |
 */
arrow::Status WindowAggregateFunctionKernel::Evaluate(const ArrayList &in) {
  // abstract following code to do common inter-window processing

  int32_t max_group_id = 0;
  std::shared_ptr<arrow::Array> group_id_array = in[1];
  auto group_ids = std::dynamic_pointer_cast<arrow::Int32Array>(group_id_array);
  accumulated_group_ids_.push_back(group_ids);
  for (int i = 0; i < group_ids->length(); i++) {
    if (group_ids->IsNull(i)) {
      continue;
    }
    if (group_ids->GetView(i) > max_group_id) {
      max_group_id = group_ids->GetView(i);
    }
  }
  
  ArrayList action_input_data;
  action_input_data.push_back(in[0]);
  std::function<arrow::Status(int)> func;
  std::function<arrow::Status()> null_func;
  RETURN_NOT_OK(action_->Get()->Submit(action_input_data, max_group_id, &func, &null_func));


  for (int row_id = 0; row_id < group_id_array->length(); row_id++) {
    if (group_ids->IsNull(row_id)) {
      RETURN_NOT_OK(null_func());
      continue;
    }
    auto group_id = group_ids->GetView(row_id);
    RETURN_NOT_OK(func(group_id));
  }

  return arrow::Status::OK();
}

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::FloatType)              \
  PROCESS(arrow::DoubleType)

arrow::Status WindowAggregateFunctionKernel::Finish(ArrayList* out) {
  std::shared_ptr<arrow::DataType> value_type = result_type_;
  switch (value_type->id()) {
#define PROCESS(NUMERIC_TYPE)                                                      \
  case NUMERIC_TYPE::type_id: {                                                    \
    RETURN_NOT_OK(Finish0<NUMERIC_TYPE>(out));                                     \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      return arrow::Status::Invalid("window function: unsupported input type");
  }
  return arrow::Status::OK();
}

template<typename ArrowType>
arrow::Status WindowAggregateFunctionKernel::Finish0(ArrayList* out) {
  ArrayList action_output;
  RETURN_NOT_OK(action_->Get()->Finish(&action_output));
  if (action_output.size() != 1) {
    return arrow::Status::Invalid("window function: got invalid result from corresponding action");
  }

  auto action_output_values = std::dynamic_pointer_cast<arrow::NumericArray<ArrowType>>(action_output.at(0));

  for (const auto& accumulated_group_ids_single_part : accumulated_group_ids_) {
    std::unique_ptr<arrow::NumericBuilder<ArrowType>> output_builder
        = std::make_unique<arrow::NumericBuilder<ArrowType>>(ctx_->memory_pool());

    for (int i = 0; i < accumulated_group_ids_single_part->length(); i++) {
      if (accumulated_group_ids_single_part->IsNull(i)) {
        RETURN_NOT_OK(output_builder->AppendNull());
        continue;
      }
      int32_t group_id = accumulated_group_ids_single_part->GetView(i);
      RETURN_NOT_OK(output_builder->Append(action_output_values->GetView(group_id)));
    }
    std::shared_ptr<arrow::Array> out_array;
    RETURN_NOT_OK(output_builder->Finish(&out_array));
    (*out).push_back(out_array);
  }
  return arrow::Status::OK();
}

WindowRankKernel::WindowRankKernel(arrow::compute::FunctionContext *ctx,
                                   std::vector<std::shared_ptr<arrow::DataType>> type_list,
                                   std::shared_ptr<WindowSortKernel::Impl> sorter,
                                   bool desc) {
  ctx_ = ctx;
  type_list_ = type_list;
  sorter_ = sorter;
  desc_ = desc;
}

arrow::Status WindowRankKernel::Make(arrow::compute::FunctionContext* ctx,
                                     std::string function_name,
                                     std::vector<std::shared_ptr<arrow::DataType>> type_list,
                                     std::shared_ptr<KernalBase>* out,
                                     bool desc) {
  std::vector<std::shared_ptr<arrow::Field>> key_fields;
  for (int i = 0; i < type_list.size(); i++) {
    key_fields.push_back(std::make_shared<arrow::Field>("sort_key" + std::to_string(i), type_list.at(i)));
  }
  std::shared_ptr<arrow::Schema> result_schema = std::make_shared<arrow::Schema>(key_fields);

  std::shared_ptr<WindowSortKernel::Impl> sorter;
  // fixme null ordering flag and collation flag 
  bool nulls_first = false;
  bool asc = !desc;
  if (key_fields.size() == 1) {
    std::shared_ptr<arrow::Field> key_field = key_fields[0];
    if (key_field->type()->id() == arrow::Type::STRING) {
      sorter.reset(
          new WindowSortOnekeyKernel<arrow::StringType, std::string>(ctx, key_fields,
                                                                     result_schema, nulls_first, asc));
    } else {
      switch (key_field->type()->id()) {
#define PROCESS(InType)                                                       \
  case InType::type_id: {                                                     \
    using CType = typename arrow::TypeTraits<InType>::CType;                  \
    sorter.reset(new WindowSortOnekeyKernel<InType, CType>(ctx, key_fields, result_schema, nulls_first, asc));  \
  } break;
        PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
        default: {
          std::cout << "WindowSortOnekeyKernel type not supported, type is "
                    << key_field->type() << std::endl;
        }
          break;
      }
    }
  } else {
    sorter.reset(new WindowSortKernel::Impl(ctx, key_fields, result_schema, nulls_first, asc));
    auto status = sorter->LoadJITFunction(key_fields, result_schema);
    if (!status.ok()) {
      std::cout << "LoadJITFunction failed, msg is " << status.message() << std::endl;
      throw;
    }
  }
  *out = std::make_shared<WindowRankKernel>(ctx, type_list, sorter, desc);
  return arrow::Status::OK();
}

arrow::Status WindowRankKernel::Evaluate(const ArrayList &in) {
  input_cache_.push_back(in);
  return arrow::Status::OK();
}

arrow::Status WindowRankKernel::Finish(ArrayList* out) {
  ArrayList values_concatenated;
  std::shared_ptr<arrow::Int32Array> group_ids_concatenated;
  for (int i = 0; i < type_list_.size() + 1; i++) {
    ArrayList column_builder;
    for (auto batch : input_cache_) {
      auto column_slice = batch.at(i);
      column_builder.push_back(column_slice);
    }
    std::shared_ptr<arrow::Array> column;
    RETURN_NOT_OK(arrow::Concatenate(column_builder, ctx_->memory_pool(), &column));
    if (i == type_list_.size()) {
      // we are at the column of partition ids
      group_ids_concatenated = std::dynamic_pointer_cast<arrow::Int32Array>(column);
      continue;
    }
    values_concatenated.push_back(column);
  }
  int32_t max_group_id = 0;
  for (int i = 0; i < group_ids_concatenated->length(); i++) {
    if (group_ids_concatenated->IsNull(i)) {
      continue;
    }
    if (group_ids_concatenated->GetView(i) > max_group_id) {
      max_group_id = group_ids_concatenated->GetView(i);
    }
  }
  // initialize partitions to be sorted
  std::vector<std::shared_ptr<arrow::UInt64Builder>> partitions_to_sort;
  for (int i = 0; i <= max_group_id; i++) {
    partitions_to_sort.push_back(std::make_shared<arrow::UInt64Builder>(ctx_->memory_pool()));
  }

  for (int i = 0; i < group_ids_concatenated->length(); i++) {
    if (group_ids_concatenated->IsNull(i)) {
      continue;
    }
    uint64_t partition_id = group_ids_concatenated->GetView(i);
    RETURN_NOT_OK(partitions_to_sort.at(partition_id)->Append(i));
  }

  std::vector<std::shared_ptr<arrow::UInt64Array>> sorted_partitions;
  RETURN_NOT_OK(SortToIndicesPrepare(values_concatenated));
  for (int i = 0; i <= max_group_id; i++) {
    std::shared_ptr<arrow::UInt64Array> partition;
    RETURN_NOT_OK(partitions_to_sort.at(i)->Finish(&partition));
    std::shared_ptr<arrow::UInt64Array> sorted_partition;
    RETURN_NOT_OK(SortToIndicesFinish(partition, &sorted_partition));
    sorted_partitions.push_back(sorted_partition);
  }
  int64_t length = group_ids_concatenated->length();
  int32_t* rank_array = new int32_t[length];
  for (int i = 0; i <= max_group_id; i++) {
    std::shared_ptr<arrow::UInt64Array> sorted_partition = sorted_partitions.at(i);
    int assumed_rank = 0;
    for (int j = 0; j < sorted_partition->length(); j++) {
      ++assumed_rank; // rank value starts from 1
      uint64_t index = sorted_partition->GetView(j);
      if (j == 0) {
        rank_array[index] = 1; // rank value starts from 1
        continue;
      }
      uint64_t last_index = sorted_partition->GetView(j - 1);
      bool same = true;
      for (int i = 0; i < type_list_.size(); i++) {
        bool s;
        std::shared_ptr<arrow::DataType> type = type_list_.at(i);
        switch (type->id()) {
#define PROCESS(InType)                                                       \
  case InType::type_id: {                                                     \
    using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;                  \
      RETURN_NOT_OK(AreTheSameValue<ArrayType>(values_concatenated.at(i), index, last_index, &s));  \
  } break;
          PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
          default: {
            std::cout << "WindowRankKernel: type not supported: "
                      << type->ToString() << std::endl; // todo use arrow::Status
          }
          break;
        }
        if (!s) {
          same = false;
          break;
        }
      }
      if (same) {
        rank_array[index] = rank_array[last_index];
        continue;
      }
      rank_array[index] = assumed_rank;
    }
  }
  int offset_in_rank_array = 0;
  for (auto batch : input_cache_) {
    auto group_id_column_slice = batch.at(type_list_.size());
    int slice_length = group_id_column_slice->length();
    std::shared_ptr<arrow::Int32Builder> rank_builder = std::make_shared<arrow::Int32Builder>(ctx_->memory_pool());
    for (int i = 0; i < slice_length; i++) {
      RETURN_NOT_OK(rank_builder->Append(rank_array[offset_in_rank_array++]));
    }
    std::shared_ptr<arrow::Int32Array> rank_slice;
    RETURN_NOT_OK(rank_builder->Finish(&rank_slice));
    out->push_back(rank_slice);
  }
  // make sure offset hits bound
  if (offset_in_rank_array != length) {
    return arrow::Status::Invalid("window: fatal error, this should not happen");
  }
  return arrow::Status::OK();
}

arrow::Status WindowRankKernel::SortToIndicesPrepare(ArrayList values) {
#ifdef DEBUG
  std::cout << "RANK: values to sort: " << values.at(0)->ToString() << std::endl;
#endif
  RETURN_NOT_OK(sorter_->Evaluate(values));
  return arrow::Status::OK();
  // todo sort algorithm
}

arrow::Status WindowRankKernel::SortToIndicesFinish(std::shared_ptr<arrow::UInt64Array> elements_to_sort,
                                              std::shared_ptr<arrow::UInt64Array>* offsets) {
  std::vector<std::shared_ptr<arrow::Array>> elements_to_sort_list = {elements_to_sort};
#ifdef DEBUG
  std::cout << "RANK: partition: " << elements_to_sort->ToString() << std::endl;
#endif
  std::shared_ptr<arrow::Array> out;
  RETURN_NOT_OK(sorter_->Finish(elements_to_sort_list, &out));
  *offsets = std::dynamic_pointer_cast<arrow::UInt64Array>(out);
#ifdef DEBUG
  std::cout << "RANK: partition sorted: " << out->ToString() << std::endl;
#endif
  return arrow::Status::OK();
  // todo sort algorithm
}

template<typename ArrayType>
arrow::Status WindowRankKernel::AreTheSameValue(std::shared_ptr<arrow::Array> values, int i, int j, bool* out) {
  auto typed_array = std::dynamic_pointer_cast<ArrayType>(values);
  *out = (typed_array->GetView(i) == typed_array->GetView(j));
  return arrow::Status::OK();
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
