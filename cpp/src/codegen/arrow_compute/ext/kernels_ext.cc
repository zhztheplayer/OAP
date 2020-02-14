#include "codegen/arrow_compute/ext/kernels_ext.h"
#include <arrow/array/builder_binary.h>
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

#define MAXBATCHNUMROWS 10000

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
      } else if (action_name_list_[action_id].compare("action_sum_count") == 0) {
        RETURN_NOT_OK(MakeSumCountAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_avgByCount") == 0) {
        RETURN_NOT_OK(MakeAvgByCountAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare(0, 20, "action_countLiteral_") ==
                 0) {
        int arg = std::stoi(action_name_list_[action_id].substr(20));
        RETURN_NOT_OK(MakeCountLiteralAction(ctx_, arg, &action));
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
    int col_id = 0;
    ArrayList cols;
    for (int i = 0; i < action_list_.size(); i++) {
      cols.clear();
      auto action = action_list_[i];
      for (int j = 0; j < action->RequiredColNum(); j++) {
        cols.push_back(in[col_id++]);
      }
      std::function<arrow::Status(int)> func;
      action->Submit(cols, max_group_id, &func);
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
      RETURN_NOT_OK(action->Finish(out));
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

  arrow::Status Evaluate(const ArrayList& in, ArrayList* out) {
    if (!in_indices_iter_) {
      return arrow::Status::Invalid(
          "ShuffleArrayListKernel Evaluate expects dependency iterator.");
    }
    std::shared_ptr<arrow::RecordBatch> child_out;
    RETURN_NOT_OK(in_indices_iter_->GetResult(&child_out));
    in_indices_ = child_out->column(iter_result_index_);
    // TODO: we can use gandiva project function
    if (in_indices_->length() == 0) {
      *out = in;
      return arrow::Status::OK();
    }
    std::vector<std::function<arrow::Status(uint32_t)>> eval_func_list;
    std::vector<std::function<arrow::Status()>> eval_null_func_list;
    for (int i = 0; i < in.size(); i++) {
      auto col = in[i];
      auto action = action_list_[i];
      std::function<arrow::Status(uint32_t)> func;
      std::function<arrow::Status()> null_func;
      action->Submit(col, in_indices_->length(), &func, &null_func);
      eval_func_list.push_back(func);
      eval_null_func_list.push_back(null_func);
    }

    uint32_t* data = (uint32_t*)std::dynamic_pointer_cast<arrow::UInt32Array>(in_indices_)
                         ->raw_values();

    if (in_indices_->null_count() != 0) {
      auto valid_reader = std::make_shared<arrow::internal::BitmapReader>(
          in_indices_->data()->buffers[0]->data(), in_indices_->data()->offset,
          in_indices_->data()->length);
      for (int i = 0; i < in_indices_->length(); i++) {
        if (valid_reader->IsNotSet()) {
          for (auto eval_func : eval_null_func_list) {
            eval_func();
          }
        } else {
          for (auto eval_func : eval_func_list) {
            eval_func(*(data + i));
          }
        }
        valid_reader->Next();
      }
    } else {
      for (int i = 0; i < in_indices_->length(); i++) {
        for (auto eval_func : eval_func_list) {
          eval_func(*(data + i));
        }
      }
    }

    for (auto action : action_list_) {
      RETURN_NOT_OK(action->FinishAndReset(out));
    }
    return arrow::Status::OK();
  }

  arrow::Status SetDependencyInput(const std::shared_ptr<arrow::Array>& in) {
    in_indices_ = in;
    return arrow::Status::OK();
  }

  arrow::Status SetDependencyIter(
      const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& in, int index) {
    in_indices_iter_ = in;
    iter_result_index_ = index;
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    if (input_cache_.size() == 0 || !in_indices_) {
      return arrow::Status::Invalid("input data is invalid");
    }

    std::vector<std::function<arrow::Status(uint64_t, uint64_t)>> eval_func_list;
    std::vector<std::function<arrow::Status()>> eval_null_func_list;
    for (int i = 0; i < input_cache_.size(); i++) {
      auto col_list = input_cache_[i];
      auto action = action_list_[i];
      std::function<arrow::Status(uint64_t, uint64_t)> func;
      std::function<arrow::Status()> null_func;
      action->Submit(col_list, in_indices_->length(), &func, &null_func);
      eval_func_list.push_back(func);
      eval_null_func_list.push_back(null_func);
    }

    ArrayItemIndex* data =
        (ArrayItemIndex*)std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(
            in_indices_)
            ->raw_values();

    std::shared_ptr<arrow::internal::BitmapReader> valid_reader;
    if (in_indices_->null_count() != 0) {
      valid_reader = std::make_shared<arrow::internal::BitmapReader>(
          in_indices_->data()->buffers[0]->data(), in_indices_->data()->offset,
          in_indices_->data()->length);
    }

    if (valid_reader) {
      for (int row_id = 0; row_id < in_indices_->length(); row_id++) {
        if (valid_reader->IsNotSet()) {
          for (auto eval_func : eval_null_func_list) {
            eval_func();
          }
        } else {
          ArrayItemIndex* item = data + row_id;
          for (auto eval_func : eval_func_list) {
            eval_func(item->array_id, item->id);
          }
        }
        valid_reader->Next();
      }
    } else {
      for (int row_id = 0; row_id < in_indices_->length(); row_id++) {
        ArrayItemIndex* item = data + row_id;
        for (auto eval_func : eval_func_list) {
          eval_func(item->array_id, item->id);
        }
      }
    }
    for (auto action : action_list_) {
      RETURN_NOT_OK(action->Finish(out));
    }
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    if (input_cache_.size() == 0) {
      return arrow::Status::Invalid("input data is invalid");
    }

    std::vector<std::function<arrow::Status(uint64_t, uint64_t)>> eval_func_list;
    std::vector<std::function<arrow::Status()>> eval_null_func_list;
    for (int i = 0; i < input_cache_.size(); i++) {
      auto col_list = input_cache_[i];
      auto action = action_list_[i];
      std::function<arrow::Status(uint64_t, uint64_t)> func;
      std::function<arrow::Status()> null_func;
      action->Submit(col_list, MAXBATCHNUMROWS, &func, &null_func);
      eval_func_list.push_back(func);
      eval_null_func_list.push_back(null_func);
    }

    if (in_indices_) {
      *out = std::make_shared<ShuffleArrayListResultIterator>(
          in_indices_, schema, action_list_, eval_func_list, eval_null_func_list);
    } else if (in_indices_iter_) {
      *out = std::make_shared<ShuffleArrayListResultIterator>(
          in_indices_iter_, iter_result_index_, schema, action_list_, eval_func_list,
          eval_null_func_list);
    } else {
      return arrow::Status::Invalid(
          "ShuffleArrayListKernel MakeResultIterator require either in_indices or "
          "in_indices_iter.");
    }

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::shared_ptr<extra::ActionBase>> action_list_;
  std::shared_ptr<arrow::Array> in_indices_;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> in_indices_iter_;
  int iter_result_index_;
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
        std::vector<std::function<arrow::Status(uint64_t, uint64_t)>> eval_func_list,
        std::vector<std::function<arrow::Status()>> eval_null_func_list)
        : action_list_(action_list),
          eval_func_list_(eval_func_list),
          eval_null_func_list_(eval_null_func_list) {
      data_ = (ArrayItemIndex*)std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(
                  in_indices)
                  ->raw_values();
      if (in_indices->null_count() != 0) {
        valid_reader_ = std::make_shared<arrow::internal::BitmapReader>(
            in_indices->data()->buffers[0]->data(), in_indices->data()->offset,
            in_indices->data()->length);
      }
      total_length_ = in_indices->length();
      schema_ = schema;
      in_indices_ = in_indices;
    }

    ShuffleArrayListResultIterator(
        std::shared_ptr<ResultIterator<arrow::RecordBatch>> child, int child_result_index,
        std::shared_ptr<arrow::Schema> schema,
        std::vector<std::shared_ptr<extra::ActionBase>> action_list,
        std::vector<std::function<arrow::Status(uint64_t, uint64_t)>> eval_func_list,
        std::vector<std::function<arrow::Status()>> eval_null_func_list)
        : action_list_(action_list),
          child_result_index_(child_result_index),
          eval_func_list_(eval_func_list),
          eval_null_func_list_(eval_null_func_list) {
      child_ = child;
      schema_ = schema;
    }

    bool HasNext() {
      if (child_) {
        return child_->HasNext();
      } else if (row_id_ < total_length_) {
        return true;
      } else {
        return false;
      }
      return false;
    }

    arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) {
      if (child_) {
        std::shared_ptr<arrow::RecordBatch> child_out;
        RETURN_NOT_OK(child_->GetResult(&child_out));
        in_indices_ = child_out->column(child_result_index_);
        data_ = (ArrayItemIndex*)std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(
                    in_indices_)
                    ->raw_values();
        valid_reader_.reset();
        if (in_indices_->null_count() != 0) {
          valid_reader_ = std::make_shared<arrow::internal::BitmapReader>(
              in_indices_->data()->buffers[0]->data(), in_indices_->data()->offset,
              in_indices_->data()->length);
        }
        total_length_ = in_indices_->length();
        row_id_ = 0;
      }

      int output_num_rows = 0;
      if (valid_reader_) {
        while (output_num_rows < max_batch_num_) {
          if (valid_reader_->IsNotSet()) {
            for (auto eval_func : eval_null_func_list_) {
              eval_func();
            }
          } else {
            ArrayItemIndex* item = data_ + row_id_;
            for (auto eval_func : eval_func_list_) {
              eval_func(item->array_id, item->id);
            }
          }
          output_num_rows++;
          valid_reader_->Next();
          if (++row_id_ >= total_length_) {
            break;
          }
        }
      } else {
        // if this array has zero null
        while (output_num_rows < max_batch_num_) {
          ArrayItemIndex* item = data_ + row_id_;
          for (auto eval_func : eval_func_list_) {
            eval_func(item->array_id, item->id);
          }
          output_num_rows++;
          if (++row_id_ >= total_length_) {
            break;
          }
        }
      }

      std::vector<std::shared_ptr<arrow::Array>> out_array_list;
      for (auto action : action_list_) {
        RETURN_NOT_OK(action->FinishAndReset(&out_array_list));
      }

      *out = arrow::RecordBatch::Make(schema_, output_num_rows, out_array_list);
      // arrow::PrettyPrint(*(*out).get(), 2, &std::cout);

      return arrow::Status::OK();
    }

   private:
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::function<arrow::Status(uint64_t, uint64_t)>> eval_func_list_;
    std::vector<std::function<arrow::Status()>> eval_null_func_list_;
    std::vector<std::shared_ptr<extra::ActionBase>> action_list_;
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> child_;
    int child_result_index_;
    std::shared_ptr<arrow::Array> in_indices_;
    ArrayItemIndex* data_;
    std::shared_ptr<arrow::internal::BitmapReader> valid_reader_;
    uint64_t total_length_;
    uint64_t row_id_ = 0;
    uint64_t max_batch_num_ = MAXBATCHNUMROWS;
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

arrow::Status ShuffleArrayListKernel::Evaluate(const ArrayList& in, ArrayList* out) {
  return impl_->Evaluate(in, out);
}

arrow::Status ShuffleArrayListKernel::Finish(ArrayList* out) {
  return impl_->Finish(out);
}

arrow::Status ShuffleArrayListKernel::SetDependencyInput(
    const std::shared_ptr<arrow::Array>& in) {
  return impl_->SetDependencyInput(in);
}

arrow::Status ShuffleArrayListKernel::SetDependencyIter(
    const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& in, int index) {
  return impl_->SetDependencyIter(in, index);
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
    hash_table_ = std::make_shared<MemoTableType>(ctx_->memory_pool());
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
class HashAggrArrayKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::DataType>> type_list)
      : ctx_(ctx) {
    // create a new result array type here
    std::vector<std::shared_ptr<gandiva::Node>> func_node_list = {};
    std::vector<std::shared_ptr<arrow::Field>> field_list = {};
    bool allUtf8 = true;
    for (auto type : type_list) {
      if (type->id() != arrow::StringType::type_id) {
        allUtf8 = false;
        break;
      }
    }

    gandiva::ExpressionPtr expr;
    if (allUtf8) {
      int index = 0;
      for (auto type : type_list) {
        auto field = arrow::field(std::to_string(index), type);
        field_list.push_back(field);
        auto field_node = gandiva::TreeExprBuilder::MakeField(field);
        func_node_list.push_back(field_node);
        if (func_node_list.size() == 2) {
          auto tmp_func_node = gandiva::TreeExprBuilder::MakeFunction(
              "concat", func_node_list, arrow::utf8());
          func_node_list.clear();
          func_node_list.push_back(tmp_func_node);
        }
        index++;
      }
      auto func_node = gandiva::TreeExprBuilder::MakeFunction(
          "hash64", {func_node_list[0]}, arrow::int64());
      expr = gandiva::TreeExprBuilder::MakeExpression(
          func_node, arrow::field("res", arrow::int64()));
    } else {
      int index = 0;
      for (auto type : type_list) {
        auto field = arrow::field(std::to_string(index), type);
        field_list.push_back(field);
        auto field_node = gandiva::TreeExprBuilder::MakeField(field);
        auto func_node = gandiva::TreeExprBuilder::MakeFunction("hash64", {field_node},
                                                                arrow::int64());
        func_node_list.push_back(func_node);
        if (func_node_list.size() == 2) {
          auto tmp_func_node = gandiva::TreeExprBuilder::MakeFunction(
              "add", func_node_list, arrow::int64());
          func_node_list.clear();
          func_node_list.push_back(tmp_func_node);
        }
        index++;
      }
      expr = gandiva::TreeExprBuilder::MakeExpression(
          func_node_list[0], arrow::field("res", arrow::int64()));
    }
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

///////////////  ProbeArrays  ////////////////
class ProbeArraysKernel::Impl {
 public:
  Impl() {}
  ~Impl() {}
  virtual arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    return arrow::Status::NotImplemented("ProbeArraysKernel::Impl Evaluate is abstract");
  }
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "ProbeArraysKernel::Impl MakeResultIterator is abstract");
  }
};

template <typename InType, typename MemoTableType>
class ProbeArraysTypedImpl : public ProbeArraysKernel::Impl {
 public:
  ProbeArraysTypedImpl(arrow::compute::FunctionContext* ctx, int join_type)
      : ctx_(ctx), join_type_(join_type) {
    hash_table_ = std::make_shared<MemoTableType>(ctx_->memory_pool());
  }

  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) override {
    // we should put items into hashmap
    auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
    auto insert_on_found = [this](int32_t i) {
      // TODO: need to be handled when key is duplicated
    };
    auto insert_on_not_found = [this](int32_t i) {
      memo_index_to_arrayid_.emplace_back(cur_array_id_, cur_id_);
    };

    cur_id_ = 0;
    for (; cur_id_ < typed_array->length(); cur_id_++) {
      if (typed_array->IsNull(cur_id_)) {
        hash_table_->GetOrInsertNull(insert_on_found, insert_on_not_found);
      } else {
        hash_table_->GetOrInsert(typed_array->GetView(cur_id_), insert_on_found,
                                 insert_on_not_found);
      }
    }
    cur_array_id_++;
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    // prepare process next function
    auto eval_func = [this, schema](const std::shared_ptr<arrow::Array>& in,
                                    std::shared_ptr<arrow::RecordBatch>* out) {
      // prepare
      std::unique_ptr<arrow::FixedSizeBinaryBuilder> left_indices_builder;
      auto left_array_type = arrow::fixed_size_binary(sizeof(ArrayItemIndex));
      left_indices_builder.reset(
          new arrow::FixedSizeBinaryBuilder(left_array_type, ctx_->memory_pool()));

      std::unique_ptr<arrow::UInt32Builder> right_indices_builder;
      right_indices_builder.reset(
          new arrow::UInt32Builder(arrow::uint32(), ctx_->memory_pool()));

      auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);

      // evaluate
      switch (join_type_) {
        case 0: { /*Inner Join*/
          for (int i = 0; i < typed_array->length(); i++) {
            if (!typed_array->IsNull(i)) {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index != -1) {
                auto tmp = memo_index_to_arrayid_[index];
                RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                RETURN_NOT_OK(right_indices_builder->Append(i));
              }
            }
          }
        } break;
        case 2: { /*Right Join*/
          for (int i = 0; i < typed_array->length(); i++) {
            if (typed_array->IsNull(i)) {
              auto index = hash_table_->GetNull();
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
              } else {
                auto tmp = memo_index_to_arrayid_[index];
                RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
              }
            } else {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
              } else {
                auto tmp = memo_index_to_arrayid_[index];
                RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
              }
            }
          }
        } break;
        default:
          return arrow::Status::Invalid(
              "ProbeArraysTypedImpl only support join type: InnerJoin, RightJoin");
      }
      // create buffer and null_vector to FixedSizeBinaryArray
      std::shared_ptr<arrow::Array> left_arr_out;
      std::shared_ptr<arrow::Array> right_arr_out;
      RETURN_NOT_OK(left_indices_builder->Finish(&left_arr_out));
      RETURN_NOT_OK(right_indices_builder->Finish(&right_arr_out));
      auto result_schema =
          arrow::schema({arrow::field("left_indices", left_array_type),
                         arrow::field("right_indices", arrow::uint32())});
      *out =
          arrow::RecordBatch::Make(result_schema, cur_id_, {left_arr_out, right_arr_out});
      return arrow::Status::OK();
    };
    *out = std::make_shared<ProbeArraysResultIterator>(eval_func);
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<InType>::ArrayType;
  struct ArrayItemIndex {
    uint64_t id = 0;
    uint64_t array_id = 0;
    ArrayItemIndex(uint64_t array_id, uint64_t id) : array_id(array_id), id(id) {}
  };

  int join_type_;
  std::shared_ptr<arrow::DataType> out_type_;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<MemoTableType> hash_table_;
  std::vector<ArrayItemIndex> memo_index_to_arrayid_;

  uint64_t cur_array_id_ = 0;
  uint64_t cur_id_ = 0;

  class ProbeArraysResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    ProbeArraysResultIterator(
        std::function<arrow::Status(const std::shared_ptr<arrow::Array>& in,
                                    std::shared_ptr<arrow::RecordBatch>* out)>
            eval_func)
        : eval_func_(eval_func) {}

    bool HasNext() override { return true; }

    arrow::Status Process(std::vector<std::shared_ptr<arrow::Array>> in,
                          std::shared_ptr<arrow::RecordBatch>* out) {
      RETURN_NOT_OK(eval_func_(in[0], out));
      return arrow::Status::OK();
    }

    arrow::Status ProcessAndCacheOne(std::vector<std::shared_ptr<arrow::Array>> in) {
      RETURN_NOT_OK(eval_func_(in[0], &out_cache_));
      return arrow::Status::OK();
    }

    arrow::Status GetResult(std::shared_ptr<arrow::RecordBatch>* out) {
      *out = out_cache_;
      return arrow::Status::OK();
    }

   private:
    std::function<arrow::Status(const std::shared_ptr<arrow::Array>& in,
                                std::shared_ptr<arrow::RecordBatch>* out)>
        eval_func_;
    std::shared_ptr<arrow::RecordBatch> out_cache_;
  };
};

arrow::Status ProbeArraysKernel::Make(arrow::compute::FunctionContext* ctx,
                                      std::shared_ptr<arrow::DataType> type,
                                      int join_type, std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ProbeArraysKernel>(ctx, type, join_type);
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
ProbeArraysKernel::ProbeArraysKernel(arrow::compute::FunctionContext* ctx,
                                     std::shared_ptr<arrow::DataType> type,
                                     int join_type) {
  switch (type->id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using MemoTableType = typename arrow::internal::HashTraits<InType>::MemoTableType; \
    impl_.reset(new ProbeArraysTypedImpl<InType, MemoTableType>(ctx, join_type));      \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  kernel_name_ = "ProbeArraysKernel";
}
#undef PROCESS_SUPPORTED_TYPES

arrow::Status ProbeArraysKernel::Evaluate(const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(in);
}

arrow::Status ProbeArraysKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
