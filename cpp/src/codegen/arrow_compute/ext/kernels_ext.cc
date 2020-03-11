#include "codegen/arrow_compute/ext/kernels_ext.h"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/count.h>
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
#include <arrow/util/hashing.h>
#include <arrow/visitor_inline.h>
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
      } else if (action_name_list_[action_id].compare("action_min") == 0) {
        RETURN_NOT_OK(MakeAvgAction(ctx_, type_list[type_id], &action));
      } else if (action_name_list_[action_id].compare("action_max") == 0) {
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
      auto length = (total_length_ - offset_) > 4096 ? 4096 : (total_length_ - offset_);
      RETURN_NOT_OK(eval_func_(offset_, length, out));
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

    std::vector<std::function<arrow::Status(uint32_t)>> eval_func_list;
    std::vector<std::function<arrow::Status()>> eval_null_func_list;
    for (int i = 0; i < in.size(); i++) {
      auto col = in[i];
      auto action = action_list_[i];
      std::function<arrow::Status(uint32_t)> func;
      std::function<arrow::Status()> null_func;
      action->Submit(col, &func, &null_func);
      eval_func_list.push_back(func);
      eval_null_func_list.push_back(null_func);
    }

    uint32_t* data = (uint32_t*)std::dynamic_pointer_cast<arrow::UInt32Array>(in_indices_)
                         ->raw_values();

    if (in_indices_->null_count() != 0) {
      for (int i = 0; i < in_indices_->length(); i++) {
        if (in_indices_->IsNull(i)) {
          for (auto eval_func : eval_null_func_list) {
            eval_func();
          }
        } else {
          for (auto eval_func : eval_func_list) {
            eval_func(*(data + i));
          }
        }
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
      action->Submit(col_list, &func, &null_func);
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
      action->Submit(col_list, &func, &null_func);
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
        max_batch_num_ = total_length_;
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
    RETURN_NOT_OK(arrow::MakeScalar(res_data_type_, res, &scalar_out));
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
    RETURN_NOT_OK(arrow::MakeScalar(arrow::int64(), res, &scalar_out));
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
    CType sum_res = 0;
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
    RETURN_NOT_OK(arrow::MakeScalar(res_data_type_, sum_res, &sum_scalar_out));
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*sum_scalar_out.get(), 1, &sum_out));

    std::shared_ptr<arrow::Array> cnt_out;
    std::shared_ptr<arrow::Scalar> cnt_scalar_out;
    RETURN_NOT_OK(arrow::MakeScalar(arrow::int64(), cnt_res, &cnt_scalar_out));
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
    res_data_type_ = sum_out.scalar()->type;
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
    CType sum_res = 0;
    CType cnt_res = 0;
    for (size_t i = 0; i < sum_scalar_list_.size(); i++) {
      auto sum_typed_scalar = std::dynamic_pointer_cast<ScalarType>(sum_scalar_list_[i]);
      auto cnt_typed_scalar = std::dynamic_pointer_cast<ScalarType>(cnt_scalar_list_[i]);
      sum_res += sum_typed_scalar->value;
      cnt_res += cnt_typed_scalar->value;
    }
    double res = sum_res * 1.0 / cnt_res;
    std::shared_ptr<arrow::Array> arr_out;
    std::shared_ptr<arrow::Scalar> scalar_out;
    RETURN_NOT_OK(arrow::MakeScalar(arrow::float64(), res, &scalar_out));
    RETURN_NOT_OK(arrow::MakeArrayFromScalar(*scalar_out.get(), 1, &arr_out));

    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::shared_ptr<arrow::DataType> res_data_type_;
  std::vector<std::shared_ptr<arrow::Scalar>> sum_scalar_list_;
  std::vector<std::shared_ptr<arrow::Scalar>> cnt_scalar_list_;
};

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
    RETURN_NOT_OK(arrow::MakeScalar(data_type_, res, &scalar_out));
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
    RETURN_NOT_OK(arrow::MakeScalar(data_type_, res, &scalar_out));
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
    if (typed_array->null_count() == 0) {
      for (; cur_id < typed_array->length(); cur_id++) {
        hash_table_->GetOrInsert(typed_array->GetView(cur_id), insert_on_found,
                                 insert_on_not_found);
      }
    } else {
      for (; cur_id < typed_array->length(); cur_id++) {
        if (typed_array->IsNull(cur_id)) {
          RETURN_NOT_OK(builder_->AppendNull());
        } else {
          hash_table_->GetOrInsert(typed_array->GetView(cur_id), insert_on_found,
                                   insert_on_not_found);
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
class HashAggrArrayKernel::Impl {
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
    std::cout << expr->ToString() << std::endl;
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
  virtual arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& selection,
                                 const std::shared_ptr<arrow::Array>& in) {
    return arrow::Status::NotImplemented("ProbeArraysKernel::Impl Evaluate is abstract");
  }  // namespace extra
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "ProbeArraysKernel::Impl MakeResultIterator is abstract");
  }
};  // namespace arrowcompute

template <typename InType, typename MemoTableType>
class ProbeArraysTypedImpl : public ProbeArraysKernel::Impl {
 public:
  ProbeArraysTypedImpl(arrow::compute::FunctionContext* ctx, int join_type)
      : ctx_(ctx), join_type_(join_type) {
    hash_table_ = std::make_shared<MemoTableType>(ctx_->memory_pool());
  }

  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& selection,
                         const std::shared_ptr<arrow::Array>& in) override {
    // we should put items into hashmap
    auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
    auto insert_on_found = [this](int32_t i) {
      memo_index_to_arrayid_[i].emplace_back(cur_array_id_, cur_id_);
    };
    auto insert_on_not_found = [this](int32_t i) {
      memo_index_to_arrayid_.push_back({ArrayItemIndex(cur_array_id_, cur_id_)});
    };

    cur_id_ = 0;
    if (selection) {
      auto selection_typed_array =
          std::dynamic_pointer_cast<arrow::UInt16Array>(selection);
      if (typed_array->null_count() == 0) {
        for (int i = 0; i < selection_typed_array->length(); i++) {
          cur_id_ = selection_typed_array->GetView(i);
          hash_table_->GetOrInsert(typed_array->GetView(cur_id_), insert_on_found,
                                   insert_on_not_found);
        }
      } else {
        for (int i = 0; i < selection_typed_array->length(); i++) {
          cur_id_ = selection_typed_array->GetView(i);
          if (typed_array->IsNull(cur_id_)) {
            hash_table_->GetOrInsertNull(insert_on_found, insert_on_not_found);
          } else {
            hash_table_->GetOrInsert(typed_array->GetView(cur_id_), insert_on_found,
                                     insert_on_not_found);
          }
        }
      }
    } else {
      if (typed_array->null_count() == 0) {
        for (; cur_id_ < typed_array->length(); cur_id_++) {
          hash_table_->GetOrInsert(typed_array->GetView(cur_id_), insert_on_found,
                                   insert_on_not_found);
        }
      } else {
        for (; cur_id_ < typed_array->length(); cur_id_++) {
          if (typed_array->IsNull(cur_id_)) {
            hash_table_->GetOrInsertNull(insert_on_found, insert_on_not_found);
          } else {
            hash_table_->GetOrInsert(typed_array->GetView(cur_id_), insert_on_found,
                                     insert_on_not_found);
          }
        }
      }
    }
    cur_array_id_++;
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) override {
    // prepare process next function
    auto eval_func = [this, schema](const std::shared_ptr<arrow::Array>& selection,
                                    const std::shared_ptr<arrow::Array>& in,
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
          if (selection) {
            auto selection_typed_array =
                std::dynamic_pointer_cast<arrow::UInt16Array>(selection);
            for (int j = 0; j < selection_typed_array->length(); j++) {
              int i = selection_typed_array->GetView(j);
              if (!typed_array->IsNull(i)) {
                auto index = hash_table_->Get(typed_array->GetView(i));
                if (index != -1) {
                  for (auto tmp : memo_index_to_arrayid_[index]) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              }
            }
          } else {
            for (int i = 0; i < typed_array->length(); i++) {
              if (!typed_array->IsNull(i)) {
                auto index = hash_table_->Get(typed_array->GetView(i));
                if (index != -1) {
                  for (auto tmp : memo_index_to_arrayid_[index]) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              }
            }
          }
        } break;
        case 2: { /*Outer Join*/
          if (selection) {
            auto selection_typed_array =
                std::dynamic_pointer_cast<arrow::UInt16Array>(selection);
            for (int j = 0; j < selection_typed_array->length(); j++) {
              int i = selection_typed_array->GetView(j);
              if (typed_array->IsNull(i)) {
                auto index = hash_table_->GetNull();
                if (index == -1) {
                  RETURN_NOT_OK(left_indices_builder->AppendNull());
                  RETURN_NOT_OK(right_indices_builder->Append(i));
                } else {
                  for (auto tmp : memo_index_to_arrayid_[index]) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              } else {
                auto index = hash_table_->Get(typed_array->GetView(i));
                if (index == -1) {
                  RETURN_NOT_OK(left_indices_builder->AppendNull());
                  RETURN_NOT_OK(right_indices_builder->Append(i));
                } else {
                  for (auto tmp : memo_index_to_arrayid_[index]) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              }
            }
          } else {
            for (int i = 0; i < typed_array->length(); i++) {
              if (typed_array->IsNull(i)) {
                auto index = hash_table_->GetNull();
                if (index == -1) {
                  RETURN_NOT_OK(left_indices_builder->AppendNull());
                  RETURN_NOT_OK(right_indices_builder->Append(i));
                } else {
                  for (auto tmp : memo_index_to_arrayid_[index]) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              } else {
                auto index = hash_table_->Get(typed_array->GetView(i));
                if (index == -1) {
                  RETURN_NOT_OK(left_indices_builder->AppendNull());
                  RETURN_NOT_OK(right_indices_builder->Append(i));
                } else {
                  for (auto tmp : memo_index_to_arrayid_[index]) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
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
    *out = std::make_shared<ProbeArraysResultIterator>(ctx_, eval_func);
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
  std::vector<std::vector<ArrayItemIndex>> memo_index_to_arrayid_;

  uint64_t cur_array_id_ = 0;
  uint64_t cur_id_ = 0;

  class ProbeArraysResultIterator : public ResultIterator<arrow::RecordBatch> {
   public:
    ProbeArraysResultIterator(
        arrow::compute::FunctionContext* ctx,
        std::function<arrow::Status(const std::shared_ptr<arrow::Array>& selection,
                                    const std::shared_ptr<arrow::Array>& in,
                                    std::shared_ptr<arrow::RecordBatch>* out)>
            eval_func)
        : eval_func_(eval_func), ctx_(ctx) {}

    bool HasNext() override { return true; }

    arrow::Status Process(std::vector<std::shared_ptr<arrow::Array>> in,
                          std::shared_ptr<arrow::RecordBatch>* out,
                          const std::shared_ptr<arrow::Array>& selection) {
      std::shared_ptr<arrow::Array> in_arr;
      if (in.size() > 1) {
        std::vector<std::shared_ptr<arrow::DataType>> type_list;
        for (int i = 0; i < in.size(); i++) {
          type_list.push_back(in[i]->type());
        }
        std::shared_ptr<extra::KernalBase> concat_kernel;
        RETURN_NOT_OK(extra::HashAggrArrayKernel::Make(ctx_, type_list, &concat_kernel));
        RETURN_NOT_OK(concat_kernel->Evaluate(in, &in_arr));
      } else {
        in_arr = in[0];
      }
      RETURN_NOT_OK(eval_func_(selection, in_arr, out));
      return arrow::Status::OK();
    }

    arrow::Status ProcessAndCacheOne(std::vector<std::shared_ptr<arrow::Array>> in,
                                     const std::shared_ptr<arrow::Array>& selection) {
      std::shared_ptr<arrow::Array> in_arr;
      if (in.size() > 1) {
        std::vector<std::shared_ptr<arrow::DataType>> type_list;
        for (int i = 0; i < in.size(); i++) {
          type_list.push_back(in[i]->type());
        }
        std::shared_ptr<extra::KernalBase> concat_kernel;
        RETURN_NOT_OK(extra::HashAggrArrayKernel::Make(ctx_, type_list, &concat_kernel));
        RETURN_NOT_OK(concat_kernel->Evaluate(in, &in_arr));
      } else {
        in_arr = in[0];
      }
      RETURN_NOT_OK(eval_func_(selection, in_arr, &out_cache_));
      return arrow::Status::OK();
    }

    arrow::Status GetResult(std::shared_ptr<arrow::RecordBatch>* out) {
      *out = out_cache_;
      return arrow::Status::OK();
    }

   private:
    std::function<arrow::Status(const std::shared_ptr<arrow::Array>& selection,
                                const std::shared_ptr<arrow::Array>& in,
                                std::shared_ptr<arrow::RecordBatch>* out)>
        eval_func_;
    std::shared_ptr<arrow::RecordBatch> out_cache_;
    arrow::compute::FunctionContext* ctx_;
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

arrow::Status ProbeArraysKernel::Evaluate(const std::shared_ptr<arrow::Array>& selection,
                                          const std::shared_ptr<arrow::Array>& in) {
  return impl_->Evaluate(selection, in);
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
