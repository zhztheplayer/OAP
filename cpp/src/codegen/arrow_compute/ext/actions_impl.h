#pragma once

#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/checked_cast.h>
#include <memory>
#include <sstream>
#include "codegen/arrow_compute/ext/array_builder_impl.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_signed_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_unsigned_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_floating_point<I>> {
  using Type = arrow::DoubleType;
};

class ActionBase {
 public:
  virtual int RequiredColNum() { return 1; }

  virtual arrow::Status Submit(ArrayList in, int max_group_id,
                               std::function<arrow::Status(int)>* on_valid,
                               std::function<arrow::Status()>* on_null) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Submit(std::vector<std::shared_ptr<arrow::Array>> in,
                               std::function<arrow::Status(uint64_t, uint64_t)>* on_valid,
                               std::function<arrow::Status()>* on_null) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Submit(const std::shared_ptr<arrow::Array>& in,
                               std::stringstream* ss,
                               std::function<arrow::Status(int)>* out) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Submit(const std::shared_ptr<arrow::Array>& in,
                               std::function<arrow::Status(uint32_t)>* on_valid,
                               std::function<arrow::Status()>* on_null) {
    return arrow::Status::NotImplemented("ActionBase Submit is abstract.");
  }
  virtual arrow::Status Finish(ArrayList* out) {
    return arrow::Status::NotImplemented("ActionBase Finish is abstract.");
  }
  virtual arrow::Status FinishAndReset(ArrayList* out) {
    return arrow::Status::NotImplemented("ActionBase FinishAndReset is abstract.");
  }
};

//////////////// UniqueAction ///////////////
template <typename DataType>
class UniqueAction : public ActionBase {
 public:
  UniqueAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct UniqueAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(array_builder.release()));
  }
  ~UniqueAction() {
#ifdef DEBUG
    std::cout << "Destruct UniqueAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    in_ = std::dynamic_pointer_cast<ArrayType>(in_list[0]);
    // prepare evaluate lambda
    row_id_ = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        if (dest_group_id >= out_size_) {
          if (in_->IsNull(row_id_)) {
            builder_->AppendNull();
          } else {
            builder_->Append(in_->GetView(row_id_));
          }
          out_size_++;
        }
        row_id_++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (dest_group_id >= out_size_) {
          builder_->Append(in_->GetView(row_id_));
          out_size_++;
        }
        row_id_++;
        return arrow::Status::OK();
      };
    }

    *on_null = [this]() {
      row_id_++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  int row_id_ = 0;
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<ArrayType> in_;
  // output
  uint64_t out_size_ = 0;
  std::unique_ptr<BuilderType> builder_;
};

//////////////// CountAction ///////////////
template <typename DataType>
class CountAction : public ActionBase {
 public:
  CountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct CountAction" << std::endl;
#endif
  }
  ~CountAction() {
#ifdef DEBUG
    std::cout << "Destruct CountAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    row_id = 0;
    // prepare evaluate lambda
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_[dest_group_id] += 1;
        }
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += 1;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() { return arrow::Status::OK(); };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);

    std::unique_ptr<ResBuilderType> builder;
    builder.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
    RETURN_NOT_OK(builder->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  using ResArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  int32_t row_id;
  // result
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
};

//////////////// CountLiteralAction ///////////////
template <typename DataType>
class CountLiteralAction : public ActionBase {
 public:
  CountLiteralAction(arrow::compute::FunctionContext* ctx, int arg)
      : ctx_(ctx), arg_(arg) {
#ifdef DEBUG
    std::cout << "Construct CountLiteralAction" << std::endl;
#endif
  }
  ~CountLiteralAction() {
#ifdef DEBUG
    std::cout << "Destruct CountLiteralAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 0; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    // prepare evaluate lambda
    *on_valid = [this](int dest_group_id) {
      cache_validity_[dest_group_id] = true;
      cache_[dest_group_id] += arg_;
      return arrow::Status::OK();
    };

    *on_null = [this]() { return arrow::Status::OK(); };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &array_builder);

    std::unique_ptr<ResBuilderType> builder;
    builder.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
    RETURN_NOT_OK(builder->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  using ResArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  int arg_;
  // result
  using CType = typename arrow::TypeTraits<DataType>::CType;
  std::vector<CType> cache_;
  std::vector<bool> cache_validity_;
};

//////////////// MinAction ///////////////
template <typename DataType>
class MinAction : public ActionBase {
 public:
  MinAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MinAction" << std::endl;
#endif
  }
  ~MinAction() {
#ifdef DEBUG
    std::cout << "Destruct MinAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          if (data_[row_id] < cache_[dest_group_id]) {
            cache_[dest_group_id] = data_[row_id];
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        if (data_[row_id] < cache_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<ResDataType>::type_singleton(), &array_builder);

    std::unique_ptr<ResBuilderType> builder;
    builder.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
    RETURN_NOT_OK(builder->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
};

//////////////// MaxAction ///////////////
template <typename DataType>
class MaxAction : public ActionBase {
 public:
  MaxAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct MaxAction" << std::endl;
#endif
  }
  ~MaxAction() {
#ifdef DEBUG
    std::cout << "Destruct MaxAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          if (data_[row_id] > cache_[dest_group_id]) {
            cache_[dest_group_id] = data_[row_id];
          }
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        if (!cache_validity_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
          cache_validity_[dest_group_id] = true;
        }
        if (data_[row_id] > cache_[dest_group_id]) {
          cache_[dest_group_id] = data_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<ResDataType>::type_singleton(), &array_builder);

    std::unique_ptr<ResBuilderType> builder;
    builder.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
    RETURN_NOT_OK(builder->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
};

//////////////// SumAction ///////////////
template <typename DataType>
class SumAction : public ActionBase {
 public:
  SumAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumAction" << std::endl;
#endif
  }
  ~SumAction() {
#ifdef DEBUG
    std::cout << "Destruct SumAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_[dest_group_id] += data_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_[dest_group_id] += data_[row_id];
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<ResDataType>::type_singleton(), &array_builder);

    std::unique_ptr<ResBuilderType> builder;
    builder.reset(
        arrow::internal::checked_cast<ResBuilderType*>(array_builder.release()));
    RETURN_NOT_OK(builder->AppendValues(cache_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using ResBuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::shared_ptr<arrow::Array> in_;
  CType* data_;
  int row_id;
  // result
  std::vector<ResCType> cache_;
  std::vector<bool> cache_validity_;
};

//////////////// AvgAction ///////////////
template <typename DataType>
class AvgAction : public ActionBase {
 public:
  AvgAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct AvgAction" << std::endl;
#endif
  }
  ~AvgAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          cache_sum_[dest_group_id] += data_[row_id];
          cache_count_[dest_group_id] += 1;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_sum_[dest_group_id] += data_[row_id];
        cache_count_[dest_group_id] += 1;
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    for (int i = 0; i < cache_sum_.size(); i++) {
      cache_sum_[i] /= cache_count_[i];
    }
    std::shared_ptr<arrow::Array> arr_out;
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(builder->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&arr_out));
    out->push_back(arr_out);

    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  // result
  std::vector<double> cache_sum_;
  std::vector<uint64_t> cache_count_;
  std::vector<bool> cache_validity_;
};

//////////////// SumCountAction ///////////////
template <typename DataType>
class SumCountAction : public ActionBase {
 public:
  SumCountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct SumCountAction" << std::endl;
#endif
  }
  ~SumCountAction() {
#ifdef DEBUG
    std::cout << "Destruct SumCountAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 1; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
    }

    in_ = in_list[0];
    // prepare evaluate lambda
    data_ = const_cast<CType*>(in_->data()->GetValues<CType>(1));
    row_id = 0;
    if (in_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          cache_sum_[dest_group_id] += data_[row_id];
          cache_count_[dest_group_id] += 1;
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_sum_[dest_group_id] += data_[row_id];
        cache_count_[dest_group_id] += 1;
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> sum_array;
    auto sum_builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(sum_builder->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(sum_builder->Finish(&sum_array));

    // get count
    std::shared_ptr<arrow::Array> count_array;
    auto count_builder = new arrow::Int64Builder(ctx_->memory_pool());
    RETURN_NOT_OK(count_builder->AppendValues(cache_count_, cache_validity_));
    RETURN_NOT_OK(count_builder->Finish(&count_array));
    out->push_back(sum_array);
    out->push_back(count_array);

    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  CType* data_;
  std::shared_ptr<arrow::Array> in_;
  int row_id;
  // result
  std::vector<double> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
};

//////////////// AvgByCountAction ///////////////
template <typename DataType>
class AvgByCountAction : public ActionBase {
 public:
  AvgByCountAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct AvgByCountAction" << std::endl;
#endif
  }
  ~AvgByCountAction() {
#ifdef DEBUG
    std::cout << "Destruct AvgByCountAction" << std::endl;
#endif
  }

  int RequiredColNum() { return 2; }

  arrow::Status Submit(ArrayList in_list, int max_group_id,
                       std::function<arrow::Status(int)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    // resize result data
    if (cache_validity_.size() <= max_group_id) {
      cache_validity_.resize(max_group_id + 1, false);
      cache_sum_.resize(max_group_id + 1, 0);
      cache_count_.resize(max_group_id + 1, 0);
    }

    in_sum_ = in_list[0];
    in_count_ = in_list[1];
    // prepare evaluate lambda
    data_sum_ = const_cast<double*>(in_sum_->data()->GetValues<double>(1));
    data_count_ = const_cast<int64_t*>(in_count_->data()->GetValues<int64_t>(1));
    row_id = 0;
    if (in_sum_->null_count()) {
      *on_valid = [this](int dest_group_id) {
        const bool is_null = in_sum_->IsNull(row_id);
        if (!is_null) {
          cache_validity_[dest_group_id] = true;
          cache_sum_[dest_group_id] += data_sum_[row_id];
          cache_count_[dest_group_id] += data_count_[row_id];
        }
        row_id++;
        return arrow::Status::OK();
      };
    } else {
      *on_valid = [this](int dest_group_id) {
        cache_validity_[dest_group_id] = true;
        cache_sum_[dest_group_id] += data_sum_[row_id];
        cache_count_[dest_group_id] += data_count_[row_id];
        row_id++;
        return arrow::Status::OK();
      };
    }
    *on_null = [this]() {
      row_id++;
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> out_arr;
    for (int i = 0; i < cache_sum_.size(); i++) {
      cache_sum_[i] /= cache_count_[i];
    }
    auto builder = new arrow::DoubleBuilder(ctx_->memory_pool());
    RETURN_NOT_OK(builder->AppendValues(cache_sum_, cache_validity_));
    RETURN_NOT_OK(builder->Finish(&out_arr));
    out->push_back(out_arr);

    return arrow::Status::OK();
  }

 private:
  using CType = typename arrow::TypeTraits<DataType>::CType;
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using ResDataType = typename FindAccumulatorType<DataType>::Type;
  using ResCType = typename arrow::TypeTraits<ResDataType>::CType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  // input
  arrow::compute::FunctionContext* ctx_;
  double* data_sum_;
  int64_t* data_count_;
  int row_id;
  std::shared_ptr<arrow::Array> in_sum_;
  std::shared_ptr<arrow::Array> in_count_;
  // result
  std::vector<double> cache_sum_;
  std::vector<int64_t> cache_count_;
  std::vector<bool> cache_validity_;
};

//////////////// ShuffleAction ///////////////
template <typename DataType>
class ShuffleAction : public ActionBase {
 public:
  ShuffleAction(arrow::compute::FunctionContext* ctx) : ctx_(ctx) {
#ifdef DEBUG
    std::cout << "Construct ShuffleAction" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(ctx_->memory_pool(),
                       arrow::TypeTraits<ResDataType>::type_singleton(), &builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(builder.release()));
  }
  ~ShuffleAction() {
#ifdef DEBUG
    std::cout << "Destruct ShuffleAction" << std::endl;
#endif
  }

  arrow::Status Submit(std::vector<std::shared_ptr<arrow::Array>> in,
                       std::function<arrow::Status(uint64_t, uint64_t)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    for (auto array : in) {
      typed_arrays_.push_back(std::dynamic_pointer_cast<ResArrayType>(array));
    }
    // prepare evaluate lambda
    *on_valid = [this](uint64_t array_id, uint64_t id) {
      if (typed_arrays_[array_id]->IsNull(id)) {
        // builder_->UnsafeAppendNull();
        RETURN_NOT_OK(builder_->AppendNull());
      } else {
        // builder_->UnsafeAppend(typed_arrays_[array_id]->GetView(id));
        RETURN_NOT_OK(builder_->Append(typed_arrays_[array_id]->GetView(id)));
      }
      return arrow::Status::OK();
    };
    *on_null = [this]() {
      RETURN_NOT_OK(builder_->AppendNull());
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Submit(const std::shared_ptr<arrow::Array>& in,
                       std::function<arrow::Status(uint32_t)>* on_valid,
                       std::function<arrow::Status()>* on_null) override {
    if (typed_arrays_.size() == 0) {
      typed_arrays_.push_back(std::dynamic_pointer_cast<ResArrayType>(in));
    } else {
      typed_arrays_[0] = std::dynamic_pointer_cast<ResArrayType>(in);
    }
    // prepare evaluate lambda
    *on_valid = [this](uint64_t id) {
      if (typed_arrays_[0]->IsNull(id)) {
        // builder_->UnsafeAppendNull();
        RETURN_NOT_OK(builder_->AppendNull());
      } else {
        // builder_->UnsafeAppend(typed_arrays_[0]->GetView(id));
        RETURN_NOT_OK(builder_->Append(typed_arrays_[0]->GetView(id)));
      }
      return arrow::Status::OK();
    };
    *on_null = [this]() {
      // builder_->UnsafeAppendNull();
      RETURN_NOT_OK(builder_->AppendNull());
      return arrow::Status::OK();
    };
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    return arrow::Status::OK();
  }

  arrow::Status FinishAndReset(ArrayList* out) override {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);

    builder_->Reset();
    return arrow::Status::OK();
  }

 private:
  using ResDataType = DataType;
  using ResArrayType = typename arrow::TypeTraits<ResDataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<ResDataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  std::vector<std::shared_ptr<ResArrayType>> typed_arrays_;
  // result
  std::shared_ptr<BuilderType> builder_;
};

///////////////////// Public Functions //////////////////
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

arrow::Status MakeUniqueAction(arrow::compute::FunctionContext* ctx,
                               std::shared_ptr<arrow::DataType> type,
                               std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                            \
  case InType::type_id: {                                          \
    auto action_ptr = std::make_shared<UniqueAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);      \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case arrow::StringType::type_id: {
      auto action_ptr = std::make_shared<UniqueAction<arrow::StringType>>(ctx);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    default: {
      std::cout << "Not Found " << type->ToString() << ", type id is " << type->id()
                << std::endl;
    } break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeCountAction(arrow::compute::FunctionContext* ctx,
                              std::shared_ptr<ActionBase>* out) {
  auto action_ptr = std::make_shared<CountAction<arrow::UInt64Type>>(ctx);
  *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
  return arrow::Status::OK();
}

arrow::Status MakeCountLiteralAction(arrow::compute::FunctionContext* ctx, int arg,
                                     std::shared_ptr<ActionBase>* out) {
  auto action_ptr = std::make_shared<CountLiteralAction<arrow::UInt64Type>>(ctx, arg);
  *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
  return arrow::Status::OK();
}

arrow::Status MakeSumAction(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<SumAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeAvgAction(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<arrow::DataType> type,
                            std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                         \
  case InType::type_id: {                                       \
    auto action_ptr = std::make_shared<AvgAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);   \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeSumCountAction(arrow::compute::FunctionContext* ctx,
                                 std::shared_ptr<arrow::DataType> type,
                                 std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                              \
  case InType::type_id: {                                            \
    auto action_ptr = std::make_shared<SumCountAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);        \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeAvgByCountAction(arrow::compute::FunctionContext* ctx,
                                   std::shared_ptr<arrow::DataType> type,
                                   std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                                \
  case InType::type_id: {                                              \
    auto action_ptr = std::make_shared<AvgByCountAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);          \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }
  return arrow::Status::OK();
}

arrow::Status MakeShuffleAction(arrow::compute::FunctionContext* ctx,
                                std::shared_ptr<arrow::DataType> type,
                                std::shared_ptr<ActionBase>* out) {
  switch (type->id()) {
#define PROCESS(InType)                                             \
  case InType::type_id: {                                           \
    auto action_ptr = std::make_shared<ShuffleAction<InType>>(ctx); \
    *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);       \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case arrow::StringType::type_id: {
      auto action_ptr = std::make_shared<ShuffleAction<arrow::StringType>>(ctx);
      *out = std::dynamic_pointer_cast<ActionBase>(action_ptr);
    } break;
    default:
      break;
  }
  return arrow::Status::OK();
}

#undef PROCESS_SUPPORTED_TYPES

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
