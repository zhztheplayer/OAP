#pragma once

#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <iostream>
namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
class ArrayBuilderImplBase {
 public:
  virtual arrow::Status AppendArray(arrow::Array* in, int group_id = 0) = 0;
  virtual arrow::Status AppendArray(arrow::Array* in, int group_id, int row_id) = 0;
  virtual arrow::Status Finish(ArrayList* out) = 0;
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) = 0;
};

template <typename ArrayType, typename T,
          typename BuilderType = typename arrow::TypeTraits<T>::BuilderType>
class ArrayBuilderImpl : public ArrayBuilderImplBase {
 public:
  static arrow::Status Make(const std::shared_ptr<arrow::DataType> type,
                            arrow::MemoryPool* pool,
                            std::shared_ptr<ArrayBuilderImpl>* builder) {
    auto builder_ptr = std::make_shared<ArrayBuilderImpl<ArrayType, T>>(type, pool);
    *builder = builder_ptr;
    return arrow::Status::OK();
  }

  ArrayBuilderImpl(const std::shared_ptr<arrow::DataType> type, arrow::MemoryPool* pool)
      : pool_(pool) {
    type_ = type;
  }

  arrow::Status InitBuilder() {
    // prepare builder, should be size of key number
    std::unique_ptr<arrow::ArrayBuilder> builder;
    RETURN_NOT_OK(arrow::MakeBuilder(pool_, type_, &builder));

    std::shared_ptr<BuilderType> builder_ptr;
    builder_ptr.reset(arrow::internal::checked_cast<BuilderType*>(builder.release()));
    builder_list_.push_back(builder_ptr);
    return arrow::Status::OK();
  }

  arrow::Status GetOrCreateBuilder(int group_id, std::shared_ptr<BuilderType>* builder) {
    while (builder_list_.size() <= group_id) {
      RETURN_NOT_OK(InitBuilder());
    }
    *builder = builder_list_[group_id];
    return arrow::Status::OK();
  }

  arrow::Status AppendArray(arrow::Array* in, int group_id, int row_id) {
    std::shared_ptr<BuilderType> builder;
    RETURN_NOT_OK(GetOrCreateBuilder(group_id, &builder));
    RETURN_NOT_OK(builder->Reserve(1));
    auto in_ = arrow::internal::checked_cast<ArrayType*>(in);
    if (in_->IsNull(row_id)) {
      builder->UnsafeAppendNull();
    } else {
      auto value = in_->GetView(row_id);
      UnsafeAppend(builder, value);
    }
    return arrow::Status::OK();
  }

  arrow::Status AppendArray(arrow::Array* in, int group_id = 0) {
    std::shared_ptr<BuilderType> builder;
    RETURN_NOT_OK(GetOrCreateBuilder(group_id, &builder));
    RETURN_NOT_OK(builder->Reserve(in->length()));
    auto in_ = arrow::internal::checked_cast<ArrayType*>(in);
    for (int row_id = 0; row_id < in_->length(); row_id++) {
      if (in_->IsNull(row_id)) {
        builder->UnsafeAppendNull();
      } else {
        auto value = in_->GetView(row_id);
#ifdef DEBUG
        std::cout << "AppendArray group_id is " << group_id << ", data is " << value
                  << std::endl;
#endif
        UnsafeAppend(builder, value);
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(ArrayList* out) {
    for (int i = 0; i < builder_list_.size(); i++) {
      std::shared_ptr<arrow::Array> out_arr;
      builder_list_[i]->Finish(&out_arr);
      out->push_back(out_arr);
    }
    return arrow::Status::OK();
  }

  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    std::shared_ptr<arrow::Array> out_arr;
    builder_list_[0]->Finish(&out_arr);
    *out = out_arr;
    return arrow::Status::OK();
  }

 private:
  std::vector<std::shared_ptr<BuilderType>> builder_list_;
  arrow::MemoryPool* pool_;
  std::shared_ptr<arrow::DataType> type_;

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
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
