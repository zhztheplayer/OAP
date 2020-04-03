#include "codegen/arrow_compute/ext/shuffle_v2_action.h"
#include <memory>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using namespace arrow;

template <typename DataType, typename CType>
class ShuffleV2ActionTypedImpl;

class ShuffleV2Action::Impl {
 public:
#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(UInt8Type)                     \
  PROCESS(Int8Type)                      \
  PROCESS(UInt16Type)                    \
  PROCESS(Int16Type)                     \
  PROCESS(UInt32Type)                    \
  PROCESS(Int32Type)                     \
  PROCESS(UInt64Type)                    \
  PROCESS(Int64Type)                     \
  PROCESS(FloatType)                     \
  PROCESS(DoubleType)
  static arrow::Status MakeShuffleV2ActionImpl(arrow::compute::FunctionContext* ctx,
                                               std::shared_ptr<arrow::DataType> type,
                                               int arg_id, std::shared_ptr<Impl>* out) {
    switch (type->id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using CType = typename TypeTraits<InType>::CType;                                  \
    auto res = std::make_shared<ShuffleV2ActionTypedImpl<InType, CType>>(ctx, arg_id); \
    *out = std::dynamic_pointer_cast<Impl>(res);                                       \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      case arrow::StringType::type_id: {
        auto res = std::make_shared<ShuffleV2ActionTypedImpl<StringType, std::string>>(
            ctx, arg_id);
        *out = std::dynamic_pointer_cast<Impl>(res);
      } break;
      default: {
        std::cout << "Not Found " << type->ToString() << ", type id is " << type->id()
                  << std::endl;
      } break;
    }
    return arrow::Status::OK();
  }
#undef PROCESS_SUPPORTED_TYPES
  virtual arrow::Status Submit(std::vector<std::function<bool()>> is_null_func_list,
                               std::vector<std::function<void*()>> get_func_list,
                               std::function<arrow::Status()>* func) = 0;

  virtual arrow::Status FinishAndReset(ArrayList* out) = 0;
};

template <typename DataType, typename CType>
class ShuffleV2ActionTypedImpl : public ShuffleV2Action::Impl {
 public:
  ShuffleV2ActionTypedImpl(arrow::compute::FunctionContext* ctx, int arg_id)
      : ctx_(ctx), arg_id_(arg_id) {
#ifdef DEBUG
    std::cout << "Construct ShuffleV2ActionTypedImpl" << std::endl;
#endif
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(ctx_->memory_pool(), arrow::TypeTraits<DataType>::type_singleton(),
                       &builder);
    builder_.reset(arrow::internal::checked_cast<BuilderType*>(builder.release()));
    exec_ = [this]() {
      CType* res;
      if (is_null_()) {
        RETURN_NOT_OK(builder_->AppendNull());
      } else {
        res = (CType*)get_();
        RETURN_NOT_OK(builder_->Append(*res));
      }
      return arrow::Status::OK();
    };
  }
  ~ShuffleV2ActionTypedImpl() {
#ifdef DEBUG
    std::cout << "Destruct ShuffleV2ActionTypedImpl" << std::endl;
#endif
  }

  arrow::Status Submit(std::vector<std::function<bool()>> is_null_func_list,
                       std::vector<std::function<void*()>> get_func_list,
                       std::function<arrow::Status()>* exec) {
    is_null_ = is_null_func_list[arg_id_];
    get_ = get_func_list[arg_id_];
    *exec = exec_;
    return arrow::Status::OK();
  }

  arrow::Status FinishAndReset(ArrayList* out) {
    std::shared_ptr<arrow::Array> arr_out;
    RETURN_NOT_OK(builder_->Finish(&arr_out));
    out->push_back(arr_out);
    builder_->Reset();
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  // input
  arrow::compute::FunctionContext* ctx_;
  int arg_id_;
  // result
  std::function<bool()> is_null_;
  std::function<void*()> get_;
  std::function<arrow::Status()> exec_;
  std::shared_ptr<BuilderType> builder_;
};

ShuffleV2Action::ShuffleV2Action(arrow::compute::FunctionContext* ctx,
                                 std::shared_ptr<arrow::DataType> type, int arg_id) {
  auto status = Impl::MakeShuffleV2ActionImpl(ctx, type, arg_id, &impl_);
}

ShuffleV2Action::~ShuffleV2Action() {}

arrow::Status ShuffleV2Action::Submit(
    std::vector<std::function<bool()>> is_null_func_list,
    std::vector<std::function<void*()>> get_func_list,
    std::function<arrow::Status()>* func) {
  RETURN_NOT_OK(impl_->Submit(is_null_func_list, get_func_list, func));
  return arrow::Status::OK();
}

arrow::Status ShuffleV2Action::FinishAndReset(ArrayList* out) {
  RETURN_NOT_OK(impl_->FinishAndReset(out));
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
