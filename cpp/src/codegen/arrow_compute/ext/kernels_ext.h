#pragma once

#include <arrow/array.h>
#include <arrow/compute/context.h>
#include <arrow/status.h>

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
class KernalBase {
 public:
  KernalBase() {}
  ~KernalBase() {}
  virtual arrow::Status Evaluate(const ArrayList& in) {
    return arrow::Status::NotImplemented("KernalBase abstract interface.");
  }
  virtual arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in) {
    return arrow::Status::NotImplemented("KernalBase abstract interface.");
  }
  virtual arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in, int group_id) {
    return arrow::Status::NotImplemented("KernalBase abstract interface.");
  }
  virtual arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                                 std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("KernalBase abstract interface.");
  }
  virtual arrow::Status Finish(ArrayList* out) {
    return arrow::Status::NotImplemented("KernalBase abstract interface.");
  }
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("KernalBase abstract interface.");
  }
};

arrow::Status SplitArrayList(arrow::compute::FunctionContext* ctx, const ArrayList& in,
                             const std::shared_ptr<arrow::Array>& dict,
                             std::vector<ArrayList>* out, std::vector<int>* out_sizes,
                             std::vector<int>* group_indices);

arrow::Status SumArray(arrow::compute::FunctionContext* ctx,
                       const std::shared_ptr<arrow::Array>& in,
                       std::shared_ptr<arrow::Array>* out);

arrow::Status CountArray(arrow::compute::FunctionContext* ctx,
                         const std::shared_ptr<arrow::Array>& in,
                         std::shared_ptr<arrow::Array>* out);

arrow::Status UniqueArray(arrow::compute::FunctionContext* ctx,
                          const std::shared_ptr<arrow::Array>& in,
                          std::shared_ptr<arrow::Array>* out);

class EncodeArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<KernalBase>* out);
  EncodeArrayKernel(arrow::compute::FunctionContext* ctx);
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                         std::shared_ptr<arrow::Array>* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  arrow::compute::FunctionContext* ctx_;
};

class AppendToCacheArrayKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<KernalBase>* out);
  AppendToCacheArrayKernel(arrow::compute::FunctionContext* ctx);
  arrow::Status Evaluate(const std::shared_ptr<arrow::Array>& in,
                         int group_id = 0) override;
  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class AppendToCacheArrayListKernel : public KernalBase {
 public:
  static arrow::Status Make(arrow::compute::FunctionContext* ctx,
                            std::shared_ptr<KernalBase>* out);
  AppendToCacheArrayListKernel(arrow::compute::FunctionContext* ctx);
  arrow::Status Evaluate(const ArrayList& in) override;
  arrow::Status Finish(ArrayList* out) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
