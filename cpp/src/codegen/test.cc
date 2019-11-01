#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>
#include <iostream>
#include <sstream>
#include "code_generator_factory.h"
using namespace arrow;

using TreeExprBuilder = gandiva::TreeExprBuilder;
using FunctionNode = gandiva::FunctionNode;

template <typename T>
arrow::Status GetOrInsert(int i, std::vector<T>* input, T* out) {
  if (i > input->size()) {
    return arrow::Status::Invalid("GetOrInser index: ", i, "  is out of range.");
  }
  if (i == input->size()) {
    T new_data;

    input->push_back(new_data);
  }
  *out = input->at(i);
  return arrow::Status::OK();
}

int main() {
  arrow::Status status;

  // prepare schema
  auto f0 = field("f0", uint8());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", uint8());
  auto f3 = field("f3", uint8());
  auto f4 = field("f4", uint8());
  auto sch = arrow::schema({f0, f1, f2, f3, f4});

  // prepare expr_vector
  auto arg_pre = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint8());
  auto n_pre_root = TreeExprBuilder::MakeFunction("getPrepareFunc", {n_pre}, uint8());
  auto pre_expr = TreeExprBuilder::MakeExpression(n_pre_root, field("res", uint8()));

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto arg2 = TreeExprBuilder::MakeField(f2);
  auto arg3 = TreeExprBuilder::MakeField(f3);
  auto arg4 = TreeExprBuilder::MakeField(f4);
  auto n_split = TreeExprBuilder::MakeFunction("splitArrayList",
                                               {arg0, arg1, arg2, arg3, arg4}, uint8());
  auto split_expr = TreeExprBuilder::MakeExpression(n_split, field("res", uint8()));

  std::vector<std::shared_ptr<gandiva::Expression>> expr_vector = {pre_expr, split_expr};

  // prepare return types
  std::vector<std::shared_ptr<Field>> ret_types = {f0, f1, f2, f3, f4};

  // prepare input record Batch
  std::string input_data = "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]";
  std::shared_ptr<arrow::Array> a0;
  arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a0);
  std::shared_ptr<arrow::Array> a1;
  arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a1);
  std::shared_ptr<arrow::Array> a2;
  arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a2);
  std::shared_ptr<arrow::Array> a3;
  arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a3);
  std::shared_ptr<arrow::Array> a4;
  arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a4);
  auto input_batch = RecordBatch::Make(sch, input_data.size(), {a0, a1, a2, a3, a4});

  std::cout << "input_batch is " << std::endl;
  arrow::PrettyPrint(*input_batch.get(), 2, &std::cout);
  std::cout << std::endl;

  std::shared_ptr<CodeGenerator> codegen;
  status = CreateCodeGenerator(sch, expr_vector, ret_types, &codegen);
  if (!status.ok()) {
    std::cerr << "CreateCodeGenerator failed: " << status.message() << std::endl;
    return 0;
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;
  status = codegen->evaluate(input_batch, &output_batch_list);
  if (!status.ok()) {
    std::cerr << "Evaluate failed: " << status.message() << std::endl;
    return 0;
  }
  std::cout << "output_batch is " << std::endl;
  for (auto output_batch : output_batch_list) {
    arrow::PrettyPrint(*output_batch.get(), 2, &std::cout);
  }
  std::cout << std::endl;
}
