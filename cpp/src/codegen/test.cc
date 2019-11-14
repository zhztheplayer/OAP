#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>
#include <iostream>
#include <memory>
#include <sstream>
#include "code_generator_factory.h"
#include "codegen/code_generator.h"
using namespace arrow;

using TreeExprBuilder = gandiva::TreeExprBuilder;
using FunctionNode = gandiva::FunctionNode;
using CodeGenerator = sparkcolumnarplugin::codegen::CodeGenerator;

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

void test(std::shared_ptr<CodeGenerator> codegen,
          std::shared_ptr<arrow::RecordBatch> input_batch) {
  std::cout << "/////////////Test////////////" << std::endl;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;
  auto status = codegen->evaluate(input_batch, &output_batch_list);
  if (!status.ok()) {
    std::cerr << "Evaluate failed: " << status.message() << std::endl;
    return;
  }
  std::cout << "output_batch is " << std::endl;
  int i = 0;
  for (auto output_batch : output_batch_list) {
    std::cout << "batch " << i++ << std::endl;
    status = arrow::PrettyPrint(*output_batch.get(), 2, &std::cout);
    if (!status.ok()) {
      return;
    }
  }
  std::cout << std::endl;
}

void MakeInputBatch(const std::string& input_data, std::shared_ptr<arrow::Schema> sch,
                    std::shared_ptr<arrow::RecordBatch>* input_batch) {
  // prepare input record Batch
  std::shared_ptr<arrow::Array> a0;
  arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a0);
  std::shared_ptr<arrow::Array> a1;
  arrow::ipc::internal::json::ArrayFromJSON(uint32(), input_data, &a1);
  std::shared_ptr<arrow::Array> a2;
  arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a2);
  std::shared_ptr<arrow::Array> a3;
  arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a3);
  std::shared_ptr<arrow::Array> a4;
  arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a4);

  *input_batch = RecordBatch::Make(sch, input_data.size(), {a0, a1, a2, a3, a4});
  return;
}

int main() {
  arrow::Status status;

  // prepare schema
  auto f0 = field("f0", uint8());
  auto f1 = field("f1", uint32());
  auto f2 = field("f2", uint8());
  auto f3 = field("f3", uint8());
  auto f4 = field("f4", uint8());
  auto sch = arrow::schema({f0, f1, f2, f3, f4});

  // prepare expr_vector
  auto arg_pre = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint8());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto arg2 = TreeExprBuilder::MakeField(f2);
  auto arg3 = TreeExprBuilder::MakeField(f3);
  auto arg4 = TreeExprBuilder::MakeField(f4);
  auto n_split = TreeExprBuilder::MakeFunction(
      "splitArrayList", {n_pre, arg0, arg1, arg2, arg3, arg4}, uint8());
  auto n_sum = TreeExprBuilder::MakeFunction("sum", {n_split, arg1}, uint8());
  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, field("res", uint8()));
  auto n_count = TreeExprBuilder::MakeFunction("count", {n_split, arg1}, uint8());
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, field("res", uint8()));

  std::vector<std::shared_ptr<gandiva::Expression>> expr_vector = {sum_expr, count_expr};
  // std::vector<std::shared_ptr<gandiva::Expression>> expr_vector = {sum_expr};

  // prepare return types
  std::vector<std::shared_ptr<Field>> ret_types = {f0};

  /*std::cout << "input_batch is " << std::endl;
  arrow::PrettyPrint(*input_batch.get(), 2, &std::cout);
  std::cout << std::endl;*/

  std::shared_ptr<CodeGenerator> codegen;
  status = CreateCodeGenerator(sch, expr_vector, ret_types, &codegen);
  if (!status.ok()) {
    std::cerr << "CreateCodeGenerator failed: " << status.message() << std::endl;
    return 0;
  }

  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::string input_data = "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]";
  MakeInputBatch(input_data, sch, &input_batch);
  test(codegen, input_batch);

  input_data = "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]";
  MakeInputBatch(input_data, sch, &input_batch);
  test(codegen, input_batch);

  input_data = "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]";
  MakeInputBatch(input_data, sch, &input_batch);
  test(codegen, input_batch);

  /*input_data = "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]";
  MakeInputBatch(input_data, sch, &input_batch);
  test(codegen, input_batch);

  input_data = "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]";
  MakeInputBatch(input_data, sch, &input_batch);
  test(codegen, input_batch);
  */
  std::cout << "Test Completed!" << std::endl;
}
