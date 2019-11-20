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

#define ASSERT_NOT_OK(status)                               \
  do {                                                      \
    ::arrow::Status __s = (status);                         \
    if (!__s.ok()) std::cout << __s.message() << std::endl; \
    assert(__s.ok());                                       \
  } while (false);

// prepare schema
auto f0 = field("f0", uint32());
auto f1 = field("f1", uint32());
auto f2 = field("f2", uint32());
auto f3 = field("f3", uint32());
auto f4 = field("f4", uint32());
auto f_unique = field("unique", uint32());
auto f_sum = field("sum", uint32());
auto f_count = field("count", uint32());
auto f_res = field("res", uint32());

////////////////////// prepare expr_vector ///////////////////////
auto arg_pre = TreeExprBuilder::MakeField(f0);
auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint32());

auto arg0 = TreeExprBuilder::MakeField(f0);
auto arg1 = TreeExprBuilder::MakeField(f1);
auto arg2 = TreeExprBuilder::MakeField(f2);
auto arg3 = TreeExprBuilder::MakeField(f3);
auto arg4 = TreeExprBuilder::MakeField(f4);
auto arg_sum = TreeExprBuilder::MakeField(f_sum);
auto arg_count = TreeExprBuilder::MakeField(f_count);
auto arg_unique = TreeExprBuilder::MakeField(f_unique);
auto arg_res = TreeExprBuilder::MakeField(f_res);
auto n_split = TreeExprBuilder::MakeFunction(
    "splitArrayList", {n_pre, arg0, arg1, arg2, arg3, arg4}, uint32());
auto n_unique = TreeExprBuilder::MakeFunction("unique", {n_split, arg0}, uint32());
auto n_sum = TreeExprBuilder::MakeFunction("sum", {n_split, arg1}, uint32());
auto n_count = TreeExprBuilder::MakeFunction("count", {n_split, arg1}, uint32());
auto n_cache_unique =
    TreeExprBuilder::MakeFunction("appendToCachedArray", {n_unique}, uint32());
auto cache_unique_expr = TreeExprBuilder::MakeExpression(n_cache_unique, f_res);
auto n_cache_sum =
    TreeExprBuilder::MakeFunction("appendToCachedArray", {n_sum}, uint32());
auto cache_sum_expr = TreeExprBuilder::MakeExpression(n_cache_sum, f_res);
auto n_cache_count =
    TreeExprBuilder::MakeFunction("appendToCachedArray", {n_count}, uint32());
auto cache_count_expr = TreeExprBuilder::MakeExpression(n_cache_count, f_res);

auto n_unique_finish = TreeExprBuilder::MakeFunction("unique", {}, uint32());
auto n_sum_finish = TreeExprBuilder::MakeFunction("sum", {}, uint32());
auto n_count_finish = TreeExprBuilder::MakeFunction("sum", {}, uint32());
auto unique_finish_expr = TreeExprBuilder::MakeExpression(n_unique_finish, f_res);
auto sum_finish_expr = TreeExprBuilder::MakeExpression(n_sum_finish, f_res);
auto count_finish_expr = TreeExprBuilder::MakeExpression(n_count_finish, f_res);

std::vector<std::shared_ptr<gandiva::Expression>> expr_vector = {
    cache_unique_expr, cache_sum_expr, cache_count_expr};
std::vector<std::shared_ptr<gandiva::Expression>> expr_finish_vector = {
    unique_finish_expr, sum_finish_expr, count_finish_expr};
auto sch = arrow::schema({f0, f1, f2, f3, f4});
std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_sum, f_count};

////////////////////// prepare expr /////////////////////////////////
std::shared_ptr<CodeGenerator> expr;

///////////////////////////////////////////////////////////////////////////////
void test(std::shared_ptr<arrow::RecordBatch> input_batch) {
  std::cout << "/////////////Test////////////" << std::endl;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));
}

void MakeInputBatch(const std::string& input_data, std::shared_ptr<arrow::Schema> sch,
                    std::shared_ptr<arrow::RecordBatch>* input_batch) {
  // prepare input record Batch
  std::shared_ptr<arrow::Array> a0;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint32(), input_data, &a0));
  std::shared_ptr<arrow::Array> a1;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint32(), input_data, &a1));
  std::shared_ptr<arrow::Array> a2;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint32(), input_data, &a2));
  std::shared_ptr<arrow::Array> a3;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint32(), input_data, &a3));
  std::shared_ptr<arrow::Array> a4;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint32(), input_data, &a4));

  *input_batch = RecordBatch::Make(sch, input_data.size(), {a0, a1, a2, a3, a4});
  return;
}
///////////////////////////////////////////////////////////////////////////////

int main() {
  ASSERT_NOT_OK(
      CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true, expr_finish_vector));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  ////////////////////// calculation /////////////////////
  std::string input_data = "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]";
  MakeInputBatch(input_data, sch, &input_batch);
  test(input_batch);

  input_data = "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]";
  MakeInputBatch(input_data, sch, &input_batch);
  test(input_batch);

  input_data = "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]";
  MakeInputBatch(input_data, sch, &input_batch);
  test(input_batch);

  ////////////////////// Final Aggregate //////////////////////////
  std::cout << "//////////// Final Aggregate //////////////" << std::endl;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));
  std::cout << "\noutput batch is " << std::endl;
  for (auto batch : result_batch) {
    ASSERT_NOT_OK(arrow::PrettyPrint(*batch.get(), 2, &std::cout));
  }
  std::cout << std::endl;

  std::cout << "Test Completed!" << std::endl;
}
