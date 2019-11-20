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
auto f0 = field("f0", uint8());
auto f1 = field("f1", uint32());
auto f2 = field("f2", uint8());
auto f3 = field("f3", uint8());
auto f4 = field("f4", uint8());
auto f_unique = field("unique", uint8());
auto f_sum = field("sum", uint8());
auto f_count = field("count", uint8());

////////////////////// prepare expr_vector ///////////////////////
auto arg_pre = TreeExprBuilder::MakeField(f0);
auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint8());

auto arg0 = TreeExprBuilder::MakeField(f0);
auto arg1 = TreeExprBuilder::MakeField(f1);
auto arg2 = TreeExprBuilder::MakeField(f2);
auto arg3 = TreeExprBuilder::MakeField(f3);
auto arg4 = TreeExprBuilder::MakeField(f4);
auto arg_sum = TreeExprBuilder::MakeField(f_sum);
auto arg_count = TreeExprBuilder::MakeField(f_count);
auto arg_unique = TreeExprBuilder::MakeField(f_unique);
auto n_split = TreeExprBuilder::MakeFunction(
    "splitArrayList", {n_pre, arg0, arg1, arg2, arg3, arg4}, uint8());
auto n_unique = TreeExprBuilder::MakeFunction("unique", {n_split, arg0}, uint8());
auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_unique);
auto n_sum = TreeExprBuilder::MakeFunction("sum", {n_split, arg1}, uint8());
auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_sum);
auto n_count = TreeExprBuilder::MakeFunction("count", {n_split, arg1}, uint8());
auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_count);
auto n_cache_unique =
    TreeExprBuilder::MakeFunction("appendToCachedArray", {arg_unique}, uint8());
auto cache_unique_expr =
    TreeExprBuilder::MakeExpression(n_cache_unique, field("res", uint8()));
auto n_cache_sum =
    TreeExprBuilder::MakeFunction("appendToCachedArray", {arg_sum}, uint8());
auto cache_sum_expr = TreeExprBuilder::MakeExpression(n_cache_sum, field("res", uint8()));
auto n_cache_count =
    TreeExprBuilder::MakeFunction("appendToCachedArray", {arg_count}, uint8());
auto cache_unique_expr =
    TreeExprBuilder::MakeExpression(n_cache_count, field("res", uint8()));

std::vector<std::shared_ptr<gandiva::Expression>> expr_vector = {unique_expr, sum_expr,
                                                                 count_expr};
auto sch = arrow::schema({f0, f1, f2, f3, f4});
std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_sum, f_count};

////////////////////// prepare expr_vector_2 ///////////////////////
auto n_merge_cache = TreeExprBuilder::MakeFunction(
    "appendToCachedBatch", {arg_unique, arg_sum, arg_count}, uint8());
auto merge_cache_expr =
    TreeExprBuilder::MakeExpression(n_merge_cache, field("res", uint8()));
std::vector<std::shared_ptr<gandiva::Expression>> expr_vector_2 = {merge_cache_expr};
auto sch_2 = arrow::schema({f_unique, f_sum, f_count});
std::vector<std::shared_ptr<Field>> ret_types_2 = {f_count};

////////////////////// prepare expr_vector_3 ///////////////////////
auto n_unique_final = TreeExprBuilder::MakeFunction("unique", {arg_unique}, uint8());
auto expr_unique_final =
    TreeExprBuilder::MakeExpression(n_unique_final, field("res", uint8()));
auto n_sum_final = TreeExprBuilder::MakeFunction("sum", {arg_sum}, uint8());
auto expr_sum_final = TreeExprBuilder::MakeExpression(n_sum_final, field("res", uint8()));
auto n_count_final = TreeExprBuilder::MakeFunction("sum", {arg_count}, uint8());
auto expr_count_final =
    TreeExprBuilder::MakeExpression(n_count_final, field("res", uint8()));
std::vector<std::shared_ptr<gandiva::Expression>> expr_vector_3 = {
    expr_unique_final, expr_sum_final, expr_count_final};
auto sch_3 = arrow::schema({f_unique, f_sum, f_count});
std::vector<std::shared_ptr<Field>> ret_types_3 = {f_unique, f_sum, f_count};

////////////////////// prepare first codegen /////////////////////////////////
////////////////////// to get sum and count of one batch /////////////////////
std::shared_ptr<CodeGenerator> expr;

///////////////////////////////////////////////////////////////////////////////
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

void test(std::shared_ptr<arrow::RecordBatch> input_batch) {
  std::cout << "/////////////Test////////////" << std::endl;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));
  if (output_batch_list.empty()) {
    ASSERT_NOT_OK(expr->finish(&output_batch_list));
  }
  std::cout << "output_batch is " << std::endl;
  int i = 0;
  for (auto output_batch : output_batch_list) {
    std::cout << "batch " << i++ << std::endl;
    ASSERT_NOT_OK(arrow::PrettyPrint(*output_batch.get(), 2, &std::cout));
  }
  std::cout << std::endl;
}

void test_2(std::shared_ptr<arrow::RecordBatch> input_batch,
            std::vector<std::shared_ptr<CodeGenerator>>* batch_cache_expr_list) {
  std::cout << "/////////////Test////////////" << std::endl;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));
  //////////// call second codegen ///////////
  int batch_index = 0;
  for (auto batch : output_batch_list) {
    // std::cout << "batch " << batch_index << std::endl;
    // ASSERT_NOT_OK(arrow::PrettyPrint(*batch.get(), 2, &std::cout));
    auto batch_cache_expr_size = batch_cache_expr_list->size();
    if (batch_cache_expr_size <= batch_index) {
      std::shared_ptr<CodeGenerator> batch_cache_expr;
      ASSERT_NOT_OK(CreateCodeGenerator(sch_2, expr_vector_2, ret_types_2,
                                        &batch_cache_expr, true));
      batch_cache_expr_list->push_back(batch_cache_expr);
    }
    std::vector<std::shared_ptr<arrow::RecordBatch>> out;
    ASSERT_NOT_OK(batch_cache_expr_list->at(batch_index)->evaluate(batch, &out));
    batch_index++;
  }
}

void MakeInputBatch(const std::string& input_data, std::shared_ptr<arrow::Schema> sch,
                    std::shared_ptr<arrow::RecordBatch>* input_batch) {
  // prepare input record Batch
  std::shared_ptr<arrow::Array> a0;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a0));
  std::shared_ptr<arrow::Array> a1;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint32(), input_data, &a1));
  std::shared_ptr<arrow::Array> a2;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a2));
  std::shared_ptr<arrow::Array> a3;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a3));
  std::shared_ptr<arrow::Array> a4;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint8(), input_data, &a4));

  *input_batch = RecordBatch::Make(sch, input_data.size(), {a0, a1, a2, a3, a4});
  return;
}

int main() {
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<CodeGenerator>> batch_cache_expr_list;
  ////////////////////// calculation /////////////////////
  std::string input_data = "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]";
  MakeInputBatch(input_data, sch, &input_batch);
  // test(input_batch);
  test_2(input_batch, &batch_cache_expr_list);

  input_data = "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]";
  MakeInputBatch(input_data, sch, &input_batch);
  // test(input_batch);
  test_2(input_batch, &batch_cache_expr_list);

  input_data = "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]";
  MakeInputBatch(input_data, sch, &input_batch);
  // test(input_batch);
  test_2(input_batch, &batch_cache_expr_list);

  ////////////////////// Final Aggregate //////////////////////////
  std::shared_ptr<CodeGenerator> final_expr_list;
  ASSERT_NOT_OK(CreateCodeGenerator(sch_3, expr_vector_3, ret_types_3, &final_expr_list));

  for (auto batch_cache_expr : batch_cache_expr_list) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> cached_batch;
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
    ASSERT_NOT_OK(batch_cache_expr->finish(&cached_batch));
    ASSERT_NOT_OK(final_expr_list->evaluate(cached_batch[0], &result_batch));
    std::cout << "\noutput batch is " << std::endl;
    for (auto batch : result_batch) {
      ASSERT_NOT_OK(arrow::PrettyPrint(*batch.get(), 2, &std::cout));
    }
    std::cout << std::endl;
  }

  std::cout << "Test Completed!" << std::endl;
}
