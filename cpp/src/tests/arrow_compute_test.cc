#include <gtest/gtest.h>
#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

void MakeInputBatch(std::vector<std::string> input_data,
                    std::shared_ptr<arrow::Schema> sch,
                    std::shared_ptr<arrow::RecordBatch>* input_batch) {
  // prepare input record Batch
  std::vector<std::shared_ptr<Array>> array_list;
  int length = -1;
  int i = 0;
  for (auto data : input_data) {
    std::shared_ptr<Array> a0;
    if (length == -1) {
      length = data.size();
    }
    assert(length == data.size());
    ASSERT_NOT_OK(
        arrow::ipc::internal::json::ArrayFromJSON(sch->field(i++)->type(), data, &a0));
    array_list.push_back(a0);
  }

  *input_batch = RecordBatch::Make(sch, length, array_list);
  return;
}

TEST(TestArrowCompute, AggregateTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f_sum = field("sum", uint64());
  auto f_count = field("count", uint64());
  auto f_res = field("res", uint64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto n_sum = TreeExprBuilder::MakeFunction("sum", {arg_0}, uint64());
  auto n_count = TreeExprBuilder::MakeFunction("count", {arg_1}, uint64());

  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sum_expr,
                                                                     count_expr};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_sum, f_count};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::string> input_data_string = {"[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]",
                                                "[8, 5, 3, 5, 11, 7, 4, 4, 6, 7]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[328]", "[10]"};
  auto res_sch = arrow::schema({f_sum, f_count});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, AggregatewithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f_sum = field("sum", uint64());
  auto f_count = field("count", uint64());
  auto f_res = field("res", uint64());
  auto arg_0 = TreeExprBuilder::MakeField(f0);
  auto arg_1 = TreeExprBuilder::MakeField(f1);
  auto n_sum = TreeExprBuilder::MakeFunction("sum", {arg_0}, uint64());
  auto n_count = TreeExprBuilder::MakeFunction("count", {arg_1}, uint64());

  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {sum_expr,
                                                                     count_expr};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_sum, f_count};
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  std::vector<std::string> input_data_string = {"[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]",
                                                "[8, 5, 3, 5, 11, 7, 4, 4, 6, 7]"};
  MakeInputBatch(input_data_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));
  std::vector<std::string> input_data_2_string = {
      "[8, 10, 9, 20, null, 42, 28, 32, 54, 70]", "[8, 5, 3, 5, 11, 7, 4, null, 6, 7]"};
  MakeInputBatch(input_data_2_string, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &result_batch));
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[328, 273]", "[10, 9]"};
  auto res_sch = arrow::schema({f_sum, f_count});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

TEST(TestArrowCompute, GroupByAggregateWithMultipleBatchTest) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto f0 = field("f0", uint32());
  auto f1 = field("f1", uint32());
  auto f_unique = field("unique", uint32());
  auto f_sum = field("sum", uint64());
  auto f_count = field("count", uint64());
  auto f_res = field("res", uint64());

  auto arg_pre = TreeExprBuilder::MakeField(f0);
  auto n_pre = TreeExprBuilder::MakeFunction("encodeArray", {arg_pre}, uint32());

  auto arg0 = TreeExprBuilder::MakeField(f0);
  auto arg1 = TreeExprBuilder::MakeField(f1);
  auto n_split = TreeExprBuilder::MakeFunction("splitArrayListWithAction",
                                               {n_pre, arg0, arg1}, uint32());
  auto arg_res = TreeExprBuilder::MakeField(f_res);
  auto n_unique =
      TreeExprBuilder::MakeFunction("action_unique", {n_split, arg0}, uint32());
  auto n_sum = TreeExprBuilder::MakeFunction("action_sum", {n_split, arg1}, uint32());
  auto n_count = TreeExprBuilder::MakeFunction("action_count", {n_split, arg1}, uint32());

  auto unique_expr = TreeExprBuilder::MakeExpression(n_unique, f_res);
  auto sum_expr = TreeExprBuilder::MakeExpression(n_sum, f_res);
  auto count_expr = TreeExprBuilder::MakeExpression(n_count, f_res);

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector = {
      unique_expr, sum_expr, count_expr};
  auto sch = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Field>> ret_types = {f_unique, f_sum, f_count};

  /////////////////////// Create Expression Evaluator ////////////////////
  std::shared_ptr<CodeGenerator> expr;
  ASSERT_NOT_OK(CreateCodeGenerator(sch, expr_vector, ret_types, &expr, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> output_batch_list;

  ////////////////////// calculation /////////////////////
  std::vector<std::string> input_data = {
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]",
      "[1, 2, 3, 4, 5, 5, 4, 1, 2, 2, 1, 1, 1, 4, 4, 3, 5, 5, 5, 5]"};
  MakeInputBatch(input_data, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_2 = {
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]",
      "[6, 7, 8, 9, 10, 10, 9, 6, 7, 7, 6, 6, 6, 9, 9, 8, 10, 10, 10, 10]"};
  MakeInputBatch(input_data_2, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  std::vector<std::string> input_data_3 = {
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]",
      "[1, 2, 3, 8, 5, 5, 10, 1, 2, 7, 6, 6, 1, 9, 4, 9, 5, 8, 5, 5]"};
  MakeInputBatch(input_data_3, sch, &input_batch);
  ASSERT_NOT_OK(expr->evaluate(input_batch, &output_batch_list));

  ////////////////////// Finish //////////////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch;
  ASSERT_NOT_OK(expr->finish(&result_batch));

  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, 4, 5, 6, 7, 8 ,9, 10]", "[8, 10, 9, 20, 55, 42, 28, 32, 54, 70]",
      "[8, 5, 3, 5, 11, 7, 4, 4, 6, 7]"};
  auto res_sch = arrow::schema({f_unique, f_sum, f_count});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  ASSERT_NOT_OK(Equals(*expected_result.get(), *(result_batch[0]).get()));
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
