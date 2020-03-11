#include <arrow/array.h>
#include <arrow/ipc/json_simple.h>
#include <gtest/gtest.h>
#include <memory>
#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

TEST(TestArrowCompute, JoinTestUsingInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "probeArraysInner", {TreeExprBuilder::MakeField(table0_f0)}, indices_type);
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_probeArrays, f_indices);

  auto n_shuffleArrayList = TreeExprBuilder::MakeFunction(
      "shuffleArrayList",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_action_0 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_action_1 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f1)},
      uint32());
  auto n_action_2 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f2)},
      uint32());

  auto n_shuffleArrayList_right = TreeExprBuilder::MakeFunction(
      "shuffleArrayList",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_action_3 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_action_4 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto action_0_expr = TreeExprBuilder::MakeExpression(n_action_0, f_res);
  auto action_1_expr = TreeExprBuilder::MakeExpression(n_action_1, f_res);
  auto action_2_expr = TreeExprBuilder::MakeExpression(n_action_2, f_res);

  auto action_3_expr = TreeExprBuilder::MakeExpression(n_action_3, f_res);
  auto action_4_expr = TreeExprBuilder::MakeExpression(n_action_4, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_shuffle_left;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0,
                                    {action_0_expr, action_1_expr, action_2_expr},
                                    {f_res, f_res, f_res}, &expr_shuffle_left, true));
  std::shared_ptr<CodeGenerator> expr_shuffle_right;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_1, {action_3_expr, action_4_expr},
                                    {f_res, f_res}, &expr_shuffle_right, false));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {"[10, 3, 1, 2]", "[10, 3, 1, 2]",
                                                "[10, 3, 1, 2]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8]", "[6, 12, 5, 8]", "[6, 12, 5, 8]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table_left;
  std::vector<std::shared_ptr<RecordBatch>> expected_table_right;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1, 2, 3, 5, 6]", "[1, 2, 3, 5, 6]",
                                                     "[1, 2, 3, 5, 6]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table_left.push_back(expected_result);

  expected_result_string = {"[8, 10, 12]", "[8, 10, 12]", "[8, 10, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table_left.push_back(expected_result);

  auto res_sch_right = arrow::schema({f_res, f_res});
  expected_result_string = {"[1, 2, 3, 5, 6]", "[1, 2, 3, 5, 6]"};
  MakeInputBatch(expected_result_string, res_sch_right, &expected_result);
  expected_table_right.push_back(expected_result);

  expected_result_string = {"[8, 10, 12]", "[8, 10, 12]"};
  MakeInputBatch(expected_result_string, res_sch_right, &expected_result);
  expected_table_right.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_shuffle_left->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_shuffle_left->SetDependency(probe_result_iterator, 0));
  ASSERT_NOT_OK(expr_shuffle_right->SetDependency(probe_result_iterator, 1));
  ASSERT_NOT_OK(expr_shuffle_left->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch_left;
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch_right;

    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne({right_batch->column(0)}));
    ASSERT_NOT_OK(shuffle_result_iterator->Next(&result_batch_left));
    ASSERT_NOT_OK(expr_shuffle_right->evaluate(right_batch, &result_batch_right));
    ASSERT_NOT_OK(Equals(*(expected_table_left[i]).get(), *result_batch_left.get()));
    ASSERT_NOT_OK(
        Equals(*(expected_table_right[i]).get(), *(result_batch_right[0]).get()));
  }
}

TEST(TestArrowCompute, JoinTestWithMultipleSamePrimaryKeyUsingInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "probeArraysInner", {TreeExprBuilder::MakeField(table0_f0)}, indices_type);
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_probeArrays, f_indices);

  auto n_shuffleArrayList = TreeExprBuilder::MakeFunction(
      "shuffleArrayList",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_action_0 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_action_1 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f1)},
      uint32());
  auto n_action_2 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f2)},
      uint32());

  auto n_shuffleArrayList_right = TreeExprBuilder::MakeFunction(
      "shuffleArrayList",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_action_3 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_action_4 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto action_0_expr = TreeExprBuilder::MakeExpression(n_action_0, f_res);
  auto action_1_expr = TreeExprBuilder::MakeExpression(n_action_1, f_res);
  auto action_2_expr = TreeExprBuilder::MakeExpression(n_action_2, f_res);

  auto action_3_expr = TreeExprBuilder::MakeExpression(n_action_3, f_res);
  auto action_4_expr = TreeExprBuilder::MakeExpression(n_action_4, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_shuffle_left;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0,
                                    {action_0_expr, action_1_expr, action_2_expr},
                                    {f_res, f_res, f_res}, &expr_shuffle_left, true));
  std::shared_ptr<CodeGenerator> expr_shuffle_right;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_1, {action_3_expr, action_4_expr},
                                    {f_res, f_res}, &expr_shuffle_right, false));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      "[10, 3, 1, 2, 3, 1]", "[10, 3, 1, 2, 13, 11]", "[10, 3, 1, 2, 13, 11]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8, 6, 10]", "[6, 12, 5, 8, 16, 110]",
                       "[6, 12, 5, 8, 16, 110]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table_left;
  std::vector<std::shared_ptr<RecordBatch>> expected_table_right;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[1, 1, 2, 3, 3, 5, 6, 6]",
                                                     "[1, 11, 2, 3, 13, 5, 6, 16]",
                                                     "[1, 11, 2, 3, 13, 5, 6, 16]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table_left.push_back(expected_result);

  expected_result_string = {"[8, 10, 10, 12]", "[8, 10, 110, 12]", "[8, 10, 110, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table_left.push_back(expected_result);

  auto res_sch_right = arrow::schema({f_res, f_res});
  expected_result_string = {"[1, 1, 2, 3, 3, 5, 6, 6]", "[1, 1, 2, 3, 3, 5, 6, 6]"};
  MakeInputBatch(expected_result_string, res_sch_right, &expected_result);
  expected_table_right.push_back(expected_result);

  expected_result_string = {"[8, 10, 10, 12]", "[8, 10, 10, 12]"};
  MakeInputBatch(expected_result_string, res_sch_right, &expected_result);
  expected_table_right.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_shuffle_left->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_shuffle_left->SetDependency(probe_result_iterator, 0));
  ASSERT_NOT_OK(expr_shuffle_right->SetDependency(probe_result_iterator, 1));
  ASSERT_NOT_OK(expr_shuffle_left->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch_left;
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch_right;

    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne({right_batch->column(0)}));
    ASSERT_NOT_OK(shuffle_result_iterator->Next(&result_batch_left));
    ASSERT_NOT_OK(expr_shuffle_right->evaluate(right_batch, &result_batch_right));
    ASSERT_NOT_OK(Equals(*(expected_table_left[i]).get(), *result_batch_left.get()));
    ASSERT_NOT_OK(
        Equals(*(expected_table_right[i]).get(), *(result_batch_right[0]).get()));
  }
}

TEST(TestArrowCompute, JoinTestWithTwoKeysUsingInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", utf8());
  auto table0_f1 = field("table0_f1", utf8());
  auto table0_f2 = field("table0_f2", int32());
  auto table1_f0 = field("table1_f0", utf8());
  auto table1_f1 = field("table1_f1", utf8());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "probeArraysInner",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1)},
      indices_type);
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_probeArrays, f_indices);

  auto n_shuffleArrayList = TreeExprBuilder::MakeFunction(
      "shuffleArrayList",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_action_0 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_action_1 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f1)},
      uint32());
  auto n_action_2 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f2)},
      uint32());

  auto n_shuffleArrayList_right = TreeExprBuilder::MakeFunction(
      "shuffleArrayList",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_action_3 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_action_4 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());
  auto f_res_utf = field("res", utf8());

  auto action_0_expr = TreeExprBuilder::MakeExpression(n_action_0, f_res);
  auto action_1_expr = TreeExprBuilder::MakeExpression(n_action_1, f_res);
  auto action_2_expr = TreeExprBuilder::MakeExpression(n_action_2, f_res);

  auto action_3_expr = TreeExprBuilder::MakeExpression(n_action_3, f_res);
  auto action_4_expr = TreeExprBuilder::MakeExpression(n_action_4, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_shuffle_left;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0,
                                    {action_0_expr, action_1_expr, action_2_expr},
                                    {f_res, f_res, f_res}, &expr_shuffle_left, true));
  std::shared_ptr<CodeGenerator> expr_shuffle_right;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_1, {action_3_expr, action_4_expr},
                                    {f_res, f_res}, &expr_shuffle_right, false));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {
      R"(["l", "c", "a", "b"])", R"(["L", "C", "A", "B"])", "[10, 3, 1, 2]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {R"(["f", "n", "e", "j"])", R"(["F", "N", "E", "J"])",
                       "[6, 12, 5, 8]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {R"(["a", "b", "c", "d", "e", "f"])",
                                                  R"(["A", "B", "C", "D", "E", "F"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {R"(["i", "j", "k", "l", "m", "n"])",
                         R"(["I", "J", "K", "L", "M", "N"])"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table_left;
  std::vector<std::shared_ptr<RecordBatch>> expected_table_right;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::cout << "0" << std::endl;
  std::vector<std::string> expected_result_string = {
      R"(["a", "b", "c", "e", "f"])", R"(["A", "B", "C", "E", "F"])", "[1, 2, 3, 5, 6]"};
  auto res_sch = arrow::schema({f_res_utf, f_res_utf, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table_left.push_back(expected_result);

  expected_result_string = {R"(["j", "l", "n"])", R"(["J", "L", "N"])", "[8, 10, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table_left.push_back(expected_result);

  auto res_sch_right = arrow::schema({f_res_utf, f_res_utf});
  expected_result_string = {R"(["a", "b", "c", "e", "f"])",
                            R"(["A", "B", "C", "E", "F"])"};
  MakeInputBatch(expected_result_string, res_sch_right, &expected_result);
  expected_table_right.push_back(expected_result);

  expected_result_string = {R"(["j", "l", "n"])", R"(["J", "L", "N"])"};
  MakeInputBatch(expected_result_string, res_sch_right, &expected_result);
  expected_table_right.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_shuffle_left->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_shuffle_left->SetDependency(probe_result_iterator, 0));
  ASSERT_NOT_OK(expr_shuffle_right->SetDependency(probe_result_iterator, 1));
  ASSERT_NOT_OK(expr_shuffle_left->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch_left;
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch_right;

    std::vector<std::shared_ptr<arrow::Array>> right_batch_cols;
    for (int j = 0; j < 2; j++) {
      right_batch_cols.push_back(right_batch->column(j));
    }
    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne(right_batch_cols));
    ASSERT_NOT_OK(shuffle_result_iterator->Next(&result_batch_left));
    ASSERT_NOT_OK(expr_shuffle_right->evaluate(right_batch, &result_batch_right));
    ASSERT_NOT_OK(Equals(*(expected_table_left[i]).get(), *result_batch_left.get()));
    ASSERT_NOT_OK(
        Equals(*(expected_table_right[i]).get(), *(result_batch_right[0]).get()));
  }
}

TEST(TestArrowCompute, JoinTestWithSelectionUsingInnerJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "probeArraysInner", {TreeExprBuilder::MakeField(table0_f0)}, indices_type);
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_probeArrays, f_indices);

  auto n_shuffleArrayList = TreeExprBuilder::MakeFunction(
      "shuffleArrayList",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_action_0 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_action_1 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f1)},
      uint32());
  auto n_action_2 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f2)},
      uint32());

  auto n_shuffleArrayList_right = TreeExprBuilder::MakeFunction(
      "shuffleArrayList",
      {TreeExprBuilder::MakeField(table1_f0), TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto n_action_3 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, TreeExprBuilder::MakeField(table1_f0)},
      uint32());
  auto n_action_4 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, TreeExprBuilder::MakeField(table1_f1)},
      uint32());
  auto f_res = field("res", uint32());

  auto action_0_expr = TreeExprBuilder::MakeExpression(n_action_0, f_res);
  auto action_1_expr = TreeExprBuilder::MakeExpression(n_action_1, f_res);
  auto action_2_expr = TreeExprBuilder::MakeExpression(n_action_2, f_res);

  auto action_3_expr = TreeExprBuilder::MakeExpression(n_action_3, f_res);
  auto action_4_expr = TreeExprBuilder::MakeExpression(n_action_4, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_shuffle_left;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0,
                                    {action_0_expr, action_1_expr, action_2_expr},
                                    {f_res, f_res, f_res}, &expr_shuffle_left, true));
  std::shared_ptr<CodeGenerator> expr_shuffle_right;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_1, {action_3_expr, action_4_expr},
                                    {f_res, f_res}, &expr_shuffle_right, false));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {"[10, 3, 1, 2]", "[10, 3, 1, 2]",
                                                "[10, 3, 1, 2]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8]", "[6, 12, 5, 8]", "[6, 12, 5, 8]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table_left;
  std::vector<std::shared_ptr<RecordBatch>> expected_table_right;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {"[2]", "[2]", "[2]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table_left.push_back(expected_result);

  expected_result_string = {"[8]", "[8]", "[8]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table_left.push_back(expected_result);

  auto res_sch_right = arrow::schema({f_res, f_res});
  expected_result_string = {"[2]", "[2]"};
  MakeInputBatch(expected_result_string, res_sch_right, &expected_result);
  expected_table_right.push_back(expected_result);

  expected_result_string = {"[8]", "[8]"};
  MakeInputBatch(expected_result_string, res_sch_right, &expected_result);
  expected_table_right.push_back(expected_result);

  std::string selection_string = "[1, 3]";
  std::shared_ptr<arrow::Array> selection_in;
  ASSERT_NOT_OK(arrow::ipc::internal::json::ArrayFromJSON(uint16(), selection_string,
                                                          &selection_in));

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(selection_in, batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_shuffle_left->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_shuffle_left->SetDependency(probe_result_iterator, 0));
  ASSERT_NOT_OK(expr_shuffle_right->SetDependency(probe_result_iterator, 1));
  ASSERT_NOT_OK(expr_shuffle_left->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto left_batch = table_0[i];
    auto right_batch = table_1[i];

    std::shared_ptr<arrow::RecordBatch> result_batch_left;
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batch_right;

    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne({right_batch->column(0)},
                                                            selection_in));
    ASSERT_NOT_OK(shuffle_result_iterator->Next(&result_batch_left));
    ASSERT_NOT_OK(expr_shuffle_right->evaluate(right_batch, &result_batch_right));
    ASSERT_NOT_OK(Equals(*(expected_table_left[i]).get(), *result_batch_left.get()));
    ASSERT_NOT_OK(
        Equals(*(expected_table_right[i]).get(), *(result_batch_right[0]).get()));
  }
}
TEST(TestArrowCompute, JoinTestUsingRightJoin) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto table0_f0 = field("table0_f0", uint32());
  auto table0_f1 = field("table0_f1", uint32());
  auto table0_f2 = field("table0_f2", uint32());
  auto table1_f0 = field("table1_f0", uint32());
  auto table1_f1 = field("table1_f1", uint32());

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "probeArraysRight", {TreeExprBuilder::MakeField(table0_f0)}, indices_type);
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_probeArrays, f_indices);

  auto n_shuffleArrayList = TreeExprBuilder::MakeFunction(
      "shuffleArrayList",
      {TreeExprBuilder::MakeField(table0_f0), TreeExprBuilder::MakeField(table0_f1),
       TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto n_action_0 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f0)},
      uint32());
  auto n_action_1 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f1)},
      uint32());
  auto n_action_2 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, TreeExprBuilder::MakeField(table0_f2)},
      uint32());
  auto f_res = field("res", uint32());

  auto action_0_expr = TreeExprBuilder::MakeExpression(n_action_0, f_res);
  auto action_1_expr = TreeExprBuilder::MakeExpression(n_action_1, f_res);
  auto action_2_expr = TreeExprBuilder::MakeExpression(n_action_2, f_res);

  auto schema_table_0 = arrow::schema({table0_f0, table0_f1, table0_f2});
  auto schema_table_1 = arrow::schema({table1_f0, table1_f1});
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0, {probeArrays_expr}, {f_indices},
                                    &expr_probe, true));
  std::shared_ptr<CodeGenerator> expr_shuffle;
  ASSERT_NOT_OK(CreateCodeGenerator(schema_table_0,
                                    {action_0_expr, action_1_expr, action_2_expr},
                                    {f_res, f_res, f_res}, &expr_shuffle, true));
  std::shared_ptr<arrow::RecordBatch> input_batch;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  std::vector<std::shared_ptr<arrow::RecordBatch>> table_0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> table_1;

  std::vector<std::string> input_data_string = {"[10, 3, 1, 2]", "[10, 3, 1, 2]",
                                                "[10, 3, 1, 2]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  input_data_string = {"[6, 12, 5, 8]", "[6, 12, 5, 8]", "[6, 12, 5, 8]"};
  MakeInputBatch(input_data_string, schema_table_0, &input_batch);
  table_0.push_back(input_batch);

  std::vector<std::string> input_data_2_string = {"[1, 2, 3, 4, 5, 6]",
                                                  "[1, 2, 3, 4, 5, 6]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  input_data_2_string = {"[7, 8, 9, 10, 11, 12]", "[7, 8, 9, 10, 11, 12]"};
  MakeInputBatch(input_data_2_string, schema_table_1, &input_batch);
  table_1.push_back(input_batch);

  //////////////////////// data prepared /////////////////////////

  std::vector<std::shared_ptr<RecordBatch>> expected_table;
  std::shared_ptr<arrow::RecordBatch> expected_result;
  std::vector<std::string> expected_result_string = {
      "[1, 2, 3, null, 5, 6]", "[1, 2, 3, null, 5, 6]", "[1, 2, 3, null, 5, 6]"};
  auto res_sch = arrow::schema({f_res, f_res, f_res});
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  expected_result_string = {"[null, 8, null, 10, null, 12]",
                            "[null, 8, null, 10, null, 12]",
                            "[null, 8, null, 10, null, 12]"};
  MakeInputBatch(expected_result_string, res_sch, &expected_result);
  expected_table.push_back(expected_result);

  ////////////////////// evaluate //////////////////////
  for (auto batch : table_0) {
    ASSERT_NOT_OK(expr_probe->evaluate(batch, &dummy_result_batches));
    ASSERT_NOT_OK(expr_shuffle->evaluate(batch, &dummy_result_batches));
  }
  ASSERT_NOT_OK(expr_probe->finish(&probe_result_iterator));
  ASSERT_NOT_OK(expr_shuffle->SetDependency(probe_result_iterator, 0));
  ASSERT_NOT_OK(expr_shuffle->finish(&shuffle_result_iterator));

  for (int i = 0; i < 2; i++) {
    auto batch = table_1[i];
    std::shared_ptr<arrow::RecordBatch> result_batch;
    ASSERT_NOT_OK(probe_result_iterator->ProcessAndCacheOne({batch->column(0)}));
    ASSERT_NOT_OK(shuffle_result_iterator->Next(&result_batch));
    ASSERT_NOT_OK(Equals(*(expected_table[i]).get(), *result_batch.get()));
  }
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
