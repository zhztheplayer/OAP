#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <chrono>
#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "codegen/common/result_iterator.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

class BenchmarkArrowComputeJoin : public ::testing::Test {
 public:
  void SetUp() override {
    // read input from parquet file
#ifdef BENCHMARK_FILE_PATH
    std::string dir_path = BENCHMARK_FILE_PATH;
#else
    std::string dir_path = "";
#endif
    std::string left_path = dir_path + "tpch_lineitem_join.parquet";
    std::string right_path = dir_path + "tpch_order_join.parquet";
    std::cout << "This Benchmark used file " << left_path << " and " << right_path
              << ", please download from server "
                 "vsr200://home/zhouyuan/sparkColumnarPlugin/source_files"
              << std::endl;
    std::shared_ptr<arrow::fs::FileSystem> right_fs;
    std::shared_ptr<arrow::fs::FileSystem> left_fs;
    std::string right_file_name;
    std::string left_file_name;
    ASSERT_NOT_OK(arrow::fs::FileSystemFromUri(right_path, &right_fs, &right_file_name));
    ASSERT_NOT_OK(arrow::fs::FileSystemFromUri(left_path, &left_fs, &left_file_name));

    ARROW_ASSIGN_OR_THROW(right_file, right_fs->OpenInputFile(right_file_name));
    ARROW_ASSIGN_OR_THROW(left_file, left_fs->OpenInputFile(left_file_name));

    parquet::ArrowReaderProperties properties(true);
    properties.set_batch_size(4096);
    auto pool = arrow::default_memory_pool();

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(left_file), properties,
        &left_parquet_reader));
    ASSERT_NOT_OK(left_parquet_reader->GetRecordBatchReader({0}, {0, 1, 2},
                                                            &left_record_batch_reader));

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(right_file), properties,
        &right_parquet_reader));
    ASSERT_NOT_OK(right_parquet_reader->GetRecordBatchReader({0}, {0, 1},
                                                             &right_record_batch_reader));

    left_schema = left_record_batch_reader->schema();
    right_schema = right_record_batch_reader->schema();
    std::cout << left_schema->ToString() << std::endl;
    std::cout << right_schema->ToString() << std::endl;

    ////////////////// expr prepration ////////////////
    left_field_list = left_record_batch_reader->schema()->fields();
    right_field_list = right_record_batch_reader->schema()->fields();
  }

 protected:
  std::shared_ptr<arrow::io::RandomAccessFile> left_file;
  std::shared_ptr<arrow::io::RandomAccessFile> right_file;
  std::unique_ptr<::parquet::arrow::FileReader> left_parquet_reader;
  std::unique_ptr<::parquet::arrow::FileReader> right_parquet_reader;
  std::shared_ptr<RecordBatchReader> left_record_batch_reader;
  std::shared_ptr<RecordBatchReader> right_record_batch_reader;
  std::shared_ptr<arrow::Schema> left_schema;
  std::shared_ptr<arrow::Schema> right_schema;

  std::vector<std::shared_ptr<::arrow::Field>> left_field_list;
  std::vector<std::shared_ptr<::arrow::Field>> right_field_list;
  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector;
  std::vector<std::shared_ptr<::arrow::Field>> ret_field_list;

  int left_primary_key_index = 0;
  int right_primary_key_index = 0;
};

TEST_F(BenchmarkArrowComputeJoin, JoinBenchmark) {
  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> left_field_node_list;
  for (auto field : left_field_list) {
    left_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> right_field_node_list;
  for (auto field : right_field_list) {
    right_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  auto indices_type = std::make_shared<FixedSizeBinaryType>(4);
  auto f_indices = field("indices", indices_type);
  auto n_probeArrays = TreeExprBuilder::MakeFunction(
      "probeArraysInner", {left_field_node_list[left_primary_key_index]}, indices_type);
  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_probeArrays, f_indices);

  auto n_shuffleArrayList =
      TreeExprBuilder::MakeFunction("shuffleArrayList", left_field_node_list, uint32());
  auto n_action_0 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, left_field_node_list[0]}, uint32());
  auto n_action_1 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, left_field_node_list[1]}, uint32());
  auto n_action_2 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList, left_field_node_list[2]}, uint32());

  auto n_shuffleArrayList_right =
      TreeExprBuilder::MakeFunction("shuffleArrayList", right_field_node_list, uint32());
  auto n_action_3 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, right_field_node_list[0]}, uint32());
  auto n_action_4 = TreeExprBuilder::MakeFunction(
      "action_dono", {n_shuffleArrayList_right, right_field_node_list[1]}, uint32());
  auto f_res = field("res", uint32());

  auto action_0_expr = TreeExprBuilder::MakeExpression(n_action_0, f_res);
  auto action_1_expr = TreeExprBuilder::MakeExpression(n_action_1, f_res);
  auto action_2_expr = TreeExprBuilder::MakeExpression(n_action_2, f_res);
  auto action_3_expr = TreeExprBuilder::MakeExpression(n_action_3, f_res);
  auto action_4_expr = TreeExprBuilder::MakeExpression(n_action_4, f_res);

  auto schema_table_0 = arrow::schema(left_field_list);
  auto schema_table_1 = arrow::schema(right_field_list);
  ///////////////////// Calculation //////////////////
  std::shared_ptr<CodeGenerator> expr_probe;
  std::shared_ptr<CodeGenerator> expr_shuffle;
  std::shared_ptr<CodeGenerator> expr_shuffle_right;

  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> probe_result_iterator;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> shuffle_result_iterator;

  ////////////////////// evaluate //////////////////////
  std::shared_ptr<arrow::RecordBatch> left_record_batch;
  std::shared_ptr<arrow::RecordBatch> right_record_batch;
  uint64_t elapse_gen = 0;
  uint64_t elapse_left_read = 0;
  uint64_t elapse_right_read = 0;
  uint64_t elapse_eval = 0;
  uint64_t elapse_process = 0;
  uint64_t num_batches = 0;
  uint64_t num_rows = 0;

  TIME_MICRO_OR_THROW(elapse_gen, CreateCodeGenerator(left_schema, {probeArrays_expr},
                                                      {f_indices}, &expr_probe, true));
  TIME_MICRO_OR_THROW(
      elapse_gen,
      CreateCodeGenerator(schema_table_0, {action_0_expr, action_1_expr, action_2_expr},
                          {f_res, f_res, f_res}, &expr_shuffle, true));

  TIME_MICRO_OR_THROW(elapse_gen,
                      CreateCodeGenerator(schema_table_1, {action_3_expr, action_4_expr},
                                          {f_res, f_res}, &expr_shuffle_right, false));
  do {
    TIME_MICRO_OR_THROW(elapse_left_read,
                        left_record_batch_reader->ReadNext(&left_record_batch));
    if (left_record_batch) {
      TIME_MICRO_OR_THROW(elapse_eval,
                          expr_probe->evaluate(left_record_batch, &dummy_result_batches));
      TIME_MICRO_OR_THROW(
          elapse_eval, expr_shuffle->evaluate(left_record_batch, &dummy_result_batches));
      num_batches += 1;
    }
  } while (left_record_batch);
  std::cout << "Readed left table with " << num_batches << " batches." << std::endl;

  TIME_MICRO_OR_THROW(elapse_eval, expr_probe->finish(&probe_result_iterator));
  TIME_MICRO_OR_THROW(elapse_eval, expr_shuffle->SetDependency(probe_result_iterator, 0));
  TIME_MICRO_OR_THROW(elapse_eval,
                      expr_shuffle_right->SetDependency(probe_result_iterator, 1));
  TIME_MICRO_OR_THROW(elapse_eval, expr_shuffle->finish(&shuffle_result_iterator));

  num_batches = 0;
  uint64_t num_output_batches = 0;
  std::shared_ptr<arrow::RecordBatch> left_out;
  std::vector<std::shared_ptr<arrow::RecordBatch>> right_out;
  do {
    TIME_MICRO_OR_THROW(elapse_right_read,
                        right_record_batch_reader->ReadNext(&right_record_batch));
    if (right_record_batch) {
      TIME_MICRO_OR_THROW(elapse_process,
                          probe_result_iterator->ProcessAndCacheOne(
                              {right_record_batch->column(right_primary_key_index)}));
      TIME_MICRO_OR_THROW(elapse_process, shuffle_result_iterator->Next(&left_out));
      TIME_MICRO_OR_THROW(elapse_process,
                          expr_shuffle_right->evaluate(right_record_batch, &right_out));
      num_batches += 1;
      num_output_batches++;
      num_rows += left_out->num_rows();
    }
  } while (right_record_batch);
  std::cout << "Readed right table with " << num_batches << " batches." << std::endl;

  std::cout << "BenchmarkArrowComputeJoin processed " << num_batches
            << " batches, then output " << num_output_batches << " batches with "
            << num_rows << " rows, to complete, it took " << TIME_TO_STRING(elapse_gen)
            << " doing codegen, took " << TIME_TO_STRING(elapse_left_read)
            << " doing left BatchRead, took " << TIME_TO_STRING(elapse_right_read)
            << " doing right BatchRead, took " << TIME_TO_STRING(elapse_eval)
            << " doing left table hashmap insert, took " << TIME_TO_STRING(elapse_process)
            << " doing right table probe and left table shuffle." << std::endl;
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
