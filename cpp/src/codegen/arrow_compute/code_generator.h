#pragma once

#include <arrow/pretty_print.h>
#include <arrow/type.h>

#include "codegen/arrow_compute/expr_visitor.h"
#include "codegen/code_generator.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {

class ArrowComputeCodeGenerator : public CodeGenerator {
 public:
  ArrowComputeCodeGenerator(
      std::shared_ptr<arrow::Schema> schema_ptr,
      std::vector<std::shared_ptr<gandiva::Expression>> expr_vector,
      std::vector<std::shared_ptr<arrow::Field>> ret_types, bool return_when_finish,
      std::vector<std::shared_ptr<::gandiva::Expression>> finish_exprs_vector)
      : schema_(schema_ptr),
        ret_types_(ret_types),
        return_when_finish_(return_when_finish) {
    int i = 0;
    for (auto expr : expr_vector) {
      std::shared_ptr<ExprVisitor> root_visitor;
      if (finish_exprs_vector.empty()) {
        auto visitor =
            MakeExprVisitor(schema_ptr, expr, &expr_visitor_cache_, &root_visitor);
        visitor_list_.push_back(root_visitor);
      } else {
        auto visitor = MakeExprVisitor(schema_ptr, expr, finish_exprs_vector[i++],
                                       &expr_visitor_cache_, &root_visitor);
        visitor_list_.push_back(root_visitor);
      }
    }
#ifdef DEBUG_DATA
    std::cout << "new ExprVisitor for " << schema_->ToString() << std::endl;
#endif
  }

  ~ArrowComputeCodeGenerator() {
    expr_visitor_cache_.clear();
    visitor_list_.clear();
  }

  arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) {
    *out = schema_;
    return arrow::Status::OK();
  }

  arrow::Status evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    arrow::Status status = arrow::Status::OK();
    std::vector<ArrayList> batch_array;
    std::vector<int> batch_size_array;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    for (auto visitor : visitor_list_) {
      RETURN_NOT_OK(visitor->Eval(in));
      if (!return_when_finish_) {
        RETURN_NOT_OK(GetResult(visitor, &batch_array, &batch_size_array, &fields));
      }
    }

    if (!return_when_finish_) {
      std::shared_ptr<arrow::Schema> res_schema;
      if (ret_types_.size() < fields.size()) {
        res_schema = arrow::schema(fields);
      } else {
        res_schema = arrow::schema(ret_types_);
      }
      for (int i = 0; i < batch_array.size(); i++) {
        out->push_back(
            arrow::RecordBatch::Make(res_schema, batch_size_array[i], batch_array[i]));
      }

      // we need to clean up this visitor chain result for next record_batch.
      for (auto visitor : visitor_list_) {
        RETURN_NOT_OK(visitor->Reset());
      }
    } else {
      for (auto visitor : visitor_list_) {
        RETURN_NOT_OK(visitor->ResetDependency());
      }
    }
    return status;
  }

  arrow::Status finish(std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    arrow::Status status = arrow::Status::OK();
    std::vector<ArrayList> batch_array;
    std::vector<int> batch_size_array;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    for (auto visitor : visitor_list_) {
      std::shared_ptr<ExprVisitor> finish_visitor;
      RETURN_NOT_OK(visitor->Finish(&finish_visitor));
      if (finish_visitor) {
        RETURN_NOT_OK(
            GetResult(finish_visitor, &batch_array, &batch_size_array, &fields));
      } else {
        RETURN_NOT_OK(GetResult(visitor, &batch_array, &batch_size_array, &fields));
      }
    }

    std::shared_ptr<arrow::Schema> res_schema;
    if (ret_types_.size() < fields.size()) {
      res_schema = arrow::schema(fields);
    } else {
      res_schema = arrow::schema(ret_types_);
    }
    for (int i = 0; i < batch_array.size(); i++) {
      auto record_batch =
          arrow::RecordBatch::Make(res_schema, batch_size_array[i], batch_array[i]);
#ifdef DEBUG_DATA
      arrow::PrettyPrint(*record_batch.get(), 2, &std::cout);
#endif
      out->push_back(record_batch);
    }

    // we need to clean up this visitor chain result for next record_batch.
    for (auto visitor : visitor_list_) {
      RETURN_NOT_OK(visitor->Reset());
    }
    return status;
  }

 private:
  std::vector<std::shared_ptr<ExprVisitor>> visitor_list_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::shared_ptr<arrow::Field>> ret_types_;
  bool return_when_finish_;
  // ExprVisitor Cache, used when multiple node depends on same node.
  ExprVisitorMap expr_visitor_cache_;

  arrow::Status MakeBatchFromArray(std::shared_ptr<arrow::Array> column, int batch_index,
                                   std::vector<ArrayList>* batch_array,
                                   std::vector<int>* batch_size_array) {
    int res_len = 0;
    RETURN_NOT_OK(GetOrInsert(batch_index, batch_size_array, &res_len));
    batch_size_array->at(batch_index) =
        (res_len < column->length()) ? column->length() : res_len;
    ArrayList batch_array_item;
    RETURN_NOT_OK(GetOrInsert(batch_index, batch_array, &batch_array_item));
    batch_array->at(batch_index).push_back(column);
#ifdef DEBUG_DATA
    std::cout << "output column " << batch_array->at(batch_index).size() << "size is "
              << column->length() << std::endl;
#endif
    return arrow::Status::OK();
  }
  arrow::Status MakeBatchFromArrayList(ArrayList column_list,
                                       std::vector<ArrayList>* batch_array,
                                       std::vector<int>* batch_size_array) {
    for (int i = 0; i < column_list.size(); i++) {
      RETURN_NOT_OK(MakeBatchFromArray(column_list[i], i, batch_array, batch_size_array));
    }
    return arrow::Status::OK();
  }
  arrow::Status MakeBatchFromBatch(ArrayList batch, std::vector<ArrayList>* batch_array,
                                   std::vector<int>* batch_size_array) {
    int length = 0;
    for (auto column : batch) {
      length = length < column->length() ? column->length() : length;
    }
    batch_array->push_back(batch);
    batch_size_array->push_back(length);
    return arrow::Status::OK();
  }

  template <typename T>
  arrow::Status GetOrInsert(int i, std::vector<T>* input, T* out) {
    if (i > input->size()) {
      return arrow::Status::Invalid("GetOrInser index: ", i, "  is out of range.");
    }
    if (i == input->size()) {
      T new_data = *out;
      input->push_back(new_data);
    }
    *out = input->at(i);
    return arrow::Status::OK();
  }

  arrow::Status GetResult(std::shared_ptr<ExprVisitor> visitor,
                          std::vector<ArrayList>* batch_array,
                          std::vector<int>* batch_size_array,
                          std::vector<std::shared_ptr<arrow::Field>>* fields) {
    auto status = arrow::Status::OK();
    std::vector<std::shared_ptr<arrow::Field>> return_fields;
    switch (visitor->GetResultType()) {
      case ArrowComputeResultType::BatchList: {
        RETURN_NOT_OK(visitor->GetResult(batch_array, batch_size_array, &return_fields));
      } break;
      case ArrowComputeResultType::Batch: {
        ArrayList result_batch;
        RETURN_NOT_OK(visitor->GetResult(&result_batch, &return_fields));
        RETURN_NOT_OK(MakeBatchFromBatch(result_batch, batch_array, batch_size_array));
      } break;
      case ArrowComputeResultType::ArrayList: {
        ArrayList result_column_list;
        RETURN_NOT_OK(visitor->GetResult(&result_column_list, &return_fields));
        RETURN_NOT_OK(
            MakeBatchFromArrayList(result_column_list, batch_array, batch_size_array));
      } break;
      case ArrowComputeResultType::Array: {
        std::shared_ptr<arrow::Array> result_column;
        RETURN_NOT_OK(visitor->GetResult(&result_column, &return_fields));
        RETURN_NOT_OK(
            MakeBatchFromArray(result_column, 0, batch_array, batch_size_array));
      } break;
      default:
        return arrow::Status::Invalid("ArrowComputeResultType is invalid.");
    }
    fields->insert(fields->end(), return_fields.begin(), return_fields.end());
    return status;
  }
};
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
