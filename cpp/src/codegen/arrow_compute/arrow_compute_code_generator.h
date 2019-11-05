#ifndef ARROW_COMPUTE_CODE_GENERATOR
#define ARROW_COMPUTE_CODE_GENERATOR

#include <arrow/type.h>

#include "codegen/arrow_compute/arrow_compute_expr_visitor.h"
#include "codegen/code_generator.h"

class ArrowComputeCodeGenerator : public CodeGenerator {
 public:
  ArrowComputeCodeGenerator(std::shared_ptr<arrow::Schema> schema_ptr,
                            std::vector<std::shared_ptr<gandiva::Expression>> expr_vector,
                            std::vector<std::shared_ptr<arrow::Field>> ret_types)
      : schema_(schema_ptr), ret_types_(ret_types) {
    // check if this expr_vector contains preparation func.
    int expr_start = 0;
    for (auto expr : expr_vector) {
      auto visitor = std::make_shared<ArrowComputeExprVisitor>(schema_ptr, expr);
      if (visitor->IfPreFunc()) {
        pre_visitor_list_.push_back(visitor->GetFuncVisitor());
        expr_start++;
      }
    }
    if (expr_start) {
      expr_vector.erase(expr_vector.begin(), expr_vector.begin() + expr_start);
    }
    for (auto expr : expr_vector) {
      auto visitor = std::make_shared<ArrowComputeExprVisitor>(schema_ptr, expr);
      visitor_list_.push_back(visitor);
    }
    // check if this expr_vector contains post-process func.
    for (auto expr : expr_vector) {
      auto visitor = std::make_shared<ArrowComputeExprVisitor>(schema_ptr, expr);
      if (visitor->IfPostFunc()) {
        post_visitor_list_.push_back(visitor->GetFuncVisitor());
      }
    }
  }

  ~ArrowComputeCodeGenerator() {}

  arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) {
    *out = schema_;
    return arrow::Status::OK();
  }

  arrow::Status evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    arrow::Status status = arrow::Status::OK();
    std::vector<ArrayList> batch_array;
    std::vector<int> batch_size_array;
    auto res_schema = arrow::schema(ret_types_);
    std::shared_ptr<ArrowComputeExprVisitor> prepare_visitor;

    if (pre_visitor_list_.size() == 1) {
      // chendi: Only support one prepare visitor in current code.
      prepare_visitor = pre_visitor_list_[0];
      RETURN_NOT_OK(prepare_visitor->eval(in));
    }

    for (auto visitor : visitor_list_) {
      if (prepare_visitor) {
        RETURN_NOT_OK(visitor->eval(in, prepare_visitor));
      } else {
        RETURN_NOT_OK(visitor->eval(in));
      }
      switch (visitor->GetResultType()) {
        case ArrowComputeResultType::BatchList: {
          RETURN_NOT_OK(visitor->GetResult(&batch_array, &batch_size_array));
        } break;
        case ArrowComputeResultType::ArrayList: {
          ArrayList result_column_list;
          RETURN_NOT_OK(visitor->GetResult(&result_column_list));
          for (int i = 0; i < result_column_list.size(); i++) {
            int res_len = 0;
            RETURN_NOT_OK(GetOrInsert(i, &batch_size_array, &res_len));
            auto result_column = result_column_list[i];
            batch_size_array[i] =
                (res_len < result_column->length()) ? result_column->length() : res_len;
            ArrayList batch_array_item;
            RETURN_NOT_OK(GetOrInsert(i, &batch_array, &batch_array_item));
            batch_array[i].push_back(result_column);
          }
        } break;
        case ArrowComputeResultType::Array: {
          std::shared_ptr<arrow::Array> result_column;
          RETURN_NOT_OK(visitor->GetResult(&result_column));
          int res_len = 0;
          RETURN_NOT_OK(GetOrInsert(0, &batch_size_array, &res_len));
          batch_size_array[0] =
              (res_len < result_column->length()) ? result_column->length() : res_len;
          ArrayList batch_array_item;
          RETURN_NOT_OK(GetOrInsert(0, &batch_array, &batch_array_item));
          batch_array[0].push_back(result_column);
        } break;
      }
    }

    for (auto visitor : post_visitor_list_) {
    }
    for (int i = 0; i < batch_array.size(); i++) {
      out->push_back(
          arrow::RecordBatch::Make(res_schema, batch_size_array[i], batch_array[i]));
    }
    return status;
  }

  arrow::Status evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                         std::vector<arrow::MapArray>* hash_map,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    return arrow::Status::OK();
  }

 private:
  std::vector<std::shared_ptr<ArrowComputeExprVisitor>> pre_visitor_list_;
  std::vector<std::shared_ptr<ArrowComputeExprVisitor>> post_visitor_list_;
  std::vector<std::shared_ptr<ArrowComputeExprVisitor>> visitor_list_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::shared_ptr<arrow::Field>> ret_types_;

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
};

#endif
