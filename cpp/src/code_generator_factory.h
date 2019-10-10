#include <arrow/type.h>
#include <gandiva/expression.h>
#include "code_generator.h"
#include "expr_visitor.h"
#include "arrow_compute_code_generator.h"
#include "gandiva_code_generator.h"
#include "compute_ext_code_generator.h"

arrow::Status CreateCodeGenerator(
    std::shared_ptr<arrow::Schema> schema_ptr,
    std::vector<std::shared_ptr<gandiva::Expression>> exprs_vector,
    std::vector<std::shared_ptr<arrow::Field>> ret_types,
    CodeGenerator** out) {
  ExprVisitor nodeVisitor;
  int codegen_type;
  auto status = nodeVisitor.create(exprs_vector, &codegen_type);
  switch(codegen_type) {
    case ARROW_COMPUTE:
      *out = new ArrowComputeCodeGenerator(schema_ptr, exprs_vector, ret_types);
      break;
    case GANDIVA:
      *out = new GandivaCodeGenerator(schema_ptr, exprs_vector, ret_types);
      break;
    case COMPUTE_EXT:
      *out = new GandivaCodeGenerator(schema_ptr, exprs_vector, ret_types);
      break;
    default:
      *out = nullptr;
      status = arrow::Status::TypeError("Unrecognized expression type.");
      break;
  }
  return status;
}
