#pragma once

#include <arrow/status.h>

template <typename T>
class ResultIterator {
 public:
  virtual bool HasNext() = 0;
  virtual arrow::Status Next(std::shared_ptr<T>* out) = 0;
};
