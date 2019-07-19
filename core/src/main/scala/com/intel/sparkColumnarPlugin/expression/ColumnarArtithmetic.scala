package com.intel.sparkcolumnarPlugin.expression

import com.intel.sparkColumnarPlugin.expression._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.types._
/**
 * A version of add that supports columnar processing for longs.
 */
class ColumnarAdd(left: Expression, right: Expression)
  extends Add(left, right) with Logging {
  override def supportsColumnar(): Boolean = left.supportsColumnar && right.supportsColumnar

  override def columnarEval(input: Any): Any = {
    var lhs: Any = null
    var rhs: Any = null
    var ret: Any = null
    
    if (input.isInstanceOf[ColumnarBatch]) {
      val args = new ArrayBuffer[Array[Int]]()

      if (left.isInstanceOf[ColumnarAdd]){
        lhs = left.columnarEval((input, args))
      } else {
        lhs = left.columnarEval(input)
        if (lhs.isInstanceOf[ColumnVector]) {
          args += lhs.asInstanceOf[OnHeapColumnVector].intData
        }
      }
      
      if (right.isInstanceOf[ColumnarAdd]) {
        rhs = right.columnarEval((input, args))
      } else {
        rhs = right.columnarEval(input)
        if (rhs.isInstanceOf[ColumnVector]) {
          args += rhs.asInstanceOf[OnHeapColumnVector].intData
        }
      }
      val batch = input.asInstanceOf[ColumnarBatch]

      // do ColumnarAdd here
      if (args.length > 0) {
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ColumnarArithmeticOptimizer.columnarBatchAdd(batch.numRows(), args.toArray, result.intData)
        ret = result
      } 
    }else {
      if (left.isInstanceOf[ColumnarAdd]) {
        lhs = left.columnarEval(input)
      } else {
        val tmp = input.asInstanceOf[Tuple2[Any, ArrayBuffer[Array[Int]]]]
        lhs = left.columnarEval(tmp._1)
        if (lhs.isInstanceOf[ColumnVector]) {
          tmp._2 += lhs.asInstanceOf[OnHeapColumnVector].intData
        }
      }
      if (right.isInstanceOf[ColumnarAdd]) {
        rhs = right.columnarEval(input)
      } else {
        val tmp = input.asInstanceOf[Tuple2[Any, ArrayBuffer[Array[Int]]]]
        rhs = right.columnarEval(tmp._1)
        if (rhs.isInstanceOf[ColumnVector]) {
          tmp._2 += rhs.asInstanceOf[OnHeapColumnVector].intData
        }
      }
    }
    ret
  }

  // Again we need to override equals because we are subclassing a case class
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarAdd]
  }
}

class ColumnarMultiply(left: Expression, right: Expression)
  extends Multiply(left, right) with Logging {
  override def supportsColumnar(): Boolean = left.supportsColumnar && right.supportsColumnar

  override def columnarEval(input: Any): Any = {
    var lhs: Any = null
    var rhs: Any = null
    var ret: Any = null
    
    if (input.isInstanceOf[ColumnarBatch]) {
      val args = new ArrayBuffer[Array[Int]]()

      if (left.isInstanceOf[ColumnarMultiply]){
        lhs = left.columnarEval((input, args))
      } else {
        lhs = left.columnarEval(input)
        if (lhs.isInstanceOf[ColumnVector]) {
          args += lhs.asInstanceOf[OnHeapColumnVector].intData
        }
      }
      
      if (right.isInstanceOf[ColumnarMultiply]) {
        rhs = right.columnarEval((input, args))
      } else {
        rhs = right.columnarEval(input)
        if (rhs.isInstanceOf[ColumnVector]) {
          args += rhs.asInstanceOf[OnHeapColumnVector].intData
        }
      }
      val batch = input.asInstanceOf[ColumnarBatch]

      // do ColumnarMultiply here
      if (args.length > 0) {
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ColumnarArithmeticOptimizer.columnarBatchMultiply(batch.numRows(), args.toArray, result.intData)
        ret = result
      } 
    }else {
      if (left.isInstanceOf[ColumnarMultiply]) {
        lhs = left.columnarEval(input)
      } else {
        val tmp = input.asInstanceOf[Tuple2[Any, ArrayBuffer[Array[Int]]]]
        lhs = left.columnarEval(tmp._1)
        if (lhs.isInstanceOf[ColumnVector]) {
          tmp._2 += lhs.asInstanceOf[OnHeapColumnVector].intData
        }
      }
      if (right.isInstanceOf[ColumnarMultiply]) {
        rhs = right.columnarEval(input)
      } else {
        val tmp = input.asInstanceOf[Tuple2[Any, ArrayBuffer[Array[Int]]]]
        rhs = right.columnarEval(tmp._1)
        if (rhs.isInstanceOf[ColumnVector]) {
          tmp._2 += rhs.asInstanceOf[OnHeapColumnVector].intData
        }
      }
    }
    ret
  }

  // Again we need to override equals because we are subclassing a case class
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarMultiply]
  }
}
