package com.intel.sparkColumnarPlugin.vectorized;

import java.io.IOException;

/**
 * This class is implemented in JNI. This provides the Java interface to invoke functions in JNI.
 * This file is used to generated the .h files required for jni. Avoid all external dependencies in
 * this file.
 */
public class ExpressionEvaluatorJniWrapper {

  /** Wrapper for native API. */
  public ExpressionEvaluatorJniWrapper() throws IOException {
    JniUtils.getInstance();
  }

  /**
   * Generates the projector module to evaluate the expressions with custom configuration.
   *
   * @param schemaBuf The schema serialized as a protobuf. See Types.proto to see the protobuf
   *     specification
   * @param exprListBuf The serialized protobuf of the expression vector. Each expression is created
   *     using TreeBuilder::MakeExpression.
   * @return A moduleId that is passed to the evaluateProjector() and closeProjector() methods
   */
  native long nativeBuild(byte[] schemaBuf, byte[] exprListBuf) throws RuntimeException;

  /**
   * Evaluate the expressions represented by the moduleId on a record batch and store the output in
   * ValueVectors. Throws an exception in case of errors
   *
   * @param moduleId moduleId representing expressions. Created using a call to buildNativeCode
   * @param numRows Number of rows in the record batch
   * @param bufAddrs An array of memory addresses. Each memory address points to a validity vector
   *     or a data vector (will add support for offset vectors later).
   * @param bufSizes An array of buffer sizes. For each memory address in bufAddrs, the size of the
   *     buffer is present in bufSizes
   * @return A list of ArrowRecordBatchBuilder which can be used to build a List of ArrowRecordBatch
   */
  native ArrowRecordBatchBuilder[] nativeEvaluate(
      long nativeHandler, int numRows, long[] bufAddrs, long[] bufSizes) throws RuntimeException;

  /**
   * Closes the projector referenced by moduleId.
   *
   * @param moduleId moduleId that needs to be closed
   */
  native void nativeClose(long nativeHandler);
}
