package com.intel.sparkColumnarPlugin.vectorized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.RuntimeException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import io.netty.buffer.ArrowBuf;

/**
 * This class is implemented in JNI. This provides the Java interface
 * to invoke functions in JNI.
 * This file is used to generated the .h files required for jni. Avoid all
 * external dependencies in this file.
 */
public class ExpressionEvaluator {

  /**
   * Generates the projector module to evaluate the expressions with
   * custom configuration.
   *
   * @param schemaBuf   The schema serialized as a protobuf. See Types.proto
   *                    to see the protobuf specification
   * @param exprListBuf The serialized protobuf of the expression vector. Each
   *                    expression is created using TreeBuilder::MakeExpression.
   * @return A moduleId that is passed to the evaluateProjector() and closeProjector() methods
   *
   */
  native long nativeBuild(byte[] schemaBuf, byte[] exprListBuf) throws RuntimeException;

  /**
   * Evaluate the expressions represented by the moduleId on a record batch
   * and store the output in ValueVectors. Throws an exception in case of errors
   *
   * @param moduleId moduleId representing expressions. Created using a call to
   *                 buildNativeCode
   * @param numRows Number of rows in the record batch
   * @param bufAddrs An array of memory addresses. Each memory address points to
   *                 a validity vector or a data vector (will add support for offset
   *                 vectors later).
   * @param bufSizes An array of buffer sizes. For each memory address in bufAddrs,
   *                 the size of the buffer is present in bufSizes
   */
  native ArrowRecordBatchBuilder nativeEvaluate(long nativeHandler, int numRows, long[] bufAddrs,
                                                long[] bufSizes) throws RuntimeException;

  /**
   * Closes the projector referenced by moduleId.
   *
   * @param moduleId moduleId that needs to be closed
   */
  native void nativeClose(long nativeHandler);

  private long nativeHandler = 0;

  /**
   * Wrapper for native API.
   */
  public ExpressionEvaluator() throws IOException {
    JniUtils.getInstance();
  }

  /**
   * Convert ExpressionTree into native function.
   */
  public void build(Schema schema, List<ExpressionTree> exprs) throws RuntimeException, IOException, GandivaException {
    nativeHandler = nativeBuild(getSchemaBytesBuf(schema), getExprListBytesBuf(exprs));
  }

  /**
   * Evaluate input data using builded native function, and output as recordBatch.
   */
  public ArrowRecordBatch evaluate(ArrowRecordBatch recordBatch)
      throws RuntimeException, IOException {
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];
    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }

    ArrowRecordBatchBuilder resRecordBatchBuilder =
      nativeEvaluate(nativeHandler, recordBatch.getLength(), bufAddrs, bufSizes);
    ArrowRecordBatchBuilderImpl resRecordBatchBuilderImpl =
        new ArrowRecordBatchBuilderImpl(resRecordBatchBuilder);
    if (resRecordBatchBuilder == null) {
      return null;
    }
    return resRecordBatchBuilderImpl.build();
  }

  public void close() {
    nativeClose(nativeHandler);  
  }

  byte[] getSchemaBytesBuf(Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    return out.toByteArray(); 
  }

  byte[] getExprListBytesBuf(List<ExpressionTree> exprs) throws GandivaException {
    GandivaTypes.ExpressionList.Builder builder = GandivaTypes.ExpressionList.newBuilder();
    for (ExpressionTree expr : exprs) {
      builder.addExprs(expr.toProtobuf());
    }
    return builder.build().toByteArray();
  }
  
}
