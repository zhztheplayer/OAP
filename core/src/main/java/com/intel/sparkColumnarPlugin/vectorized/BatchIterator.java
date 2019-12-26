package com.intel.sparkColumnarPlugin.vectorized;

import java.io.IOException;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

public class BatchIterator {
  private native ArrowRecordBatchBuilder nativeNext(long nativeHandler);

  private native void nativeClose(long nativeHandler);

  private long nativeHandler = 0;
  private boolean closed = false;

  public BatchIterator() throws IOException {
  }

  public BatchIterator(long instance_id) throws IOException {
    JniUtils.getInstance();
    nativeHandler = instance_id;
  }

  public ArrowRecordBatch next() throws IOException {
    if (nativeHandler == 0) {
      return null;
    }
    ArrowRecordBatchBuilder resRecordBatchBuilder = nativeNext(nativeHandler);
    if (resRecordBatchBuilder == null) {
      return null;
    }
    ArrowRecordBatchBuilderImpl resRecordBatchBuilderImpl =
        new ArrowRecordBatchBuilderImpl(resRecordBatchBuilder);
    return resRecordBatchBuilderImpl.build();
  }

  public void close() {
    if (!closed) {
      nativeClose(nativeHandler);
      closed = true;
    }
  }
}
