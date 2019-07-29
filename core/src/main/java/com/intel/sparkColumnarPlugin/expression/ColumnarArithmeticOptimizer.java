package com.intel.sparkColumnarPlugin.expression;

import java.util.Random;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;

public class ColumnarArithmeticOptimizer {

  public static void columnarBatchAdd(int numRows, OnHeapColumnVector[] inputVectors, OnHeapColumnVector resultVector) {
    int[][] input = new int[inputVectors.length][];
    for (int j = 0; j < inputVectors.length; j++) {
      input[j] = inputVectors[j].intData;
    }
    columnarBatchAdd(numRows, input, resultVector.intData);
  }

  public static void columnarBatchAdd(int numRows, int[][] input, int[] result) {
  //public static void columnarBatchAdd(int numRows, float[][] input, float[] result) {
    //System.out.println("java columnarBatchAdd");
    for (int j = 0; j < input.length; j++) {
      for (int i = 0; i < numRows; i++) {
        result[i] += input[j][i];
      }
    }
  }

  public static void columnarBatchMultiply(int numRows, OnHeapColumnVector[] inputVectors, OnHeapColumnVector resultVector) {
    int[][] input = new int[inputVectors.length][];
    for (int j = 0; j < inputVectors.length; j++) {
      input[j] = inputVectors[j].intData;
    }
    columnarBatchMultiply(numRows, input, resultVector.intData);
  }

  public static void columnarBatchMultiply(int numRows, int[][] input, int[] result) {
    //System.out.println("java columnarBatchMultiply");
    for (int j = 0; j < input.length; j++) {
      for (int i = 0; i < numRows; i++) {
        result[i] *= input[j][i];
      }
    }
  }

  public static void main(String[] args) {
    int input[][] = new int[10][];
    //float input[][] = new float[10][];
    Random rand = new Random();
    int length = 200 * 1024 * 1024;
    for (int j = 0; j < 10; j++) {
        input[j] = new int[length];
        //input[j] = new float[length];
      for (int i = 0; i < length; i++) {
        input[j][i] = rand.nextInt(1024);
      }
    }

    int result[] = new int[length];
    //float result[] = new float[length];
    java.util.Arrays.fill(result, 0);

    long start = 0;
    long interval = 0;

    start = System.currentTimeMillis();
    columnarBatchAdd(length, input, result);
    interval = System.currentTimeMillis() - start;
    System.out.println("columnarBatchAdd: Process time is " + interval + " ms.");

  }
}
