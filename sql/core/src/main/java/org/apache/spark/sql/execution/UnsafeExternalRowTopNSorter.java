/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.math.Ordering;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.RankLimit;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;

public abstract class UnsafeExternalRowTopNSorter extends AbstractUnsafeExternalRowSorter {

  private final StructType schema;
  protected final Comparator<UnsafeRow> unsafeRowComparator;
  protected final PriorityQueue<UnsafeRow> heap;
  protected final int rankLimit;
  protected long totalSortTimeNanos = 0L;

  protected UnsafeExternalRowTopNSorter(
      StructType schema,
      Ordering<InternalRow> orderingInWindow,
      int rankLimit) {
    this.schema = schema;
    this.unsafeRowComparator = new UnsafeRowComparator(orderingInWindow);
    this.heap = new PriorityQueue<UnsafeRow>(
      rankLimit, this.unsafeRowComparator.reversed());
    this.rankLimit = rankLimit;
  }

  @Override
  public Iterator<InternalRow> sort() throws IOException {
    long start = System.nanoTime();
    int heapSize = heap.size();
    Stack<UnsafeRow> stack = new Stack<UnsafeRow>();
    for (int i = 0; i < heapSize; i++) {
      UnsafeRow top = heap.peek();
      stack.push(top);
      heap.poll();
    }

    TopNSortIterator topNSortIterator = new TopNSortIterator(stack);
    totalSortTimeNanos += System.nanoTime() - start;
    return topNSortIterator.toScala();
  }

  @Override
  public Iterator<InternalRow> sort(Iterator<UnsafeRow> inputIterator) throws IOException {
    while (inputIterator.hasNext()) {
      insertRow(inputIterator.next());
    }
    return sort();
  }

  @Override
  public Iterator<InternalRow> getIterator() throws IOException {
    throw new IOException("This method is not supported.");
  }

  @Override
  public long getPeakMemoryUsage() {
    return 0;
  }

  @Override
  public long getSortTimeNanos() {
    return totalSortTimeNanos;
  }

  @Override
  public void cleanupResources() {
    return;
  }

  @Override
  void setTestSpillFrequency(int frequency) {
    return;
  }

  private static final class UnsafeRowComparator
    implements Comparator<UnsafeRow> {

    private final Ordering<InternalRow> ordering;

    UnsafeRowComparator(
      Ordering<InternalRow> ordering) {
      this.ordering = ordering;
    }

    @Override
    public int compare(UnsafeRow r1, UnsafeRow r2) {
      return this.ordering.compare(r1, r2);
    }
  }

  private static final class TopNSortIterator extends RowIterator {

    Stack<UnsafeRow> stack;

    TopNSortIterator(Stack<UnsafeRow> stack) {
      this.stack = stack;
      // Add a pad for start. The first call of
      // advanceNext will get rid of this pad.
      this.stack.push(null);
    }

    @Override
    public boolean advanceNext() {
      this.stack.pop();
      return !this.stack.isEmpty();
    }

    @Override
    public UnsafeRow getRow() {
      return this.stack.peek();
    }
  }
}
