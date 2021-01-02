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

public final class UnsafeExternalRowTopNRankSorter extends UnsafeExternalRowTopNSorter {

  private int topRank;

  public static UnsafeExternalRowTopNRankSorter create(
    StructType schema,
    Ordering<InternalRow> orderingInWindow,
    int rankLimit) {
    return new UnsafeExternalRowTopNRankSorter(
      schema, orderingInWindow, rankLimit);
  }

  private UnsafeExternalRowTopNRankSorter(
      StructType schema,
      Ordering<InternalRow> orderingInWindow,
      int rankLimit) {
    super(schema, orderingInWindow, rankLimit);
    // This parameter records the rank of the top row (head node) in the heap
    this.topRank = 0;
  }

  /* We count the number of rows that are the same as the head of the heap. Then the remaining
   * rows that are not counted yet are all less than the head of the heap. So we update the
   * rank of the heap top to be the number of uncounted rows in the heap plus one.
   */
  void reCalculateTopRank() {
    Queue<UnsafeRow> topRowBuffer = new LinkedList<UnsafeRow>();
    UnsafeRow top = heap.peek();
    UnsafeRow newTop = heap.peek();
    while (newTop != null && unsafeRowComparator.compare(top, newTop) == 0) {
      heap.poll();
      topRowBuffer.add(newTop);
      newTop = heap.peek();
    }
    topRank = heap.size() + 1;
    while (!topRowBuffer.isEmpty()) {
      top = topRowBuffer.peek();
      topRowBuffer.remove();
      heap.add(top);
    }
  }

  @Override
  public void insertRow(UnsafeRow row) throws IOException {
    long start = System.nanoTime();
    boolean addToHeap = false;
    UnsafeRow top = heap.peek();

    /* We define the final row returned by the sorter as the row with the maximum weight.
     * We maintain a max heap, which means the top of the heap has the maximum weight. 
     * If the new row is equal to the top of the heap, we always insert it, the rank
     * of the top is not affected after the insertion;
     * If the new row is greater than the top of the heap, the insertion will only happen
     * when the heap is not full yet and after the insertion the newly inserted row will
     * become the new head and the rank of new head will be updated to the existing heap
     * size plus one.
     * If the new row is less than the top of the heap, we always insert it, the head
     * of the heap needs to be removed if the heap reaches the rank limit.
     * Any insertion will cause the increase of the rank of the next potential new head.
     */
    if (top == null) {
      addToHeap = true;
      topRank = topRank + 1;
    }
    else if (unsafeRowComparator.compare(row, top) == 0) { // equal to the top
      addToHeap = true;
    } else if (unsafeRowComparator.compare(row, top) < 0) { // less than the top
      addToHeap = true;
      topRank = topRank + 1;
    } else { // greater than the top
      int nextRank = heap.size() + 1;
      if (nextRank <= rankLimit) {
        addToHeap = true;
        topRank = nextRank;
      }
    }

    if (rankLimit == 0 || !addToHeap) {
      return;
    }

    UnsafeRow rowCopy = row.copy();
    heap.add(rowCopy);

    if (topRank > rankLimit) {
      UnsafeRow newTop = heap.peek();
      while (unsafeRowComparator.compare(top, newTop) == 0) {
        heap.poll();
        newTop = heap.peek();
      }
      // We remove the old top and now we have a new top. So we need to re-calculate
      // the rank of the new top.
      reCalculateTopRank();
    }

    totalSortTimeNanos += System.nanoTime() - start;
  }
}
