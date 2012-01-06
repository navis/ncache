/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nexr.cache.util;

public abstract class PriorityQueue<T> {

    int size;

    final T[] heap;
    final int maxSize;
    final boolean ascending;

    @SuppressWarnings("unchecked")
    public PriorityQueue(int maxSize, boolean ascending) {
        this.ascending = ascending;
        this.heap = (T[]) new Comparable[maxSize + 1];
        this.maxSize = maxSize;
    }

    protected abstract boolean lessThan(T a, T b);

    protected void set(int index, T element) {
        heap[index] = element;
    }

    public synchronized T get(int index) {
        return heap[index];
    }

    public synchronized void put(T element) {
        size++;
        set(size, element);
        upHeap(size);
    }

    public synchronized boolean insert(T element) {
        if (size < maxSize) {
            put(element);
            return true;
        }
        if (size > 0 && !lessThan(element, top())) {
            set(1, element);
            adjustTop();
            return true;
        }
        return false;
    }

    public synchronized T top() {
        return size > 0 ? heap[1] : null;
    }

    public synchronized T pop() {
        if (size <= 0) {
            return null;
        }
        T result = heap[1];        // save first value
        set(1, heap[size]);        // move last to first
        set(size, null);           // permit GC of objects
        size--;
        downHeap(1);                  // adjust heap
        return result;
    }

    public synchronized void adjustTop() {
        downHeap(1);
    }

    public synchronized int size() {
        return size;
    }

    public synchronized void clear() {
        for (int i = 0; i <= size; i++) {
            set(i, null);
        }
        size = 0;
    }

    protected void upHeap(int i) {
        T node = heap[i];              // save bottom node
        int j = i >>> 1;
        while (j > 0 && lessThan(node, heap[j])) {
            set(i, heap[j]);              // shift parents down
            i = j;
            j = j >>> 1;
        }
        set(i, node);                  // install saved node
    }

    protected int downHeap(int start) {
        int i = start;
        T node = heap[i];                // save node
        int j = i << 1;                  // find smaller child
        int k = j + 1;
        if (k <= size && lessThan(heap[k], heap[j])) {
            j = k;
        }
        while (j <= size && lessThan(heap[j], node)) {
            set(i, heap[j]);              // shift up child
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= size && lessThan(heap[k], heap[j])) {
                j = k;
            }
        }
        set(i, node);                  // install saved node
        return i;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++) {
            builder.append(heap[i + 1]);
            if (i + 1 < size) {
                builder.append(' ');
            }
        }
        return builder.toString();
    }
}
