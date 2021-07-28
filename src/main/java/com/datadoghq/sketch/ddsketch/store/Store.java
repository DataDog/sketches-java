/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import static com.datadoghq.sketch.ddsketch.Serializer.sizeOfBin;

import com.datadoghq.sketch.ddsketch.Serializer;
import com.datadoghq.sketch.ddsketch.encoding.BinEncodingMode;
import com.datadoghq.sketch.ddsketch.encoding.Flag;
import com.datadoghq.sketch.ddsketch.encoding.Input;
import com.datadoghq.sketch.ddsketch.encoding.Output;
import com.datadoghq.sketch.ddsketch.encoding.VarEncodingHelper;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An object that maps integers to counters. It can be seen as a collection of {@link Bin}, which
 * are pairs of indices and counters.
 */
public interface Store {

  /**
   * Increments the counter at the specified index.
   *
   * @param index the index of the counter to be incremented
   */
  default void add(int index) {
    add(index, 1);
  }

  /**
   * Updates the counter at the specified index.
   *
   * @param index the index of the counter to be updated
   * @param count a non-negative integer value
   * @throws IllegalArgumentException if {@code count} is negative
   */
  default void add(int index, long count) {
    add(index, (double) count);
  }

  /**
   * Updates the counter at the specified index.
   *
   * @param index the index of the counter to be updated
   * @param count a non-negative value
   * @throws IllegalArgumentException if {@code count} is negative
   */
  void add(int index, double count);

  /**
   * Updates the counter at the specified index.
   *
   * @param bin the bin to be used for updating the counter
   */
  default void add(Bin bin) {
    add(bin.getIndex(), bin.getCount());
  }

  /**
   * Merges another store into this one. This should be equivalent as running the {@code add}
   * operations that have been run on the other {@code store} on this one.
   *
   * @param store the store to be merged into this one
   */
  default void mergeWith(Store store) {
    store.forEach(this::add);
  }

  /** @return a (deep) copy of this store */
  Store copy();

  /**
   * Zeros all counts in the store. The store behaves as if empty after this call, but no underlying
   * storage is released.
   */
  void clear();

  /** @return {@code true} iff the {@code Store} does not contain any non-zero counter */
  default boolean isEmpty() {
    return getStream().mapToDouble(Bin::getCount).allMatch(count -> count == 0);
  }

  /** @return the sum of the counters of this store */
  default double getTotalCount() {
    return getStream().mapToDouble(Bin::getCount).sum();
  }

  /**
   * @return the index of the lowest non-zero counter
   * @throws java.util.NoSuchElementException if the store is empty
   */
  default int getMinIndex() {
    return getAscendingStream()
        .filter(bin -> bin.getCount() > 0)
        .findFirst()
        .orElseThrow(NoSuchElementException::new)
        .getIndex();
  }

  /**
   * @return the index of the highest non-zero counter
   * @throws java.util.NoSuchElementException if the store is empty
   */
  default int getMaxIndex() {
    return getDescendingStream()
        .filter(bin -> bin.getCount() > 0)
        .findFirst()
        .orElseThrow(NoSuchElementException::new)
        .getIndex();
  }

  /**
   * Supplies each bin to the acceptor
   *
   * @param acceptor consumes this store's bins
   */
  default void forEach(BinAcceptor acceptor) {
    getStream().forEach(bin -> acceptor.accept(bin.getIndex(), bin.getCount()));
  }

  /** @return a stream with the non-empty bins of this store as its source */
  default Stream<Bin> getStream() {
    return getAscendingStream();
  }

  /**
   * @return an ordered stream (from lowest to highest index) with the non-empty bins of this store
   *     as its source
   */
  default Stream<Bin> getAscendingStream() {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(getAscendingIterator(), 0), false);
  }

  /**
   * @return an ordered stream (from highest to lowest index) with the non-empty bins of this store
   *     as its source
   */
  default Stream<Bin> getDescendingStream() {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(getDescendingIterator(), 0), false);
  }

  /**
   * @return an iterator that iterates over the non-empty bins of this store, from lowest to highest
   *     index
   */
  // Needed because of JDK-8194952
  Iterator<Bin> getAscendingIterator();

  /**
   * @return an iterator that iterates over the non-empty bins of this store, from highest to lowest
   *     index
   */
  // Needed because of JDK-8194952
  Iterator<Bin> getDescendingIterator();

  void encode(Output output, Flag.Type storeFlagType) throws IOException;

  default void decodeAndMergeWith(Input input, BinEncodingMode encodingMode) throws IOException {
    switch (encodingMode) {
      case INDEX_DELTAS_AND_COUNTS:
        {
          final long numBins = VarEncodingHelper.decodeUnsignedVarLong(input);
          long index = 0;
          for (long i = 0; i != numBins; i++) {
            final long indexDelta = VarEncodingHelper.decodeSignedVarLong(input);
            final double count = VarEncodingHelper.decodeVarDouble(input);
            index += indexDelta;
            add(Math.toIntExact(index), count);
          }
        }
        break;
      case INDEX_DELTAS:
        {
          final long numBins = VarEncodingHelper.decodeUnsignedVarLong(input);
          long index = 0;
          for (long i = 0; i != numBins; i++) {
            final long indexDelta = VarEncodingHelper.decodeSignedVarLong(input);
            index += indexDelta;
            add(Math.toIntExact(index));
          }
        }
        break;
      case CONTIGUOUS_COUNTS:
        {
          final long numBins = VarEncodingHelper.decodeUnsignedVarLong(input);
          long index = VarEncodingHelper.decodeSignedVarLong(input);
          final long indexDelta = VarEncodingHelper.decodeSignedVarLong(input);
          for (long i = 0; i != numBins; i++, index += indexDelta) {
            final double count = VarEncodingHelper.decodeVarDouble(input);
            add(Math.toIntExact(index), count);
          }
        }
        break;
      default:
        throw new IllegalStateException("The bin encoding mode is not handled.");
    }
  }

  default int serializedSize() {
    int[] size = {0};
    forEach((index, count) -> size[0] += sizeOfBin(1, index, count));
    return size[0];
  }

  default void serialize(Serializer serializer) {
    forEach((index, count) -> serializer.writeBin(1, index, count));
  }
}
