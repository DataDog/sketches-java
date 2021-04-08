/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.util.function.Supplier;

public class StoreProtoBinding {

    /**
     * Generates a protobuf representation of this {@code Store}.
     *
     * @return a protobuf representation of this {@code Store}
     */
    public static com.datadoghq.sketch.ddsketch.proto.Store toProto(Store store) {
        if (store instanceof DenseStore) {
            return toProtoDense((DenseStore) store);
        }
        return toProtoSparse(store);
    }

    private static com.datadoghq.sketch.ddsketch.proto.Store toProtoSparse(Store store) {
        final com.datadoghq.sketch.ddsketch.proto.Store.Builder storeBuilder =
                com.datadoghq.sketch.ddsketch.proto.Store.newBuilder();
        // In the general case, we use the sparse representation to encode bin counts.
        store.forEach(storeBuilder::putBinCounts);
        return storeBuilder.build();
    }

    private static com.datadoghq.sketch.ddsketch.proto.Store toProtoDense(DenseStore store) {
        final com.datadoghq.sketch.ddsketch.proto.Store.Builder builder =
                com.datadoghq.sketch.ddsketch.proto.Store.newBuilder();
        if (!store.isEmpty()) {
            // For the dense store, we use the dense representation to encode the bin counts.
            builder.setContiguousBinIndexOffset(store.minIndex);
            for (int i = store.minIndex - store.offset; i <= store.maxIndex - store.offset; i++) {
                builder.addContiguousBinCounts(store.counts[i]);
            }
        }
        return builder.build();
    }

    /**
     * Builds a new instance of {@code Store} based on the provided protobuf representation.
     *
     * @param storeSupplier the constructor of the {@link Store} of type {@code S} implementation
     * @param proto         the protobuf representation of a {@code Store}
     * @param <S>           the type of the {@code Store} to build
     * @return an instance of {@code Store} of type {@code S} that matches the protobuf representation
     */
    public static <S extends Store> S fromProto(
            Supplier<? extends S> storeSupplier,
            com.datadoghq.sketch.ddsketch.proto.Store proto) {
        final S store = storeSupplier.get();
        proto.getBinCountsMap().forEach(store::add);
        int index = proto.getContiguousBinIndexOffset();
        for (final double count : proto.getContiguousBinCountsList()) {
            store.add(index++, count);
        }
        return store;
    }
}
