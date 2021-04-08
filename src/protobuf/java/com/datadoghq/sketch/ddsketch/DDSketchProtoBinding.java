/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.ddsketch.mapping.IndexMappingProtoBinding;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.StoreProtoBinding;

import java.util.function.Supplier;

public class DDSketchProtoBinding {

    /**
     * Generates a protobuf representation of the provided {@code DDSketch}.
     *
     * If protobuf-java is too heavyweight a dependency, or if the intermediate
     * allocation of the proto object is unacceptable, {@code DDSketch.serialize()}
     * can be used to produce the same sequence of bytes.
     *
     * @param sketch the sketch to convert to protobuf
     * @return a protobuf representation of this {@code DDSketch}
     */
    public static com.datadoghq.sketch.ddsketch.proto.DDSketch toProto(DDSketch sketch) {
        return com.datadoghq.sketch.ddsketch.proto.DDSketch.newBuilder()
                .setPositiveValues(StoreProtoBinding.toProto(sketch.getPositiveValueStore()))
                .setNegativeValues(StoreProtoBinding.toProto(sketch.getNegativeValueStore()))
                .setZeroCount(sketch.getZeroCount())
                .setMapping(IndexMappingProtoBinding.toProto(sketch.getIndexMapping()))
                .build();
    }

    /**
     * Builds a new instance of {@code DDSketch} based on the provided protobuf representation.
     *
     * @param storeSupplier the constructor of the {@link Store} implementation to be used for encoding bin counters
     * @param proto the protobuf representation of a sketch
     * @return an instance of {@code DDSketch} that matches the protobuf representation
     */
    public static DDSketch fromProto(
            Supplier<? extends Store> storeSupplier,
            com.datadoghq.sketch.ddsketch.proto.DDSketch proto) {
        return new DDSketch(
                IndexMappingProtoBinding.fromProto(proto.getMapping()),
                StoreProtoBinding.fromProto(storeSupplier, proto.getNegativeValues()),
                StoreProtoBinding.fromProto(storeSupplier, proto.getPositiveValues()),
                proto.getZeroCount()
        );
    }
}
