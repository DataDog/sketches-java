package com.datadoghq.sketch.ddsketch.mapping;

import com.datadoghq.sketch.ddsketch.proto.IndexMapping;

public class IndexMappingProtoBinding {

    public static IndexMapping toProto(com.datadoghq.sketch.ddsketch.mapping.IndexMapping indexMapping) {
        if (indexMapping instanceof LogLikeIndexMapping) {
            return toProtoLogLike((LogLikeIndexMapping) indexMapping);
        } else if (indexMapping instanceof BitwiseLinearlyInterpolatedMapping) {
            return toProtoBitwiseLinear((BitwiseLinearlyInterpolatedMapping) indexMapping);
        } else {
            throw new IllegalArgumentException("Unknown indexmapping " + indexMapping.getClass());
        }
    }

    private static IndexMapping toProtoLogLike(LogLikeIndexMapping indexMapping) {
        return com.datadoghq.sketch.ddsketch.proto.IndexMapping.newBuilder()
                .setGamma(indexMapping.gamma())
                .setIndexOffset(indexMapping.indexOffset())
                .setInterpolation(interpolation(indexMapping.interpolation()))
                .build();
    }

    private static IndexMapping toProtoBitwiseLinear(BitwiseLinearlyInterpolatedMapping indexMapping) {
        return com.datadoghq.sketch.ddsketch.proto.IndexMapping.newBuilder()
                .setGamma(indexMapping.gamma())
                .setInterpolation(com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation.LINEAR)
                .build();
    }

    public static LogLikeIndexMapping fromProto(com.datadoghq.sketch.ddsketch.proto.IndexMapping proto) {
        final double gamma = proto.getGamma();
        final double indexOffset = proto.getIndexOffset();
        switch (proto.getInterpolation()) {
            case NONE:
                return new LogarithmicMapping(gamma, indexOffset);
            case LINEAR:
                return new LinearlyInterpolatedMapping(gamma, indexOffset);
            case QUADRATIC:
                return new QuadraticallyInterpolatedMapping(gamma, indexOffset);
            case CUBIC:
                return new CubicallyInterpolatedMapping(gamma, indexOffset);
            default:
                throw new IllegalArgumentException("unrecognized interpolation");
        }
    }

    private static IndexMapping.Interpolation interpolation(Interpolation interpolation) {
        switch (interpolation) {
            case NONE:
                return IndexMapping.Interpolation.NONE;
            case LINEAR:
                return IndexMapping.Interpolation.LINEAR;
            case QUADRATIC:
                return IndexMapping.Interpolation.QUADRATIC;
            case CUBIC:
                return IndexMapping.Interpolation.CUBIC;
            default:
                throw new IllegalArgumentException("unrecognized interpolation");
        }
    }
}
