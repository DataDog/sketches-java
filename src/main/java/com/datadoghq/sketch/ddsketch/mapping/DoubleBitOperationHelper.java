package com.datadoghq.sketch.ddsketch.mapping;

class DoubleBitOperationHelper {

    static final int SIGNIFICAND_WIDTH = 53;
    static final long SIGNIFICAND_MASK = 0x000fffffffffffffL;

    static final long EXPONENT_MASK = 0x7FF0000000000000L;
    static final int EXPONENT_SHIFT = SIGNIFICAND_WIDTH - 1;
    static final int EXPONENT_BIAS = 1023;

    private static final long ONE = 0x3ff0000000000000L;

    private DoubleBitOperationHelper() {
    }

    static long getExponent(long longBits) {
        return ((longBits & EXPONENT_MASK) >> EXPONENT_SHIFT) - EXPONENT_BIAS;
    }

    static double getSignificandPlusOne(long longBits) {
        return Double.longBitsToDouble((longBits & SIGNIFICAND_MASK) | ONE);
    }

    /**
     * @param exponent should be >= -1022 and <= 1023
     * @param significandPlusOne should be >= 1 and < 2
     */
    static double buildDouble(long exponent, double significandPlusOne) {
        return Double.longBitsToDouble(
                (((exponent + EXPONENT_BIAS) << EXPONENT_SHIFT) & EXPONENT_MASK) |
                        (Double.doubleToRawLongBits(significandPlusOne) & SIGNIFICAND_MASK)
        );
    }
}
