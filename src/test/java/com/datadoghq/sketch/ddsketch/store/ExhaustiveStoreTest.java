/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

abstract class ExhaustiveStoreTest extends StoreTest {

    @Override
    Map<Integer, Long> getCounts(Bin... bins) {
        return Arrays.stream(bins)
            .collect(Collectors.groupingBy(
                Bin::getIndex,
                Collectors.summingLong(Bin::getCount)
            ));
    }
}
