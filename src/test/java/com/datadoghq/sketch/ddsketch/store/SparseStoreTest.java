/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

class SparseStoreTest extends ExhaustiveStoreTest {

    @Override
    protected Store newStore() {
        return new SparseStore();
    }
}
