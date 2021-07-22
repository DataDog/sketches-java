/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.CubicallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.PaginatedStore;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import java.util.function.DoubleFunction;
import java.util.function.Supplier;

public enum DDSketchOption {
  FAST(BitwiseLinearlyInterpolatedMapping::new, UnboundedSizeDenseStore::new),
  MEMORY_OPTIMAL(LogarithmicMapping::new, UnboundedSizeDenseStore::new),
  BALANCED(CubicallyInterpolatedMapping::new, UnboundedSizeDenseStore::new),
  PAGINATED(BitwiseLinearlyInterpolatedMapping::new, PaginatedStore::new);

  private final DoubleFunction<IndexMapping> indexMapping;
  private final Supplier<Store> storeSupplier;

  DDSketchOption(DoubleFunction<IndexMapping> indexMapping, Supplier<Store> storeSupplier) {
    this.indexMapping = indexMapping;
    this.storeSupplier = storeSupplier;
  }

  public DDSketch create(double relativeAccuracy) {
    return new DDSketch(indexMapping.apply(relativeAccuracy), storeSupplier);
  }

  public Supplier<Store> getStoreSupplier() {
    return storeSupplier;
  }
}
