/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.io.IOException;

/**
 * Signals that the input data is in an unrecognized or inappropriate format and cannot be decoded.
 */
public class MalformedInputException extends IOException {
  public MalformedInputException() {}

  public MalformedInputException(String msg) {
    super(msg);
  }
}
