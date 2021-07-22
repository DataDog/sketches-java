/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

/** Signals that an encoded flag does not match any valid flag. */
public class InvalidFlagException extends MalformedInputException {
  public InvalidFlagException() {}

  public InvalidFlagException(String msg) {
    super(msg);
  }
}
