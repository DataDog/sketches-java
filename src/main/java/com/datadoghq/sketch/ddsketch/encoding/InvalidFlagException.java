package com.datadoghq.sketch.ddsketch.encoding;

public class InvalidFlagException extends MalformedInputException {

    public InvalidFlagException() {
    }

    public InvalidFlagException(String msg) {
        super(msg);
    }
}
