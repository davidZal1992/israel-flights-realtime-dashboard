package com.dzaltsman.israelflightdashboard.exceptions;

public class SparkConsumerException extends RuntimeException {

    private static final String INIT_MESSAGE = "Exception occurred while consuming messages from Kafka.";

    public SparkConsumerException(String message, Throwable cause) {
        super(INIT_MESSAGE + message, cause);
    }
}
