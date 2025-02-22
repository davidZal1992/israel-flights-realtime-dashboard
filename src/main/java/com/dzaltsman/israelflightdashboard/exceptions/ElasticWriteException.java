package com.dzaltsman.israelflightdashboard.exceptions;

public class ElasticWriteException extends RuntimeException
{
    private static final String INIT_MESSAGE = "Exception occurred while writing to ElasticSearch.";

    public ElasticWriteException(String message, Throwable cause)
    {
        super(INIT_MESSAGE + message, cause);
    }
}
