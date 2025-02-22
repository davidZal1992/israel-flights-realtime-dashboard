package com.dzaltsman.israelflightdashboard.services;

import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public interface FlightConsumerService {

    public void handle() throws TimeoutException, StreamingQueryException;
}
