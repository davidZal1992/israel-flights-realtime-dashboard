package com.dzaltsman.israelflightdashboard.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.spark.sql.SparkSession;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("FlightDashboardTracker")
                .master("local[4]")
                .config("spark.executor.instances", "2")
                .config("spark.executor.memory", "2g")
                .config("spark.es.nodes", "localhost")
                .config("spark.es.port", "9200")
                .config("spark.es.nodes.discovery", false)
                .config("spark.executor.cores", "2")
                .config("spark.driver.host", "localhost")
                .getOrCreate();

    }
}
