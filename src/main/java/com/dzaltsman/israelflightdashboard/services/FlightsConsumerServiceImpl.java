package com.dzaltsman.israelflightdashboard.services;

import com.dzaltsman.israelflightdashboard.exceptions.ElasticWriteException;
import com.dzaltsman.israelflightdashboard.exceptions.SparkConsumerException;
import com.dzaltsman.israelflightdashboard.props.ConsumerProps;
import com.dzaltsman.israelflightdashboard.props.ElasticProps;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Scheduled;

@Service
@Slf4j
public class FlightsConsumerServiceImpl implements FlightConsumerService {

    private final SparkSession sparkSession;

    private final ConsumerProps consumerProps;
    private final ElasticProps elasticProps;
    private final SparkTransformationService sparkTransformationService;

    private final int SCHEDULE_FIXED_RATE = 60000;

    @Autowired
    public FlightsConsumerServiceImpl(SparkSession sparkSession, SparkTransformationService sparkTransformationService, ConsumerProps consumerProps,
                                      ElasticProps elasticProps) {
        this.sparkSession = sparkSession;
        this.sparkTransformationService = sparkTransformationService;
        this.consumerProps = consumerProps;
        this.elasticProps = elasticProps;
    }

    @Scheduled(fixedRate = SCHEDULE_FIXED_RATE)
    public void handle() {

        try {
            log.info("-- Starting to consume messages from Kafka topic: {}", consumerProps.getTopicName());
            Dataset<Row> topicMessages = consume();

            log.info("-- Consumed messages from Kafka topic: {} successfully. Starting to transform the data", consumerProps.getTopicName());
            Dataset<Row> transformedToFlights = sparkTransformationService.transform(topicMessages);

            log.info("-- Transformed the data successfully. Writing to ElasticSearch index: {}", elasticProps.getElasticIndex());
            write(transformedToFlights);

        } catch (Exception e) {
            log.error("Error while consuming messages from Kafka topic: {}", consumerProps.getTopicName(), e);
        }

    }

    private Dataset<Row> consume() {
        Dataset<Row> messages;
        try {
            messages = sparkSession.read()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", consumerProps.getBootstrapServers())
                    .option("subscribe", consumerProps.getTopicName())
                    .option("auto.offset.reset", "latest")
                    .option("startingOffsetsByTimestampStrategy", "latest")
                    .option("failOnDataLoss", "false")
                    .load();
        } catch (Exception e) {
            throw new SparkConsumerException(e.getMessage(), e.getCause());
        }
        return messages;
    }

    private void write(Dataset<Row> flightsDf) {
        try {
            flightsDf.write()
                    .format("es")
                    .mode("overwrite")
                    .option("spark.es.nodes.discovery", false)
                    .option("es.read.metadata", "false")
                    .option("es.nodes.wan.only", "true")
                    .save(elasticProps.getElasticIndex());
        } catch (Exception e) {
            throw new ElasticWriteException(e.getMessage(), e.getCause());
        }
    }
}
