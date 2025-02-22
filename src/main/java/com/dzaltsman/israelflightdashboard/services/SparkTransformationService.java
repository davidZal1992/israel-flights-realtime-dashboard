package com.dzaltsman.israelflightdashboard.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SparkTransformationService implements TransformationService<Dataset<Row>> {
    @Override
    public Dataset<Row> transform(Dataset<Row> transformationInput) {
        var sparkSchema = getSchema();

//        log.info("[Start] -- Transformation of Kafka message payload to DataFrame started.");

//        Dataset<Row> dfConverted = transformationInput.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        Dataset<Row> latestMessage = transformationInput.orderBy(functions.col("timestamp").desc()).limit(1);

        Dataset<Row> kafkaMessagePayloadAsDF = latestMessage.withColumn("parsed_payload", transformationInput.col("value").cast(DataTypes.StringType))
                .select("parsed_payload");

        Dataset<Row> kafkaMessagePayloadJsonAsDF = kafkaMessagePayloadAsDF.select(functions.from_json(kafkaMessagePayloadAsDF.col("parsed_payload"), sparkSchema).as("payload_as_json"))
                .select("payload_as_json.*");

        Dataset<Row> flightsDf = kafkaMessagePayloadJsonAsDF.selectExpr("explode(result.records) as record")
                .select("record.*")
                .withColumnRenamed("_id", "id")
                .withColumn("CHLOCCT", functions.initcap(functions.lower(functions.col("CHLOCCT"))))
                .dropDuplicates("CHPTOL", "CHLOCCT")
                .dropDuplicates("CHFLTN", "CHSTOL")// remove duplicates of share codes flights.
                .filter(dayBackInterval());

//        log.info("[End] -- Transformation of Kafka message payload to DataFrame finished. Number of columns are: {}", flightsDf.columns().length);

        return flightsDf;
    }

    private static Column dayBackInterval() {
        return functions.col("CHPTOL")
                .geq(functions.expr("current_timestamp() - interval 24 hours"))
                .and(functions.col("CHPTOL").leq(functions.current_timestamp()));
    }

    private StructType getSchema() {
        return new StructType(new StructField[]{
                new StructField("help", DataTypes.StringType, false, Metadata.empty()),
                new StructField("success", DataTypes.StringType, false, Metadata.empty()),
                new StructField("result", DataTypes.createStructType(
                        new StructField[]{
                                new StructField("records", DataTypes.createArrayType(
                                        new StructType(new StructField[]{
                                                new StructField("_id", DataTypes.IntegerType, false, Metadata.empty()),
                                                new StructField("CHOPER", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHFLTN", DataTypes.IntegerType, true, Metadata.empty()),
                                                new StructField("CHOPERD", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHSTOL", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHPTOL", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHAORD", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHLOC1", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHLOC1D", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHLOC1TH", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHLOC1T", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHLOC1CH", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHLOCCT", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHTERM", DataTypes.IntegerType, true, Metadata.empty()),
                                                new StructField("CHCINT", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHCKZN", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHRMINE", DataTypes.StringType, true, Metadata.empty()),
                                                new StructField("CHRMINH", DataTypes.StringType, true, Metadata.empty())
                                        })
                                ), false, Metadata.empty())
                        }), false, Metadata.empty())
        });
    }
}
