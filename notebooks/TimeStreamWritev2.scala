// Databricks notebook source
//Define Package for AWS TimeStream Class

package com.corrdyn.timestream

import java.util.ArrayList
import java.util.List
import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.functions.date_format
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient
import software.amazon.awssdk.services.timestreamwrite.model.ConflictException
import software.amazon.awssdk.services.timestreamwrite.model.CreateDatabaseRequest
import software.amazon.awssdk.services.timestreamwrite.model.CreateTableRequest
import software.amazon.awssdk.services.timestreamwrite.model.Dimension
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType
import software.amazon.awssdk.services.timestreamwrite.model.Record
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecord
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException
import software.amazon.awssdk.services.timestreamwrite.model.RetentionProperties
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsResponse
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import java.time.Duration
import software.amazon.awssdk.regions.Region


class CorrDynTimeStreamWriter(DATABASE_NAME: String, TABLE_NAME: String, DIMENSION_LIST: scala.collection.immutable.List[String], TIME_KEY: String) extends ForeachWriter[Row] {

  lazy val timestreamWriteClient: TimestreamWriteClient = buildWriteClient()
  val HT_TTL_HOURS: Long = 24L
  val CT_TTL_DAYS: Long = 7L
  
  def buildWriteClient(): TimestreamWriteClient = {
    val httpClientBuilder: ApacheHttpClient.Builder =
      ApacheHttpClient.builder()
    httpClientBuilder.maxConnections(5000)
    val retryPolicy: RetryPolicy.Builder = RetryPolicy.builder()
    retryPolicy.numRetries(10)
    val overrideConfig: ClientOverrideConfiguration.Builder =
      ClientOverrideConfiguration.builder()
    overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20))
    overrideConfig.retryPolicy(retryPolicy.build())
    TimestreamWriteClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .overrideConfiguration(overrideConfig.build())
      .region(Region.US_EAST_1)
      .build()
  }
  
  def open(partitionId: Long, epochId: Long) = {
      timestreamWriteClient  // force the initialization of the client
      createDatabase()
      createTable()
      true
  }
  
  def process(row: Row) = {
    
    // Create Maps from Row
    val dimsAsMap = row.getValuesMap(DIMENSION_LIST).asInstanceOf[Map[String,Any]]
    val measuresAsMap = row.getValuesMap(row.schema.fieldNames.filterNot(DIMENSION_LIST.toSet)).asInstanceOf[Map[String,Any]]
    
    // Create empty list of Records
    val records: List[Record] = new ArrayList[Record]()
    
    // Create empty list of Dimensions
    val dimensions: List[Dimension] = new ArrayList[Dimension]()
    
    for ((k, v) <- dimsAsMap) {
      if (k != TIME_KEY)
        dimensions.add(Dimension.builder().name(k).value(v.toString).build())
    }
    
    val commonAttributes: Record = Record
      .builder()
      .dimensions(dimensions)
      .measureValueType(MeasureValueType.DOUBLE)
      .time(dimsAsMap(TIME_KEY).toString)
      .build()
    
    for ((k, v) <- measuresAsMap) {
        records.add(
          Record.builder()
          .measureName(k)
          .measureValue(v.toString)
          .build()
        )
    }
    
    val writeRecordsRequest: WriteRecordsRequest = WriteRecordsRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .commonAttributes(commonAttributes)
      .records(records)
      .build()
    try {
      timestreamWriteClient.writeRecords(writeRecordsRequest)
      
    } catch {
      case e: RejectedRecordsException => printRejectedRecordsException(e)

      case e: Exception => println("Error: " + e)

    }
  }
  
  def close(errorOrNull: Throwable) = {
    timestreamWriteClient.close()
  }
  
  def createDatabase(): Unit = {
    println("Creating database")
    val request: CreateDatabaseRequest =
      CreateDatabaseRequest.builder().databaseName(DATABASE_NAME).build()
    try {
      timestreamWriteClient.createDatabase(request)
      println("Database [" + DATABASE_NAME + "] created successfully")
    } catch {
      case e: ConflictException =>
        println(
          "Database [" + DATABASE_NAME + "] exists. Skipping database creation")

    }
  }

  def createTable(): Unit = {
    println("Creating table")
    val retentionProperties: RetentionProperties = RetentionProperties
      .builder()
      .memoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
      .magneticStoreRetentionPeriodInDays(CT_TTL_DAYS)
      .build()
    val createTableRequest: CreateTableRequest = CreateTableRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .retentionProperties(retentionProperties)
      .build()
    try {
      timestreamWriteClient.createTable(createTableRequest)
      println("Table [" + TABLE_NAME + "] successfully created.")
    } catch {
      case e: ConflictException =>
        println(
          "Table [" + TABLE_NAME + "] exists on database [" + DATABASE_NAME +
            "] . Skipping database creation")

    }
  }

  private def printRejectedRecordsException(
      e: RejectedRecordsException): Unit = {
    println("RejectedRecords: " + e)
    e.rejectedRecords().forEach(System.out.println)
  }

}

// COMMAND ----------

import com.corrdyn.timestream._

import org.apache.spark.sql.types._                  
import org.apache.spark.sql.functions._ 

val DATABASE_NAME: String = "corrdynSample"
val TABLE_NAME: String = "someTable"
val kinesisStreamName = "sampleIoT"
val kinesisRegion = "us-east-1"

case class DeviceData (device: String)

val time_key: String = "ts"

// Include the time_key in the Dimensions List
val dimensionsList: List[String] = List(
  "argument",
  "bench",
  "bench_id",
  "bench_in",
  "bench_out",
  "command",
  "machine",
  "machine_id",
  "ts"
)

val jsonSchema = new StructType()
        .add("a_seal_psi", FloatType)
        .add("air_tempc", FloatType)
        .add("argument", ShortType)
        .add("b_seal_psi", FloatType)
        .add("bench", StringType)
        .add("bench_id", ShortType)
        .add("bench_in", StringType)
        .add("bench_out", StringType)
        .add("chamber_psi", FloatType)
        .add("command", StringType)
        .add("machine", StringType)
        .add("machine_id", ShortType)
        .add("pk_activator", FloatType)
        .add("ts", LongType)

// COMMAND ----------

val kinesis = spark.readStream
  .format("kinesis")
  .option("streamName", kinesisStreamName)
  .option("region", kinesisRegion)
  .option("initialPosition", "TRIM_HORIZON")
  .load()
val device_data = kinesis.selectExpr("CAST(data as STRING) as device")
  .toDF("device").as[DeviceData]
  .select(from_json($"device", jsonSchema) as "device").select($"device.*")
  .select()
display(device_data)

// COMMAND ----------

device_data.writeStream
      .foreach(new CorrDynTimeStreamWriter(DATABASE_NAME, TABLE_NAME, dimensionList, time_key))
      .start()

// COMMAND ----------

